// PagerDuty OAuth authorize (pagerduty.py's authorize_pagerduty). The
// callback half (code exchange, region discovery, credential persistence,
// compensating revoke) is a separate, much larger unit and is not in this
// file.
package admin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// canonicalIncidentIngestionFeature is licensing/registry.py's
// CANONICAL_INCIDENT_INGESTION_FEATURE.
const canonicalIncidentIngestionFeature = "canonical_incident_ingestion"

// pagerDutyFeatureDisabledDetail is _FEATURE_DISABLED_DETAIL.
const pagerDutyFeatureDisabledDetail = "Canonical incident ingestion is not enabled for this organization"

// pagerDutyAuthorizationTTL is PagerDutyAuthorizationRequestStore.create's
// default ttl.
const pagerDutyAuthorizationTTL = 15 * time.Minute

func (h *handlers) pagerDutyAuthorizeRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: pagerDutyPrefix + "/authorize", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.authorizePagerDuty))},
	}
}

// requireCanonicalIncidentIngestion is pagerduty.py's
// _require_canonical_incident_ingestion: a malformed org id, any read
// failure, and a closed decision are ALL the same 403 with one fixed
// string detail -- Python's `except SQLAlchemyError: allowed = False` never
// lets a storage fault answer 500 here (unlike a route that fails closed
// with a different status). It answers the refusal itself and returns
// false.
func (h *handlers) requireCanonicalIncidentIngestion(ctx context.Context, w http.ResponseWriter, orgID string) bool {
	allowed := false
	if parsed, err := pythonparity.ParseUUID(orgID); err == nil {
		state, loadErr := licensing.LoadState(ctx, h.store.Pool, parsed.String(), canonicalIncidentIngestionFeature, h.store.now().UTC())
		if loadErr != nil {
			h.logger.ErrorContext(ctx, "admin: canonical incident ingestion feature check failed; denying", "org_id", orgID, "error", loadErr)
		} else {
			allowed = licensing.Decide(canonicalIncidentIngestionFeature, state).Allowed
		}
	}
	if !allowed {
		policy.WriteDetail(w, http.StatusForbidden, pagerDutyFeatureDisabledDetail, nil)
		return false
	}
	return true
}

// authorizePagerDuty is pagerduty.py's authorize_pagerduty. The body is an
// empty model (`extra="forbid"`, no fields): any key at all is an
// extra_forbidden error.
func (h *handlers) authorizePagerDuty(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	if ok {
		errs.ForbidExtra(object)
	}
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	if !h.requireCanonicalIncidentIngestion(ctx, w, orgID) {
		return
	}
	if h.pagerDuty.ClientID == "" {
		policy.WriteDetail(w, http.StatusBadRequest, "PAGER_DUTY_CLIENT_ID is not configured", nil)
		return
	}

	request, err := providerfoundation.BuildPagerDutyAuthorizationRequest(h.pagerDuty)
	if err != nil {
		h.internalError(ctx, w, "build pagerduty authorization request", err)
		return
	}
	sealed, err := h.decryptor.Encrypt([]byte(request.CodeVerifier))
	if err != nil {
		h.internalError(ctx, w, "encrypt pagerduty code verifier", err)
		return
	}

	// PagerDutyAuthorizationRequestStore.create: this org's OWN expired
	// rows are removed first (never another org's), then the new
	// state-hash-keyed row is stored, both in the one unit of work.
	now := h.store.now().UTC()
	tx, err := h.store.Pool.Begin(ctx)
	if err != nil {
		h.internalError(ctx, w, "begin pagerduty authorization request", err)
		return
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	if _, err := tx.Exec(ctx, `DELETE FROM pagerduty_oauth_authorization_requests WHERE org_id = $1 AND expires_at <= $2`, orgID, now); err != nil {
		h.internalError(ctx, w, "purge expired pagerduty authorization requests", err)
		return
	}
	stateHash := sha256.Sum256([]byte(request.State))
	if _, err := tx.Exec(ctx,
		`INSERT INTO pagerduty_oauth_authorization_requests (state_hash, org_id, code_verifier_encrypted, created_at, expires_at) VALUES ($1, $2, $3, $4, $5)`,
		hex.EncodeToString(stateHash[:]), orgID, sealed.Reveal(), now, now.Add(pagerDutyAuthorizationTTL)); err != nil {
		h.internalError(ctx, w, "store pagerduty authorization request", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.internalError(ctx, w, "commit pagerduty authorization request", err)
		return
	}
	committed = true

	out := pyjson.NewObject()
	out.Set("authorize_url", request.URL)
	policy.WriteModel(w, http.StatusOK, out, nil)
}
