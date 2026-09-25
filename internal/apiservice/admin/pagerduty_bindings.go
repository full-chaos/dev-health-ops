// PagerDuty webhook-binding admin (api/admin/routers/pagerduty_bindings.py
// over providers/pagerduty/webhook_bindings.py's
// PagerDutyWebhookBindingService): create, rotate, activate, revoke and read
// of the routing record that ties a PagerDuty subscription's signing secret
// to an integration source. Pure Postgres; no call to PagerDuty is made.
package admin

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const pagerDutyBindingsPrefix = pagerDutyPrefix + "/webhook-bindings"

// The status values a binding moves through (webhook_bindings.py's
// _CANDIDATE_STATUS and friends) and the key version a new secret carries
// (KEY_VERSION_PREFIX without its colon).
const (
	bindingCandidate  = "candidate"
	bindingReady      = "ready"
	bindingActive     = "active"
	bindingInactive   = "inactive"
	bindingKeyVersion = "v1"
)

func (h *handlers) pagerDutyBindingRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: pagerDutyBindingsPrefix, Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.createPagerDutyBinding))},
		{Method: http.MethodPost, Pattern: pagerDutyBindingsPrefix + "/{binding_id}/rotate", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.rotatePagerDutyBinding))},
		{Method: http.MethodPost, Pattern: pagerDutyBindingsPrefix + "/{binding_id}/activate", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.activatePagerDutyBinding))},
		{Method: http.MethodPost, Pattern: pagerDutyBindingsPrefix + "/{binding_id}/revoke", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.revokePagerDutyBinding))},
		{Method: http.MethodGet, Pattern: pagerDutyBindingsPrefix + "/{binding_id}", Allow: "GET", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.getPagerDutyBinding))},
	}
}

// bindingRow is one pagerduty_webhook_bindings row without its secret.
type bindingRow struct {
	ID, OrgID, SourceID uuid.UUID
	CredentialID        *uuid.UUID
	SubscriptionID      string
	KeyVersion, Status  string
	CreatedAt           time.Time
	RotatedAt           *time.Time
	RevokedAt           *time.Time
}

const bindingColumns = `id, org_id, integration_source_id, credential_id, provider_subscription_id, signing_secret_key_version, status, created_at, rotated_at, revoked_at`

func scanBinding(row pgx.Row) (bindingRow, error) {
	var b bindingRow
	err := row.Scan(&b.ID, &b.OrgID, &b.SourceID, &b.CredentialID, &b.SubscriptionID, &b.KeyVersion, &b.Status, &b.CreatedAt, &b.RotatedAt, &b.RevokedAt)
	return b, err
}

func scanBindings(rows pgx.Rows) ([]bindingRow, error) {
	defer rows.Close()
	var out []bindingRow
	for rows.Next() {
		b, err := scanBinding(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// bindingResponse is PagerDutyWebhookBindingResponse in declared order.
func bindingResponse(b bindingRow) *pyjson.Object {
	timestamp := func(at *time.Time) pyjson.Value {
		if at == nil {
			return nil
		}
		return pytime.Pydantic(pytime.UTC(*at))
	}
	out := pyjson.NewObject()
	out.Set("id", b.ID.String())
	out.Set("integration_source_id", b.SourceID.String())
	if b.CredentialID == nil {
		out.Set("credential_id", nil)
	} else {
		out.Set("credential_id", b.CredentialID.String())
	}
	out.Set("provider_subscription_id", b.SubscriptionID)
	out.Set("signing_secret_key_version", b.KeyVersion)
	out.Set("status", b.Status)
	created := b.CreatedAt
	out.Set("created_at", timestamp(&created))
	out.Set("rotated_at", timestamp(b.RotatedAt))
	out.Set("revoked_at", timestamp(b.RevokedAt))
	return out
}

// bindingRequest is PagerDutyWebhookBindingRequest.
type bindingRequest struct {
	SourceID, CredentialID uuid.UUID
	SubscriptionID         string
	SigningSecret          string
}

func parseBindingRequest(body pybody.Body, errs *pybody.Errors) bindingRequest {
	var out bindingRequest
	object, ok := errs.Object(body)
	if !ok {
		return out
	}
	out.SourceID, _ = errs.RequiredUUID(object, "integration_source_id")
	out.CredentialID, _ = errs.RequiredUUID(object, "credential_id")
	out.SubscriptionID, _ = errs.RequiredString(object, "provider_subscription_id", 1, 0)
	out.SigningSecret, _ = errs.RequiredString(object, "signing_secret", 1, 0)
	errs.ForbidExtra(object, "integration_source_id", "credential_id", "provider_subscription_id", "signing_secret")
	return out
}

// bindingOrgID is the router's `_org_id`: a claim that is not a UUID is a
// 400 (only reachable on the routes without the feature gate, whose own
// check answers a bad organisation id with its 403 first).
func bindingOrgID(w http.ResponseWriter, orgID string) (uuid.UUID, bool) {
	parsed, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid organization id", nil)
		return uuid.Nil, false
	}
	return parsed, true
}

// lockBindingSource serialises the lifecycle transitions of one integration
// source (rotate and activate) for the rest of the transaction. The row lock
// the transitions take covers only the rows that exist when it is taken, so
// two concurrent rotations could each find no candidate and each insert one;
// this transaction-scoped advisory lock, keyed by the source, makes the
// second wait, then read the first's committed candidate. Python has no such
// lock (the race is a known gap there); this is a named divergence.
func lockBindingSource(ctx context.Context, tx pgx.Tx, sourceID uuid.UUID) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))`, sourceID.String())
	return err
}

func (h *handlers) bindingNow() time.Time { return h.store.now().UTC().Truncate(time.Microsecond) }

// requireBindingGraph is _require_active_pagerduty_graph: the source, its
// integration and the credential must all exist, be PagerDuty's, belong to
// the caller's organisation, be linked to each other and be enabled.
func requireBindingGraph(ctx context.Context, tx pgx.Tx, orgID uuid.UUID, req bindingRequest) (string, error) {
	var ok *bool
	err := tx.QueryRow(ctx, `
SELECT (s.org_id = $3 AND i.org_id = $3 AND c.org_id = $3
        AND s.provider = 'pagerduty' AND i.provider = 'pagerduty' AND c.provider = 'pagerduty'
        AND i.credential_id = c.id AND s.is_enabled AND i.is_active AND c.is_active)
FROM integration_sources s
JOIN integrations i ON s.integration_id = i.id
JOIN integration_credentials c ON c.id = $2
WHERE s.id = $1`, req.SourceID, req.CredentialID, orgID.String()).Scan(&ok)
	if errors.Is(err, pgx.ErrNoRows) {
		return "PagerDuty binding graph was not found", nil
	}
	if err != nil {
		return "", err
	}
	if ok == nil || !*ok {
		return "PagerDuty binding graph is not active and same-org", nil
	}
	return "", nil
}

// insertCandidate is the repository's create with status candidate: the
// secret encrypted with the settings key, key version v1.
func (h *handlers) insertCandidate(ctx context.Context, tx pgx.Tx, orgID uuid.UUID, req bindingRequest, at time.Time) (bindingRow, error) {
	sealed, err := h.decryptor.Encrypt([]byte(req.SigningSecret))
	if err != nil {
		return bindingRow{}, err
	}
	return scanBinding(tx.QueryRow(ctx, `
INSERT INTO pagerduty_webhook_bindings (id, org_id, integration_source_id, credential_id, provider_subscription_id, signing_secret_encrypted, signing_secret_key_version, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
RETURNING `+bindingColumns,
		uuid.New(), orgID, req.SourceID, req.CredentialID, req.SubscriptionID, sealed.Reveal(), bindingKeyVersion, bindingCandidate, at))
}

// prepareBindingWrite runs the request steps create and rotate share, up to
// the transaction: body, organisation, the feature gate, the org id claim.
func (h *handlers) prepareBindingWrite(w http.ResponseWriter, r *http.Request, pathErrs *pybody.Errors) (bindingRequest, uuid.UUID, bool) {
	ctx := r.Context()
	errs := *pathErrs
	req := parseBindingRequest(bodyFromContext(ctx), &errs)
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return req, uuid.Nil, false
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return req, uuid.Nil, false
	}
	if !h.requireCanonicalIncidentIngestion(ctx, w, orgID) {
		return req, uuid.Nil, false
	}
	orgUUID, ok := bindingOrgID(w, orgID)
	if !ok {
		return req, uuid.Nil, false
	}
	return req, orgUUID, true
}

// checkBindingInput is service._require_secret/_require_subscription_id and
// the graph check; a failure is the route's 400.
func (h *handlers) checkBindingInput(ctx context.Context, w http.ResponseWriter, tx pgx.Tx, orgUUID uuid.UUID, req bindingRequest) bool {
	if req.SigningSecret == "" {
		policy.WriteDetail(w, http.StatusBadRequest, "signing secret must not be empty", nil)
		return false
	}
	if pythonparity.Strip(req.SubscriptionID) == "" {
		policy.WriteDetail(w, http.StatusBadRequest, "provider subscription id must not be blank", nil)
		return false
	}
	detail, err := requireBindingGraph(ctx, tx, orgUUID, req)
	if err != nil {
		h.internalError(ctx, w, "check pagerduty binding graph", err)
		return false
	}
	if detail != "" {
		policy.WriteDetail(w, http.StatusBadRequest, detail, nil)
		return false
	}
	return true
}

func (h *handlers) createPagerDutyBinding(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var none pybody.Errors
	req, orgUUID, ok := h.prepareBindingWrite(w, r, &none)
	if !ok {
		return
	}
	tx, err := h.store.Pool.Begin(ctx)
	if err != nil {
		h.internalError(ctx, w, "begin pagerduty binding create", err)
		return
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if !h.checkBindingInput(ctx, w, tx, orgUUID, req) {
		return
	}
	created, err := h.insertCandidate(ctx, tx, orgUUID, req, h.bindingNow())
	if err != nil {
		h.internalError(ctx, w, "create pagerduty binding", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.internalError(ctx, w, "commit pagerduty binding create", err)
		return
	}
	policy.WriteModel(w, http.StatusCreated, bindingResponse(created), nil)
}

func (h *handlers) rotatePagerDutyBinding(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var pathErrs pybody.Errors
	bindingID, _ := pathErrs.PathUUID("binding_id", r.PathValue("binding_id"))
	req, orgUUID, ok := h.prepareBindingWrite(w, r, &pathErrs)
	if !ok {
		return
	}
	tx, err := h.store.Pool.Begin(ctx)
	if err != nil {
		h.internalError(ctx, w, "begin pagerduty binding rotate", err)
		return
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if !h.checkBindingInput(ctx, w, tx, orgUUID, req) {
		return
	}
	notFound := func() {
		policy.WriteDetail(w, http.StatusNotFound, "Active webhook binding not found", nil)
	}
	// active_by_id: the id alone, any organisation.
	existing, err := scanBinding(tx.QueryRow(ctx, `SELECT `+bindingColumns+` FROM pagerduty_webhook_bindings WHERE id = $1 AND status = 'active'`, bindingID))
	if errors.Is(err, pgx.ErrNoRows) {
		notFound()
		return
	}
	if err != nil {
		h.internalError(ctx, w, "load pagerduty binding", err)
		return
	}
	if err := lockBindingSource(ctx, tx, existing.SourceID); err != nil {
		h.internalError(ctx, w, "lock pagerduty binding source", err)
		return
	}
	// The source's lifecycle rows, locked in id order.
	rows, err := tx.Query(ctx, `SELECT `+bindingColumns+` FROM pagerduty_webhook_bindings
WHERE integration_source_id = $1 AND status IN ('active', 'candidate', 'ready') ORDER BY id FOR UPDATE`, existing.SourceID)
	if err != nil {
		h.internalError(ctx, w, "lock pagerduty source bindings", err)
		return
	}
	sourceBindings, err := scanBindings(rows)
	if err != nil {
		h.internalError(ctx, w, "read pagerduty source bindings", err)
		return
	}
	var active *bindingRow
	for i := range sourceBindings {
		if sourceBindings[i].ID == bindingID && sourceBindings[i].Status == bindingActive {
			active = &sourceBindings[i]
			break
		}
	}
	if active == nil {
		notFound()
		return
	}
	if active.OrgID != orgUUID || active.SourceID != req.SourceID {
		policy.WriteDetail(w, http.StatusBadRequest, "rotation candidate must belong to the active binding source", nil)
		return
	}
	for _, binding := range sourceBindings {
		if binding.Status == bindingCandidate || binding.Status == bindingReady {
			policy.WriteDetail(w, http.StatusBadRequest, "a rotation candidate already exists for this source", nil)
			return
		}
	}
	created, err := h.insertCandidate(ctx, tx, orgUUID, req, h.bindingNow())
	if err != nil {
		h.internalError(ctx, w, "create pagerduty rotation candidate", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.internalError(ctx, w, "commit pagerduty binding rotate", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, bindingResponse(created), nil)
}

// bindingPathAndOrg is the shared start of the routes without a body: the
// path id (a 422 when it is not a UUID, after the organisation check) and
// the organisation.
func (h *handlers) bindingPathAndOrg(w http.ResponseWriter, r *http.Request) (uuid.UUID, string, bool) {
	ctx := r.Context()
	var errs pybody.Errors
	bindingID, _ := errs.PathUUID("binding_id", r.PathValue("binding_id"))
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return uuid.Nil, "", false
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return uuid.Nil, "", false
	}
	return bindingID, orgID, true
}

func (h *handlers) activatePagerDutyBinding(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bindingID, orgID, ok := h.bindingPathAndOrg(w, r)
	if !ok {
		return
	}
	if !h.requireCanonicalIncidentIngestion(ctx, w, orgID) {
		return
	}
	orgUUID, ok := bindingOrgID(w, orgID)
	if !ok {
		return
	}
	notFound := func() {
		policy.WriteDetail(w, http.StatusNotFound, "Ready webhook binding candidate not found", nil)
	}
	tx, err := h.store.Pool.Begin(ctx)
	if err != nil {
		h.internalError(ctx, w, "begin pagerduty binding activate", err)
		return
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	// cutover_ready_candidate: by_id_for_org, then the source's lifecycle rows.
	candidate, err := scanBinding(tx.QueryRow(ctx, `SELECT `+bindingColumns+` FROM pagerduty_webhook_bindings WHERE id = $1 AND org_id = $2`, bindingID, orgUUID))
	if errors.Is(err, pgx.ErrNoRows) {
		notFound()
		return
	}
	if err != nil {
		h.internalError(ctx, w, "load pagerduty binding candidate", err)
		return
	}
	if err := lockBindingSource(ctx, tx, candidate.SourceID); err != nil {
		h.internalError(ctx, w, "lock pagerduty binding source", err)
		return
	}
	// Only the caller's organisation's rows are swapped (Python locks and
	// swaps the source's rows whatever their organisation: a named
	// divergence, unreachable through any route since a source belongs to
	// one organisation).
	rows, err := tx.Query(ctx, `SELECT `+bindingColumns+` FROM pagerduty_webhook_bindings
WHERE integration_source_id = $1 AND org_id = $2 AND status IN ('active', 'candidate', 'ready') ORDER BY id FOR UPDATE`, candidate.SourceID, orgUUID)
	if err != nil {
		h.internalError(ctx, w, "lock pagerduty source bindings", err)
		return
	}
	sourceBindings, err := scanBindings(rows)
	if err != nil {
		h.internalError(ctx, w, "read pagerduty source bindings", err)
		return
	}
	var locked, active *bindingRow
	for i := range sourceBindings {
		if sourceBindings[i].ID == bindingID && locked == nil {
			locked = &sourceBindings[i]
		}
		if sourceBindings[i].Status == bindingActive && active == nil {
			active = &sourceBindings[i]
		}
	}
	if locked == nil || locked.Status != bindingReady {
		notFound()
		return
	}
	at := h.bindingNow()
	if active != nil {
		if _, err := tx.Exec(ctx, `UPDATE pagerduty_webhook_bindings SET status = 'inactive', revoked_at = $2, rotated_at = $2, updated_at = $2 WHERE id = $1`, active.ID, at); err != nil {
			h.internalError(ctx, w, "deactivate pagerduty binding", err)
			return
		}
	}
	updated, err := scanBinding(tx.QueryRow(ctx, `UPDATE pagerduty_webhook_bindings SET status = 'active', updated_at = $2 WHERE id = $1 RETURNING `+bindingColumns, bindingID, at))
	if err != nil {
		h.internalError(ctx, w, "activate pagerduty binding", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.internalError(ctx, w, "commit pagerduty binding activate", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, bindingResponse(updated), nil)
}

func (h *handlers) revokePagerDutyBinding(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bindingID, orgID, ok := h.bindingPathAndOrg(w, r)
	if !ok {
		return
	}
	orgUUID, ok := bindingOrgID(w, orgID)
	if !ok {
		return
	}
	tx, err := h.store.Pool.Begin(ctx)
	if err != nil {
		h.internalError(ctx, w, "begin pagerduty binding revoke", err)
		return
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	// receivable_by_id_for_update, then the organisation check.
	binding, err := scanBinding(tx.QueryRow(ctx, `SELECT `+bindingColumns+` FROM pagerduty_webhook_bindings
WHERE id = $1 AND status IN ('active', 'candidate', 'ready') FOR UPDATE`, bindingID))
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && binding.OrgID != orgUUID) {
		policy.WriteDetail(w, http.StatusNotFound, "Active webhook binding not found", nil)
		return
	}
	if err != nil {
		h.internalError(ctx, w, "load pagerduty binding", err)
		return
	}
	at := h.bindingNow()
	updated, err := scanBinding(tx.QueryRow(ctx, `UPDATE pagerduty_webhook_bindings SET status = 'inactive', revoked_at = COALESCE(revoked_at, $2), updated_at = $2 WHERE id = $1 RETURNING `+bindingColumns, bindingID, at))
	if err != nil {
		h.internalError(ctx, w, "revoke pagerduty binding", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.internalError(ctx, w, "commit pagerduty binding revoke", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, bindingResponse(updated), nil)
}

func (h *handlers) getPagerDutyBinding(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bindingID, orgID, ok := h.bindingPathAndOrg(w, r)
	if !ok {
		return
	}
	orgUUID, ok := bindingOrgID(w, orgID)
	if !ok {
		return
	}
	binding, err := scanBinding(h.store.Pool.QueryRow(ctx, `SELECT `+bindingColumns+` FROM pagerduty_webhook_bindings WHERE id = $1 AND org_id = $2`, bindingID, orgUUID))
	if errors.Is(err, pgx.ErrNoRows) {
		policy.WriteDetail(w, http.StatusNotFound, "Webhook binding not found", nil)
		return
	}
	if err != nil {
		h.internalError(ctx, w, "load pagerduty binding", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, bindingResponse(binding), nil)
}
