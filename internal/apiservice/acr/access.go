package acr

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

// PrincipalAccessPattern is the principal access read (CHAOS-12084): acr-api
// calls it on every protected call by a user-class credential to re-check, live,
// the user's membership, the bound session, the ask_dev and
// agent_context_runtime entitlements and the org's repository inventory
// (North Star check 18). Contract:
// .github/docs-legacy/architecture/acr-entitlement-route.md, "Principal access
// read".
const PrincipalAccessPattern = "/api/v1/internal/acr/orgs/{org_id}/principals/{user_id}/access"

// PrincipalAccessScope is the scope a svc_acr_ credential must hold for the
// principal access read. A credential that holds only entitlements:read is
// refused with 403 until it is rotated with this scope.
const PrincipalAccessScope = "principal_access:read"

// AccessAttemptLimit is the principal access read's own attempt bucket, per
// client address. acr-api calls the read once per protected request (no cache),
// so it cannot share the 100/minute bucket of the entitlement and health routes
// (AuthAttemptLimit), which acr reaches about once per org per 30 s.
var AccessAttemptLimit = httpapi.Limit{ID: "internal_acr_access_attempt", Count: 6000, Window: time.Minute}

const (
	routePrincipalAccess     = "principal_access"
	principalAccessPath      = "/api/v1/internal/acr/access"
	principalAccessSchema    = "acr_principal_access.v1"
	outcomeInvalidRequest    = "invalid_request"
	outcomeAccessUnavailable = "access_read_failed"

	// InvalidSessionCode and InvalidQueryCode are the stable machine codes of
	// the read's 400 answers: a sid that is not a session id in its wire form,
	// or a query string other than one sid.
	InvalidSessionCode = "acr_access_invalid_sid"
	InvalidQueryCode   = "acr_access_invalid_query"

	// RepositoryScopeRepos and RepositoryScopeNone are repository_scope.kind.
	// "none" is an empty inventory and carries no repos array: an empty list
	// never stands for any scope.
	RepositoryScopeRepos = "repos"
	RepositoryScopeNone  = "none"
)

// AccessQuery is one principal access read: the organization and the user from
// the path, and the session the sid query parameter names (nil without one).
type AccessQuery struct {
	OrgID     uuid.UUID
	UserID    uuid.UUID
	SessionID *uuid.UUID
}

// SessionAccess is the session half of the answer.
type SessionAccess struct {
	// Live is true only when the session exists, belongs to the user, is not
	// revoked, carries a token_version pin, and that pin equals the user's
	// current users.token_version.
	Live bool
	// Impersonating is true while the user holds an active impersonation
	// (impersonation_sessions is keyed by the admin user, not by session).
	Impersonating bool
	// Reason names why Live is false ("live" when it is true). It is logged,
	// never served.
	Reason string
}

// PrincipalAccess is the read's answer.
type PrincipalAccess struct {
	// Member is true only when the users row is active and a memberships row
	// joins the user to the organization. There is no superuser bypass.
	Member bool
	// Session is nil when the query names no session.
	Session             *SessionAccess
	AskDev              bool
	AgentContextRuntime bool
	// Repositories is the org's repository inventory (repoinventory), sorted.
	// Empty means none.
	Repositories []string
}

// PrincipalAccessStore reads one principal's access. Errors are ErrOrgUnknown
// (a well-formed org id with no organizations row) or ErrUnavailable (a read
// failed); a closed answer is not an error.
type PrincipalAccessStore interface {
	Access(ctx context.Context, query AccessQuery) (PrincipalAccess, error)
}

func principalAccessHandler(store PrincipalAccessStore, auth routeAuth, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		orgRaw, userRaw := r.PathValue("org_id"), r.PathValue("user_id")
		cred, ok := auth.authenticate(w, r, orgRaw)
		if !ok {
			return
		}
		refuse := func(status int, outcome, code string, attrs ...slog.Attr) {
			countInternal(ctx, routePrincipalAccess, outcome)
			logger.LogAttrs(ctx, slog.LevelWarn, "acr principal access: request refused", append([]slog.Attr{
				slog.String("org_id", orgRaw), slog.String("user_id", userRaw), slog.String("credential_id", cred.ID),
				slog.String("outcome", outcome), slog.Int("status", status), slog.String("code", code),
			}, attrs...)...)
			body := pyjson.NewObject()
			if status == http.StatusNotFound {
				body.Set("detail", "Not found")
			} else {
				body.Set("detail", "Invalid request")
			}
			if code != "" {
				body.Set("code", code)
			}
			writeModel(w, status, body)
		}
		orgID, orgErr := uuid.Parse(orgRaw)
		userID, userErr := uuid.Parse(userRaw)
		if orgErr != nil || userErr != nil {
			refuse(http.StatusNotFound, outcomeOrgNotFound, "")
			return
		}
		query := AccessQuery{OrgID: orgID, UserID: userID}
		values := r.URL.Query()
		for key := range values {
			if key != "sid" {
				refuse(http.StatusBadRequest, outcomeInvalidRequest, InvalidQueryCode, slog.String("parameter", key))
				return
			}
		}
		if sids, present := values["sid"]; present {
			if len(sids) != 1 {
				refuse(http.StatusBadRequest, outcomeInvalidRequest, InvalidQueryCode, slog.Int("sid_values", len(sids)))
				return
			}
			id, err := sessionid.Parse(sids[0])
			if err != nil {
				refuse(http.StatusBadRequest, outcomeInvalidRequest, InvalidSessionCode)
				return
			}
			query.SessionID = &id
		}
		if store == nil {
			logger.ErrorContext(ctx, "acr principal access: no access store configured",
				slog.String("org_id", orgRaw), slog.String("user_id", userRaw))
			countInternal(ctx, routePrincipalAccess, "unconfigured")
			writeDetail(w, http.StatusServiceUnavailable, "Service unavailable")
			return
		}
		started := time.Now()
		access, err := store.Access(ctx, query)
		elapsed := time.Since(started)
		switch {
		case err == nil:
		case errors.Is(err, ErrOrgUnknown):
			countInternal(ctx, routePrincipalAccess, outcomeOrgNotFound)
			logger.WarnContext(ctx, "acr principal access: organization unknown",
				slog.String("org_id", orgRaw), slog.String("user_id", userRaw), slog.String("credential_id", cred.ID))
			body := pyjson.NewObject()
			body.Set("detail", "Not found")
			body.Set("code", UnknownOrganizationCode)
			writeModel(w, http.StatusNotFound, body)
			return
		default:
			countInternal(ctx, routePrincipalAccess, outcomeAccessUnavailable)
			logger.ErrorContext(ctx, "acr principal access: read failed; answering 503",
				slog.String("org_id", orgRaw), slog.String("user_id", userRaw), slog.String("credential_id", cred.ID),
				slog.Bool("unavailable", errors.Is(err, ErrUnavailable)), slog.String("missing_grant", policy.MissingGrant(err)),
				slog.Duration("elapsed", elapsed), slog.Any("error", err))
			writeDetail(w, http.StatusServiceUnavailable, "Service unavailable")
			return
		}
		attrs := []slog.Attr{
			slog.String("org_id", orgRaw), slog.String("user_id", userRaw), slog.String("credential_id", cred.ID),
			slog.Bool("member", access.Member), slog.Bool("ask_dev", access.AskDev),
			slog.Bool("agent_context_runtime", access.AgentContextRuntime),
			slog.Int("repositories", len(access.Repositories)), slog.Duration("elapsed", elapsed),
		}
		if access.Session != nil {
			attrs = append(attrs, slog.Bool("session_live", access.Session.Live),
				slog.String("session_reason", access.Session.Reason), slog.Bool("impersonating", access.Session.Impersonating))
		}
		countInternal(ctx, routePrincipalAccess, outcomeSuccess)
		logger.LogAttrs(ctx, slog.LevelInfo, "acr principal access: answered", attrs...)
		writeModel(w, http.StatusOK, principalAccessBody(orgRaw, userRaw, access))
	})
}

// principalAccessBody is the acr_principal_access.v1 document.
func principalAccessBody(orgID, userID string, access PrincipalAccess) *pyjson.Object {
	body := pyjson.NewObject()
	body.Set("schema_version", principalAccessSchema)
	body.Set("org_id", orgID)
	body.Set("user_id", userID)
	body.Set("member", access.Member)
	if access.Session != nil {
		session := pyjson.NewObject()
		session.Set("live", access.Session.Live)
		session.Set("impersonating", access.Session.Impersonating)
		body.Set("session", session)
	}
	entitlements := pyjson.NewObject()
	entitlements.Set("ask_dev", access.AskDev)
	entitlements.Set("agent_context_runtime", access.AgentContextRuntime)
	body.Set("entitlements", entitlements)
	scope := pyjson.NewObject()
	if len(access.Repositories) == 0 {
		scope.Set("kind", RepositoryScopeNone)
	} else {
		scope.Set("kind", RepositoryScopeRepos)
		scope.Set("repos", append([]string(nil), access.Repositories...))
	}
	body.Set("repository_scope", scope)
	return body
}
