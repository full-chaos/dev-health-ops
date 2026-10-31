package acr

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/projectinventory"
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

	// ProjectScopeProjects and ProjectScopeNone are project_scope.kind. "none"
	// is an empty project inventory and carries no projects array.
	ProjectScopeProjects = "projects"
	ProjectScopeNone     = "none"
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
	// Impersonating mirrors PrincipalAccess.Impersonating, the contract's
	// session.impersonating.
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
	// Impersonating is true while the user holds an active impersonation as
	// the admin. impersonation_sessions is keyed by the user, not by session,
	// so it is read on every call, with or without sid: acr denies every
	// user-class call while it is true (design §3.1 step 3, chris decision 6).
	Impersonating bool
	// Session is nil when the query names no session.
	Session             *SessionAccess
	AskDev              bool
	AgentContextRuntime bool
	// Repositories is the org's repository inventory (repoinventory), sorted.
	// Empty means none.
	Repositories []string
	// ProjectScope is the org's project inventory, served as project_scope. It
	// is nil, and the answer carries no project_scope, unless the project scope
	// flag is on and the org has no repository inventory.
	ProjectScope *ProjectScope
}

// ProjectScope is project_scope: the org's projects (projectinventory),
// sorted. An empty list is kind "none".
type ProjectScope struct {
	Projects []projectinventory.Item
}

// PrincipalAccessStore reads one principal's access. Errors are ErrOrgUnknown
// (a well-formed org id with no organizations row) or ErrUnavailable (a read
// failed); a closed answer is not an error.
type PrincipalAccessStore interface {
	Access(ctx context.Context, query AccessQuery) (PrincipalAccess, error)
}

// principalAccessRoute is the principal access read's registration; Routes
// mounts it with deps.Access and deps.AccessLimiter. Its endpoint-profile row
// anchors to the Pattern line below, which reached main before the mount did
// (the auth-test history check reads every anchor at source_commit).
//
// The route is not audited: acr-api calls it on every protected request, so a
// row and a last_used_at stamp per call would put a write on the read path;
// refusals are logged and counted (routeAuth).
//
// deps carries the svc_acr_ credential store and the workload auth; access is
// the store (nil answers 503) and limiter its own attempt bucket
// (AccessAttemptLimit; nil does not rate limit).
func principalAccessRoute(deps Deps, access PrincipalAccessStore, limiter *httpapi.KeyedLimiter, logger *slog.Logger) httpapi.Route {
	limiter.Declare()
	return httpapi.Route{
		Method: http.MethodGet, Pattern: PrincipalAccessPattern,
		Handler: principalAccessHandler(access, routeAuth{
			store: deps.Auth, limiter: limiter, logger: logger, route: routePrincipalAccess,
			workload: deps.Workload, scope: PrincipalAccessScope, limitPath: principalAccessPath,
		}, logger),
	}
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
			writeAccess(w, status, body)
		}
		orgID, orgErr := uuid.Parse(orgRaw)
		userID, userErr := uuid.Parse(userRaw)
		if orgErr != nil || userErr != nil {
			refuse(http.StatusNotFound, outcomeOrgNotFound, "")
			return
		}
		query := AccessQuery{OrgID: orgID, UserID: userID}
		// url.ParseQuery, not r.URL.Query: Query drops a pair it cannot decode
		// ("sid=%zz"), which would read as no sid and skip the session check.
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			// No query text reaches a log line (the parse error quotes it); only the reason.
			refuse(http.StatusBadRequest, outcomeInvalidRequest, InvalidQueryCode, slog.String("reason", "undecodable_query"))
			return
		}
		for key := range values {
			if key != "sid" {
				refuse(http.StatusBadRequest, outcomeInvalidRequest, InvalidQueryCode, slog.String("reason", "unknown_parameter"))
				return
			}
		}
		if sids, present := values["sid"]; present {
			if len(sids) != 1 {
				refuse(http.StatusBadRequest, outcomeInvalidRequest, InvalidQueryCode, slog.String("reason", "repeated_sid"))
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
			writeAccess(w, http.StatusNotFound, body)
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
			slog.Bool("member", access.Member), slog.Bool("impersonating", access.Impersonating), slog.Bool("ask_dev", access.AskDev),
			slog.Bool("agent_context_runtime", access.AgentContextRuntime),
			slog.Int("repositories", len(access.Repositories)), slog.Duration("elapsed", elapsed),
		}
		if access.Session != nil {
			attrs = append(attrs, slog.Bool("session_live", access.Session.Live),
				slog.String("session_reason", access.Session.Reason))
		}
		if access.ProjectScope != nil {
			attrs = append(attrs, slog.Int("projects", len(access.ProjectScope.Projects)))
		}
		countInternal(ctx, routePrincipalAccess, outcomeSuccess)
		logger.LogAttrs(ctx, slog.LevelInfo, "acr principal access: answered", attrs...)
		writeAccess(w, http.StatusOK, principalAccessBody(orgRaw, userRaw, access))
	})
}

// principalAccessBody is the acr_principal_access.v1 document.
func principalAccessBody(orgID, userID string, access PrincipalAccess) *pyjson.Object {
	body := pyjson.NewObject()
	body.Set("schema_version", principalAccessSchema)
	body.Set("org_id", orgID)
	body.Set("user_id", userID)
	body.Set("member", access.Member)
	body.Set("impersonating", access.Impersonating)
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
	if access.ProjectScope != nil {
		projects := pyjson.NewObject()
		if len(access.ProjectScope.Projects) == 0 {
			projects.Set("kind", ProjectScopeNone)
		} else {
			projects.Set("kind", ProjectScopeProjects)
			list := make([]pyjson.Value, 0, len(access.ProjectScope.Projects))
			for _, project := range access.ProjectScope.Projects {
				item := pyjson.NewObject()
				item.Set("provider", project.Provider)
				item.Set("id", project.ID)
				list = append(list, item)
			}
			projects.Set("projects", list)
		}
		body.Set("project_scope", projects)
	}
	return body
}

// writeAccess writes one answer of the principal access read. The read is a
// Go-only route (no FastAPI ancestor, so no response-model entry): it writes
// with the JSONResponse writer, as the llm-config route does, never WriteModel,
// which records a writer violation on a route the table does not name.
func writeAccess(w http.ResponseWriter, status int, body *pyjson.Object) {
	policy.WriteJSON(w, status, body, http.Header{"X-Content-Type-Options": {"nosniff"}})
}
