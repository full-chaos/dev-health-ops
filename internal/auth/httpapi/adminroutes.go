package httpapi

import (
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// AdminRoutes is the one table of every "METHOD pattern" the api mounts under
// /api/v1/admin (admin, credentials, customerpush, syncadmin, integrationsadmin,
// githubapp, teamsidentity); the no-store middleware and the venue oracle (admin
// no-store divergence) both read it. A test in package apiservice walks the real
// route set and fails when the table and the mounted routes differ.
var AdminRoutes = []string{
	"DELETE /api/v1/admin/credentials/{provider}/{name}",
	"DELETE /api/v1/admin/ip-allowlist/{entry_id}",
	"DELETE /api/v1/admin/llm-settings",
	"DELETE /api/v1/admin/orgs/{org_id}",
	"DELETE /api/v1/admin/orgs/{org_id}/feature-overrides/{override_id}",
	"DELETE /api/v1/admin/orgs/{org_id}/members/{user_id}",
	"DELETE /api/v1/admin/retention-policies/{policy_id}",
	"DELETE /api/v1/admin/settings/{category}/{key}",
	"DELETE /api/v1/admin/sync-configs/{config_id}",
	"DELETE /api/v1/admin/teams/{team_id}",
	"DELETE /api/v1/admin/users/{user_id}",
	"GET /api/v1/admin/audit-logs",
	"GET /api/v1/admin/audit-logs/resource/{resource_type}/{resource_id}",
	"GET /api/v1/admin/audit-logs/user/{user_id}",
	"GET /api/v1/admin/audit-logs/{log_id}",
	"GET /api/v1/admin/backfill-jobs",
	"GET /api/v1/admin/backfill-jobs/{job_id}",
	"GET /api/v1/admin/credentials",
	"GET /api/v1/admin/credentials/{provider}/{name}",
	"GET /api/v1/admin/customer-push/batches/{ingestion_id}",
	"GET /api/v1/admin/customer-push/schemas",
	"GET /api/v1/admin/customer-push/schemas/{schema_version}",
	"GET /api/v1/admin/customer-push/sources",
	"GET /api/v1/admin/customer-push/sources/{source_id}",
	"GET /api/v1/admin/customer-push/sources/{source_id}/batches",
	"GET /api/v1/admin/customer-push/sources/{source_id}/tokens",
	"GET /api/v1/admin/customer-push/tokens",
	"GET /api/v1/admin/feature-flags",
	"GET /api/v1/admin/identities",
	"GET /api/v1/admin/impersonate/status",
	"GET /api/v1/admin/integrations",
	"GET /api/v1/admin/integrations/pagerduty/services",
	"GET /api/v1/admin/integrations/pagerduty/status",
	"GET /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}",
	"GET /api/v1/admin/integrations/{integration_id}",
	"GET /api/v1/admin/integrations/{integration_id}/datasets",
	"GET /api/v1/admin/integrations/{integration_id}/sources",
	"GET /api/v1/admin/ip-allowlist",
	"GET /api/v1/admin/ip-allowlist/{entry_id}",
	"GET /api/v1/admin/llm-settings",
	"GET /api/v1/admin/llm-settings/budget",
	"GET /api/v1/admin/llm-settings/models",
	"GET /api/v1/admin/llm-settings/own-model",
	"GET /api/v1/admin/llm-settings/prompt-cache",
	"GET /api/v1/admin/llm-settings/service-model",
	"GET /api/v1/admin/llm-settings/spend",
	"GET /api/v1/admin/llm-settings/status",
	"GET /api/v1/admin/orgs",
	"GET /api/v1/admin/orgs/{org_id}",
	"GET /api/v1/admin/orgs/{org_id}/feature-overrides",
	"GET /api/v1/admin/orgs/{org_id}/members",
	"GET /api/v1/admin/platform/audit-logs",
	"GET /api/v1/admin/platform/stats",
	"GET /api/v1/admin/retention-policies",
	"GET /api/v1/admin/retention-policies/{policy_id}",
	"GET /api/v1/admin/settings/{category}",
	"GET /api/v1/admin/settings/{category}/{key}",
	"GET /api/v1/admin/setup/status",
	"GET /api/v1/admin/sync-configs",
	"GET /api/v1/admin/sync-configs/auto-import-capabilities",
	"GET /api/v1/admin/sync-configs/{config_id}",
	"GET /api/v1/admin/sync-configs/{config_id}/coverage",
	"GET /api/v1/admin/sync-configs/{config_id}/jobs",
	"GET /api/v1/admin/sync-configs/{config_id}/repositories",
	"GET /api/v1/admin/sync-runs/{run_id}",
	"GET /api/v1/admin/sync-runs/{run_id}/units",
	"GET /api/v1/admin/sync-targets",
	"GET /api/v1/admin/teams",
	"GET /api/v1/admin/teams/{team_id}",
	"GET /api/v1/admin/teams/{team_id}/discover-members",
	"GET /api/v1/admin/teams/{team_id}/infer-members",
	"GET /api/v1/admin/users",
	"GET /api/v1/admin/users/{user_id}",
	"PATCH /api/v1/admin/credentials/{provider}/{name}",
	"PATCH /api/v1/admin/customer-push/sources/{source_id}",
	"PATCH /api/v1/admin/feature-flags/{flag_id}",
	"PATCH /api/v1/admin/integrations/{integration_id}",
	"PATCH /api/v1/admin/integrations/{integration_id}/datasets",
	"PATCH /api/v1/admin/integrations/{integration_id}/sources/{source_id}",
	"PATCH /api/v1/admin/ip-allowlist/{entry_id}",
	"PATCH /api/v1/admin/orgs/{org_id}",
	"PATCH /api/v1/admin/orgs/{org_id}/feature-overrides/{override_id}",
	"PATCH /api/v1/admin/orgs/{org_id}/members/{user_id}",
	"PATCH /api/v1/admin/retention-policies/{policy_id}",
	"PATCH /api/v1/admin/sync-configs/{config_id}",
	"PATCH /api/v1/admin/teams/{team_id}",
	"PATCH /api/v1/admin/users/{user_id}",
	"POST /api/v1/admin/credentials",
	"POST /api/v1/admin/credentials/test",
	"POST /api/v1/admin/customer-push/sources",
	"POST /api/v1/admin/customer-push/sources/{source_id}/tokens",
	"POST /api/v1/admin/customer-push/sources/{source_id}/validate",
	"POST /api/v1/admin/customer-push/tokens",
	"POST /api/v1/admin/customer-push/tokens/{token_id}/revoke",
	"POST /api/v1/admin/customer-push/tokens/{token_id}/rotate",
	"POST /api/v1/admin/identities",
	"POST /api/v1/admin/impersonate",
	"POST /api/v1/admin/impersonate/stop",
	"POST /api/v1/admin/integrations",
	"POST /api/v1/admin/integrations/github/install-callback",
	"POST /api/v1/admin/integrations/github/install-url",
	"POST /api/v1/admin/integrations/pagerduty/api-token",
	"POST /api/v1/admin/integrations/pagerduty/authorize",
	"POST /api/v1/admin/integrations/pagerduty/callback",
	"POST /api/v1/admin/integrations/pagerduty/client-credentials",
	"POST /api/v1/admin/integrations/pagerduty/disconnect",
	"POST /api/v1/admin/integrations/pagerduty/preflight",
	"POST /api/v1/admin/integrations/pagerduty/webhook-bindings",
	"POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/activate",
	"POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/revoke",
	"POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/rotate",
	"POST /api/v1/admin/integrations/{integration_id}/backfill",
	"POST /api/v1/admin/integrations/{integration_id}/discover",
	"POST /api/v1/admin/integrations/{integration_id}/sync",
	"POST /api/v1/admin/ip-allowlist",
	"POST /api/v1/admin/ip-allowlist/{entry_id}",
	"POST /api/v1/admin/llm-settings/readiness",
	"POST /api/v1/admin/orgs",
	"POST /api/v1/admin/orgs/{org_id}/feature-overrides",
	"POST /api/v1/admin/orgs/{org_id}/invites",
	"POST /api/v1/admin/orgs/{org_id}/members",
	"POST /api/v1/admin/orgs/{org_id}/transfer-ownership",
	"POST /api/v1/admin/retention-policies",
	"POST /api/v1/admin/retention-policies/{policy_id}/execute",
	"POST /api/v1/admin/settings",
	"POST /api/v1/admin/sync-configs",
	"POST /api/v1/admin/sync-configs/batch",
	"POST /api/v1/admin/sync-configs/{config_id}/backfill",
	"POST /api/v1/admin/sync-configs/{config_id}/trigger",
	"POST /api/v1/admin/teams",
	"POST /api/v1/admin/teams/{team_id}",
	"POST /api/v1/admin/teams/{team_id}/approve-changes",
	"POST /api/v1/admin/teams/{team_id}/confirm-inferred-members",
	"POST /api/v1/admin/teams/{team_id}/confirm-members",
	"POST /api/v1/admin/teams/{team_id}/dismiss-changes",
	"POST /api/v1/admin/users",
	"POST /api/v1/admin/users/{user_id}/password",
	"PUT /api/v1/admin/llm-settings",
	"PUT /api/v1/admin/llm-settings/own-model",
	"PUT /api/v1/admin/llm-settings/prompt-cache",
	"PUT /api/v1/admin/llm-settings/service-model",
	"PUT /api/v1/admin/settings/{category}/{key}",
	"PUT /api/v1/admin/sync-configs/{config_id}/repositories",
}

// DecodePath routes on the percent-decoded path, as Starlette does: an encoded
// slash ("%2F") in a request target is a path separator for route matching, so
// /x/a%2Fb matches no one-segment {param} route. net/http's mux would otherwise
// keep "a%2Fb" as one segment. One implementation, used by the api's
// DecodedPathRouting middleware and by NoStoreRoutes.
func DecodePath(r *http.Request) *http.Request {
	if r.URL.RawPath != "" && strings.Contains(strings.ToLower(r.URL.RawPath), "%2f") {
		clone := *r.URL
		clone.RawPath = ""
		r = r.Clone(r.Context())
		r.URL = &clone
	}
	return r
}

// NoStoreRoutes answers Cache-Control: no-store on every request the serving
// router would route to one of its patterns, however early a layer in front of
// the router writes the response. The decision is the router's own: the
// request is decoded (DecodePath) and given strictPaths' treatment, then asked
// of the mux (the server's own, once bound by ServerOptions.NoStore; else a
// private mux of just these patterns, which is what the venue oracle uses), and
// matches only when the mux picked one of the listed "METHOD pattern" routes.
type NoStoreRoutes struct {
	patterns map[string]bool
	once     sync.Once
	own      *http.ServeMux
	// ownRedirect is the unbound router's slash redirector (the venue oracle's).
	ownRedirect *slashRedirector
	router      atomic.Pointer[boundRouter]
}

type boundRouter struct {
	mux      *http.ServeMux
	strict   bool
	redirect *slashRedirector
}

// NewNoStoreRoutes lists the "METHOD pattern" routes that answer no-store.
func NewNoStoreRoutes(patterns []string) *NoStoreRoutes {
	set := make(map[string]bool, len(patterns))
	for _, pattern := range patterns {
		set[pattern] = true
	}
	return &NoStoreRoutes{patterns: set}
}

func (n *NoStoreRoutes) bind(mux *http.ServeMux, strict bool, redirect *slashRedirector) {
	n.router.Store(&boundRouter{mux: mux, strict: strict, redirect: redirect})
}

func (n *NoStoreRoutes) route() *boundRouter {
	if bound := n.router.Load(); bound != nil {
		return bound
	}
	n.once.Do(func() {
		n.own = http.NewServeMux()
		none := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
		seen := map[string]bool{}
		var paths []string
		for pattern := range n.patterns {
			n.own.Handle(pattern, none)
			if path := pattern[strings.IndexByte(pattern, ' ')+1:]; !seen[path] {
				seen[path] = true
				paths = append(paths, path)
			}
		}
		n.ownRedirect = newSlashRedirector(paths, ParseForwardedTrust(""))
	})
	return &boundRouter{mux: n.own, strict: true, redirect: n.ownRedirect}
}

// Matches reports whether the router routes r to one of the listed patterns,
// or answers it with its trailing-slash 307 redirect to a path of the table.
func (n *NoStoreRoutes) Matches(r *http.Request) bool {
	router := n.route()
	decoded := DecodePath(r)
	target, routable := decoded, true
	if router.strict {
		target, routable = strictTarget(decoded)
	}
	if routable {
		if _, pattern := router.mux.Handler(target); n.patterns[pattern] {
			return true
		}
	}
	return router.redirect != nil && n.redirectsToTable(router.redirect, decoded)
}

// redirectsToTable reports whether the router answers target with its 307
// slash redirect to a path of the table: no route matches target itself and the
// toggled path matches a table path. The 307 names the admin path, so it
// carries no-store like the route it points at.
func (n *NoStoreRoutes) redirectsToTable(redirect *slashRedirector, target *http.Request) bool {
	if _, routed := redirect.matches(target, target.URL.Path); routed {
		return false
	}
	toggled, ok := toggledSlash(target.URL.Path)
	if !ok {
		return false
	}
	pattern, ok := redirect.matches(target, toggled)
	if !ok {
		return false
	}
	for entry := range n.patterns {
		if strings.HasSuffix(entry, " "+pattern) {
			return true
		}
	}
	return false
}

// Middleware sets Cache-Control: no-store before next runs when Matches, so a
// response written by any later layer (scope, origin check, rate limit, body
// bound, guard, handler) carries it. It must sit ahead of every layer that can
// write a response for these routes.
func (n *NoStoreRoutes) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Matches(r) {
			w.Header().Set("Cache-Control", "no-store")
		}
		next.ServeHTTP(w, r)
	})
}

// SortedAdminRoutes returns a sorted copy of AdminRoutes.
func SortedAdminRoutes() []string {
	out := append([]string(nil), AdminRoutes...)
	sort.Strings(out)
	return out
}
