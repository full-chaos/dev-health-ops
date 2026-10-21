package httpapi

import (
	"net/http"
	"sort"
)

// AdminRoutes is the one table of every "METHOD pattern" of admin.Routes; the
// no-store middleware and the venue oracle (admin no-store divergence) both read
// it, and other /api/v1/admin-prefixed packages are not in it. A test in package admin_test
// keeps it equal to the real route table.
var AdminRoutes = []string{
	"DELETE /api/v1/admin/credentials/{provider}/{name}",
	"DELETE /api/v1/admin/ip-allowlist/{entry_id}",
	"DELETE /api/v1/admin/llm-settings",
	"DELETE /api/v1/admin/orgs/{org_id}",
	"DELETE /api/v1/admin/orgs/{org_id}/feature-overrides/{override_id}",
	"DELETE /api/v1/admin/orgs/{org_id}/members/{user_id}",
	"DELETE /api/v1/admin/retention-policies/{policy_id}",
	"DELETE /api/v1/admin/settings/{category}/{key}",
	"DELETE /api/v1/admin/users/{user_id}",
	"GET /api/v1/admin/audit-logs",
	"GET /api/v1/admin/audit-logs/resource/{resource_type}/{resource_id}",
	"GET /api/v1/admin/audit-logs/user/{user_id}",
	"GET /api/v1/admin/audit-logs/{log_id}",
	"GET /api/v1/admin/feature-flags",
	"GET /api/v1/admin/impersonate/status",
	"GET /api/v1/admin/integrations/pagerduty/services",
	"GET /api/v1/admin/integrations/pagerduty/status",
	"GET /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}",
	"GET /api/v1/admin/ip-allowlist",
	"GET /api/v1/admin/ip-allowlist/{entry_id}",
	"GET /api/v1/admin/llm-settings",
	"GET /api/v1/admin/llm-settings/budget",
	"GET /api/v1/admin/llm-settings/models",
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
	"GET /api/v1/admin/users",
	"GET /api/v1/admin/users/{user_id}",
	"PATCH /api/v1/admin/feature-flags/{flag_id}",
	"PATCH /api/v1/admin/ip-allowlist/{entry_id}",
	"PATCH /api/v1/admin/orgs/{org_id}",
	"PATCH /api/v1/admin/orgs/{org_id}/feature-overrides/{override_id}",
	"PATCH /api/v1/admin/orgs/{org_id}/members/{user_id}",
	"PATCH /api/v1/admin/retention-policies/{policy_id}",
	"PATCH /api/v1/admin/users/{user_id}",
	"POST /api/v1/admin/impersonate",
	"POST /api/v1/admin/impersonate/stop",
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
	"POST /api/v1/admin/users",
	"POST /api/v1/admin/users/{user_id}/password",
	"PUT /api/v1/admin/llm-settings",
	"PUT /api/v1/admin/llm-settings/service-model",
	"PUT /api/v1/admin/settings/{category}/{key}",
}

var matcher = func() *http.ServeMux {
	mux := http.NewServeMux()
	none := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	for _, entry := range AdminRoutes {
		mux.Handle(entry, none)
	}
	return mux
}()

// MatchesAdminRoute reports whether a request of method and path is served by an admin
// route, by net/http's own pattern rules.
func MatchesAdminRoute(r *http.Request) bool {
	_, pattern := matcher.Handler(r)
	return pattern != ""
}

// NoStoreAdminRoutes sets Cache-Control: no-store before next runs on every request that
// Matches, so a response written by any later layer (scope, origin check,
// rate limit, body bound, guard, handler) carries it.
func NoStoreAdminRoutes(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if MatchesAdminRoute(r) {
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
