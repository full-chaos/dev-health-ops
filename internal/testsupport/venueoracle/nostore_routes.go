package venueoracle

import "strings"

// adminNoStoreRoutes is the "METHOD pattern" table of internal/apiservice/admin
// Routes: the only routes the declared "admin no-store" divergence covers. A
// test in package admin_test keeps it equal to the real route table. Other
// /api/v1/admin-prefixed packages are not in it.
var adminNoStoreRoutes = []string{
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

// AdminNoStoreRoutes returns the table, for the test that pins it to the real
// admin route table.
func AdminNoStoreRoutes() []string { return append([]string(nil), adminNoStoreRoutes...) }

func adminNoStoreRoute(method, path string) bool {
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	segments := strings.Split(path, "/")
	for _, entry := range adminNoStoreRoutes {
		routeMethod, pattern, _ := strings.Cut(entry, " ")
		if routeMethod != method {
			continue
		}
		want := strings.Split(pattern, "/")
		if len(want) != len(segments) {
			continue
		}
		matched := true
		for i := range want {
			if !(strings.HasPrefix(want[i], "{") && strings.HasSuffix(want[i], "}") && segments[i] != "") && want[i] != segments[i] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}
