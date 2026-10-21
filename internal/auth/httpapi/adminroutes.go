package httpapi

import (
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
	router   atomic.Pointer[boundRouter]
}

type boundRouter struct {
	mux    *http.ServeMux
	strict bool
}

// NewNoStoreRoutes lists the "METHOD pattern" routes that answer no-store.
func NewNoStoreRoutes(patterns []string) *NoStoreRoutes {
	set := make(map[string]bool, len(patterns))
	for _, pattern := range patterns {
		set[pattern] = true
	}
	return &NoStoreRoutes{patterns: set}
}

func (n *NoStoreRoutes) bind(mux *http.ServeMux, strict bool) {
	n.router.Store(&boundRouter{mux: mux, strict: strict})
}

func (n *NoStoreRoutes) route() *boundRouter {
	if bound := n.router.Load(); bound != nil {
		return bound
	}
	n.once.Do(func() {
		n.own = http.NewServeMux()
		none := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
		for pattern := range n.patterns {
			n.own.Handle(pattern, none)
		}
	})
	return &boundRouter{mux: n.own, strict: true}
}

// Matches reports whether the router routes r to one of the listed patterns.
func (n *NoStoreRoutes) Matches(r *http.Request) bool {
	router := n.route()
	target := DecodePath(r)
	if router.strict {
		var ok bool
		if target, ok = strictTarget(target); !ok {
			return false
		}
	}
	_, pattern := router.mux.Handler(target)
	return n.patterns[pattern]
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
