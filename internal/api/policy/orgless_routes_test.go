package policy

import (
	"strings"
	"testing"
)

func TestOrglessMayReachIsExactlyTheEleven(t *testing.T) {
	listed := []string{
		"POST /api/v1/auth/onboard",
		"GET /api/v1/auth/onboarding/state",
		"HEAD /api/v1/auth/onboarding/state",
		"POST /api/v1/auth/onboarding/skip-integration",
		"POST /api/v1/auth/accept-invite",
		"GET /api/v1/auth/me",
		"HEAD /api/v1/auth/me",
		"GET /api/v1/auth/me/organizations",
		"HEAD /api/v1/auth/me/organizations",
		"POST /api/v1/auth/switch-org",
		"POST /api/v1/auth/change-password",
	}
	if len(orglessRoutes) != len(listed) {
		t.Fatalf("%d org-less routes, want %d", len(orglessRoutes), len(listed))
	}
	for _, pair := range listed {
		method, requestPath, _ := strings.Cut(pair, " ")
		if !OrglessMayReach(method, requestPath) {
			t.Errorf("%s is not reachable without an organization", pair)
		}
	}
	for _, c := range [][2]string{
		{"DELETE", "/api/v1/auth/me"}, {"PUT", "/api/v1/auth/me"}, {"GET", "/api/v1/auth/onboard"},
		{"POST", "/api/v1/auth/onboard/extra"}, {"GET", "/api/v1/orgs/me"}, {"GET", "/api/v1/auth/../orgs/me"},
		{"POST", "/api/v1/billing/checkout"}, {"GET", "/api/v1/billing/subscriptions"},
	} {
		if OrglessMayReach(c[0], c[1]) {
			t.Errorf("%s %s is reachable without an organization", c[0], c[1])
		}
	}
	if !OrglessMayReach("POST", "/api/v1/auth/../auth/onboard") {
		t.Error("a path that cleans to a listed route is not reachable")
	}
}
