package admin

import (
	"slices"
	"strings"
	"testing"
)

// memberships:write is retired: the auth-service provisioning routes take the
// go-api workload token only. No service may hold it and no mint may ask for
// it; the session issuer keeps sessions:issue.
func TestNoServiceMayHoldTheRetiredMembershipsWriteScope(t *testing.T) {
	for service, scopes := range serviceScopes {
		if slices.Contains(scopes, "memberships:write") {
			t.Errorf("%s may still hold memberships:write", service)
		}
	}
	for _, service := range []string{ServiceAuthSessionIssuer, ServiceACR, ServiceWorkerOperator} {
		_, err := ServiceCredentialScopes(service, []string{"memberships:write"})
		if err == nil || !strings.Contains(err.Error(), "unsupported internal service credential scope") {
			t.Errorf("%s mint with memberships:write: %v, want the unsupported-scope refusal", service, err)
		}
	}
	if _, err := ServiceCredentialScopes(ServiceAuthSessionIssuer, []string{"memberships:write", "sessions:issue"}); err == nil {
		t.Error("the session issuer may mint memberships:write beside sessions:issue")
	}
	got, err := ServiceCredentialScopes(ServiceAuthSessionIssuer, []string{"sessions:issue"})
	if err != nil || strings.Join(got, ",") != "sessions:issue" {
		t.Fatalf("issuer scopes %v err %v, want sessions:issue", got, err)
	}
}
