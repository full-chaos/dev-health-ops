package admin

import (
	"slices"
	"strings"
	"testing"
)

// memberships:write lets its holder write memberships, invites and an
// organization deprovision through auth-service. Only the session issuer
// credential (go-api's own) may hold it.
func TestOnlyTheSessionIssuerMayHoldMembershipsWrite(t *testing.T) {
	var holders []string
	for service, scopes := range serviceScopes {
		if slices.Contains(scopes, "memberships:write") {
			holders = append(holders, service)
		}
	}
	if len(holders) != 1 || holders[0] != ServiceAuthSessionIssuer {
		t.Fatalf("memberships:write holders = %v, want only %s", holders, ServiceAuthSessionIssuer)
	}
	got, err := ServiceCredentialScopes(ServiceAuthSessionIssuer, []string{"memberships:write", "sessions:issue"})
	if err != nil || strings.Join(got, ",") != "memberships:write,sessions:issue" {
		t.Fatalf("issuer scopes %v err %v", got, err)
	}
	for _, service := range []string{ServiceACR, ServiceWorkerOperator} {
		if _, err := ServiceCredentialScopes(service, []string{"memberships:write"}); err == nil {
			t.Errorf("%s may mint memberships:write", service)
		}
	}
}
