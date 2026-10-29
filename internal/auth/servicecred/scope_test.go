package servicecred

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

func TestCheckScopeRowRule(t *testing.T) {
	both := `["memberships:write", "sessions:issue"]`
	for _, c := range []struct {
		name             string
		service, scopes  string
		revoked, expired bool
		scope            string
		want             Refusal
	}{
		{"issuer holding the scope", ServiceSessionIssuer, both, false, false, ScopeMembershipsWrite, ""},
		{"issuer holding only sessions:issue", ServiceSessionIssuer, `["sessions:issue"]`, false, false, ScopeMembershipsWrite, RefusedScope},
		{"issuer holding only memberships:write asks sessions:issue", ServiceSessionIssuer, `["memberships:write"]`, false, false, ScopeSessionsIssue, RefusedScope},
		{"another service holding the scope", "acr", both, false, false, ScopeMembershipsWrite, RefusedService},
		{"revoked", ServiceSessionIssuer, both, true, false, ScopeMembershipsWrite, RefusedRevoked},
		{"expired", ServiceSessionIssuer, both, false, true, ScopeMembershipsWrite, RefusedExpired},
		{"scopes not a JSON list", ServiceSessionIssuer, `"memberships:write"`, false, false, ScopeMembershipsWrite, RefusedScope},
		{"a scope that only starts the same", ServiceSessionIssuer, `["memberships:write:all"]`, false, false, ScopeMembershipsWrite, RefusedScope},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, refused, err := decide("cred-1", c.service, c.scopes, c.revoked, c.expired, c.scope)
			if err != nil || refused != c.want {
				t.Fatalf("decide = %q, %v; want %q", refused, err, c.want)
			}
			if (refused == "") != (got.ID == "cred-1") {
				t.Fatalf("credential = %+v with refusal %q", got, refused)
			}
		})
	}
}

// A token of the wrong shape is refused before any lookup: a nil pool is never read.
func TestCheckScopeRefusesAMalformedTokenWithoutALookup(t *testing.T) {
	for _, token := range []string{"", "svc_authsess_short", "svc_acr_" + "A123456789012345678901234567890123456789012"} {
		_, refused, err := PG{}.CheckScope(context.Background(), contracts.NewSecretString(token), ScopeMembershipsWrite)
		if err != nil || refused != RefusedMalformed {
			t.Fatalf("CheckScope = %q, %v; want malformed", refused, err)
		}
	}
}
