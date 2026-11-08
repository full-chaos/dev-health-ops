package oidc

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

// Legacy parity (go-api internal/api/sso/provision.go checkAllowedDomain): an empty or NULL allowed_domains is no
// domain filter, so with JIT on a new account is created, joined and signed in, as legacy answers 200.
func TestJITNewAccountWithNoDomainListMatchesLegacy(t *testing.T) {
	for _, protocol := range []string{"oidc", "oauth"} {
		for name, list := range map[string][]string{"empty": {}, "null": nil} {
			t.Run(protocol+"/"+name, func(t *testing.T) {
				ids := &resolveIdentities{err: ErrIdentityNotFound}
				jit := &resolveJIT{}
				sessions := &resolveSessions{}
				svc := &Service{cfg: Config{Identities: ids, Memberships: resolveMembers{}, Sessions: sessions, MembershipJIT: jit}}
				row := Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: protocol, AutoProvision: true, AllowedDomains: list}
				v := validated{profile: profile{Email: "p@example.com", EmailVerified: true}}
				login, failure := svc.resolve(context.Background(), row, v, nil)
				if failure != nil {
					t.Fatalf("resolve = %d %s, want the legacy 200", failure.Status, failure.Reason)
				}
				if len(jit.new) != 1 || jit.new[0].Email != "p@example.com" || len(sessions.issued) != 0 {
					t.Fatalf("jit accounts %+v, separate sessions %d; want 1 and 0", jit.new, len(sessions.issued))
				}
				if login.OrgID == nil || *login.OrgID != row.OrgID || login.Role != "member" || login.PrincipalID == uuid.Nil {
					t.Fatalf("login %+v, want a member session in org %s", login, row.OrgID)
				}
			})
		}
	}
}
