//go:build integration

package identity

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

// Before the users cutover the legacy fallback of an OIDC or SAML key reads the same carriers as the owned re-link:
// an account is the identity of the provider row it is bound to (pgmigrate revision 0155).

func (e *env) identityRows(t *testing.T, ctx context.Context, subject string) int {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM auth.external_identities WHERE subject = $1`, subject)
}

// TestSwitchOffLegacyFallbackIgnoresARowBoundToAnotherProvider: a member account bound to another provider row of
// the same protocol is that provider's identity; a sign-in at this provider row is a miss and links nothing.
func TestSwitchOffLegacyFallbackIgnoresARowBoundToAnotherProvider(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for _, protocol := range []string{"oidc", "saml"} {
		t.Run(protocol, func(t *testing.T) {
			f := e.relinkFixture(t, ctx, protocol)
			other := e.seedSSOProvider(t, ctx, f.org, protocol)
			raw := protocol + "-off-other-" + uuid.NewString()
			e.seedBound(t, ctx, protocol, raw, &other, f.org, true)
			key, legacy := f.key(raw)
			if p, found, err := e.store.IdentityBySubject(ctx, key.Issuer, key.Provider, key.Subject, legacy); err != nil || found {
				t.Fatalf("switch off, sign-in at P1 with the subject of a P2-bound row = %+v, %v, %v; want a miss", p, found, err)
			}
			if n := e.identityRows(t, ctx, key.Subject); n != 0 {
				t.Fatalf("identity rows = %d, want none", n)
			}
		})
	}
}

// TestSwitchOffLegacyFallbackResolvesEachProviderOfOneSubject: one subject held by two accounts bound to two provider
// rows is two identities; each provider row resolves its own account.
func TestSwitchOffLegacyFallbackResolvesEachProviderOfOneSubject(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for _, protocol := range []string{"oidc", "saml"} {
		t.Run(protocol, func(t *testing.T) {
			f1, f2 := e.relinkFixture(t, ctx, protocol), e.relinkFixture(t, ctx, protocol)
			raw := protocol + "-off-each-" + uuid.NewString()
			user1 := e.seedBound(t, ctx, protocol, raw, &f1.provider, f1.org, true)
			user2 := e.seedBound(t, ctx, protocol, raw, &f2.provider, f2.org, true)
			for _, c := range []struct {
				f    relinkFixture
				want uuid.UUID
			}{{f1, user1}, {f2, user2}} {
				key, legacy := c.f.key(raw)
				if p, found, err := e.store.IdentityBySubject(ctx, key.Issuer, key.Provider, key.Subject, legacy); err != nil || !found || p.ID != c.want {
					t.Fatalf("switch off, sign-in at %s = %+v, %v, %v; want %v", c.f.provider, p, found, err, c.want)
				}
			}
		})
	}
}

// TestSwitchOffLegacyFallbackRefusesAnUnboundCarrier: an account that carries the subject but is bound to no provider
// row is no provider's identity. The sign-in is refused (ErrRelinkRefused, an ambiguous identity), member of the
// provider organization or not, and nothing is linked.
func TestSwitchOffLegacyFallbackRefusesAnUnboundCarrier(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for _, protocol := range []string{"oidc", "saml"} {
		for _, member := range []bool{true, false} {
			t.Run(protocol+map[bool]string{true: "/member", false: "/not-member"}[member], func(t *testing.T) {
				f := e.relinkFixture(t, ctx, protocol)
				org := f.org
				if !member {
					org = e.seedOrg(t, ctx)
				}
				raw := protocol + "-off-unbound-" + uuid.NewString()
				e.seedBound(t, ctx, protocol, raw, nil, org, true)
				key, legacy := f.key(raw)
				p, found, err := e.store.IdentityBySubject(ctx, key.Issuer, key.Provider, key.Subject, legacy)
				if !errors.Is(err, ErrRelinkRefused) || !errors.Is(err, ErrMultipleUsers) || found {
					t.Fatalf("switch off, unbound carrier = %+v, %v, %v; want ErrRelinkRefused", p, found, err)
				}
				if n := e.identityRows(t, ctx, key.Subject); n != 0 {
					t.Fatalf("identity rows = %d, want none", n)
				}
			})
		}
	}
}

// TestSwitchOffLegacyFallbackRefusesAStaleBoundCarrier: an account bound to a provider row of another protocol is
// bound to no provider of its own protocol: the sign-in is refused, as for an unbound account.
func TestSwitchOffLegacyFallbackRefusesAStaleBoundCarrier(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for _, protocol := range []string{"oidc", "saml"} {
		t.Run(protocol, func(t *testing.T) {
			f := e.relinkFixture(t, ctx, protocol)
			otherProtocol := map[string]string{"oidc": "saml", "saml": "oidc"}[protocol]
			wrong := e.seedSSOProvider(t, ctx, f.org, otherProtocol)
			raw := protocol + "-off-stale-" + uuid.NewString()
			e.seedBound(t, ctx, protocol, raw, &wrong, f.org, true)
			key, legacy := f.key(raw)
			if p, found, err := e.store.IdentityBySubject(ctx, key.Issuer, key.Provider, key.Subject, legacy); !errors.Is(err, ErrRelinkRefused) || found {
				t.Fatalf("switch off, carrier bound to a %s row = %+v, %v, %v; want ErrRelinkRefused", otherProtocol, p, found, err)
			}
			if n := e.identityRows(t, ctx, key.Subject); n != 0 {
				t.Fatalf("identity rows = %d, want none", n)
			}
		})
	}
}

// TestSwitchOffLegacyFallbackProjectsARowBoundToTheKeyProvider: a member account bound to the key's provider row is
// projected and linked under the key, as before.
func TestSwitchOffLegacyFallbackProjectsARowBoundToTheKeyProvider(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for _, protocol := range []string{"oidc", "saml"} {
		t.Run(protocol, func(t *testing.T) {
			f := e.relinkFixture(t, ctx, protocol)
			raw := protocol + "-off-own-" + uuid.NewString()
			user := e.seedBound(t, ctx, protocol, raw, &f.provider, f.org, false)
			key, legacy := f.key(raw)
			if p, found, err := e.store.IdentityBySubject(ctx, key.Issuer, key.Provider, key.Subject, legacy); err != nil || !found || p.ID != user {
				t.Fatalf("switch off, row bound to the key's provider = %+v, %v, %v; want %v", p, found, err, user)
			}
			if n := e.count(t, ctx, `SELECT count(*) FROM auth.external_identities WHERE principal_id = $1 AND provider = $2 AND subject = $3`,
				user, string(key.Provider), key.Subject); n != 1 {
				t.Fatalf("identity rows = %d, want 1", n)
			}
		})
	}
}

// TestSwitchOffLegacyFallbackOfAnOAuthTypeLoginIsUnchanged: an OAuth-type login (go-api auth_provider github, gitlab
// or google, no provider column) is not in the OIDC/SAML carrier rule: a member account is projected as before.
func TestSwitchOffLegacyFallbackOfAnOAuthTypeLoginIsUnchanged(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	f := e.relinkFixture(t, ctx, "oauth")
	subject := "oauth-off-" + uuid.NewString()
	user := e.seedLegacy(t, ctx, uniqueEmail("oauth-off"), "github", subject, false)
	e.seedMembership(t, ctx, user, f.org)
	org := f.org
	if p, found, err := e.store.IdentityBySubject(ctx, "https://github.com", f.provider, subject, &LegacyMatch{AuthProvider: "github", MembershipOrg: &org}); err != nil || !found || p.ID != user {
		t.Fatalf("switch off, OAuth-type sign-in = %+v, %v, %v; want %v", p, found, err, user)
	}
}
