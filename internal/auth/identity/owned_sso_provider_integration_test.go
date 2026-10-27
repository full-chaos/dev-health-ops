//go:build integration

package identity

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

// After the users cutover, the public.users row of an OIDC or SAML account carries the provider row it belongs to
// (users.sso_provider_id, pgmigrate revision 0155), as go-api's own SSO account creation writes it. go-api keys its
// sign-in on (auth_provider, sso_provider_id, auth_provider_id), so the row stays an identity of its provider after a
// switch back.

func (e *env) publicProvider(t *testing.T, ctx context.Context, user uuid.UUID) *uuid.UUID {
	t.Helper()
	var bound *uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT sso_provider_id FROM public.users WHERE id = $1`, user).Scan(&bound); err != nil {
		t.Fatalf("read public.users.sso_provider_id: %v", err)
	}
	return bound
}

func ssoProvisionRequest(f relinkFixture, raw string) ProvisionRequest {
	key, legacy := f.key(raw)
	stored := raw
	if legacy.Subject != "" {
		stored = legacy.Subject
	}
	return ProvisionRequest{Key: key, Email: uniqueEmailCom(f.protocol + "-owned"), Actor: SelfSignUp(nil),
		LegacyProvider: f.protocol, LegacySubject: stored}
}

// TestOwnedSSOProvisioningBindsTheProviderRow: an OIDC or SAML account provisioned under users-owned is bound to the
// key's provider row in public.users.
func TestOwnedSSOProvisioningBindsTheProviderRow(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for _, protocol := range []string{"oidc", "saml"} {
		t.Run(protocol, func(t *testing.T) {
			f := e.relinkFixture(t, ctx, protocol)
			id, err := e.owned.ProvisionFederated(ctx, ssoProvisionRequest(f, protocol+"-bind-"+uuid.NewString()))
			if err != nil {
				t.Fatalf("ProvisionFederated: %v", err)
			}
			want := uuid.MustParse(string(f.provider))
			if got := e.publicProvider(t, ctx, id); got == nil || *got != want {
				t.Fatalf("public.users.sso_provider_id = %v, want %v", got, want)
			}
		})
	}
}

// TestOwnedProvisioningOfASocialOrOAuthTypeKeyLeavesNoProvider: a social account and an OAuth-type account (go-api
// auth_provider github, gitlab, google) carry no provider row, as go-api writes them.
func TestOwnedProvisioningOfASocialOrOAuthTypeKeyLeavesNoProvider(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	issuer, _ := SocialIssuer(ProviderGitHub)
	social, err := e.owned.ProvisionFederated(ctx, ProvisionRequest{Key: ExternalKey{Issuer: issuer, Provider: ProviderGitHub, Subject: "gh-owned-" + uuid.NewString()},
		Email: uniqueEmailCom("gh-owned"), Actor: SelfSignUp(nil)})
	if err != nil {
		t.Fatalf("social ProvisionFederated: %v", err)
	}
	if got := e.publicProvider(t, ctx, social); got != nil {
		t.Fatalf("social account sso_provider_id = %v, want NULL", *got)
	}
	f := e.relinkFixture(t, ctx, "oauth")
	subject := "oauth-owned-" + uuid.NewString()
	oauthType, err := e.owned.ProvisionFederated(ctx, ProvisionRequest{Key: ExternalKey{Issuer: "https://github.com", Provider: f.provider, Subject: subject},
		Email: uniqueEmailCom("oauth-owned"), Actor: SelfSignUp(nil), LegacyProvider: "github", LegacySubject: subject})
	if err != nil {
		t.Fatalf("OAuth-type ProvisionFederated: %v", err)
	}
	if got := e.publicProvider(t, ctx, oauthType); got != nil {
		t.Fatalf("OAuth-type account sso_provider_id = %v, want NULL", *got)
	}
}

// TestOwnedSSOProvisioningRefusesAProviderRowOfAnotherProtocol: an OIDC or SAML account is bound only to a provider
// row of its own protocol. A key whose provider row is of another protocol, or does not exist, is refused with
// ErrLegacyProjection and nothing is written.
func TestOwnedSSOProvisioningRefusesAProviderRowOfAnotherProtocol(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for _, protocol := range []string{"oidc", "saml"} {
		otherProtocol := map[string]string{"oidc": "saml", "saml": "oidc"}[protocol]
		for name, provider := range map[string]func(f relinkFixture) Provider{
			"row of another protocol": func(f relinkFixture) Provider { return e.seedSSOProvider(t, ctx, f.org, otherProtocol) },
			"no such row":             func(relinkFixture) Provider { return Provider(uuid.NewString()) },
		} {
			t.Run(protocol+"/"+name, func(t *testing.T) {
				f := e.relinkFixture(t, ctx, protocol)
				f.provider = provider(f)
				req := ssoProvisionRequest(f, protocol+"-crossed-"+uuid.NewString())
				before := e.writeCounts(t, ctx)
				if id, err := e.owned.ProvisionFederated(ctx, req); !errors.Is(err, ErrLegacyProjection) {
					t.Fatalf("ProvisionFederated at a %s key row = %v, %v; want ErrLegacyProjection", name, id, err)
				}
				if after := e.writeCounts(t, ctx); after != before {
					t.Fatalf("a refused provisioning wrote rows: before %v, after %v", before, after)
				}
				if n := e.identityRows(t, ctx, req.Key.Subject); n != 0 {
					t.Fatalf("identity rows = %d, want none", n)
				}
			})
		}
	}
}

// TestOwnedSSOProvisioningOfASubjectHeldAtTheProviderRowIsIdentityTaken: a go-api account bound to the provider row
// already holds the subject there; a provisioning of the same (provider row, subject) with another email is
// ErrIdentityTaken (not ErrEmailTaken), and nothing is written.
func TestOwnedSSOProvisioningOfASubjectHeldAtTheProviderRowIsIdentityTaken(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for _, protocol := range []string{"oidc", "saml"} {
		t.Run(protocol, func(t *testing.T) {
			f := e.relinkFixture(t, ctx, protocol)
			raw := protocol + "-held-" + uuid.NewString()
			e.seedBound(t, ctx, protocol, raw, &f.provider, f.org, false)
			before := e.writeCounts(t, ctx)
			if id, err := e.owned.ProvisionFederated(ctx, ssoProvisionRequest(f, raw)); !errors.Is(err, ErrIdentityTaken) {
				t.Fatalf("ProvisionFederated of a subject held at the provider row = %v, %v; want ErrIdentityTaken", id, err)
			}
			if after := e.writeCounts(t, ctx); after != before {
				t.Fatalf("a refused provisioning wrote rows: before %v, after %v", before, after)
			}
		})
	}
}

// writeCounts counts the rows a provisioning writes: auth principals, auth users, identities, public users, the
// provisioning audit rows, and the auth outbox events.
func (e *env) writeCounts(t *testing.T, ctx context.Context) [6]int {
	t.Helper()
	return [6]int{
		e.count(t, ctx, `SELECT count(*) FROM auth.principals`),
		e.count(t, ctx, `SELECT count(*) FROM auth.users`),
		e.count(t, ctx, `SELECT count(*) FROM auth.external_identities`),
		e.count(t, ctx, `SELECT count(*) FROM public.users`),
		e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.principal.provisioned'`),
		e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events`),
	}
}
