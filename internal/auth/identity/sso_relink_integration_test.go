//go:build integration

package identity

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
)

// seedBound inserts an OIDC or SAML account as go-api writes it since revision 0155: auth_provider, the stored
// subject and the provider row it is bound to (nil = unbound). The auth principal and user rows are written only
// when backfilled is set, as the users backfill does before the users cutover.
func (e *env) seedBound(t *testing.T, ctx context.Context, protocol, subject string, bound *Provider, org uuid.UUID, backfilled bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	var providerRow *uuid.UUID
	if bound != nil {
		row := uuid.MustParse(string(*bound))
		providerRow = &row
	}
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, auth_provider, auth_provider_id, sso_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, true, true, false, 0, now(), now())`, id, uniqueEmail(protocol), protocol, subject, providerRow); err != nil {
		t.Fatalf("seed bound public.users: %v", err)
	}
	e.seedMembership(t, ctx, id, org)
	if backfilled {
		e.project(t, ctx, id)
	}
	return id
}

type relinkFixture struct {
	org      uuid.UUID
	provider Provider
	issuer   string
	protocol string
}

func (e *env) relinkFixture(t *testing.T, ctx context.Context, protocol string) relinkFixture {
	t.Helper()
	org := e.seedOrg(t, ctx)
	return relinkFixture{org: org, provider: e.seedSSOProvider(t, ctx, org, protocol),
		issuer: "https://idp.example.com/" + uuid.NewString()[:8], protocol: protocol}
}

// key and legacy are what the OIDC and SAML flows pass for a sign-in of raw (oidc/service.go resolve,
// saml/service.go ConsumeFor): SAML keys the identity by the composite subject and matches go-api's raw NameID.
func (f relinkFixture) key(raw string) (ExternalKey, *LegacyMatch) {
	org := f.org
	if f.protocol == "saml" {
		return ExternalKey{Issuer: f.issuer, Provider: f.provider, Subject: "saml1|urn:oasis:names:tc:SAML:2.0:nameid-format:persistent||" + raw},
			&LegacyMatch{AuthProvider: "saml", MembershipOrg: &org, Subject: raw}
	}
	return ExternalKey{Issuer: f.issuer, Provider: f.provider, Subject: raw}, &LegacyMatch{AuthProvider: "oidc", MembershipOrg: &org}
}

func (e *env) principals(t *testing.T, ctx context.Context) int {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM auth.principals`)
}

// TestOwnedSSOSignInRelinksTheBackfilledPrincipal: after the users cutover, the first sign-in of a backfilled OIDC or
// SAML account bound to the provider row links the key to that account, whatever e-mail the IdP sends now. Before
// the fix the key missed and the flow provisioned a second principal (a fork).
func TestOwnedSSOSignInRelinksTheBackfilledPrincipal(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for _, protocol := range []string{"oidc", "saml"} {
		t.Run(protocol, func(t *testing.T) {
			f := e.relinkFixture(t, ctx, protocol)
			raw := protocol + "-relink-" + uuid.NewString()
			user := e.seedBound(t, ctx, protocol, raw, &f.provider, f.org, true)
			key, legacy := f.key(raw)
			before := e.principals(t, ctx)
			publicBefore := e.count(t, ctx, `SELECT count(*) FROM public.users`)

			p, found, err := e.owned.IdentityBySubject(ctx, key.Issuer, key.Provider, key.Subject, legacy)
			if err == nil && !found {
				forked, perr := e.owned.ProvisionFederated(ctx, ProvisionRequest{Key: key, Email: "changed-at-idp-" + uuid.NewString()[:8] + "@example.com",
					Actor: SelfSignUp(nil), LegacyProvider: protocol, LegacySubject: raw})
				t.Fatalf("owned sign-in of a backfilled %s account missed; the flow then provisioned principal %v (%v) beside %v: a fork",
					protocol, forked, perr, user)
			}
			if err != nil || p.ID != user || !p.IsActive {
				t.Fatalf("owned sign-in = %+v, %v, %v; want the backfilled account %v", p, found, err, user)
			}
			if n := e.count(t, ctx, `SELECT count(*) FROM auth.external_identities WHERE principal_id = $1 AND issuer = $2 AND provider = $3 AND subject = $4`,
				user, key.Issuer, string(key.Provider), key.Subject); n != 1 {
				t.Fatalf("identity rows for the key = %d, want 1", n)
			}
			if got := e.principals(t, ctx); got != before {
				t.Fatalf("principals %d -> %d: the re-link created a principal", before, got)
			}
			if got := e.count(t, ctx, `SELECT count(*) FROM public.users`); got != publicBefore {
				t.Fatalf("public.users rows %d -> %d: the re-link wrote public.users", publicBefore, got)
			}
			if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.identity.relinked'
AND subject_principal_id::text = $1 AND attributes->>'reason_code' = 'identity_relinked' AND NOT attributes ? 'provider'
AND attributes->>'target_kind' = 'external_identity'`, user.String()); n != 1 {
				t.Fatalf("re-link audit rows = %d, want 1", n)
			}
			if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'external_identity.linked'
AND aggregate_type = 'external_identity' AND payload->'change'->>'principal_id' = 'prn_' || $1`, user.String()); n != 1 {
				t.Fatalf("re-link outbox events = %d, want 1", n)
			}
			// The second sign-in hits the key and writes nothing.
			if p, found, err := e.owned.IdentityBySubject(ctx, key.Issuer, key.Provider, key.Subject, legacy); err != nil || !found || p.ID != user {
				t.Fatalf("second sign-in = %+v, %v, %v", p, found, err)
			}
			if n := e.auditRows(t, ctx, "auth.identity.relinked", user.String()); n != 1 {
				t.Fatalf("a hit wrote re-link audit rows: %d", n)
			}
		})
	}
}

// TestOwnedSSOSignInRefusesAnUnboundCarrier: a public account that carries the subject for this protocol but cannot
// be re-linked (unbound, bound to a row of another protocol, or never backfilled) refuses the sign-in as an
// ambiguous identity. It is never a miss, so the flow never provisions a second principal for it.
func TestOwnedSSOSignInRefusesAnUnboundCarrier(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for _, protocol := range []string{"oidc", "saml"} {
		t.Run(protocol, func(t *testing.T) {
			f := e.relinkFixture(t, ctx, protocol)
			otherProtocol := "saml"
			if protocol == "saml" {
				otherProtocol = "oidc"
			}
			wrongRow := e.seedSSOProvider(t, ctx, f.org, otherProtocol)
			for name, seed := range map[string]func(raw string) uuid.UUID{
				"unbound": func(raw string) uuid.UUID { return e.seedBound(t, ctx, protocol, raw, nil, f.org, true) },
				"bound to the key's row, never backfilled": func(raw string) uuid.UUID { return e.seedBound(t, ctx, protocol, raw, &f.provider, f.org, false) },
				"bound to a row of another protocol":       func(raw string) uuid.UUID { return e.seedBound(t, ctx, protocol, raw, &wrongRow, f.org, true) },
			} {
				raw := protocol + "-refuse-" + uuid.NewString()
				seed(raw)
				key, legacy := f.key(raw)
				before := e.principals(t, ctx)
				p, found, err := e.owned.IdentityBySubject(ctx, key.Issuer, key.Provider, key.Subject, legacy)
				if !errors.Is(err, ErrMultipleUsers) || found {
					t.Errorf("%s: owned sign-in = %+v, %v, %v; want ErrMultipleUsers (an ambiguous identity), never a miss", name, p, found, err)
				}
				if n := e.count(t, ctx, `SELECT count(*) FROM auth.external_identities WHERE subject = $1`, key.Subject); n != 0 {
					t.Errorf("%s: identity rows = %d, want none", name, n)
				}
				if got := e.principals(t, ctx); got != before {
					t.Errorf("%s: principals %d -> %d", name, before, got)
				}
			}
		})
	}
}

// TestOwnedSSOSignInIgnoresARowBoundToAnotherProvider (acp narrowing): the same subject on a row bound to another
// provider row of the same protocol is that provider's identity; a sign-in at this provider is an ordinary miss, so
// a new user of this provider is not blocked.
func TestOwnedSSOSignInIgnoresARowBoundToAnotherProvider(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for _, protocol := range []string{"oidc", "saml"} {
		t.Run(protocol, func(t *testing.T) {
			f := e.relinkFixture(t, ctx, protocol)
			other := e.seedSSOProvider(t, ctx, f.org, protocol)
			raw := protocol + "-elsewhere-" + uuid.NewString()
			e.seedBound(t, ctx, protocol, raw, &other, f.org, true)
			key, legacy := f.key(raw)
			if p, found, err := e.owned.IdentityBySubject(ctx, key.Issuer, key.Provider, key.Subject, legacy); err != nil || found {
				t.Fatalf("sign-in at P1 with the subject of a P2-bound row = %+v, %v, %v; want a miss", p, found, err)
			}
			if n := e.count(t, ctx, `SELECT count(*) FROM auth.external_identities WHERE subject = $1`, key.Subject); n != 0 {
				t.Fatalf("identity rows = %d, want none", n)
			}
		})
	}
}

// TestOwnedSSORelinkKeepsOneIdentityPerProvider: an account that already holds another subject at the provider row
// is not re-linked to a second one.
func TestOwnedSSORelinkKeepsOneIdentityPerProvider(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	f := e.relinkFixture(t, ctx, "oidc")
	raw := "oidc-second-" + uuid.NewString()
	user := e.seedBound(t, ctx, "oidc", raw, &f.provider, f.org, true)
	held := ExternalKey{Issuer: f.issuer, Provider: f.provider, Subject: "oidc-first-" + uuid.NewString()}
	if _, err := e.owned.LinkIdentity(ctx, held, user, Actor{}); err != nil {
		t.Fatal(err)
	}
	key, legacy := f.key(raw)
	p, found, err := e.owned.IdentityBySubject(ctx, key.Issuer, key.Provider, key.Subject, legacy)
	if !errors.Is(err, ErrProviderAlreadyLinked) || found || p.ID != user {
		t.Fatalf("re-link beside a held subject = %+v, %v, %v; want ErrProviderAlreadyLinked naming %v", p, found, err, user)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.external_identities WHERE principal_id = $1`, user); n != 1 {
		t.Fatalf("identity rows = %d, want only the held one", n)
	}
}

// TestSwitchOffLegacyFallbackIsUnchangedForABoundRow: before the cutover a bound account outside the provider
// organization is still ErrLegacyNotMember (the legacy fallback), and the same row is re-linked only by the owned
// store, which leaves the membership to the flow.
func TestSwitchOffLegacyFallbackIsUnchangedForABoundRow(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	f := e.relinkFixture(t, ctx, "oidc")
	elsewhere := e.seedOrg(t, ctx)
	raw := "oidc-off-" + uuid.NewString()
	user := e.seedBound(t, ctx, "oidc", raw, &f.provider, elsewhere, true)
	key, legacy := f.key(raw)
	if p, found, err := e.store.IdentityBySubject(ctx, key.Issuer, key.Provider, key.Subject, legacy); !errors.Is(err, ErrLegacyNotMember) || found {
		t.Fatalf("switch off = %+v, %v, %v; want ErrLegacyNotMember", p, found, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.external_identities WHERE principal_id = $1`, user); n != 0 {
		t.Fatalf("the switch-off path linked %d identities", n)
	}
	if p, found, err := e.owned.IdentityBySubject(ctx, key.Issuer, key.Provider, key.Subject, legacy); err != nil || !found || p.ID != user {
		t.Fatalf("owned = %+v, %v, %v; want the re-linked account %v", p, found, err, user)
	}
}

// TestOwnedSSOSignInOfAnOAuthTypeLoginIsUnchanged: an OAuth-type login (legacy provider github/gitlab/google) is not
// re-linked by this rule; it stays a miss under the cutover.
func TestOwnedSSOSignInOfAnOAuthTypeLoginIsUnchanged(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	f := e.relinkFixture(t, ctx, "oauth")
	subject := "oauth-" + uuid.NewString()
	user := e.seedLegacy(t, ctx, uniqueEmail("oauth"), "github", subject, false)
	e.seedMembership(t, ctx, user, f.org)
	e.project(t, ctx, user)
	org := f.org
	if p, found, err := e.owned.IdentityBySubject(ctx, "https://github.com", f.provider, subject, &LegacyMatch{AuthProvider: "github", MembershipOrg: &org}); err != nil || found {
		t.Fatalf("owned OAuth-type sign-in = %+v, %v, %v; want a miss (not in this rule)", p, found, err)
	}
}

func TestRaceOwnedSSORelinkOfOneSubjectWritesOneAuditRow(t *testing.T) {
	if raceEnabled {
		t.Logf("%s %s", raceMarker, t.Name())
	}
	e := getEnv(t)
	ctx := context.Background()
	f := e.relinkFixture(t, ctx, "saml")
	raw := "saml-race-" + uuid.NewString()
	user := e.seedBound(t, ctx, "saml", raw, &f.provider, f.org, true)
	key, legacy := f.key(raw)
	errs := make([]error, 8)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			p, found, err := e.owned.IdentityBySubject(ctx, key.Issuer, key.Provider, key.Subject, legacy)
			if err == nil && (!found || p.ID != user) {
				err = fmt.Errorf("found %v principal %v", found, p.ID)
			}
			errs[i] = err
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("sign-in %d: %v", i, err)
		}
	}
	if n := e.auditRows(t, ctx, "auth.identity.relinked", user.String()); n != 1 {
		t.Fatalf("concurrent re-links wrote %d audit rows, want 1", n)
	}
}
