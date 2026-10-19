//go:build integration

package social

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

func github(subject string) identity.ExternalKey {
	return identity.ExternalKey{Issuer: SocialIssuer(GitHub), Provider: GitHub, Subject: subject}
}

func key(p Provider, subject string) identity.ExternalKey {
	return identity.ExternalKey{Issuer: SocialIssuer(p), Provider: p, Subject: subject}
}

func (e *env) signedIn(principal uuid.UUID, age time.Duration) SignedIn {
	return SignedIn{PrincipalID: principal, AuthTime: e.now.Add(-age)}
}

func wantConflict(t *testing.T, err error, method string) {
	t.Helper()
	var conflict *ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("err = %v, want a conflict", err)
	}
	if conflict.ExistingMethod != method {
		t.Fatalf("existing method = %q, want %q", conflict.ExistingMethod, method)
	}
}

func TestProviderEmailCollisionIsAConflict(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, ctx)
	// A projected account carries no password verifier (the projection never
	// copies one), so after the cutover its method is unknown ("").
	for _, owns := range []bool{false, true} {
		svc := e.service(t, owns)
		want := map[bool]string{false: "local", true: ""}[owns]
		t.Run(fmt.Sprintf("local password account, users owned by auth %v", owns), func(t *testing.T) {
			email := fmt.Sprintf("local-%v@example.com", owns)
			holder := e.localUser(t, ctx, email)
			token := e.providers.token(t.Name(), account{provider: GitHub, id: 100, email: strings.ToUpper(email), verified: true, public: true})
			before := e.state(t, ctx)
			res, err := svc.Exchange(ctx, GitHub, token, nil)
			wantConflict(t, err, want)
			if res.PrincipalID == holder {
				t.Fatal("signed in to the existing account")
			}
			e.unchanged(t, ctx, before)
		})
	}
	// Before the users cutover the accounts are go-api's (public.users);
	// after it they are auth-service's, every one projected by the backfill.
	for _, owns := range []bool{false, true} {
		svc := e.service(t, owns)
		seed := func(t *testing.T, email string, provider Provider, providerID string) uuid.UUID {
			id := e.goAPIUser(t, ctx, email, provider, providerID, true)
			if owns {
				if err := e.store(false).Project(ctx, id); err != nil {
					t.Fatalf("Project: %v", err)
				}
			}
			return id
		}
		method := func(beforeCutover, afterCutover string) string {
			if owns {
				return afterCutover
			}
			return beforeCutover
		}
		t.Run(fmt.Sprintf("password account, users owned by auth %v", owns), func(t *testing.T) {
			email := fmt.Sprintf("goapi-local-%v@example.com", owns)
			holder := seed(t, email, "", "")
			token := e.providers.token(t.Name(), account{provider: Google, googleID: "g-1", email: email, verified: true})
			before := e.state(t, ctx)
			res, err := svc.Exchange(ctx, Google, token, nil)
			wantConflict(t, err, method("local", ""))
			if res.PrincipalID == holder {
				t.Fatal("signed in to the existing account")
			}
			e.unchanged(t, ctx, before)
		})
		t.Run(fmt.Sprintf("second subject at the same provider, users owned by auth %v", owns), func(t *testing.T) {
			email := fmt.Sprintf("same-provider-%v@example.com", owns)
			holder := seed(t, email, GitHub, fmt.Sprintf("200%v", owns))
			subject := fmt.Sprintf("9002%d", map[bool]int{false: 0, true: 1}[owns])
			id := int64(90020)
			if owns {
				id = 90021
			}
			token := e.providers.token(t.Name(), account{provider: GitHub, id: id, email: email, verified: true, public: true})
			before := e.state(t, ctx)
			res, err := svc.Exchange(ctx, GitHub, token, nil)
			wantConflict(t, err, method("github", ""))
			if res.PrincipalID == holder {
				t.Fatal("signed in to the existing account")
			}
			e.unchanged(t, ctx, before)
			if owner, ok := e.identityOwner(t, ctx, github(subject)); ok {
				t.Fatalf("the second subject was bound to %v", owner)
			}
		})
		t.Run(fmt.Sprintf("other provider, users owned by auth %v", owns), func(t *testing.T) {
			email := fmt.Sprintf("other-provider-%v@example.com", owns)
			seed(t, email, GitLab, fmt.Sprintf("300%v", owns))
			id := int64(3010)
			if owns {
				id = 3011
			}
			token := e.providers.token(t.Name(), account{provider: GitHub, id: id, email: email, verified: true, public: true})
			before := e.state(t, ctx)
			_, err := svc.Exchange(ctx, GitHub, token, nil)
			wantConflict(t, err, method("gitlab", ""))
			e.unchanged(t, ctx, before)
		})
	}
	t.Run("after the cutover the conflict names a method to sign in with", func(t *testing.T) {
		owned := e.service(t, true)
		projected := func(t *testing.T, email string) uuid.UUID {
			id := e.goAPIUser(t, ctx, email, GitLab, "proj-"+email, true)
			if err := e.store(false).Project(ctx, id); err != nil {
				t.Fatalf("Project: %v", err)
			}
			return id
		}
		cases := []struct {
			name  string
			setup func(t *testing.T, id uuid.UUID)
			want  string
		}{
			{"a password", func(t *testing.T, id uuid.UUID) {
				e.guarded(t, ctx, `UPDATE auth.users SET password_hash = 'not-a-real-verifier', password_algorithm = 'bcrypt' WHERE principal_id = $1`, id)
			}, "local"},
			{"the oldest linked identity", func(t *testing.T, id uuid.UUID) {
				e.guarded(t, ctx, `INSERT INTO auth.external_identities (principal_id, issuer, provider, subject, created_at)
					VALUES ($1, $2, 'google', $3, now() - interval '1 day')`, id, SocialIssuer(Google), "old-"+id.String())
				e.guarded(t, ctx, `INSERT INTO auth.external_identities (principal_id, issuer, provider, subject, created_at)
					VALUES ($1, $2, 'gitlab', $3, now())`, id, SocialIssuer(GitLab), "new-"+id.String())
			}, "google"},
			{"no method", func(*testing.T, uuid.UUID) {}, ""},
		}
		for i, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				email := fmt.Sprintf("method-%d@example.com", i)
				holder := projected(t, email)
				c.setup(t, holder)
				token := e.providers.token(t.Name(), account{provider: GitHub, id: int64(5000 + i), email: email, verified: true, public: true})
				before := e.state(t, ctx)
				res, err := owned.Exchange(ctx, GitHub, token, nil)
				wantConflict(t, err, c.want)
				if res.PrincipalID == holder {
					t.Fatal("signed in to the existing account")
				}
				e.unchanged(t, ctx, before)
			})
		}
	})
	svc := e.service(t, false)
	t.Run("the conflict response carries nothing to redeem", func(t *testing.T) {
		e.localUser(t, ctx, "redeem@example.com")
		token := e.providers.token(t.Name(), account{provider: GitHub, id: 400, email: "redeem@example.com", verified: true, public: true})
		h := Handlers{Service: svc}
		for i := 0; i < 2; i++ {
			body, _ := json.Marshal(map[string]string{"provider": "github", "provider_access_token": token.Reveal()})
			rec := httptest.NewRecorder()
			h.Exchange(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(body))))
			if rec.Code != http.StatusConflict {
				t.Fatalf("attempt %d: status %d, want 409: %s", i, rec.Code, rec.Body)
			}
			var got map[string]map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || len(got["detail"]) != 2 || got["detail"]["existing_provider"] != "local" {
				t.Fatalf("attempt %d: body %s", i, rec.Body)
			}
		}
	})
}

func TestUnverifiedProviderEmailIsRefused(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, ctx)
	svc := e.service(t, true)
	cases := []struct {
		name string
		acct account
	}{
		{"gitlab unconfirmed", account{provider: GitLab, id: 500, verified: false}},
		{"google unverified", account{provider: Google, googleID: "g-500", verified: false}},
		{"github no verified address", account{provider: GitHub, id: 500, verified: false}},
	}
	for _, c := range cases {
		t.Run(c.name+" matching an existing account", func(t *testing.T) {
			email := strings.ReplaceAll(c.name, " ", "-") + "@example.com"
			holder := e.localUser(t, ctx, email)
			c.acct.email = email
			before := e.state(t, ctx)
			res, err := svc.Exchange(ctx, c.acct.provider, e.providers.token(t.Name(), c.acct), nil)
			if !errors.Is(err, ErrEmailUnverified) {
				t.Fatalf("err = %v, want ErrEmailUnverified", err)
			}
			if res.PrincipalID == holder {
				t.Fatal("signed in to the existing account")
			}
			e.unchanged(t, ctx, before)
		})
		t.Run(c.name+" with no account", func(t *testing.T) {
			c.acct.email = "new-" + strings.ReplaceAll(c.name, " ", "-") + "@example.com"
			c.acct.id++
			c.acct.googleID += "-new"
			before := e.state(t, ctx)
			if _, err := svc.Exchange(ctx, c.acct.provider, e.providers.token(t.Name(), c.acct), nil); !errors.Is(err, ErrEmailUnverified) {
				t.Fatalf("err = %v, want ErrEmailUnverified", err)
			}
			e.unchanged(t, ctx, before)
		})
	}
}

func TestSubjectIsScopedByIssuer(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, ctx)
	svc := e.service(t, true)
	holder := e.localUser(t, ctx, "scoped@example.com")
	e.linked(t, ctx, holder, identity.ExternalKey{Issuer: "https://github.example", Provider: GitHub, Subject: "700"})
	e.linked(t, ctx, holder, key(GitLab, "701"))

	t.Run("same subject under another issuer", func(t *testing.T) {
		token := e.providers.token(t.Name(), account{provider: GitHub, id: 700, email: "scoped-a@example.com", verified: true, public: true})
		res, err := svc.Exchange(ctx, GitHub, token, nil)
		if err != nil {
			t.Fatalf("Exchange: %v", err)
		}
		if res.PrincipalID == holder || !res.Created {
			t.Fatalf("result %+v: want a new principal, not %v", res, holder)
		}
	})
	t.Run("same subject at another provider", func(t *testing.T) {
		token := e.providers.token(t.Name(), account{provider: GitHub, id: 701, email: "scoped-b@example.com", verified: true, public: true})
		res, err := svc.Exchange(ctx, GitHub, token, nil)
		if err != nil {
			t.Fatalf("Exchange: %v", err)
		}
		if res.PrincipalID == holder {
			t.Fatal("signed in to the principal of another provider's subject")
		}
	})
	t.Run("the bound identity signs in", func(t *testing.T) {
		token := e.providers.token(t.Name(), account{provider: GitLab, id: 701, email: "anything@example.com", verified: false})
		res, err := svc.Exchange(ctx, GitLab, token, nil)
		if err != nil || res.PrincipalID != holder || res.Created {
			t.Fatalf("Exchange = %+v, %v; want %v", res, err, holder)
		}
	})
}

func TestLinkRequiresExplicitAuditedAction(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, ctx)
	svc := e.service(t, false)
	owner := e.localUser(t, ctx, "owner@example.com")
	token := e.providers.token("owner-github", account{provider: GitHub, id: 800, email: "owner@example.com", verified: true, public: true})

	t.Run("an exchange alone never links", func(t *testing.T) {
		before := e.state(t, ctx)
		_, err := svc.Exchange(ctx, GitHub, token, nil)
		wantConflict(t, err, "local")
		e.unchanged(t, ctx, before)
	})
	t.Run("a stale sign-in cannot link", func(t *testing.T) {
		before := e.state(t, ctx)
		if _, err := svc.Link(ctx, e.signedIn(owner, 11*time.Minute), GitHub, token); !errors.Is(err, ErrReauthenticationRequired) {
			t.Fatalf("err = %v, want ErrReauthenticationRequired", err)
		}
		if _, err := svc.Link(ctx, SignedIn{AuthTime: e.now}, GitHub, token); !errors.Is(err, ErrReauthenticationRequired) {
			t.Fatalf("no principal: err = %v, want ErrReauthenticationRequired", err)
		}
		e.unchanged(t, ctx, before)
	})
	t.Run("an explicit link writes the identity, its audit row and its event together", func(t *testing.T) {
		before := e.state(t, ctx)
		linked, err := svc.Link(ctx, e.signedIn(owner, time.Minute), GitHub, token)
		if err != nil {
			t.Fatalf("Link: %v", err)
		}
		after := e.state(t, ctx)
		want := before
		want.identities, want.audits, want.outbox = before.identities+1, before.audits+1, before.outbox+1
		if after != want {
			t.Fatalf("rows after link %+v, want %+v", after, want)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events
			WHERE event_type = 'auth.identity.linked' AND actor_principal_id = $1 AND subject_principal_id = $1
			AND attributes->>'target_id' = $2 AND attributes->>'provider' = 'github'`, owner.String(), linked.ID.String()); n != 1 {
			t.Fatalf("audit rows for the link = %d, want 1", n)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'external_identity.linked' AND aggregate_id = $1`,
			linked.ID.String()); n != 1 {
			t.Fatalf("outbox rows for the link = %d, want 1", n)
		}
		res, err := svc.Exchange(ctx, GitHub, token, nil)
		if err != nil || res.PrincipalID != owner {
			t.Fatalf("Exchange after link = %+v, %v; want %v", res, err, owner)
		}
		again, err := svc.Link(ctx, e.signedIn(owner, time.Minute), GitHub, token)
		if err != nil || again.ID != linked.ID {
			t.Fatalf("relink = %+v, %v; want the same identity", again, err)
		}
	})
	t.Run("an identity bound to another principal is a conflict", func(t *testing.T) {
		other := e.localUser(t, ctx, "other@example.com")
		before := e.state(t, ctx)
		if _, err := svc.Link(ctx, e.signedIn(other, time.Minute), GitHub, token); !errors.Is(err, identity.ErrIdentityConflict) {
			t.Fatalf("err = %v, want ErrIdentityConflict", err)
		}
		e.unchanged(t, ctx, before)
		if got, _ := e.identityOwner(t, ctx, github("800")); got != owner {
			t.Fatalf("identity owner = %v, want %v", got, owner)
		}
	})
	t.Run("an identity held by a go-api account stays with it", func(t *testing.T) {
		legacy := e.goAPIUser(t, ctx, "legacy-link@example.com", GitHub, "801", true)
		other := e.localUser(t, ctx, "taker@example.com")
		legacyToken := e.providers.token(t.Name(), account{provider: GitHub, id: 801, email: "legacy-link@example.com", verified: true, public: true})
		if _, err := svc.Link(ctx, e.signedIn(other, time.Minute), GitHub, legacyToken); !errors.Is(err, identity.ErrIdentityConflict) {
			t.Fatalf("err = %v, want ErrIdentityConflict", err)
		}
		if got, _ := e.identityOwner(t, ctx, github("801")); got != legacy {
			t.Fatalf("identity owner = %v, want the go-api account %v", got, legacy)
		}
	})
	t.Run("a second identity at the same provider is refused", func(t *testing.T) {
		second := e.providers.token(t.Name(), account{provider: GitHub, id: 802, email: "owner2@example.com", verified: true, public: true})
		before := e.state(t, ctx)
		if _, err := svc.Link(ctx, e.signedIn(owner, time.Minute), GitHub, second); !errors.Is(err, ErrProviderLinked) {
			t.Fatalf("err = %v, want ErrProviderLinked", err)
		}
		e.unchanged(t, ctx, before)
	})
}

func TestReplayedExchangeIsRefused(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, ctx)
	svc := e.service(t, true)
	holder := e.localUser(t, ctx, "holder@example.com")
	e.linked(t, ctx, holder, github("900"))
	e.linked(t, ctx, holder, key(GitLab, "900"))
	e.linked(t, ctx, holder, key(Google, "g-900"))

	otherClient := []struct {
		name string
		acct account
	}{
		{"github token of another client", account{provider: GitHub, id: 900, email: "holder@example.com", verified: true, public: true, clientID: otherClientID}},
		{"gitlab token of another client", account{provider: GitLab, id: 900, email: "holder@example.com", verified: true, clientID: otherClientID}},
		{"google token of another client", account{provider: Google, googleID: "g-900", email: "holder@example.com", verified: true, clientID: otherClientID}},
	}
	for _, c := range otherClient {
		t.Run(c.name, func(t *testing.T) {
			before := e.state(t, ctx)
			res, err := svc.Exchange(ctx, c.acct.provider, e.providers.token(t.Name(), c.acct), nil)
			if !errors.Is(err, ErrProviderRefused) {
				t.Fatalf("err = %v, want ErrProviderRefused", err)
			}
			if res.PrincipalID == holder {
				t.Fatal("signed in with a token issued to another client")
			}
			e.unchanged(t, ctx, before)
		})
	}
	for _, p := range Providers() {
		t.Run(string(p)+" token whose profile is another account", func(t *testing.T) {
			mixed := account{provider: p, id: 901, googleID: "g-901", email: "other-account@example.com", verified: true, public: true, profileOf: t.Name() + "-holder"}
			e.providers.token(t.Name()+"-holder", account{provider: p, id: 900, googleID: "g-900", email: "holder@example.com", verified: true, public: true})
			before := e.state(t, ctx)
			res, err := svc.Exchange(ctx, p, e.providers.token(t.Name(), mixed), nil)
			if !errors.Is(err, ErrProviderRefused) {
				t.Fatalf("err = %v, want ErrProviderRefused", err)
			}
			if res.PrincipalID == holder {
				t.Fatal("signed in to the profile's account with another account's token")
			}
			e.unchanged(t, ctx, before)
		})
	}
	t.Run("a link replayed by a second principal is a conflict", func(t *testing.T) {
		first := e.localUser(t, ctx, "first@example.com")
		second := e.localUser(t, ctx, "second@example.com")
		token := e.providers.token(t.Name(), account{provider: GitHub, id: 902, email: "first@example.com", verified: true, public: true})
		if _, err := svc.Link(ctx, e.signedIn(first, time.Minute), GitHub, token); err != nil {
			t.Fatalf("Link: %v", err)
		}
		before := e.state(t, ctx)
		if _, err := svc.Link(ctx, e.signedIn(second, time.Minute), GitHub, token); !errors.Is(err, identity.ErrIdentityConflict) {
			t.Fatalf("err = %v, want ErrIdentityConflict", err)
		}
		e.unchanged(t, ctx, before)
	})
}

func TestUnlinkIsExplicitAndAudited(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, ctx)
	svc := e.service(t, false)
	owner := e.localUser(t, ctx, "unlink@example.com")
	token := e.providers.token("unlink", account{provider: GitHub, id: 1000, email: "unlink@example.com", verified: true, public: true})
	linked, err := svc.Link(ctx, e.signedIn(owner, time.Minute), GitHub, token)
	if err != nil {
		t.Fatalf("Link: %v", err)
	}

	t.Run("another principal cannot unlink it", func(t *testing.T) {
		other := e.localUser(t, ctx, "not-owner@example.com")
		before := e.state(t, ctx)
		if err := svc.Unlink(ctx, e.signedIn(other, time.Minute), linked.ID); !errors.Is(err, identity.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
		e.unchanged(t, ctx, before)
	})
	t.Run("a stale sign-in cannot unlink", func(t *testing.T) {
		before := e.state(t, ctx)
		if err := svc.Unlink(ctx, e.signedIn(owner, time.Hour), linked.ID); !errors.Is(err, ErrReauthenticationRequired) {
			t.Fatalf("err = %v, want ErrReauthenticationRequired", err)
		}
		e.unchanged(t, ctx, before)
	})
	t.Run("the owner unlinks it with an audit row", func(t *testing.T) {
		before := e.state(t, ctx)
		if err := svc.Unlink(ctx, e.signedIn(owner, time.Minute), linked.ID); err != nil {
			t.Fatalf("Unlink: %v", err)
		}
		after := e.state(t, ctx)
		if after.identities != before.identities-1 || after.audits != before.audits+1 || after.outbox != before.outbox+1 {
			t.Fatalf("rows before %+v, after %+v", before, after)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events
			WHERE event_type = 'auth.identity.unlinked' AND actor_principal_id = $1 AND attributes->>'target_id' = $2`,
			owner.String(), linked.ID.String()); n != 1 {
			t.Fatalf("unlink audit rows = %d, want 1", n)
		}
		if _, err := svc.Exchange(ctx, GitHub, token, nil); !errors.Is(err, ErrAccountConflict) {
			t.Fatalf("Exchange after unlink: err = %v, want a conflict", err)
		}
	})
	t.Run("the last sign-in method stays", func(t *testing.T) {
		owns := e.service(t, true)
		only := e.providers.token(t.Name(), account{provider: Google, googleID: "g-1001", email: "only@example.com", verified: true})
		res, err := owns.Exchange(ctx, Google, only, nil)
		if err != nil || !res.Created {
			t.Fatalf("Exchange = %+v, %v; want a new account", res, err)
		}
		identities, err := e.store(true).ListIdentities(ctx, res.PrincipalID)
		if err != nil || len(identities) != 1 {
			t.Fatalf("identities = %v, %v", identities, err)
		}
		before := e.state(t, ctx)
		if err := owns.Unlink(ctx, e.signedIn(res.PrincipalID, time.Minute), identities[0].ID); !errors.Is(err, identity.ErrLastSignInMethod) {
			t.Fatalf("err = %v, want ErrLastSignInMethod", err)
		}
		e.unchanged(t, ctx, before)
	})
}

func TestDisabledPrincipalIsRefused(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, ctx)
	svc := e.service(t, true)
	holder := e.localUser(t, ctx, "disabled@example.com")
	e.linked(t, ctx, holder, github("1100"))
	e.guarded(t, ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, holder.String())
	token := e.providers.token("disabled", account{provider: GitHub, id: 1100, email: "disabled@example.com", verified: true, public: true})
	if _, err := svc.Exchange(ctx, GitHub, token, nil); !errors.Is(err, ErrPrincipalDisabled) {
		t.Fatalf("err = %v, want ErrPrincipalDisabled", err)
	}
	e.goAPIUser(t, ctx, "inactive@example.com", GitLab, "1101", false)
	gitlab := e.providers.token("inactive", account{provider: GitLab, id: 1101, email: "inactive@example.com", verified: true})
	// Before the users cutover the go-api account is found by its subject.
	if _, err := e.service(t, false).Exchange(ctx, GitLab, gitlab, nil); !errors.Is(err, ErrPrincipalDisabled) {
		t.Fatalf("go-api inactive account: err = %v, want ErrPrincipalDisabled", err)
	}
}

func TestExistingGoAPISocialUserSignsIn(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, ctx)
	svc := e.service(t, false)
	for _, p := range Providers() {
		t.Run(string(p), func(t *testing.T) {
			subject := "1200"
			acct := account{provider: p, id: 1200, email: string(p) + "-existing@example.com", verified: true, public: true}
			if p == Google {
				subject, acct.googleID = "g-1200", "g-1200"
			}
			existing := e.goAPIUser(t, ctx, acct.email, p, subject, true)
			token := e.providers.token(t.Name(), acct)
			before := e.state(t, ctx)
			res, err := svc.Exchange(ctx, p, token, nil)
			if err != nil || res.PrincipalID != existing || res.Created {
				t.Fatalf("Exchange = %+v, %v; want the go-api account %v", res, err, existing)
			}
			id := key(p, subject)
			if owner, ok := e.identityOwner(t, ctx, id); !ok || owner != existing {
				t.Fatalf("projected identity owner = %v, %v", owner, ok)
			}
			after := e.state(t, ctx)
			if after.identities != before.identities+1 || after.audits != before.audits+1 {
				t.Fatalf("projection rows before %+v, after %+v", before, after)
			}
			res, err = svc.Exchange(ctx, p, token, nil)
			if err != nil || res.PrincipalID != existing {
				t.Fatalf("second Exchange = %+v, %v", res, err)
			}
			e.unchanged(t, ctx, after)
		})
	}
}

func TestSignupNeedsUsersOwnedByAuth(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, ctx)
	token := e.providers.token("signup", account{provider: GitHub, id: 1300, email: "New.User@example.com", verified: true, public: true})
	t.Run("off", func(t *testing.T) {
		before := e.state(t, ctx)
		if _, err := e.service(t, false).Exchange(ctx, GitHub, token, nil); !errors.Is(err, identity.ErrSignupNotAvailable) {
			t.Fatalf("err = %v, want ErrSignupNotAvailable", err)
		}
		e.unchanged(t, ctx, before)
	})
	t.Run("on, concurrent first sign-ins agree", func(t *testing.T) {
		svc := e.service(t, true)
		const n = 8
		results := make([]ExchangeResult, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i], errs[i] = svc.Exchange(ctx, GitHub, token, nil)
			}()
		}
		wg.Wait()
		created := 0
		for i := range n {
			if errs[i] != nil {
				t.Fatalf("Exchange %d: %v", i, errs[i])
			}
			if results[i].PrincipalID != results[0].PrincipalID {
				t.Fatalf("Exchange %d signed in to %v, Exchange 0 to %v", i, results[i].PrincipalID, results[0].PrincipalID)
			}
			if results[i].Created {
				created++
			}
		}
		if created != 1 {
			t.Fatalf("created = %d, want 1", created)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND email_lower = 'new.user@example.com'
			AND email_verified_at IS NOT NULL AND password_hash IS NULL`, results[0].PrincipalID.String()); n != 1 {
			t.Fatalf("user rows = %d, want 1", n)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.principal.provisioned' AND subject_principal_id = $1 AND actor_principal_id = $1`,
			results[0].PrincipalID.String()); n != 1 {
			t.Fatalf("audit rows = %d, want 1", n)
		}
	})
	t.Run("the audit row carries the server request id", func(t *testing.T) {
		requestID := uuid.NewString()
		other := e.providers.token(t.Name(), account{provider: GitHub, id: 1301, email: "request-id@example.com", verified: true, public: true})
		res, err := e.service(t, true).Exchange(ctx, GitHub, other, &requestID)
		if err != nil || !res.Created {
			t.Fatalf("Exchange = %+v, %v", res, err)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.principal.provisioned'
			AND subject_principal_id = $1 AND request_id = $2`, res.PrincipalID.String(), requestID); n != 1 {
			t.Fatalf("audit rows with the request id = %d, want 1", n)
		}
	})
}
