//go:build integration

package authruntime

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The remove route, as configure builds it: the removed member's sessions of
// the organization end with the removal; the member's session with no
// organization and the owner's session stay live.
func TestProvisioningRemoveEndsTheMembersSessionsOfTheOrganization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	owner, org := e.seed(t, ctx)
	member := e.account(t, ctx, acctMail(), false, true)
	e.addVia(t, header, org, member, owner, "member")
	open := func(principal uuid.UUID, org *uuid.UUID) uuid.UUID {
		issued, err := e.sessions.Create(ctx, principal, org, []string{"pwd"})
		if err != nil {
			t.Fatalf("session Create: %v", err)
		}
		return issued.Session.ID
	}
	inOrg, noOrg, ownerInOrg := open(member, &org), open(member, nil), open(owner, &org)
	state := func(id uuid.UUID) (revoked bool, liveCredentials int) {
		if err := e.admin.QueryRow(ctx, `SELECT s.revoked_at IS NOT NULL,
  (SELECT count(*) FROM auth.refresh_credentials r WHERE r.session_id = s.id AND r.revoked_at IS NULL)
FROM auth.sessions s WHERE s.id = $1`, id).Scan(&revoked, &liveCredentials); err != nil {
			t.Fatal(err)
		}
		return revoked, liveCredentials
	}

	remove := jsonBody(t, map[string]any{"organization_id": org.String(), "principal_id": member.String(), "actor_principal_id": owner.String()})
	if got := e.doWith(t, "/internal/v1/memberships/remove", remove, header); got.status != http.StatusOK {
		t.Fatalf("remove: %d %s", got.status, got.body)
	}
	if revoked, live := state(inOrg); !revoked || live != 0 {
		t.Fatalf("the member's session of the organization: revoked %v, live credentials %d; want ended", revoked, live)
	}
	for name, id := range map[string]uuid.UUID{"member, no organization": noOrg, "owner": ownerInOrg} {
		if revoked, live := state(id); revoked || live == 0 {
			t.Fatalf("session (%s): revoked %v, live credentials %d; want live", name, revoked, live)
		}
	}
}

// The suspend and deprovision routes, as configure builds them: the organization's sessions and go-api refresh tokens end
// with the verb, a session of no organization stays live, the answers are unchanged, and a row another writer holds is
// the 503 with nothing written.
func TestProvisioningOrgRoutesEndTheOrganizationsSessions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	super := e.account(t, ctx, acctMail(), true, true)
	open := func(principal uuid.UUID, org *uuid.UUID) uuid.UUID {
		issued, err := e.sessions.Create(ctx, principal, org, []string{"pwd"})
		if err != nil {
			t.Fatalf("session Create: %v", err)
		}
		return issued.Session.ID
	}
	revoked := func(id uuid.UUID) bool {
		var done bool
		if err := e.admin.QueryRow(ctx, `SELECT revoked_at IS NOT NULL FROM auth.sessions WHERE id = $1`, id).Scan(&done); err != nil {
			t.Fatal(err)
		}
		return done
	}
	token := func(user, org uuid.UUID) uuid.UUID {
		id := uuid.New()
		if _, err := e.admin.Exec(ctx, `INSERT INTO public.refresh_tokens (id, user_id, org_id, token_hash, family_id, expires_at, created_at)
VALUES ($1, $2, $3, $4, gen_random_uuid(), now() + interval '1 day', now())`, id, user, org, id.String()); err != nil {
			t.Fatalf("seed go-api refresh token: %v", err)
		}
		return id
	}
	tokenRevoked := func(id uuid.UUID) bool {
		var done bool
		if err := e.admin.QueryRow(ctx, `SELECT revoked_at IS NOT NULL FROM public.refresh_tokens WHERE id = $1`, id).Scan(&done); err != nil {
			t.Fatal(err)
		}
		return done
	}
	for _, route := range []struct {
		path, answerKey string
	}{{"/internal/v1/organizations/suspend", "suspended"}, {"/internal/v1/organizations/deprovision", "suspended"}} {
		t.Run(route.path, func(t *testing.T) {
			owner, org := e.seed(t, ctx)
			inOrg, noOrg := open(owner, &org), open(owner, nil)
			goToken := token(owner, org)
			body := jsonBody(t, map[string]any{"organization_id": org.String(), "actor_principal_id": super.String()})

			held, err := e.admin.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = held.Rollback(context.Background()) }()
			if _, err := held.Exec(ctx, `SELECT 1 FROM auth.sessions WHERE id = $1 FOR UPDATE`, inOrg); err != nil {
				t.Fatal(err)
			}
			if got := e.doWith(t, route.path, body, header); got.status != http.StatusServiceUnavailable {
				t.Fatalf("with the session held: %d %s; want 503", got.status, got.body)
			}
			if revoked(inOrg) || tokenRevoked(goToken) {
				t.Fatal("the refused verb ended a credential")
			}
			if err := held.Rollback(ctx); err != nil {
				t.Fatal(err)
			}

			got := e.doWith(t, route.path, body, header)
			if got.status != http.StatusOK || object(t, got)[route.answerKey] != true {
				t.Fatalf("after the release: %d %s", got.status, got.body)
			}
			if !revoked(inOrg) || !tokenRevoked(goToken) {
				t.Fatalf("session revoked %v, go-api token revoked %v; want both ended", revoked(inOrg), tokenRevoked(goToken))
			}
			if revoked(noOrg) {
				t.Fatal("the session with no organization was ended")
			}
		})
	}
}
