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
