//go:build integration

package lockorder

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
)

// TestGoAPIRoutesAnswerAsBefore pins the answers of the go-api routes whose
// lock statements the auth lock order sets: an add, an invite accept and an
// organization delete answer the same with or without those statements.
func TestGoAPIRoutesAnswerAsBefore(t *testing.T) {
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)

	t.Run("add_member", func(t *testing.T) {
		inviter, user := e.seedPair(t, ctx)
		org := e.seedOrg(t, ctx)
		created := e.addMember(ctx, org, user, &inviter)
		if created.status != http.StatusCreated || created.body["user_id"] != user.String() || created.body["role"] != "member" ||
			created.body["invited_by_id"] != inviter.String() || created.body["org_id"] != org.String() {
			t.Fatalf("add: %d %s", created.status, created.raw)
		}
		again := e.addMember(ctx, org, user, &inviter)
		if again.status != http.StatusBadRequest || again.body["detail"] != "User is already a member of this organization" {
			t.Fatalf("add again: %d %s", again.status, again.raw)
		}
		// An inviter go-api does not have fails the insert's foreign key.
		unknown := uuid.New()
		other := e.seedUser(t, ctx)
		if got := e.addMember(ctx, org, other, &unknown); got.status != http.StatusInternalServerError {
			t.Fatalf("add with an unknown inviter: %d %s", got.status, got.raw)
		}
		if got := e.publicRole(t, ctx, org, other); got != "-" {
			t.Fatalf("add with an unknown inviter wrote a row: %q", got)
		}
		alone := e.seedUser(t, ctx)
		if got := e.addMember(ctx, org, alone, nil); got.status != http.StatusCreated || got.body["invited_by_id"] != nil {
			t.Fatalf("add with no inviter: %d %s", got.status, got.raw)
		}
	})

	t.Run("accept-invite", func(t *testing.T) {
		inviter, acceptor := e.seedPair(t, ctx)
		org := e.seedOrg(t, ctx)
		inviteID, token := e.goInvite(t, ctx, org, inviter, email(acceptor))
		accepted := e.acceptInvite(ctx, acceptor, token)
		if accepted.status != http.StatusOK || accepted.body["org_id"] != org.String() || accepted.body["role"] != "member" {
			t.Fatalf("accept: %d %s", accepted.status, accepted.raw)
		}
		if got := e.scalar(t, ctx, `SELECT role || ':' || coalesce(invited_by_id::text, '-') FROM public.memberships
WHERE org_id = $1 AND user_id = $2`, org, acceptor); got != "member:"+inviter.String() {
			t.Fatalf("accept wrote %q, want member:%s", got, inviter)
		}
		if got := e.scalar(t, ctx, `SELECT status FROM public.org_invites WHERE id = $1`, inviteID); got != "accepted" {
			t.Fatalf("invite status = %q, want accepted", got)
		}
		again := e.acceptInvite(ctx, acceptor, token)
		if again.status != http.StatusBadRequest || again.body["detail"] != "Invalid or expired invite" {
			t.Fatalf("accept again: %d %s", again.status, again.raw)
		}
	})

	t.Run("delete organization", func(t *testing.T) {
		org := e.seedOrg(t, ctx)
		owner, member := e.seedPair(t, ctx)
		e.add(t, ctx, org, owner, "owner", nil)
		e.add(t, ctx, org, member, "member", nil)
		e.goInvite(t, ctx, org, owner, "pending-"+uuid.NewString()[:8]+"@example.test")
		deleted := e.deleteOrg(ctx, org)
		if deleted.status != http.StatusOK || deleted.body["organization_id"] != org.String() || deleted.body["dry_run"] != false {
			t.Fatalf("delete: %d %s", deleted.status, deleted.raw)
		}
		if got := e.scalar(t, ctx, `SELECT (SELECT count(*) FROM public.memberships WHERE org_id = $1)
  + (SELECT count(*) FROM public.org_invites WHERE org_id = $1) + (SELECT count(*) FROM public.organizations WHERE id = $1)`, org); got != "0" {
			t.Fatalf("delete left %s rows", got)
		}
	})
}
