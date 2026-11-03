//go:build integration

package lockorder

import (
	"bytes"
	"context"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/invite"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
)

// The cells below are the conflicting pairs of the auth lock order
// (membership.Store readPairs). Each one runs the real writers of both sides
// and holds them in the interleaving that closes the pair's cycle: out of
// order, Postgres ends it with a deadlock (40P01); in order, both commit.
// Every cell checks its own choreography and fails by name when it drifts.

func cellContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func (e *env) cleanup(t *testing.T) {
	t.Helper()
	e.resetGoLog()
	t.Cleanup(e.goBarrier.Release)
	t.Cleanup(e.authBarrier.Release)
}

func wantStatus(t *testing.T, cell string, p *party, status int) {
	t.Helper()
	if p.resp.status != status {
		t.Fatalf("%s: %s, want %d", cell, p, status)
	}
}

func wantNoErr(t *testing.T, cell string, p *party) {
	t.Helper()
	if p.err != nil {
		t.Fatalf("%s: %s", cell, p)
	}
}

// P1, the inviter is the auth verb's target: go-api's admin add of X invited
// by Y, held at its INSERT, and a role change of Y by X (Y < X).
func TestP1AddMemberAndRoleChangeOfItsInviter(t *testing.T) {
	const cell = "P1/target"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	y, x := e.seedPair(t, ctx)
	org, org2 := e.seedOrg(t, ctx), e.seedOrg(t, ctx)
	e.add(t, ctx, org, y, "member", nil)

	reached := e.goBarrier.Arm("INSERT INTO memberships")
	g := goAPI("go-api add_member", func() response { return e.addMember(ctx, org2, x, &y) })
	awaitReached(t, cell, "add_member's INSERT", reached, g)
	s := goAuth("membership ChangeRole", func() error {
		_, err := e.members.ChangeRole(ctx, membership.Change{OrgID: org, PrincipalID: y, Role: "admin",
			Actor: membership.Actor{PrincipalID: &x}, Authorize: allowAll})
		return err
	})
	e.awaitWaiters(t, ctx, cell, 1, s, g)
	e.goBarrier.Release()
	e.finish(t, cell, s, g)

	wantNoErr(t, cell, s)
	wantStatus(t, cell, g, http.StatusCreated)
	if got := e.publicRole(t, ctx, org2, x); got != "member" {
		t.Fatalf("%s: go-api role of X = %q, want member", cell, got)
	}
	if got := e.authState(t, ctx, org, y); got != "active:admin" {
		t.Fatalf("%s: auth row of Y = %q, want active:admin", cell, got)
	}
}

// P1, the inviter is the auth verb's inviter: go-api's admin add of X invited
// by Y, held at its INSERT, and an auth Add of X to another organization,
// invited by Y.
func TestP1AddMemberAndAuthAddWithTheSameInviter(t *testing.T) {
	const cell = "P1/inviter"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	y, x := e.seedPair(t, ctx)
	org2, org3 := e.seedOrg(t, ctx), e.seedOrg(t, ctx)

	reached := e.goBarrier.Arm("INSERT INTO memberships")
	g := goAPI("go-api add_member", func() response { return e.addMember(ctx, org2, x, &y) })
	awaitReached(t, cell, "add_member's INSERT", reached, g)
	s := goAuth("membership Add", func() error {
		_, err := e.members.Add(ctx, membership.Change{OrgID: org3, PrincipalID: x, Role: "member", InvitedBy: &y, Authorize: allowAll})
		return err
	})
	e.awaitWaiters(t, ctx, cell, 1, s, g)
	e.goBarrier.Release()
	e.finish(t, cell, s, g)

	wantNoErr(t, cell, s)
	wantStatus(t, cell, g, http.StatusCreated)
	if a, b := e.publicRole(t, ctx, org2, x), e.publicRole(t, ctx, org3, x); a != "member" || b != "member" {
		t.Fatalf("%s: go-api roles of X = %q, %q, want member, member", cell, a, b)
	}
}

// P1, the inviter is the auth verb's actor: go-api's admin add of X invited
// by Y, held at its INSERT, and Y's revoke of an invite go-api made, invited
// by X.
func TestP1AddMemberAndInviteRevokeByItsInviter(t *testing.T) {
	const cell = "P1/actor"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	y, x := e.seedPair(t, ctx)
	org, org2 := e.seedOrg(t, ctx), e.seedOrg(t, ctx)
	inviteID, _ := e.goInvite(t, ctx, org, x, "invitee-"+uuid.NewString()[:8]+"@example.com")

	reached := e.goBarrier.Arm("INSERT INTO memberships")
	g := goAPI("go-api add_member", func() response { return e.addMember(ctx, org2, x, &y) })
	awaitReached(t, cell, "add_member's INSERT", reached, g)
	s := goAuth("invite Revoke", func() error {
		return e.invites.Revoke(ctx, invite.RevokeInput{OrgID: org, InviteID: inviteID, Actor: invite.Actor{PrincipalID: &y}, Authorize: allowInvite})
	})
	e.awaitWaiters(t, ctx, cell, 1, s, g)
	e.goBarrier.Release()
	e.finish(t, cell, s, g)

	wantNoErr(t, cell, s)
	wantStatus(t, cell, g, http.StatusCreated)
	if got := e.scalar(t, ctx, `SELECT status FROM public.org_invites WHERE id = $1`, inviteID); got != "revoked" {
		t.Fatalf("%s: go-api invite status = %q, want revoked", cell, got)
	}
}

// fkOrder is the order in which an INSERT into public.memberships checks its
// two foreign keys to public.users (RI triggers fire in name order).
func (e *env) fkOrder(t *testing.T, ctx context.Context, cell string) []string {
	t.Helper()
	rows, err := e.admin.Query(ctx, `SELECT a.attname FROM pg_trigger tg
JOIN pg_constraint c ON c.oid = tg.tgconstraint
JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
WHERE tg.tgrelid = 'public.memberships'::regclass AND c.contype = 'f' AND c.confrelid = 'public.users'::regclass
  AND NOT c.condeferrable AND tg.tgfoid = 'pg_catalog."RI_FKey_check_ins"'::regproc
ORDER BY tg.tgname COLLATE "C"`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || out[0] == out[1] || (out[0] != "user_id" && out[0] != "invited_by_id") || (out[1] != "user_id" && out[1] != "invited_by_id") {
		t.Fatalf("%s: choreography: public.memberships checks its users foreign keys as %v, want user_id and invited_by_id, immediate", cell, out)
	}
	return out
}

// P2: go-api's invite accept (session route) checks the account and the
// inviter as foreign keys of its INSERT, one after the other; an ownership
// transfer between the two runs in id order. A blocker holds the row checked
// first FOR NO KEY UPDATE (which a key-share check passes), so the transfer
// holds the other row and waits there while the accept reaches its INSERT.
func TestP2InviteAcceptAndTransferBetweenAccountAndInviter(t *testing.T) {
	const cell = "P2"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	order := e.fkOrder(t, ctx, cell)
	lo, hi := e.seedPair(t, ctx)
	// The row checked first is hi: the transfer locks lo first.
	acceptor, inviter := lo, hi
	if order[0] == "user_id" {
		acceptor, inviter = hi, lo
	}
	org, org2 := e.seedOrg(t, ctx), e.seedOrg(t, ctx)
	e.add(t, ctx, org2, lo, "owner", nil)
	e.add(t, ctx, org2, hi, "member", nil)
	_, token := e.goInvite(t, ctx, org, inviter, email(acceptor))

	blocker, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(context.Background()) }()
	if _, err := blocker.Exec(ctx, `SELECT 1 FROM public.users WHERE id = $1 FOR NO KEY UPDATE`, hi); err != nil {
		t.Fatal(err)
	}
	s := goAuth("membership TransferOwnership", func() error {
		return e.members.TransferOwnership(ctx, membership.Transfer{OrgID: org2, From: lo, To: hi, Authorize: allowAll})
	})
	e.awaitWaiters(t, ctx, cell, 1, s)
	g := goAPI("go-api accept-invite", func() response { return e.acceptInvite(ctx, acceptor, token) })
	e.awaitWaiters(t, ctx, cell, 2, s, g)
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	e.finish(t, cell, s, g)

	wantNoErr(t, cell, s)
	wantStatus(t, cell, g, http.StatusOK)
	if got := e.publicRole(t, ctx, org, acceptor); got != "member" {
		t.Fatalf("%s: go-api role of the account = %q, want member", cell, got)
	}
	if a, b := e.publicRole(t, ctx, org2, lo), e.publicRole(t, ctx, org2, hi); a != "admin" || b != "owner" {
		t.Fatalf("%s: roles after the transfer = %q, %q, want admin, owner", cell, a, b)
	}
}

// P3: the reactivation of a suspended membership writes go-api's row again,
// naming the inviter its auth row keeps (Z). go-api's role change of Z, held
// before its UPDATE, holds Z's users row; Z is an owner, so the reactivation
// holds Z's membership row.
func TestP3ReactivationAndRoleChangeOfTheOldInviter(t *testing.T) {
	const cell = "P3"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org := e.seedOrg(t, ctx)
	z, w, member := e.seedUser(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org, z, "owner", nil)
	e.add(t, ctx, org, w, "owner", nil)
	e.add(t, ctx, org, member, "member", &z)
	if _, err := e.members.Suspend(ctx, membership.Change{OrgID: org, PrincipalID: member, Authorize: allowAll}); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	if got := e.publicRole(t, ctx, org, member); got != "-" {
		t.Fatalf("%s: choreography: a suspended membership keeps go-api's row (%q)", cell, got)
	}

	reached := e.goBarrier.Arm("UPDATE memberships SET role")
	g := goAPI("go-api update_member_role", func() response { return e.setMemberRole(ctx, org, z, "admin") })
	awaitReached(t, cell, "update_member_role's UPDATE", reached, g)
	s := goAuth("membership Reactivate", func() error {
		_, err := e.members.Reactivate(ctx, membership.Change{OrgID: org, PrincipalID: member, Authorize: allowAll})
		return err
	})
	e.awaitWaiters(t, ctx, cell, 1, s, g)
	e.goBarrier.Release()
	e.finish(t, cell, s, g)

	wantNoErr(t, cell, s)
	wantStatus(t, cell, g, http.StatusOK)
	if got := e.publicRole(t, ctx, org, z); got != "admin" {
		t.Fatalf("%s: go-api role of Z = %q, want admin", cell, got)
	}
	if got := e.scalar(t, ctx, `SELECT role || ':' || coalesce(invited_by_id::text, '-') FROM public.memberships
WHERE org_id = $1 AND user_id = $2`, org, member); got != "member:"+z.String() {
		t.Fatalf("%s: go-api row of the member = %q, want member:%s", cell, got, z)
	}
}

// P4: an invite accept (the invite in auth.invites) and go-api's
// organization delete. The accept is held before it marks go-api's invite row
// accepted, after it locked the organization's membership rows; the delete
// deletes the invite rows before the membership rows.
func TestP4InviteAcceptAndOrganizationDelete(t *testing.T) {
	const cell = "P4"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org := e.seedOrg(t, ctx)
	owner, acceptor := e.seedUser(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org, owner, "owner", nil)
	created, err := e.invites.Create(ctx, invite.CreateInput{OrgID: org, Email: email(acceptor), Role: "member", InvitedBy: owner,
		Actor: invite.Actor{PrincipalID: &owner}, Authorize: allowInvite})
	if err != nil {
		t.Fatalf("%s: fixture: invite Create: %v", cell, err)
	}

	reached := e.authBarrier.Arm("UPDATE public.org_invites SET status = 'accepted'")
	s := goAuth("invite Accept", func() error {
		_, err := e.invites.Accept(ctx, invite.AcceptInput{Token: created.Token, PrincipalID: acceptor, Actor: invite.Actor{PrincipalID: &acceptor}})
		return err
	})
	awaitReached(t, cell, "Accept's update of go-api's invite row", reached, s)
	g := goAPI("go-api delete organization", func() response { return e.deleteOrg(ctx, org) })
	e.awaitWaiters(t, ctx, cell, 1, s, g)
	e.authBarrier.Release()
	e.finish(t, cell, s, g)

	wantNoErr(t, cell, s)
	wantStatus(t, cell, g, http.StatusOK)
	if got := e.scalar(t, ctx, `SELECT count(*)::text FROM public.memberships WHERE org_id = $1`, org); got != "0" {
		t.Fatalf("%s: %s go-api membership rows left after the delete", cell, got)
	}
}

// P5: an ownership transfer locks the organization's membership rows in id
// order; go-api's organization delete deletes them in scan order. The rows are
// seeded so that the row with the higher id comes first in both the heap and
// the user order. A blocker holds the lower row; the transfer queues on it,
// the delete deletes the higher row and queues behind the transfer.
func TestP5TransferAndOrganizationDelete(t *testing.T) {
	const cell = "P5"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	var org, from, to, low uuid.UUID
	found := false
	for range 40 {
		org = e.seedOrg(t, ctx)
		a, b := e.seedUser(t, ctx), e.seedUser(t, ctx)
		e.add(t, ctx, org, a, "member", nil)
		e.add(t, ctx, org, b, "owner", nil)
		idA := uuid.MustParse(e.scalar(t, ctx, `SELECT id::text FROM public.memberships WHERE org_id = $1 AND user_id = $2`, org, a))
		idB := uuid.MustParse(e.scalar(t, ctx, `SELECT id::text FROM public.memberships WHERE org_id = $1 AND user_id = $2`, org, b))
		heapFirst := e.scalar(t, ctx, `SELECT ((SELECT ctid FROM public.memberships WHERE id = $1) < (SELECT ctid FROM public.memberships WHERE id = $2))::text`, idA, idB)
		if bytes.Compare(idA[:], idB[:]) > 0 && bytes.Compare(a[:], b[:]) < 0 && heapFirst == "true" {
			from, to, low, found = b, a, idB, true
			break
		}
	}
	if !found {
		t.Fatalf("%s: choreography: no seed in 40 had the higher membership id first in heap and user order", cell)
	}

	blocker, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(context.Background()) }()
	if _, err := blocker.Exec(ctx, `SELECT 1 FROM public.memberships WHERE id = $1 FOR UPDATE`, low); err != nil {
		t.Fatal(err)
	}
	s := goAuth("membership TransferOwnership", func() error {
		return e.members.TransferOwnership(ctx, membership.Transfer{OrgID: org, From: from, To: to, Authorize: allowAll})
	})
	e.awaitWaiters(t, ctx, cell, 1, s)
	g := goAPI("go-api delete organization", func() response { return e.deleteOrg(ctx, org) })
	e.awaitWaiters(t, ctx, cell, 2, s, g)
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	e.finish(t, cell, s, g)

	wantNoErr(t, cell, s)
	wantStatus(t, cell, g, http.StatusOK)
	if got := e.scalar(t, ctx, `SELECT count(*)::text FROM public.memberships WHERE org_id = $1`, org); got != "0" {
		t.Fatalf("%s: %s go-api membership rows left after the delete", cell, got)
	}
}

// P6: go-api's session invite accept and go-api's organization delete. The
// accept is held before it marks the invite row accepted, after its membership
// INSERT (whose foreign key check holds the organization row); the delete
// takes the invite rows before the membership rows and deletes the
// organization row last.
func TestP6GoAPIInviteAcceptAndOrganizationDelete(t *testing.T) {
	const cell = "P6"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org := e.seedOrg(t, ctx)
	owner, acceptor := e.seedPair(t, ctx)
	e.add(t, ctx, org, owner, "owner", nil)
	_, token := e.goInvite(t, ctx, org, owner, email(acceptor))

	reached := e.goBarrier.Arm("UPDATE org_invites SET status = 'accepted'")
	g := goAPI("go-api accept-invite", func() response { return e.acceptInvite(ctx, acceptor, token) })
	awaitReached(t, cell, "accept-invite's update of the invite row", reached, g)
	d := goAPI("go-api delete organization", func() response { return e.deleteOrg(ctx, org) })
	e.awaitWaiters(t, ctx, cell, 1, g, d)
	// In order, the delete waits for the accept; the delete is then held before
	// it deletes the organization row until the accept has answered, so the
	// accept's token write (it names the organization) never races the delete.
	e.goBarrier.Release()
	e.goBarrier.Arm("DELETE FROM organizations WHERE")
	select {
	case <-g.done:
	case <-time.After(2 * choreographyBound):
		t.Fatalf("%s: %s did not end within %s", cell, g.name, 2*choreographyBound)
	}
	e.goBarrier.Release()
	e.finish(t, cell, g, d)

	wantStatus(t, cell, g, http.StatusOK)
	wantStatus(t, cell, d, http.StatusOK)
	if got := e.scalar(t, ctx, `SELECT (SELECT count(*) FROM public.memberships WHERE org_id = $1)
  + (SELECT count(*) FROM public.org_invites WHERE org_id = $1) + (SELECT count(*) FROM public.organizations WHERE id = $1)`, org); got != "0" {
		t.Fatalf("%s: %s rows of the organization left after the delete", cell, got)
	}
}

// heldAuthorize is an Authorize that signals once and holds the verb, with
// its locks, until release.
type heldAuthorize struct {
	once    sync.Once
	in      chan struct{}
	release chan struct{}
	relOnce sync.Once
}

func newHeld(t *testing.T) *heldAuthorize {
	h := &heldAuthorize{in: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(h.free)
	return h
}

func (h *heldAuthorize) authorize(context.Context, audit.TxOps, membership.Locked) error {
	h.once.Do(func() { close(h.in) })
	<-h.release
	return nil
}

func (h *heldAuthorize) free() { h.relOnce.Do(func() { close(h.release) }) }

// runsAlongside asserts that second ends while first holds its locks: a lock
// wait of second fails the cell by name.
func (e *env) runsAlongside(t *testing.T, ctx context.Context, cell, why string, first, second *party) {
	t.Helper()
	deadline := time.Now().Add(choreographyBound)
	for !second.finished() {
		if first.finished() {
			t.Fatalf("%s: choreography: %s ended while held: %s", cell, first.name, first)
		}
		if n := e.waiters(t, ctx); n > 0 {
			t.Fatalf("%s: %s waits on a lock of %s: %s", cell, second.name, first.name, why)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %s neither ended nor waited within %s", cell, second.name, choreographyBound)
		}
		time.Sleep(10 * time.Millisecond)
	}
	wantNoErr(t, cell, second)
}

// Mode: two adds naming one inviter run together (the inviter's rows are
// locked FOR KEY SHARE).
func TestModeTwoAddsNamingOneInviterRunTogether(t *testing.T) {
	const cell = "mode/inviter"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	y, x1, x2 := e.seedUser(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
	o1, o2 := e.seedOrg(t, ctx), e.seedOrg(t, ctx)
	held := newHeld(t)
	first := goAuth("first Add", func() error {
		_, err := e.members.Add(ctx, membership.Change{OrgID: o1, PrincipalID: x1, Role: "member", InvitedBy: &y, Authorize: held.authorize})
		return err
	})
	awaitReached(t, cell, "the first Add's Authorize", held.in, first)
	second := goAuth("second Add", func() error {
		_, err := e.members.Add(ctx, membership.Change{OrgID: o2, PrincipalID: x2, Role: "member", InvitedBy: &y, Authorize: allowAll})
		return err
	})
	e.runsAlongside(t, ctx, cell, "the inviter is locked stronger than FOR KEY SHARE", first, second)
	held.free()
	e.finish(t, cell, first, second)
	wantNoErr(t, cell, first)
}

// Mode: two verbs by one actor run together (the actor's rows are locked FOR
// SHARE).
func TestModeTwoVerbsByOneActorRunTogether(t *testing.T) {
	const cell = "mode/actor"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	actor, t1, t2 := e.seedUser(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
	o1, o2 := e.seedOrg(t, ctx), e.seedOrg(t, ctx)
	e.add(t, ctx, o1, t1, "member", nil)
	e.add(t, ctx, o2, t2, "member", nil)
	held := newHeld(t)
	first := goAuth("first ChangeRole", func() error {
		_, err := e.members.ChangeRole(ctx, membership.Change{OrgID: o1, PrincipalID: t1, Role: "admin",
			Actor: membership.Actor{PrincipalID: &actor}, Authorize: held.authorize})
		return err
	})
	awaitReached(t, cell, "the first ChangeRole's Authorize", held.in, first)
	second := goAuth("second ChangeRole", func() error {
		_, err := e.members.ChangeRole(ctx, membership.Change{OrgID: o2, PrincipalID: t2, Role: "admin",
			Actor: membership.Actor{PrincipalID: &actor}, Authorize: allowAll})
		return err
	})
	e.runsAlongside(t, ctx, cell, "the actor is locked stronger than FOR SHARE", first, second)
	held.free()
	e.finish(t, cell, first, second)
	wantNoErr(t, cell, first)
}

// Mode: two go-api admin adds naming one inviter, of different targets, run
// together (go-api takes the inviter's users row FOR KEY SHARE). The first is
// held at its INSERT, after its locks. Two id layouts: the inviter below the
// targets (locked before the target's guard lock) and above them (after it).
func TestGoAPIModeTwoAddMembersNamingOneInviterRunTogether(t *testing.T) {
	for _, layout := range []string{"inviter-below", "inviter-above"} {
		t.Run(layout, func(t *testing.T) {
			cell := "go-api mode/inviter/" + layout
			e, ctx := getEnv(t), cellContext(t)
			e.cleanup(t)
			ids := []uuid.UUID{e.seedUser(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)}
			sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
			inviter, x1, x2 := ids[0], ids[1], ids[2]
			if layout == "inviter-above" {
				x1, x2, inviter = ids[0], ids[1], ids[2]
			}
			below := inviter.String() < x1.String() && inviter.String() < x2.String()
			if below != (layout == "inviter-below") {
				t.Fatalf("%s: choreography: the inviter %s is not %s the targets %s, %s", cell, inviter, layout, x1, x2)
			}
			o1, o2 := e.seedOrg(t, ctx), e.seedOrg(t, ctx)

			reached := e.goBarrier.Arm("INSERT INTO memberships")
			first := goAPI("first go-api add_member", func() response { return e.addMember(ctx, o1, x1, &inviter) })
			awaitReached(t, cell, "the first add_member's INSERT", reached, first)
			second := goAPI("second go-api add_member", func() response { return e.addMember(ctx, o2, x2, &inviter) })
			e.runsAlongside(t, ctx, cell, "the inviter is locked stronger than FOR KEY SHARE", first, second)
			e.goBarrier.Release()
			e.finish(t, cell, first, second)

			wantStatus(t, cell, first, http.StatusCreated)
			wantStatus(t, cell, second, http.StatusCreated)
			for _, m := range []struct{ org, user uuid.UUID }{{o1, x1}, {o2, x2}} {
				if got := e.scalar(t, ctx, `SELECT coalesce(invited_by_id::text, '-') FROM public.memberships WHERE org_id = $1 AND user_id = $2`,
					m.org, m.user); got != inviter.String() {
					t.Fatalf("%s: go-api row of %s names inviter %q, want %s", cell, m.user, got, inviter)
				}
			}
		})
	}
}

// Order: go-api's admin add of X invited by Y (Y < X) is held after it locked
// Y and before it locks X; a role change of Y by X then waits for Y. An auth
// verb that locked X first would hold X while go-api waits for it: a deadlock.
func TestOrderAuthVerbLocksInAscendingIDOrder(t *testing.T) {
	const cell = "order/ascending"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	y, x := e.seedPair(t, ctx)
	org, org2 := e.seedOrg(t, ctx), e.seedOrg(t, ctx)
	e.add(t, ctx, org, y, "member", nil)

	reached := e.goBarrier.Arm("SELECT is_superuser FROM users")
	g := goAPI("go-api add_member", func() response { return e.addMember(ctx, org2, x, &y) })
	awaitReached(t, cell, "add_member's target lock", reached, g)
	s := goAuth("membership ChangeRole", func() error {
		_, err := e.members.ChangeRole(ctx, membership.Change{OrgID: org, PrincipalID: y, Role: "admin",
			Actor: membership.Actor{PrincipalID: &x}, Authorize: allowAll})
		return err
	})
	e.awaitWaiters(t, ctx, cell, 1, s, g)
	e.goBarrier.Release()
	e.finish(t, cell, s, g)

	wantNoErr(t, cell, s)
	wantStatus(t, cell, g, http.StatusCreated)
}

// Onboarding holds its principal, then public.users. A removal by that
// principal waits for it at step 1 and takes no row onboarding wants, so both
// end, in the auth lock order.
func TestOnboardOrgAndMembershipRemove(t *testing.T) {
	const cell = "onboard/remove"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	user, target := e.seedPair(t, ctx)
	org := e.seedOrg(t, ctx)
	e.add(t, ctx, org, target, "member", nil)
	reached := e.authBarrier.Arm("INSERT INTO public.organizations")
	onboard := goAuth("OnboardOrganization", func() error {
		_, err := e.identity.OnboardOrganization(ctx, identity.OnboardOrganizationRequest{Principal: user, Name: "Held"})
		return err
	})
	awaitReached(t, cell, "the onboarding insert", reached, onboard)
	remove := goAuth("membership Remove", func() error {
		_, err := e.members.Remove(ctx, membership.Change{OrgID: org, PrincipalID: target, Actor: membership.Actor{PrincipalID: &user}, Authorize: allowAll})
		return err
	})
	e.awaitWaiters(t, ctx, cell, 1, onboard, remove)
	e.authBarrier.Release()
	e.finish(t, cell, onboard, remove)
	wantNoErr(t, cell, onboard)
	wantNoErr(t, cell, remove)
}

// An invite accept waits for an onboarding of the same account; each
// organization ends with one membership of it.
func TestOnboardOrgRaceWithInviteAcceptOneMembership(t *testing.T) {
	const cell = "onboard/accept"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	user, inviter := e.seedPair(t, ctx)
	org := e.seedOrg(t, ctx)
	e.add(t, ctx, org, inviter, "owner", nil)
	ttl := time.Hour
	created, err := e.invites.Create(ctx, invite.CreateInput{OrgID: org, Email: email(user), Role: "member", TTL: &ttl, InvitedBy: inviter,
		Actor: invite.Actor{PrincipalID: &inviter}, Authorize: allowInvite})
	if err != nil {
		t.Fatal(err)
	}
	reached := e.authBarrier.Arm("INSERT INTO public.organizations")
	onboard := goAuth("OnboardOrganization", func() error {
		_, err := e.identity.OnboardOrganization(ctx, identity.OnboardOrganizationRequest{Principal: user, Name: "Held Too"})
		return err
	})
	awaitReached(t, cell, "the onboarding insert", reached, onboard)
	accept := goAuth("invite Accept", func() error {
		_, err := e.invites.Accept(ctx, invite.AcceptInput{Token: created.Token, PrincipalID: user, Actor: invite.Actor{PrincipalID: &user},
			EnsureOrganization: e.identity.EnsureOrganization})
		return err
	})
	e.awaitWaiters(t, ctx, cell, 1, onboard, accept)
	e.authBarrier.Release()
	e.finish(t, cell, onboard, accept)
	wantNoErr(t, cell, onboard)
	wantNoErr(t, cell, accept)
	if got := e.scalar(t, ctx, `SELECT count(DISTINCT org_id)::text || '/' || count(*)::text FROM public.memberships WHERE user_id = $1`, user); got != "2/2" {
		t.Fatalf("%s: memberships = %s, want one in each of 2 organizations", cell, got)
	}
}
