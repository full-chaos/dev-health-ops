//go:build integration

package identity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func (e *env) membershipID(t *testing.T, ctx context.Context, org, user uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT id FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2`, org, user).Scan(&id); err != nil {
		t.Fatalf("read the membership id: %v", err)
	}
	return id
}

// An inviter projected after the insert took its principal locks stays unresolved for this run: the insert resolves it
// only by its locked read. The next run fills it.
func TestInsertResolvesTheInviterByTheLockedRead(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	org := e.seedAuthOwnedOrg(t, ctx)
	inviter := e.seedUser(t, ctx, uniqueEmail("ni"), true, true)
	member := e.seedUser(t, ctx, uniqueEmail("nm"), true, true)
	e.project(t, ctx, member)
	t0 := time.Now().UTC().Truncate(time.Second).Add(-72 * time.Hour)
	afterInsertLock = func(ctx context.Context) {
		afterInsertLock = nil
		e.project(t, ctx, inviter)
	}
	t.Cleanup(func() { afterInsertLock = nil })
	target := MembershipBackfillTarget{OrgID: org, UserID: member, Role: "member", JoinedAt: &t0, InvitedBy: &inviter}
	res, err := e.store.BackfillMembership(ctx, target, false)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Inserted || res.InviterSet || !res.InviterUnresolved {
		t.Fatalf("first run: got inserted=%v inviter_set=%v inviter_unresolved=%v, want true, false, true", res.Inserted, res.InviterSet, res.InviterUnresolved)
	}
	id := e.membershipID(t, ctx, org, member)
	var got *uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT invited_by FROM auth.memberships WHERE id = $1`, id).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("invited_by after the first run: got %v, want nil", got)
	}
	res, err = e.store.BackfillMembership(ctx, target, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.Inserted || !res.InviterSet || res.InviterUnresolved {
		t.Fatalf("second run: got inserted=%v inviter_set=%v inviter_unresolved=%v, want false, true, false", res.Inserted, res.InviterSet, res.InviterUnresolved)
	}
}

// An insert held after its principal locks and a TransferOwnership naming the same inviter run in the auth lock order:
// the transfer waits for the insert's key-share lock on the inviter (exactly one lock waiter), and both then finish with no deadlock.
func TestInsertAndTransferOwnershipShareOneLockOrder(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	ms, err := membership.New(membership.Config{Pool: e.runtime, Schema: schema, OwnsUsers: true})
	if err != nil {
		t.Fatal(err)
	}
	allow := func(context.Context, audit.TxOps, membership.Locked) error { return nil }
	org := e.seedAuthOwnedOrg(t, ctx)
	owner := e.seedUser(t, ctx, uniqueEmail("xo"), true, true)
	other := e.seedUser(t, ctx, uniqueEmail("xq"), true, true)
	newcomer := e.seedUser(t, ctx, uniqueEmail("xn"), true, true)
	for _, u := range []uuid.UUID{owner, other, newcomer} {
		e.project(t, ctx, u)
	}
	if _, err := ms.Add(ctx, membership.Change{OrgID: org, PrincipalID: owner, Role: "owner", Authorize: allow}); err != nil {
		t.Fatalf("add the owner: %v", err)
	}
	if _, err := ms.Add(ctx, membership.Change{OrgID: org, PrincipalID: other, Role: "member", Authorize: allow}); err != nil {
		t.Fatalf("add the member: %v", err)
	}
	locked, release := make(chan struct{}), make(chan struct{})
	afterInsertLock = func(context.Context) {
		afterInsertLock = nil
		close(locked)
		<-release
	}
	t.Cleanup(func() { afterInsertLock = nil })
	joined := time.Now().UTC().Truncate(time.Second)
	insertErr, transferErr := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := e.store.BackfillMembership(ctx, MembershipBackfillTarget{OrgID: org, UserID: newcomer, Role: "member", JoinedAt: &joined, InvitedBy: &owner}, false)
		insertErr <- err
	}()
	select {
	case <-locked:
	case err := <-insertErr:
		t.Fatalf("the insert ended before it held its principal locks: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("the insert never reached its lock point")
	}
	go func() {
		transferErr <- ms.TransferOwnership(ctx, membership.Transfer{OrgID: org, From: owner, To: other, Authorize: allow})
	}()
	waiters := -1
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		waiters = e.count(t, ctx, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database()`)
		if waiters >= 1 {
			break
		}
	}
	if waiters != 1 {
		close(release)
		t.Fatalf("lock waiters while the insert holds its principal locks: got %d, want 1", waiters)
	}
	close(release)
	for name, ch := range map[string]chan error{"insert": insertErr, "transfer": transferErr} {
		select {
		case err := <-ch:
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
				t.Fatalf("%s: deadlock: %v", name, err)
			}
			if err != nil {
				t.Fatalf("%s: got error %v, want nil", name, err)
			}
		case <-time.After(60 * time.Second):
			t.Fatalf("%s did not finish after the release", name)
		}
	}
}

// An insert that loses ON CONFLICT to another insert of the same membership reports nothing, and one audit row exists.
func TestInsertLosingTheConflictReportsNothing(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	org := e.seedAuthOwnedOrg(t, ctx)
	inviter := e.seedUser(t, ctx, uniqueEmail("ci"), true, true)
	member := e.seedUser(t, ctx, uniqueEmail("cm"), true, true)
	e.project(t, ctx, inviter)
	e.project(t, ctx, member)
	t0 := time.Now().UTC().Truncate(time.Second).Add(-72 * time.Hour)
	target := MembershipBackfillTarget{OrgID: org, UserID: member, Role: "member", JoinedAt: &t0, InvitedBy: &inviter}
	var winner MembershipBackfillResult
	var winnerErr error
	afterInsertLock = func(ctx context.Context) {
		afterInsertLock = nil
		winner, winnerErr = e.store.BackfillMembership(ctx, target, false)
	}
	t.Cleanup(func() { afterInsertLock = nil })
	loser, err := e.store.BackfillMembership(ctx, target, false)
	if err != nil {
		t.Fatal(err)
	}
	if winnerErr != nil || !winner.Inserted || !winner.InviterSet {
		t.Fatalf("the competing insert: got %+v err %v, want inserted with the inviter", winner, winnerErr)
	}
	if loser.Inserted || loser.InviterSet || loser.InviterUnresolved {
		t.Fatalf("the insert that lost ON CONFLICT: got %+v, want nothing reported", loser)
	}
	id := e.membershipID(t, ctx, org, member)
	if got := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.membership.projected' AND attributes->>'target_id' = $1`, id.String()); got != 1 {
		t.Fatalf("projected-membership audit rows: got %d, want 1", got)
	}
}

// A role update that changes no row (another writer set the role first) is not counted as a role change.
func TestRoleUpdateThatChangesNothingReportsNothing(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	org := e.seedAuthOwnedOrg(t, ctx)
	member := e.seedUser(t, ctx, uniqueEmail("rm"), true, true)
	e.project(t, ctx, member)
	t0 := time.Now().UTC().Truncate(time.Second).Add(-72 * time.Hour)
	if _, err := e.store.BackfillMembership(ctx, MembershipBackfillTarget{OrgID: org, UserID: member, Role: "member", JoinedAt: &t0}, false); err != nil {
		t.Fatal(err)
	}
	id := e.membershipID(t, ctx, org, member)
	target := MembershipBackfillTarget{OrgID: org, UserID: member, Role: "admin", JoinedAt: &t0}
	var winner MembershipBackfillResult
	var winnerErr error
	afterRoleRead = func(ctx context.Context) {
		afterRoleRead = nil
		winner, winnerErr = e.store.BackfillMembership(ctx, target, false)
	}
	t.Cleanup(func() { afterRoleRead = nil })
	loser, err := e.store.BackfillMembership(ctx, target, false)
	if err != nil {
		t.Fatal(err)
	}
	if winnerErr != nil || !winner.RoleUpdated {
		t.Fatalf("the competing update: got %+v err %v, want a role update", winner, winnerErr)
	}
	if loser.RoleUpdated {
		t.Fatalf("the update that changed no row: got role_updated=true, want false")
	}
	if got := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.membership.role_changed' AND attributes->>'target_id' = $1`, id.String()); got != 1 {
		t.Fatalf("role-changed audit rows: got %d, want 1", got)
	}
}

// A plan whose only item is an unresolved inviter still reads the inviter under the lock: an inviter projected after the
// first read is resolved by the transaction, filled, and no longer counted unresolved.
func TestUnresolvedOnlyPlanReportsFromTheLockedRead(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	org := e.seedAuthOwnedOrg(t, ctx)
	inviter := e.seedUser(t, ctx, uniqueEmail("ui"), true, true)
	member := e.seedUser(t, ctx, uniqueEmail("um"), true, true)
	e.project(t, ctx, member)
	t0 := time.Now().UTC().Truncate(time.Second).Add(-72 * time.Hour)
	if _, err := e.store.BackfillMembership(ctx, MembershipBackfillTarget{OrgID: org, UserID: member, Role: "member", JoinedAt: &t0}, false); err != nil {
		t.Fatal(err)
	}
	id := e.membershipID(t, ctx, org, member)
	afterFillRead = func(ctx context.Context) {
		afterFillRead = nil
		e.project(t, ctx, inviter)
	}
	t.Cleanup(func() { afterFillRead = nil })
	res, err := e.store.BackfillMembership(ctx, MembershipBackfillTarget{OrgID: org, UserID: member, Role: "member", JoinedAt: &t0, InvitedBy: &inviter}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !res.InviterSet || res.InviterUnresolved || res.JoinedSet {
		t.Fatalf("report: got inviter_set=%v inviter_unresolved=%v joined_set=%v, want true, false, false", res.InviterSet, res.InviterUnresolved, res.JoinedSet)
	}
	var got *uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT invited_by FROM auth.memberships WHERE id = $1`, id).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != inviter {
		t.Fatalf("invited_by: got %v, want %v", got, inviter)
	}
}
