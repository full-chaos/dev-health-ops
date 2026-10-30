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
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// setAuthJoin writes a membership's joined_at through audit.Commit (the runtime role has no raw path); nil clears it.
func (e *env) setAuthJoin(t *testing.T, ctx context.Context, org, user uuid.UUID, at *time.Time) uuid.UUID {
	t.Helper()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	var id uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT id FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2`, org, user).Scan(&id); err != nil {
		t.Fatalf("read the membership: %v", err)
	}
	orgText, subject, target := org.String(), user.String(), id.String()
	if err := audit.Commit(ctx, e.runtime, schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `UPDATE auth.memberships SET joined_at = $2, updated_at = clock_timestamp() WHERE id = $1`, id, at)
			return err
		},
		Audit: audit.AuditEvent{
			EventType: "auth.membership.status_changed", Outcome: audit.OutcomeAllowed, OrganizationID: &orgText, SubjectPrincipalID: &subject,
			Attributes: map[string]any{"reason_code": "membership_status_changed", "target_kind": "membership", "target_id": target},
		},
		Events: []audit.OutboxEvent{{AggregateType: "membership", AggregateID: provisioning.WireID("membership", target),
			EventType: "membership.activated", Change: map[string]any{"status": "active"}}},
	}); err != nil {
		t.Fatalf("set joined_at: %v", err)
	}
	return id
}

// A writer fills joined_at between the backfill's read and its write: the report and the stored audit row name only
// what this run changed (invited_by), and the row ends with both values.
func TestFillReportsOnlyTheFieldsThisRunChanged(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	org := e.seedAuthOwnedOrg(t, ctx)
	inviter := e.seedUser(t, ctx, uniqueEmail("fi"), true, true)
	member := e.seedUser(t, ctx, uniqueEmail("fm"), true, true)
	e.project(t, ctx, inviter)
	e.project(t, ctx, member)
	t0 := time.Now().UTC().Truncate(time.Second).Add(-72 * time.Hour)
	if _, err := e.store.BackfillMembership(ctx, MembershipBackfillTarget{OrgID: org, UserID: member, Role: "member", JoinedAt: &t0}, false); err != nil {
		t.Fatal(err)
	}
	id := e.setAuthJoin(t, ctx, org, member, nil)
	t1, t2 := t0.Add(time.Hour), t0.Add(2*time.Hour)
	afterFillRead = func(ctx context.Context) {
		afterFillRead = nil
		e.setAuthJoin(t, ctx, org, member, &t2)
	}
	t.Cleanup(func() { afterFillRead = nil })
	res, err := e.store.BackfillMembership(ctx, MembershipBackfillTarget{OrgID: org, UserID: member, Role: "member", JoinedAt: &t1, InvitedBy: &inviter}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !res.InviterSet || res.JoinedSet {
		t.Fatalf("report: got inviter_set=%v joined_set=%v, want true and false", res.InviterSet, res.JoinedSet)
	}
	var gotInviter *uuid.UUID
	var gotJoined *time.Time
	if err := e.admin.QueryRow(ctx, `SELECT invited_by, joined_at FROM auth.memberships WHERE id = $1`, id).Scan(&gotInviter, &gotJoined); err != nil {
		t.Fatal(err)
	}
	if gotInviter == nil || *gotInviter != inviter || gotJoined == nil || !gotJoined.Equal(t2) {
		t.Fatalf("row: got invited_by=%v joined_at=%v, want %v and %v", gotInviter, gotJoined, inviter, t2)
	}
	if got := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE attributes->>'target_id' = $1 AND attributes->'changed_fields' = '["invited_by"]'::jsonb`, id.String()); got != 1 {
		t.Fatalf("audit rows whose changed_fields are exactly [invited_by]: got %d, want 1", got)
	}
	if got := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE attributes->>'target_id' = $1 AND attributes->'changed_fields' ? 'joined_at'`, id.String()); got != 0 {
		t.Fatalf("audit rows naming joined_at as changed by the fill: got %d, want 0", got)
	}
}

// The fill of a member naming an inviter and a TransferOwnership between the inviter and the member take the
// auth lock order (principals before the membership rows): started in that order, both commit and PostgreSQL
// reports no deadlock.
func TestFillAndTransferOwnershipShareOneLockOrder(t *testing.T) {
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
	inviter := e.seedUser(t, ctx, uniqueEmail("ti"), true, true)
	member := e.seedUser(t, ctx, uniqueEmail("tm"), true, true)
	e.project(t, ctx, inviter)
	e.project(t, ctx, member)
	if _, err := ms.Add(ctx, membership.Change{OrgID: org, PrincipalID: inviter, Role: "owner", Authorize: allow}); err != nil {
		t.Fatalf("add the owner: %v", err)
	}
	if _, err := ms.Add(ctx, membership.Change{OrgID: org, PrincipalID: member, Role: "member", Authorize: allow}); err != nil {
		t.Fatalf("add the member: %v", err)
	}
	e.setAuthJoin(t, ctx, org, member, nil)

	locked, release := make(chan struct{}), make(chan struct{})
	afterFillRowLock = func(context.Context) {
		afterFillRowLock = nil
		close(locked)
		<-release
	}
	t.Cleanup(func() { afterFillRowLock = nil })
	joined := time.Now().UTC().Truncate(time.Second)
	fillErr, transferErr := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := e.store.BackfillMembership(ctx, MembershipBackfillTarget{OrgID: org, UserID: member, Role: "member", JoinedAt: &joined, InvitedBy: &inviter}, false)
		fillErr <- err
	}()
	select {
	case <-locked:
	case err := <-fillErr:
		t.Fatalf("the fill ended before it held the membership row: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("the fill never reached the membership row lock")
	}
	go func() {
		transferErr <- ms.TransferOwnership(ctx, membership.Transfer{OrgID: org, From: inviter, To: member, Authorize: allow})
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
		t.Fatalf("lock waiters while the fill holds the membership row: got %d, want 1", waiters)
	}
	close(release)
	for name, ch := range map[string]chan error{"fill": fillErr, "transfer": transferErr} {
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
