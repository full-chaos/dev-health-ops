//go:build integration

package session

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// guardedCase is one user, member of A and B, with a session in A (the
// predecessor) and a second session in A (another device).
type guardedCase struct {
	user, orgA, orgB uuid.UUID
	predecessor      Issued
	other            Issued
}

func (d *delegationEnv) guardedCase(t *testing.T, ctx context.Context) guardedCase {
	t.Helper()
	c := guardedCase{user: d.user(t, ctx, false), orgA: d.org(t, ctx), orgB: d.org(t, ctx)}
	d.member(t, ctx, c.orgA, c.user, "admin")
	d.member(t, ctx, c.orgB, c.user, "member")
	var err error
	if c.predecessor, err = d.owned.Create(ctx, c.user, &c.orgA, []string{"pwd"}); err != nil {
		t.Fatal(err)
	}
	if c.other, err = d.owned.Create(ctx, c.user, &c.orgA, []string{"pwd"}); err != nil {
		t.Fatal(err)
	}
	return c
}

func (d *delegationEnv) liveIn(t *testing.T, ctx context.Context, user, org uuid.UUID) int {
	t.Helper()
	var n int
	if err := d.admin.QueryRow(ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1 AND organization_id = $2 AND revoked_at IS NULL`,
		user.String(), org.String()).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (d *delegationEnv) revoked(t *testing.T, ctx context.Context, id uuid.UUID) bool {
	t.Helper()
	var revoked bool
	if err := d.admin.QueryRow(ctx, `SELECT revoked_at IS NOT NULL FROM auth.sessions WHERE id = $1`, id.String()).Scan(&revoked); err != nil {
		t.Fatal(err)
	}
	return revoked
}

// TestCreateSupersedingGuarded runs the guarded supersede cells over one
// PostgreSQL, as the auth runtime role with the users owned: each cell takes
// its own user and organizations.
func TestCreateSupersedingGuarded(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	for _, cell := range []struct {
		name string
		run  func(*testing.T, context.Context, *delegationEnv)
	}{
		{"RunsGuardInTxBeforeTheWrites", testSupersedingGuardedRunsGuardInTxBeforeTheWrites},
		{"GuardRefusalRollsBackTheRevoke", testSupersedingGuardedRefusalRollsBackTheRevoke},
		{"GuardErrorFailsAndWritesNothing", testSupersedingGuardedErrorFailsAndWritesNothing},
		{"PublicRowCommitsWithTheSwitch", testSupersedingGuardedPublicRowCommitsWithTheSwitch},
		{"PredecessorRefusalsStand", testSupersedingGuardedPredecessorRefusalsStand},
	} {
		t.Run(cell.name, func(t *testing.T) { cell.run(t, ctx, d) })
	}
}

// The guard runs on the switch's own transaction, with the predecessor's
// record, while the predecessor is still live and before the successor
// exists, and can hold the membership FOR SHARE as the runtime role.
func testSupersedingGuardedRunsGuardInTxBeforeTheWrites(t *testing.T, ctx context.Context, d *delegationEnv) {
	c := d.guardedCase(t, ctx)
	var (
		seen              Record
		predecessorLive   bool
		successorsAtGuard int
		role              string
	)
	guard := func(ctx context.Context, q Queryer, record Record) error {
		seen = record
		if err := q.QueryRow(ctx, `SELECT revoked_at IS NULL FROM auth.sessions WHERE id = $1`, record.ID.String()).Scan(&predecessorLive); err != nil {
			return err
		}
		if err := q.QueryRow(ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1 AND organization_id = $2`,
			c.user.String(), c.orgB.String()).Scan(&successorsAtGuard); err != nil {
			return err
		}
		return q.QueryRow(ctx, `SELECT role FROM public.memberships WHERE user_id = $1 AND org_id = $2 LIMIT 1 FOR SHARE`, c.user, c.orgB).Scan(&role)
	}
	issued, err := d.owned.CreateSupersedingGuarded(ctx, c.user, &c.orgB, []string{"pwd"}, c.predecessor.Session.ID,
		sessionEvent("auth.session.issued"), guard)
	if err != nil {
		t.Fatalf("CreateSupersedingGuarded: %v", err)
	}
	if seen.ID != c.predecessor.Session.ID || !predecessorLive || successorsAtGuard != 0 || role != "member" {
		t.Fatalf("guard saw record %s live=%v successors=%d role=%q, want the live predecessor, none, member",
			seen.ID, predecessorLive, successorsAtGuard, role)
	}
	if !d.revoked(t, ctx, c.predecessor.Session.ID) || d.revoked(t, ctx, c.other.Session.ID) {
		t.Fatal("want the predecessor revoked and the other session live")
	}
	if issued.Session.OrganizationID == nil || *issued.Session.OrganizationID != c.orgB || d.liveIn(t, ctx, c.user, c.orgB) != 1 {
		t.Fatalf("successor %+v, want one live session in B", issued.Session)
	}
}

// A guard refusal is ErrGuardRefused and writes nothing: the predecessor
// stays live, no session exists in B.
func testSupersedingGuardedRefusalRollsBackTheRevoke(t *testing.T, ctx context.Context, d *delegationEnv) {
	c := d.guardedCase(t, ctx)
	_, err := d.owned.CreateSupersedingGuarded(ctx, c.user, &c.orgB, []string{"pwd"}, c.predecessor.Session.ID,
		sessionEvent("auth.session.issued"), func(context.Context, Queryer, Record) error { return ErrGuardRefused })
	if !errors.Is(err, ErrGuardRefused) {
		t.Fatalf("err = %v, want ErrGuardRefused", err)
	}
	if d.revoked(t, ctx, c.predecessor.Session.ID) || d.liveIn(t, ctx, c.user, c.orgB) != 0 {
		t.Fatal("a refused switch revoked the predecessor or created a session")
	}
}

func testSupersedingGuardedErrorFailsAndWritesNothing(t *testing.T, ctx context.Context, d *delegationEnv) {
	c := d.guardedCase(t, ctx)
	down := errors.New("read failed")
	_, err := d.owned.CreateSupersedingGuarded(ctx, c.user, &c.orgB, []string{"pwd"}, c.predecessor.Session.ID,
		sessionEvent("auth.session.issued"), func(context.Context, Queryer, Record) error { return down })
	if !errors.Is(err, ErrStore) || errors.Is(err, ErrGuardRefused) {
		t.Fatalf("err = %v, want ErrStore", err)
	}
	if d.revoked(t, ctx, c.predecessor.Session.ID) || d.liveIn(t, ctx, c.user, c.orgB) != 0 {
		t.Fatal("a failed guard revoked the predecessor or created a session")
	}
	if _, err := d.owned.CreateSupersedingGuarded(ctx, c.user, &c.orgB, []string{"pwd"}, c.predecessor.Session.ID,
		sessionEvent("auth.session.issued"), nil); !errors.Is(err, ErrStore) {
		t.Fatalf("a nil guard: %v, want ErrStore", err)
	}
}

// The context's public row runs in the switch's transaction with the
// successor's principal and organization; its failure rolls the switch back.
func testSupersedingGuardedPublicRowCommitsWithTheSwitch(t *testing.T, ctx context.Context, d *delegationEnv) {
	c := d.guardedCase(t, ctx)
	allow := func(context.Context, Queryer, Record) error { return nil }
	failed := WithPublicRow(ctx, func(context.Context, audit.TxOps, uuid.UUID, *uuid.UUID) error { return errors.New("row refused") })
	if _, err := d.owned.CreateSupersedingGuarded(failed, c.user, &c.orgB, []string{"pwd"}, c.predecessor.Session.ID,
		sessionEvent("auth.session.issued"), allow); err == nil {
		t.Fatal("a failed public row committed")
	}
	if d.revoked(t, ctx, c.predecessor.Session.ID) || d.liveIn(t, ctx, c.user, c.orgB) != 0 {
		t.Fatal("a failed public row left the switch written")
	}
	var principal uuid.UUID
	var org *uuid.UUID
	var successors int
	written := WithPublicRow(ctx, func(ctx context.Context, tx audit.TxOps, p uuid.UUID, o *uuid.UUID) error {
		principal, org = p, o
		return tx.QueryRow(ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1 AND organization_id = $2 AND revoked_at IS NULL`,
			p.String(), o.String()).Scan(&successors)
	})
	if _, err := d.owned.CreateSupersedingGuarded(written, c.user, &c.orgB, []string{"pwd"}, c.predecessor.Session.ID,
		sessionEvent("auth.session.issued"), allow); err != nil {
		t.Fatal(err)
	}
	if principal != c.user || org == nil || *org != c.orgB || successors != 1 {
		t.Fatalf("public row saw %s %v with %d successors, want the user, B and the successor", principal, org, successors)
	}
}

// The predecessor checks of CreateSupersedingAudited stand: an ended
// predecessor, another principal's, or a delegated one is refused before the
// guard runs.
func testSupersedingGuardedPredecessorRefusalsStand(t *testing.T, ctx context.Context, d *delegationEnv) {
	c := d.guardedCase(t, ctx)
	ran := false
	guard := func(context.Context, Queryer, Record) error { ran = true; return nil }
	stranger := d.user(t, ctx, false)
	if _, err := d.owned.CreateSupersedingGuarded(ctx, stranger, &c.orgB, []string{"pwd"}, c.predecessor.Session.ID,
		sessionEvent("auth.session.issued"), guard); !errors.Is(err, ErrPredecessorOtherPrincipal) {
		t.Fatalf("another principal's predecessor: %v", err)
	}
	if _, err := d.owned.CreateSupersedingGuarded(ctx, c.user, &c.orgB, []string{"pwd"}, uuid.New(),
		sessionEvent("auth.session.issued"), guard); !errors.Is(err, ErrPredecessorNotLive) {
		t.Fatalf("an unknown predecessor: %v", err)
	}
	if _, err := d.owned.Revoke(ctx, c.predecessor.Session.ID, Actor{}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.owned.CreateSupersedingGuarded(ctx, c.user, &c.orgB, []string{"pwd"}, c.predecessor.Session.ID,
		sessionEvent("auth.session.issued"), guard); !errors.Is(err, ErrPredecessorNotLive) {
		t.Fatalf("a revoked predecessor: %v", err)
	}
	if ran {
		t.Fatal("the guard ran for a refused predecessor")
	}
}
