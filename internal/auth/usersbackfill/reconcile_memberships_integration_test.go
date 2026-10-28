//go:build integration

package usersbackfill

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// memberDiffs reads the membership counts in one read-only snapshot.
func (e *reconEnv) memberDiffs(t *testing.T, ctx context.Context) Reconciliation {
	t.Helper()
	tx, err := OpenSnapshot(ctx, e.runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	rep := Reconciliation{Diffs: map[string]int{}}
	if err := ReconcileMemberships(ctx, TxTarget{Tx: tx, Schema: e.schema}, &rep); err != nil {
		t.Fatal(err)
	}
	return rep
}

type memberCounts struct{ publicMissing, inactive, roleDiff, authMissing int }

func countsOf(r Reconciliation) memberCounts {
	return memberCounts{r.MembershipPublicMissing, r.Diffs[DiffMembershipInactiveInPublic], r.MembershipRoleDiff, r.MembershipAuthMissing}
}

func (c memberCounts) minus(b memberCounts) memberCounts {
	return memberCounts{c.publicMissing - b.publicMissing, c.inactive - b.inactive, c.roleDiff - b.roleDiff, c.authMissing - b.authMissing}
}

// removeAuthMembership sets an auth membership to removed through
// audit.Commit, as an auth-side status writer would.
func (e *reconEnv) removeAuthMembership(t *testing.T, ctx context.Context, org, user uuid.UUID) {
	t.Helper()
	var id uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT id FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2`, org, user).Scan(&id); err != nil {
		t.Fatal(err)
	}
	orgText, subject, target := org.String(), user.String(), id.String()
	err := audit.Commit(ctx, e.runtime, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `UPDATE auth.memberships SET status = 'removed', updated_at = clock_timestamp() WHERE id = $1`, id)
			return err
		},
		Audit: audit.AuditEvent{
			EventType: "auth.membership.status_changed", Outcome: audit.OutcomeAllowed, OrganizationID: &orgText, SubjectPrincipalID: &subject,
			Attributes: map[string]any{"reason_code": "membership_status_changed", "target_kind": "membership", "target_id": target},
		},
		Events: []audit.OutboxEvent{{AggregateType: "membership", AggregateID: provisioning.WireID("membership", target),
			EventType: "membership.removed", Change: map[string]any{"status": "removed"}}},
	})
	if err != nil {
		t.Fatalf("set the auth membership removed: %v", err)
	}
}

// After the organizations backfill the membership counts do not move; each
// go-api change and each auth status change shows up in exactly its class,
// and only an auth status that is not active over a public member is gated.
func TestUsersReconcileMembershipClasses(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	// Earlier tests in the package share the database: compare counts with a baseline.
	base := countsOf(e.memberDiffs(t, ctx))
	org, _ := e.seedOrg(t, ctx, true)
	removed := e.seed(t, ctx, uniq("mr"), true, true)
	roled := e.seed(t, ctx, uniq("mr"), true, true)
	inactive := e.seed(t, ctx, uniq("mr"), true, true)
	publicOnly := e.seed(t, ctx, uniq("mr"), true, true)
	e.backfill(t, ctx)
	e.seedMember(t, ctx, org, removed, "member")
	e.seedMember(t, ctx, org, roled, "member")
	e.seedMember(t, ctx, org, inactive, "admin")
	e.runOrgs(t, ctx, false)
	if d := countsOf(e.memberDiffs(t, ctx)).minus(base); d != (memberCounts{}) {
		t.Fatalf("after the organizations backfill: %+v, want no change", d)
	}

	if _, err := e.admin.Exec(ctx, `DELETE FROM public.memberships WHERE org_id = $1 AND user_id = $2`, org, removed); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE public.memberships SET role = 'admin' WHERE org_id = $1 AND user_id = $2`, org, roled); err != nil {
		t.Fatal(err)
	}
	e.seedMember(t, ctx, org, publicOnly, "viewer")
	// A public user the auth schema does not hold is out of scope (users-reconcile counts it as missing_in_auth).
	notInAuth := e.seed(t, ctx, uniq("mr"), true, true)
	e.seedMember(t, ctx, org, notInAuth, "member")
	e.removeAuthMembership(t, ctx, org, inactive)
	rep := e.memberDiffs(t, ctx)
	if d := countsOf(rep).minus(base); d != (memberCounts{publicMissing: 1, inactive: 1, roleDiff: 1, authMissing: 1}) {
		t.Fatalf("after the planted changes: %+v, want one in each class", d)
	}
	if rep.GatedTotal() != base.inactive+1 {
		t.Fatalf("gated = %d, want only the inactive class (%d)", rep.GatedTotal(), base.inactive+1)
	}

	// go-api removes the inactive member too: nothing is gated for it any more.
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.memberships WHERE org_id = $1 AND user_id = $2`, org, inactive); err != nil {
		t.Fatal(err)
	}
	if d := countsOf(e.memberDiffs(t, ctx)).minus(base); d != (memberCounts{publicMissing: 1, roleDiff: 1, authMissing: 1}) {
		t.Fatalf("after the public removal: %+v, want no inactive diff and the other classes unchanged", d)
	}
}

// The users-reconcile verb fails on an inactive auth membership over a public
// member and prints the membership lines without an id.
func TestVerbGatesMembershipInactiveInPublic(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	org, _ := e.seedOrg(t, ctx, true)
	user := e.seed(t, ctx, uniq("mv"), true, true)
	e.backfill(t, ctx)
	e.seedMember(t, ctx, org, user, "member")
	e.runOrgs(t, ctx, false)
	dsn := e.runtime.Config().ConnString()
	run := func() (int, string) {
		var stdout, stderr bytes.Buffer
		code := reconcileVerbForTest().Run(ctx, cli.Env{
			Lookup: func(k string) (string, bool) {
				if k == dsnEnv {
					return dsn, true
				}
				return "", false
			}, Stdout: &stdout, Stderr: &stderr})
		return code, stdout.String() + stderr.String()
	}
	before := countsOf(e.memberDiffs(t, ctx)).inactive
	e.removeAuthMembership(t, ctx, org, user)
	code, out := run()
	if code != cli.ExitFailure || !strings.Contains(out, "info.membership_public_missing=") ||
		!strings.Contains(out, "diff.membership_inactive_in_public="+strconv.Itoa(before+1)) {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if strings.Contains(out, user.String()) || strings.Contains(out, org.String()) {
		t.Fatal("the verb printed an id")
	}
	// Leave the shared database with no gated membership row.
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.memberships WHERE org_id = $1 AND user_id = $2`, org, user); err != nil {
		t.Fatal(err)
	}
}
