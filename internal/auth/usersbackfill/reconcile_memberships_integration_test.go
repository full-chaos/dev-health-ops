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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// orgDiffs reads the membership counts of one organization in one read-only
// snapshot, so rows other tests leave in the shared database do not count.
func (e *reconEnv) orgDiffs(t *testing.T, ctx context.Context, org uuid.UUID) MembershipDiffs {
	t.Helper()
	tx, err := OpenSnapshot(ctx, e.runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	d, err := TxTarget{Tx: tx, Schema: e.schema}.membershipDiffsIn(ctx, &org)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// wholeInactive is the gated membership count over the whole shared database.
func (e *reconEnv) wholeInactive(t *testing.T, ctx context.Context) int {
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
	return rep.Diffs[DiffMembershipInactiveInPublic]
}

// setAuthMembershipStatus sets an auth membership's status through
// audit.Commit, as an auth-side status writer would. An equal status is left
// as it is.
func (e *reconEnv) setAuthMembershipStatus(t *testing.T, ctx context.Context, org, user uuid.UUID, status string) {
	t.Helper()
	var id uuid.UUID
	var current string
	if err := e.admin.QueryRow(ctx, `SELECT id, status FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2`, org, user).
		Scan(&id, &current); err != nil {
		t.Fatal(err)
	}
	if current == status {
		return
	}
	event := map[string]string{"active": "membership.activated", "removed": "membership.removed"}[status]
	if event == "" {
		t.Fatalf("no outbox event for status %q", status)
	}
	orgText, subject, target := org.String(), user.String(), id.String()
	err := audit.Commit(ctx, e.runtime, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `UPDATE auth.memberships SET status = $2, updated_at = clock_timestamp() WHERE id = $1`, id, status)
			return err
		},
		Audit: audit.AuditEvent{
			EventType: "auth.membership.status_changed", Outcome: audit.OutcomeAllowed, OrganizationID: &orgText, SubjectPrincipalID: &subject,
			Attributes: map[string]any{"reason_code": "membership_status_changed", "target_kind": "membership", "target_id": target},
		},
		Events: []audit.OutboxEvent{{AggregateType: "membership", AggregateID: provisioning.WireID("membership", target),
			EventType: event, Change: map[string]any{"status": status}}},
	})
	if err != nil {
		t.Fatalf("set the auth membership %s: %v", status, err)
	}
}

// matchOrgOnCleanup leaves every membership pair of org matched when the test
// ends, whatever it changed: each auth membership active with a public row of
// its role, no public membership without an auth row, and the listed public
// users (never copied to the auth schema) deleted. No membership row of org is
// then auth-only, public-only, inactive over a public member or of another
// role, so the whole-database checks of the other tests in the package do not
// see it. The auth rows stay (the runtime role has no DELETE on them).
func (e *reconEnv) matchOrgOnCleanup(t *testing.T, org uuid.UUID, publicOnlyUsers *[]uuid.UUID) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := e.admin.Exec(ctx, `DELETE FROM public.memberships p WHERE p.org_id = $1 AND NOT EXISTS
  (SELECT 1 FROM auth.memberships a WHERE a.organization_id = p.org_id AND a.principal_id = p.user_id)`, org); err != nil {
			t.Errorf("cleanup public-only memberships: %v", err)
			return
		}
		rows, err := e.admin.Query(ctx, `SELECT principal_id, role FROM auth.memberships WHERE organization_id = $1`, org)
		if err != nil {
			t.Errorf("cleanup read: %v", err)
			return
		}
		type pair struct {
			user uuid.UUID
			role string
		}
		var pairs []pair
		for rows.Next() {
			var p pair
			if err := rows.Scan(&p.user, &p.role); err != nil {
				rows.Close()
				t.Errorf("cleanup scan: %v", err)
				return
			}
			pairs = append(pairs, p)
		}
		rows.Close()
		for _, p := range pairs {
			e.setAuthMembershipStatus(t, ctx, org, p.user, "active")
			if _, err := e.admin.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at, created_at, updated_at)
SELECT $1, $2, $3, $4, now(), now(), now()
WHERE NOT EXISTS (SELECT 1 FROM public.memberships WHERE user_id = $2 AND org_id = $3)`, uuid.New(), p.user, org, p.role); err != nil {
				t.Errorf("cleanup restore public row: %v", err)
				return
			}
			if _, err := e.admin.Exec(ctx, `UPDATE public.memberships SET role = $3 WHERE user_id = $1 AND org_id = $2 AND role <> $3`, p.user, org, p.role); err != nil {
				t.Errorf("cleanup restore public role: %v", err)
				return
			}
		}
		if publicOnlyUsers != nil && len(*publicOnlyUsers) > 0 {
			if _, err := e.admin.Exec(ctx, `DELETE FROM public.users WHERE id = ANY($1)`, *publicOnlyUsers); err != nil {
				t.Errorf("cleanup public-only users: %v", err)
				return
			}
		}
		if d := e.orgDiffs(t, ctx, org); d != (MembershipDiffs{}) {
			t.Errorf("after cleanup the organization still has membership differences: %+v", d)
		}
	})
}

// After the organizations backfill an organization has no membership
// difference; each go-api change and each auth status change shows up in
// exactly its class, counted for this organization only.
func TestUsersReconcileMembershipClasses(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	org, _ := e.seedOrg(t, ctx, true)
	var publicOnlyUsers []uuid.UUID
	e.matchOrgOnCleanup(t, org, &publicOnlyUsers)
	removed := e.seed(t, ctx, uniq("mr"), true, true)
	roled := e.seed(t, ctx, uniq("mr"), true, true)
	inactive := e.seed(t, ctx, uniq("mr"), true, true)
	publicOnly := e.seed(t, ctx, uniq("mr"), true, true)
	e.backfill(t, ctx)
	e.seedMember(t, ctx, org, removed, "member")
	e.seedMember(t, ctx, org, roled, "member")
	e.seedMember(t, ctx, org, inactive, "admin")
	e.runOrgs(t, ctx, false)
	if d := e.orgDiffs(t, ctx, org); d != (MembershipDiffs{}) {
		t.Fatalf("after the organizations backfill: %+v, want none", d)
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
	publicOnlyUsers = append(publicOnlyUsers, notInAuth)
	e.seedMember(t, ctx, org, notInAuth, "member")
	e.setAuthMembershipStatus(t, ctx, org, inactive, "removed")
	if d := e.orgDiffs(t, ctx, org); d != (MembershipDiffs{PublicMissing: 1, InactiveInPublic: 1, RoleDiff: 1, AuthMissing: 1}) {
		t.Fatalf("after the planted changes: %+v, want one in each class", d)
	}

	// go-api removes the inactive member too: nothing is inactive over a public member any more.
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.memberships WHERE org_id = $1 AND user_id = $2`, org, inactive); err != nil {
		t.Fatal(err)
	}
	if d := e.orgDiffs(t, ctx, org); d != (MembershipDiffs{PublicMissing: 1, RoleDiff: 1, AuthMissing: 1}) {
		t.Fatalf("after the public removal: %+v, want no inactive diff and the other classes unchanged", d)
	}
}

// A sign-in join, of a known account and of a new one, leaves no membership
// difference and no organization mismatch.
func TestSignInJoinLeavesNoMembershipDiff(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	org, _ := e.seedOrg(t, ctx, true)
	e.matchOrgOnCleanup(t, org, nil)
	known := e.seed(t, ctx, uniq("sj"), true, true)
	e.backfill(t, ctx)
	row := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.sso_providers (id, org_id, name, protocol, status, config)
VALUES ($1, $2, $3, 'oidc', 'active', '{}'::json)`, row, org, "oidc-"+row.String()); err != nil {
		t.Fatal(err)
	}
	// The owned role in a pool of this cell's own: TestLegacyLinkPurgeVerbEndToEnd closes the shared one (the verb's defer pool.Close()).
	pool, err := pgxpool.NewWithConfig(ctx, e.ownedRuntime(t).Config())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	owned, err := identity.New(identity.Config{Pool: pool, Schema: e.schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	join := identity.SSOJoin{OrgID: org, Role: "viewer"}
	if err := owned.JoinOnSignIn(ctx, known, join, identity.SelfSignUp(nil), nil); err != nil {
		t.Fatalf("join: %v", err)
	}
	subject := "sub-" + uuid.NewString()
	if _, err := owned.ProvisionAndJoin(ctx, identity.ProvisionRequest{
		Key:   identity.ExternalKey{Issuer: "https://idp.example.test", Provider: identity.Provider(row.String()), Subject: subject},
		Email: "sj-" + uuid.NewString()[:8] + "@example.com", Actor: identity.SelfSignUp(nil), LegacyProvider: "oidc", LegacySubject: subject,
	}, join, nil); err != nil {
		t.Fatalf("new account: %v", err)
	}
	if d := e.orgDiffs(t, ctx, org); d != (MembershipDiffs{}) {
		t.Fatalf("after a sign-in join: %+v, want none", d)
	}
	if classes, ok := classesOf(e.orgsSnapshot(t, ctx), org); ok {
		t.Fatalf("a sign-in join is a mismatch: classes %v", classes)
	}
}

// The users-reconcile verb fails on an inactive auth membership over a public
// member and prints the membership lines without an id.
func TestVerbGatesMembershipInactiveInPublic(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	org, _ := e.seedOrg(t, ctx, true)
	e.matchOrgOnCleanup(t, org, nil)
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
	before := e.wholeInactive(t, ctx)
	e.setAuthMembershipStatus(t, ctx, org, user, "removed")
	code, out := run()
	if code != cli.ExitFailure || !strings.Contains(out, "info.membership_public_missing=") ||
		!strings.Contains(out, "diff.membership_inactive_in_public="+strconv.Itoa(before+1)) {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if strings.Contains(out, user.String()) || strings.Contains(out, org.String()) {
		t.Fatal("the verb printed an id")
	}
}
