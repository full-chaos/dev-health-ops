//go:build integration

package usersbackfill

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func (e *reconEnv) count(t *testing.T, ctx context.Context, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func (e *reconEnv) seedOrg(t *testing.T, ctx context.Context, active bool) (uuid.UUID, string) {
	t.Helper()
	id, slug := uuid.New(), "org-"+uuid.NewString()[:8]
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name, is_active, tier, created_at, updated_at)
VALUES ($1, $2, $3, $4, 'free', now(), now())`, id, slug, "Name "+slug, active); err != nil {
		t.Fatalf("seed public.organizations: %v", err)
	}
	return id, slug
}

func (e *reconEnv) seedMember(t *testing.T, ctx context.Context, org, user uuid.UUID, role string) {
	t.Helper()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, now(), now(), now())`, uuid.New(), user, org, role); err != nil {
		t.Fatalf("seed public.memberships: %v", err)
	}
}

func (e *reconEnv) runOrgs(t *testing.T, ctx context.Context, dry bool) OrgReport {
	t.Helper()
	rep, err := RunOrgs(ctx, PGOrgSource{Pool: e.runtime}, e.store, PGAuthOnly{Pool: e.runtime, Schema: e.schema}, Options{DryRun: dry, BatchSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func (e *reconEnv) writes(t *testing.T, ctx context.Context) [3]int {
	t.Helper()
	return [3]int{
		e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events`),
		e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events`),
		e.count(t, ctx, `SELECT coalesce(sum(revision), 0) FROM auth.aggregate_revisions`),
	}
}

func TestOrgsBackfillInsertsThenRerunChangesNothing(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	active, slugA := e.seedOrg(t, ctx, true)
	inactive, _ := e.seedOrg(t, ctx, false)
	u1 := e.seed(t, ctx, uniq("om"), true, true)
	u2 := e.seed(t, ctx, uniq("om"), true, true)
	e.backfill(t, ctx)
	e.seedMember(t, ctx, active, u1, "owner")
	e.seedMember(t, ctx, active, u2, "viewer")
	e.seedMember(t, ctx, inactive, u1, "member")

	before := e.writes(t, ctx)
	dry := e.runOrgs(t, ctx, true)
	if dry.OrgsInserted < 2 || dry.MembershipsInserted != 3 || dry.SkippedTotal() != 0 || e.writes(t, ctx) != before {
		t.Fatalf("dry run: %+v", dry)
	}
	if e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1`, active) != 0 {
		t.Fatal("dry run wrote an organization")
	}

	rep := e.runOrgs(t, ctx, false)
	if rep.OrgsInserted < 2 || rep.MembershipsInserted != 3 || rep.SkippedTotal() != 0 || rep.AuthOnlyMemberships != 0 {
		t.Fatalf("first run counts: got %+v, want 3 inserted, 1 inviter set, 1 unresolved, 0 skipped", rep)
	}
	if e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND status = 'active' AND role IN ('owner','viewer')`, active) != 2 {
		t.Fatal("memberships not as mapped")
	}
	if e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND slug = $2 AND disabled_at IS NULL`, active, slugA) != 1 ||
		e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NOT NULL`, inactive) != 1 {
		t.Fatal("organizations not as mapped")
	}
	if e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE payload->>'organization_id' = 'org_'||$1::text`, active) < 3 {
		t.Fatal("no provisioning events for the organization")
	}

	after := e.writes(t, ctx)
	var disabledBefore time.Time
	if err := e.admin.QueryRow(ctx, `SELECT disabled_at FROM auth.organizations WHERE id = $1`, inactive).Scan(&disabledBefore); err != nil {
		t.Fatal(err)
	}
	again := e.runOrgs(t, ctx, false)
	if again.OrgsInserted+again.OrgsUpdated+again.MembershipsInserted+again.MembershipsRoleSet != 0 || again.SkippedTotal() != 0 {
		t.Fatalf("rerun wrote: %+v", again)
	}
	if e.writes(t, ctx) != after {
		t.Fatal("rerun changed audit rows, outbox rows or revisions")
	}
	var disabledAfter time.Time
	if err := e.admin.QueryRow(ctx, `SELECT disabled_at FROM auth.organizations WHERE id = $1`, inactive).Scan(&disabledAfter); err != nil {
		t.Fatal(err)
	}
	if !disabledAfter.Equal(disabledBefore) {
		t.Fatal("a re-run moved disabled_at of an inactive organization")
	}
}

func TestOrgsBackfillKeepsSuspensionRoleChangeAndDivergence(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	org, _ := e.seedOrg(t, ctx, false)
	u1 := e.seed(t, ctx, uniq("or"), true, true)
	u2 := e.seed(t, ctx, uniq("or"), true, true)
	e.backfill(t, ctx)
	e.seedMember(t, ctx, org, u1, "member")
	e.seedMember(t, ctx, org, u2, "admin")
	e.runOrgs(t, ctx, false)

	if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET is_active = true WHERE id = $1`, org); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE public.memberships SET role = 'owner' WHERE org_id = $1 AND user_id = $2`, org, u1); err != nil {
		t.Fatal(err)
	}
	// go-api's reactivation does not reactivate the auth organization: a
	// suspension in the auth schema ends only by an auth-service verb.
	rep := e.runOrgs(t, ctx, false)
	if rep.OrgsReactivated != 0 || rep.MembershipsRoleSet != 1 || rep.SkippedTotal() != 0 {
		t.Fatalf("second run: %+v", rep)
	}
	if e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NOT NULL`, org) != 1 {
		t.Fatal("the backfill cleared disabled_at of a suspended organization")
	}
	if e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'organization.reactivated' AND aggregate_id = 'org_'||$1::text`, org) != 0 {
		t.Fatal("an organization.reactivated event")
	}
	if e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2 AND role = 'owner'`, org, u1) != 1 ||
		e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'membership.role_changed'`) < 1 {
		t.Fatal("role change not applied with its event")
	}

	// A status the auth schema set is never overwritten: reported, not re-activated.
	if _, err := e.admin.Exec(ctx, `ALTER TABLE auth.memberships DISABLE TRIGGER memberships_commit_guard`); err != nil {
		t.Fatalf("test setup needs owner access: %v", err)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE auth.memberships SET status = 'removed' WHERE organization_id = $1 AND principal_id = $2`, org, u2); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `ALTER TABLE auth.memberships ENABLE TRIGGER memberships_commit_guard`); err != nil {
		t.Fatal(err)
	}
	before := e.writes(t, ctx)
	rep = e.runOrgs(t, ctx, false)
	if rep.AuthDiverged != 1 || rep.MembershipsInserted+rep.MembershipsRoleSet != 0 || e.writes(t, ctx) != before {
		t.Fatalf("divergence run: %+v", rep)
	}
	if e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2 AND status = 'removed'`, org, u2) != 1 {
		t.Fatal("a removed membership was re-activated")
	}
	// Leave the pair matched (active, same role in both schemas): a removed auth membership over a public member is
	// a gated users-reconcile class, and one without a public row is an auth-only membership.
	e.setAuthMembershipStatus(t, ctx, org, u2, "active")
}

func TestOrgsBackfillRefusalsAndAuthOnly(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	org, slug := e.seedOrg(t, ctx, true)
	known := e.seed(t, ctx, uniq("rf"), true, true)
	other := e.seed(t, ctx, uniq("rf"), true, true)
	e.backfill(t, ctx)
	e.seedMember(t, ctx, org, known, "owner")
	e.seedMember(t, ctx, org, other, "superadmin")
	rep := e.runOrgs(t, ctx, false)
	if rep.Skipped[SkipMembershipUnknownRole] != 1 {
		t.Fatalf("unknown role: %+v", rep.Skipped)
	}
	if e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2`, org, other) != 0 {
		t.Fatal("a membership with an unknown role was written")
	}
	if strings.Contains(strings.Join(rep.Lines(), "\n"), slug) {
		t.Fatal("slug in the report")
	}

	// An auth membership without a public row is counted, never deleted.
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.memberships WHERE org_id = $1 AND user_id = $2`, org, known); err != nil {
		t.Fatal(err)
	}
	rep = e.runOrgs(t, ctx, false)
	if rep.AuthOnlyMemberships < 1 {
		t.Fatalf("auth_only_memberships = %d", rep.AuthOnlyMemberships)
	}
	if e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2`, org, known) != 1 {
		t.Fatal("an auth-only membership was deleted")
	}
	// Leave the shared database with no auth-only row and no refused row for the tests that follow.
	e.seedMember(t, ctx, org, known, "owner")
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.memberships WHERE org_id = $1 AND user_id = $2`, org, other); err != nil {
		t.Fatal(err)
	}
}

func TestSuperuserBecomesPlatformAdminThroughUsersBackfill(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	id := e.seed(t, ctx, uniq("su"), true, true)
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_superuser = true WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	e.backfill(t, ctx)
	if e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1 AND role_key = 'platform_admin'`, id) != 1 {
		t.Fatal("is_superuser did not become platform_admin")
	}
	if e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE principal_id = $1`, id) != 0 {
		t.Fatal("a platform role created a membership")
	}
}

// A page of one row sees every public row once; the unique key on public.memberships refuses a duplicate.
func TestOrgsBackfillBatchOneSeesEveryRowAndDuplicatesAreRefusedByTheSource(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	org1, _ := e.seedOrg(t, ctx, true)
	org2, _ := e.seedOrg(t, ctx, true)
	u1 := e.seed(t, ctx, uniq("b1"), true, true)
	u2 := e.seed(t, ctx, uniq("b1"), true, true)
	e.backfill(t, ctx)
	e.seedMember(t, ctx, org1, u1, "owner")
	e.seedMember(t, ctx, org1, u2, "member")
	e.seedMember(t, ctx, org2, u1, "viewer")

	rep, err := RunOrgs(ctx, PGOrgSource{Pool: e.runtime}, e.store, PGAuthOnly{Pool: e.runtime, Schema: e.schema}, Options{DryRun: true, BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if want := e.count(t, ctx, `SELECT count(*) FROM public.organizations`); rep.OrgsSeen != want {
		t.Fatalf("orgs_seen = %d, public.organizations = %d", rep.OrgsSeen, want)
	}
	if want := e.count(t, ctx, `SELECT count(*) FROM public.memberships`); rep.MembershipsSeen != want {
		t.Fatalf("memberships_seen = %d, public.memberships = %d", rep.MembershipsSeen, want)
	}

	_, err = e.admin.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), u1, org1)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("a duplicate (user_id, org_id) was not refused with 23505: %v", err)
	}
}

// An auth organization without a public row is counted, kept, and reported with the STOP line; the exit stays 0.
func TestOrgsVerbAuthOnlyOrganizationStopsTheCutoverButExitsZero(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	before, err := PGAuthOnly{Pool: e.runtime, Schema: e.schema}.AuthOnly(ctx)
	if err != nil || before.Orgs != 0 || before.Memberships != 0 {
		t.Fatalf("the shared database starts with auth-only rows: %+v %v", before, err)
	}
	orphan, _ := e.seedOrg(t, ctx, true)
	if rep := e.runOrgs(t, ctx, false); rep.OrgsInserted < 1 || rep.SkippedTotal() != 0 {
		t.Fatalf("seeding run: %+v", rep)
	}
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.organizations WHERE id = $1`, orphan); err != nil {
		t.Fatal(err)
	}

	dsn := e.runtime.Config().ConnString()
	lookup := func(name string) (string, bool) {
		switch name {
		case "AUTH_DATABASE_URI":
			return dsn, true
		case "AUTH_DATABASE_SCHEMA":
			return "auth", true
		}
		return "", false
	}
	var stdout, stderr bytes.Buffer
	code := runOrgs(ctx, cli.Env{Args: nil, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	if code != cli.ExitOK {
		t.Fatalf("exit = %d, stderr %q", code, stderr.String())
	}
	for _, line := range []string{"auth_only_orgs=1", "auth_only_memberships=0"} {
		if !strings.Contains(stdout.String(), line+"\n") {
			t.Fatalf("stdout lacks %q: %q", line, stdout.String())
		}
	}
	if !strings.Contains(stderr.String(), "auth_only_orgs is above zero: STOP for the cutover") {
		t.Fatalf("stderr lacks the STOP line: %q", stderr.String())
	}
	if strings.Contains(stderr.String(), "auth_only_memberships is above zero") {
		t.Fatal("the memberships STOP line printed with zero auth-only memberships")
	}
	if e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1`, orphan) != 1 {
		t.Fatal("the auth-only organization was removed")
	}
}

func (e *reconEnv) seedMemberInvitedBy(t *testing.T, ctx context.Context, org, user uuid.UUID, role string, inviter *uuid.UUID) {
	t.Helper()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, invited_by_id, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, now(), now(), now())`, uuid.New(), user, org, role, inviter); err != nil {
		t.Fatalf("seed public.memberships: %v", err)
	}
}

func (e *reconEnv) authInviter(t *testing.T, ctx context.Context, org, user uuid.UUID) *uuid.UUID {
	t.Helper()
	var v *uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT invited_by FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2`, org, user).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

// nullAuthJoin clears an auth membership's joined_at through audit.Commit (the runtime role has no raw path).
func (e *reconEnv) nullAuthJoin(t *testing.T, ctx context.Context, org, user uuid.UUID) {
	t.Helper()
	var id uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT id FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2`, org, user).Scan(&id); err != nil {
		t.Fatal(err)
	}
	orgText, subject, target := org.String(), user.String(), id.String()
	if err := audit.Commit(ctx, e.runtime, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `UPDATE auth.memberships SET joined_at = NULL, updated_at = clock_timestamp() WHERE id = $1`, id)
			return err
		},
		Audit: audit.AuditEvent{
			EventType: "auth.membership.status_changed", Outcome: audit.OutcomeAllowed, OrganizationID: &orgText, SubjectPrincipalID: &subject,
			Attributes: map[string]any{"reason_code": "membership_status_changed", "target_kind": "membership", "target_id": target},
		},
		Events: []audit.OutboxEvent{{AggregateType: "membership", AggregateID: provisioning.WireID("membership", target),
			EventType: "membership.activated", Change: map[string]any{"status": "active"}}},
	}); err != nil {
		t.Fatalf("clear joined_at: %v", err)
	}
}

func TestOrgsBackfillFillsInviterAndJoinAndKeepsWhatAuthHolds(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	org, _ := e.seedOrg(t, ctx, true)
	inv1 := e.seed(t, ctx, uniq("iv"), true, true)
	inv2 := e.seed(t, ctx, uniq("iv"), true, true)
	m1 := e.seed(t, ctx, uniq("iv"), true, true)
	m2 := e.seed(t, ctx, uniq("iv"), true, true)
	m3 := e.seed(t, ctx, uniq("iv"), true, true)
	e.backfill(t, ctx)
	// The organization is in the auth schema before the memberships: a dry run over a not-yet-inserted organization cannot resolve an inviter.
	e.runOrgs(t, ctx, false)
	// An inviter go-api has but the auth schema has no principal for.
	ghost := e.seed(t, ctx, uniq("iv"), true, true)
	e.seedMemberInvitedBy(t, ctx, org, m1, "member", &inv1)
	e.seedMemberInvitedBy(t, ctx, org, m2, "member", nil)
	e.seedMemberInvitedBy(t, ctx, org, m3, "member", &ghost)

	before := e.writes(t, ctx)
	dry := e.runOrgs(t, ctx, true)
	if dry.MembershipsInserted != 3 || dry.InvitersSet != 1 || dry.InviterUnresolved != 1 || dry.SkippedTotal() != 0 {
		t.Fatalf("dry run counts: got %+v, want 3 inserted, 1 inviter set, 1 unresolved, 0 skipped", dry)
	}
	if got := e.writes(t, ctx); got != before {
		t.Fatalf("dry run wrote: got audit/outbox/revision %v, want %v", got, before)
	}
	if got := e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2`, org, m1); got != 0 {
		t.Fatalf("auth membership rows of m1 after a dry run: got %d, want 0", got)
	}

	rep := e.runOrgs(t, ctx, false)
	if rep.MembershipsInserted != 3 || rep.InvitersSet != 1 || rep.InviterUnresolved != 1 || rep.SkippedTotal() != 0 {
		t.Fatalf("first run: %+v", rep)
	}
	if got := e.authInviter(t, ctx, org, m1); got == nil || *got != inv1 {
		t.Fatalf("m1 inviter = %v, want %v", got, inv1)
	}
	for name, u := range map[string]uuid.UUID{"m2 (no public inviter)": m2, "m3 (unresolved inviter)": m3} {
		if got := e.authInviter(t, ctx, org, u); got != nil {
			t.Fatalf("%s: auth inviter got %v, want nil", name, got)
		}
	}

	// A value auth holds is never overwritten by public.
	if _, err := e.admin.Exec(ctx, `UPDATE public.memberships SET invited_by_id = $3 WHERE org_id = $1 AND user_id = $2`, org, m1, inv2); err != nil {
		t.Fatal(err)
	}
	// A missing inviter is filled once the principal exists and public has one.
	if _, err := e.admin.Exec(ctx, `UPDATE public.memberships SET invited_by_id = $3 WHERE org_id = $1 AND user_id = $2`, org, m2, inv2); err != nil {
		t.Fatal(err)
	}
	rep = e.runOrgs(t, ctx, false)
	if rep.InvitersSet != 1 || rep.MembershipsInserted != 0 {
		t.Fatalf("fill run counts: got %+v, want 1 inviter set, 0 inserted", rep)
	}
	if got := e.authInviter(t, ctx, org, m1); got == nil || *got != inv1 {
		t.Fatalf("auth inviter of m1 overwritten: got %v, want %v", got, inv1)
	}
	if got := e.authInviter(t, ctx, org, m2); got == nil || *got != inv2 {
		t.Fatalf("m2 inviter = %v, want %v", got, inv2)
	}
	if got := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.membership.projected'
AND attributes->'changed_fields' = '["invited_by"]'::jsonb AND attributes->>'reason_code' = 'membership_added'`); got != 1 {
		t.Fatalf("projected-membership audit rows naming invited_by: got %d, want 1", got)
	}

	// A cleared auth join time is filled from public, once.
	e.nullAuthJoin(t, ctx, org, m1)
	rep = e.runOrgs(t, ctx, false)
	if rep.JoinedSet != 1 || rep.InvitersSet != 0 {
		t.Fatalf("join fill run counts: got %+v, want 1 joined set, 0 inviter set", rep)
	}
	if got := e.count(t, ctx, `SELECT count(*) FROM auth.memberships a JOIN public.memberships p ON p.org_id = a.organization_id AND p.user_id = a.principal_id
WHERE a.organization_id = $1 AND a.principal_id = $2 AND a.joined_at = p.joined_at`, org, m1); got != 1 {
		t.Fatalf("auth rows whose joined_at equals the public one: got %d, want 1", got)
	}

	// A second run writes nothing: no audit row, no outbox row, no revision.
	after := e.writes(t, ctx)
	again := e.runOrgs(t, ctx, false)
	if again.InvitersSet+again.JoinedSet+again.MembershipsInserted+again.MembershipsRoleSet != 0 || again.SkippedTotal() != 0 {
		t.Fatalf("rerun counts: got %+v, want 0 set/inserted/role-set, 0 skipped", again)
	}
	if got := e.writes(t, ctx); got != after {
		t.Fatalf("rerun wrote: got audit/outbox/revision %v, want %v", got, after)
	}
	if again.InviterUnresolved != 1 {
		t.Fatalf("unresolved inviter count on rerun: got %d, want 1", again.InviterUnresolved)
	}
	// Leave the shared database matched: the inviter's user projected, every membership active in both schemas.
	e.backfill(t, ctx)
	e.runOrgs(t, ctx, false)
}

func TestOrgsBackfillLeavesAnInactiveMembershipAlone(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	org, _ := e.seedOrg(t, ctx, true)
	inv := e.seed(t, ctx, uniq("ia"), true, true)
	m := e.seed(t, ctx, uniq("ia"), true, true)
	e.backfill(t, ctx)
	e.seedMemberInvitedBy(t, ctx, org, m, "member", nil)
	e.runOrgs(t, ctx, false)
	e.setAuthMembershipStatus(t, ctx, org, m, "removed")
	if _, err := e.admin.Exec(ctx, `UPDATE public.memberships SET invited_by_id = $3 WHERE org_id = $1 AND user_id = $2`, org, m, inv); err != nil {
		t.Fatal(err)
	}
	type rowState struct {
		ID      uuid.UUID
		Status  string
		Inviter *uuid.UUID
		Joined  *time.Time
		Updated time.Time
	}
	readRow := func() rowState {
		var r rowState
		if err := e.admin.QueryRow(ctx, `SELECT id, status, invited_by, joined_at, updated_at FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2`, org, m).
			Scan(&r.ID, &r.Status, &r.Inviter, &r.Joined, &r.Updated); err != nil {
			t.Fatal(err)
		}
		return r
	}
	auditRows := func(id uuid.UUID) int {
		return e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE attributes->>'target_id' = $1`, id.String())
	}
	rowBefore := readRow()
	auditBefore := auditRows(rowBefore.ID)
	before := e.writes(t, ctx)
	rep := e.runOrgs(t, ctx, false)
	if rep.InvitersSet != 0 || rep.JoinedSet != 0 || rep.AuthDiverged != 1 {
		t.Fatalf("inactive run counts: got %+v, want 0 inviter set, 0 joined set, 1 diverged", rep)
	}
	if got := e.writes(t, ctx); got != before {
		t.Fatalf("inactive run wrote: got audit/outbox/revision %v, want %v", got, before)
	}
	rowAfter := readRow()
	if rowAfter.Status != "removed" || rowAfter.Inviter != nil || !rowAfter.Updated.Equal(rowBefore.Updated) ||
		(rowAfter.Joined == nil) != (rowBefore.Joined == nil) || (rowAfter.Joined != nil && !rowAfter.Joined.Equal(*rowBefore.Joined)) {
		t.Fatalf("inactive row: got %+v, want status removed, inviter nil and the other columns as before %+v", rowAfter, rowBefore)
	}
	if got := auditRows(rowBefore.ID); got != auditBefore {
		t.Fatalf("audit rows naming the inactive membership: got %d, want %d", got, auditBefore)
	}
	e.setAuthMembershipStatus(t, ctx, org, m, "active")
}

// The grant lands before the backfill: without SELECT on invited_by_id the source read fails and nothing is written.
func TestOrgsBackfillWithoutTheInviterGrantFailsAndWritesNothing(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	org, _ := e.seedOrg(t, ctx, true)
	inv := e.seed(t, ctx, uniq("ig"), true, true)
	m := e.seed(t, ctx, uniq("ig"), true, true)
	e.backfill(t, ctx)
	e.runOrgs(t, ctx, false)
	e.seedMemberInvitedBy(t, ctx, org, m, "member", &inv)
	var role string
	if err := e.runtime.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `REVOKE SELECT (invited_by_id) ON public.memberships FROM `+pgx.Identifier{role}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := e.admin.Exec(context.Background(), `GRANT SELECT (invited_by_id) ON public.memberships TO `+pgx.Identifier{role}.Sanitize()); err != nil {
			t.Errorf("restore the invited_by_id grant: %v", err)
		}
		e.runOrgs(t, context.Background(), false)
	})
	before := e.writes(t, ctx)
	if _, err := RunOrgs(ctx, PGOrgSource{Pool: e.runtime}, e.store, PGAuthOnly{Pool: e.runtime, Schema: e.schema}, Options{BatchSize: 2}); err == nil {
		t.Fatal("the run succeeded without SELECT on invited_by_id: got nil error, want a refused read")
	}
	if got := e.writes(t, ctx); got != before {
		t.Fatalf("a refused read wrote: got audit/outbox/revision %v, want %v", got, before)
	}
	if got := e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2`, org, m); got != 0 {
		t.Fatalf("auth rows for the member after a refused read: got %d, want 0", got)
	}
}

// A missing source grant fails before any write, even on a fresh organization nothing has projected yet.
func TestOrgsBackfillWithoutTheInviterGrantOnAFreshOrganizationWritesNothing(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	inv := e.seed(t, ctx, uniq("if"), true, true)
	m := e.seed(t, ctx, uniq("if"), true, true)
	e.backfill(t, ctx)
	e.runOrgs(t, ctx, false)
	org, _ := e.seedOrg(t, ctx, true)
	e.seedMemberInvitedBy(t, ctx, org, m, "member", &inv)
	var role string
	if err := e.runtime.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `REVOKE SELECT (invited_by_id) ON public.memberships FROM `+pgx.Identifier{role}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := e.admin.Exec(context.Background(), `GRANT SELECT (invited_by_id) ON public.memberships TO `+pgx.Identifier{role}.Sanitize()); err != nil {
			t.Errorf("restore the invited_by_id grant: %v", err)
		}
		e.runOrgs(t, context.Background(), false)
	})
	before := e.writes(t, ctx)
	_, err := RunOrgs(ctx, PGOrgSource{Pool: e.runtime}, e.store, PGAuthOnly{Pool: e.runtime, Schema: e.schema}, Options{BatchSize: 2})
	if err == nil || err.Error() != "usersbackfill: reading public.memberships failed" {
		t.Fatalf("error: got %v, want usersbackfill: reading public.memberships failed", err)
	}
	if got := e.writes(t, ctx); got != before {
		t.Fatalf("a refused read wrote: got audit/outbox/revision %v, want %v", got, before)
	}
	if got := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1`, org); got != 0 {
		t.Fatalf("auth organization rows of the fresh organization: got %d, want 0", got)
	}
	if got := e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1`, org); got != 0 {
		t.Fatalf("auth membership rows of the fresh organization: got %d, want 0", got)
	}
}
