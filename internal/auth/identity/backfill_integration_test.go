//go:build integration

package identity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

func bcryptFor(t *testing.T, material string) *string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(material), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	s := string(h)
	return &s
}

func TestBackfillInsertsThenRerunWritesNothing(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	target := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bf"), DisplayName: "Display One", Active: true, Verified: true,
		PasswordHash: bcryptFor(t, "backfill-test-material"), Identity: &ExternalKey{Issuer: "https://github.com", Provider: ProviderGitHub, Subject: uuid.NewString()}}
	res, err := e.store.BackfillUser(ctx, target, false)
	if err != nil || !res.Inserted || !res.IdentityLinked {
		t.Fatalf("first run: %+v %v", res, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND password_algorithm = 'bcrypt' AND password_updated_at IS NULL AND email_verified_at IS NOT NULL`, target.ID); n != 1 {
		t.Fatalf("user row not as mapped: %d", n)
	}
	auditBefore := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1`, target.ID)
	res, err = e.store.BackfillUser(ctx, target, false)
	if err != nil || res != (BackfillResult{}) {
		t.Fatalf("rerun wrote something: %+v %v", res, err)
	}
	if after := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1`, target.ID); after != auditBefore {
		t.Fatalf("rerun wrote audit rows: %d -> %d", auditBefore, after)
	}
}

func TestBackfillDryRunWritesNothing(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	target := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfd"), Active: true}
	res, err := e.store.BackfillUser(ctx, target, true)
	if err != nil || !res.Inserted {
		t.Fatalf("dry run: %+v %v", res, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.principals WHERE id = $1`, target.ID); n != 0 {
		t.Fatalf("dry run wrote a row")
	}
}

func TestBackfillUpdatesOnlyWhatDiffers(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	target := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfu"), Active: true, Verified: false}
	if _, err := e.store.BackfillUser(ctx, target, false); err != nil {
		t.Fatal(err)
	}
	target.Active, target.Verified, target.PasswordHash = false, true, bcryptFor(t, "second-test-material")
	res, err := e.store.BackfillUser(ctx, target, false)
	if err != nil || !res.Updated || res.Inserted {
		t.Fatalf("update: %+v %v", res, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.principals p JOIN auth.users u ON u.principal_id = p.id
WHERE p.id = $1 AND p.disabled_at IS NOT NULL AND u.email_verified_at IS NOT NULL AND u.password_hash IS NOT NULL`, target.ID); n != 1 {
		t.Fatalf("row not updated: %d", n)
	}
	target.DisplayName = "Renamed"
	res, err = e.store.BackfillUser(ctx, target, false)
	if err != nil || !res.DisplayNameDrift || res.Updated {
		t.Fatalf("name drift: %+v %v", res, err)
	}
}

func TestBackfillEmailCollisionAndIdentityConflictAreRefused(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	email := uniqueEmail("bfc")
	first := BackfillTarget{ID: uuid.New(), Email: email, Active: true,
		Identity: &ExternalKey{Issuer: "https://gitlab.com", Provider: ProviderGitLab, Subject: uuid.NewString()}}
	if _, err := e.store.BackfillUser(ctx, first, false); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.BackfillUser(ctx, BackfillTarget{ID: uuid.New(), Email: email, Active: true}, false); !errors.Is(err, ErrEmailCollision) {
		t.Fatalf("collision: %v", err)
	}
	second := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfc"), Active: true, Identity: first.Identity}
	if _, err := e.store.BackfillUser(ctx, second, false); !errors.Is(err, ErrIdentityConflict) {
		t.Fatalf("identity conflict: %v", err)
	}
}

func TestBackfillCarriesSuperuserTokenVersionAndLastLogin(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	login := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	target := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfx"), Active: true, Superuser: true, TokenVersion: 3, LastLoginAt: &login}
	if res, err := e.store.BackfillUser(ctx, target, false); err != nil || !res.Inserted {
		t.Fatalf("insert: %+v %v", res, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.users u JOIN auth.platform_role_assignments r ON r.principal_id = u.principal_id
WHERE u.principal_id = $1 AND u.legacy_token_version = 3 AND u.last_login_at = $2 AND r.role_key = 'platform_admin' AND r.granted_by IS NULL`, target.ID, login); n != 1 {
		t.Fatalf("row not as mapped: %d", n)
	}
	if res, err := e.store.BackfillUser(ctx, target, false); err != nil || res != (BackfillResult{}) {
		t.Fatalf("rerun wrote something: %+v %v", res, err)
	}

	target.TokenVersion = 5
	res, err := e.store.BackfillUser(ctx, target, false)
	if err != nil || !res.Updated {
		t.Fatalf("token version update: %+v %v", res, err)
	}
	later := login.Add(time.Minute)
	target.LastLoginAt = &later
	res, err = e.store.BackfillUser(ctx, target, false)
	if err != nil || !res.LastLoginDrift || res.Updated {
		t.Fatalf("last login alone must be drift: %+v %v", res, err)
	}

	target.Superuser = false
	res, err = e.store.BackfillUser(ctx, target, false)
	if err != nil || !res.RoleRevoked {
		t.Fatalf("revoke: %+v %v", res, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1`, target.ID); n != 0 {
		t.Fatalf("assignment left: %d", n)
	}
}

// While auth-service owns platform roles the backfill neither grants nor
// revokes one: it reports each difference as RoleSkipped and still carries the
// other fields.
func TestBackfillLeavesPlatformRolesAloneWhileOwned(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	roles := func(id uuid.UUID) int {
		return e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1`, id)
	}

	fresh := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfo"), Active: true, Superuser: true, PlatformRolesOwned: true}
	res, err := e.store.BackfillUser(ctx, fresh, false)
	if err != nil || !res.Inserted || !res.RoleSkipped || res.RoleGranted {
		t.Fatalf("insert of a superuser while owned: %+v %v", res, err)
	}
	if n := roles(fresh.ID); n != 0 {
		t.Fatalf("insert while owned wrote %d assignments", n)
	}

	held := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfp"), Active: true, Superuser: true}
	if _, err := e.store.BackfillUser(ctx, held, false); err != nil || roles(held.ID) != 1 {
		t.Fatalf("seed a backfilled admin: %v (rows %d)", err, roles(held.ID))
	}
	held.Superuser, held.PlatformRolesOwned, held.TokenVersion = false, true, 4
	res, err = e.store.BackfillUser(ctx, held, false)
	if err != nil || !res.RoleSkipped || res.RoleRevoked || !res.Updated {
		t.Fatalf("revoke case while owned: %+v %v", res, err)
	}
	if n := roles(held.ID); n != 1 {
		t.Fatalf("revoke case while owned left %d assignments, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND legacy_token_version = 4`, held.ID); n != 1 {
		t.Fatal("the token version was not carried while owned")
	}

	plain := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfq"), Active: true}
	if _, err := e.store.BackfillUser(ctx, plain, false); err != nil {
		t.Fatal(err)
	}
	plain.Superuser, plain.PlatformRolesOwned = true, true
	res, err = e.store.BackfillUser(ctx, plain, false)
	if err != nil || !res.RoleSkipped || res.RoleGranted || res.Updated {
		t.Fatalf("grant case while owned: %+v %v", res, err)
	}
	if n := roles(plain.ID); n != 0 {
		t.Fatalf("grant case while owned wrote %d assignments", n)
	}

	same := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfr"), Active: true, Superuser: true}
	if _, err := e.store.BackfillUser(ctx, same, false); err != nil {
		t.Fatal(err)
	}
	same.PlatformRolesOwned = true
	if res, err := e.store.BackfillUser(ctx, same, false); err != nil || res != (BackfillResult{}) {
		t.Fatalf("no difference while owned: %+v %v", res, err)
	}
}

// plantExpiredRole replaces the platform_admin assignments of principal with
// one that expired a minute ago, granted by grantedBy (nil = the backfill), in
// one audit.Commit.
func plantExpiredRole(t *testing.T, ctx context.Context, e *env, principal uuid.UUID, grantedBy *uuid.UUID) uuid.UUID {
	t.Helper()
	var old []uuid.UUID
	rows, err := e.admin.Query(ctx, `SELECT id FROM auth.platform_role_assignments WHERE principal_id = $1 AND role_key = $2`, principal, PlatformAdminRole)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		old = append(old, id)
	}
	rows.Close()
	planted := uuid.New()
	events := []audit.OutboxEvent{roleEvent(planted, "platform_role_assignment.granted", "active")}
	for _, id := range old {
		events = append(events, roleEvent(id, "platform_role_assignment.revoked", "revoked"))
	}
	err = audit.Commit(ctx, e.store.pool, e.store.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if _, err := tx.Exec(ctx, `DELETE FROM `+e.store.q+`.platform_role_assignments WHERE principal_id = $1 AND role_key = $2`, principal, PlatformAdminRole); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO `+e.store.q+`.platform_role_assignments (id, principal_id, role_key, granted_by, expires_at)
VALUES ($1, $2, $3, $4, statement_timestamp() - interval '1 minute')`, planted, principal, PlatformAdminRole, grantedBy)
			return err
		},
		Audit:  roleAudit("auth.platform_role.granted", "role_granted", principal.String()),
		Events: events,
	})
	if err != nil {
		t.Fatalf("plant an expired assignment: %v", err)
	}
	return planted
}

// An expired assignment is not held: a public superuser whose only assignment
// is an expired one the backfill made gets a fresh one, which replaces the
// expired row in one commit. An expired assignment a person granted is left as
// it is and reported by its id.
func TestBackfillReplacesAnExpiredAssignmentOfASuperuser(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	granter := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfg2"), Active: true, Superuser: true}
	if _, err := e.store.BackfillUser(ctx, granter, false); err != nil {
		t.Fatal(err)
	}
	for name, by := range map[string]*uuid.UUID{"backfilled": nil} {
		target := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfe"), Active: true, Superuser: true}
		if _, err := e.store.BackfillUser(ctx, target, false); err != nil {
			t.Fatal(err)
		}
		expired := plantExpiredRole(t, ctx, e, target.ID, by)
		res, err := e.store.BackfillUser(ctx, target, false)
		if err != nil || !res.RoleGranted || !res.Updated {
			t.Fatalf("%s: a superuser with only an expired assignment: %+v %v", name, res, err)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1 AND granted_by IS NULL
			AND expires_at IS NULL`, target.ID); n != 1 {
			t.Fatalf("%s: active backfilled assignments = %d, want 1", name, n)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE id = $1`, expired); n != 0 {
			t.Fatalf("%s: the expired row is still there", name)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'platform_role_assignment.revoked' AND aggregate_id = $1`,
			"pra_"+expired.String()); n != 1 {
			t.Fatalf("%s: revoked events for the expired row = %d, want 1", name, n)
		}
		if res, err := e.store.BackfillUser(ctx, target, false); err != nil || res != (BackfillResult{}) {
			t.Fatalf("%s: rerun wrote something: %+v %v", name, res, err)
		}
	}
	// A superuser whose expired assignment has a granter: nothing is written,
	// the assignment is reported by id, on every run.
	held := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfk"), Active: true, Superuser: true}
	if _, err := e.store.BackfillUser(ctx, held, false); err != nil {
		t.Fatal(err)
	}
	granted := plantExpiredRole(t, ctx, e, held.ID, &granter.ID)
	audits := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1`, held.ID)
	for run := 0; run < 2; run++ {
		res, err := e.store.BackfillUser(ctx, held, false)
		if err != nil || res.RoleExpiredGranted == nil || *res.RoleExpiredGranted != granted || res.RoleGranted || res.Updated {
			t.Fatalf("run %d: a superuser with an expired granted assignment: %+v %v", run, res, err)
		}
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1`, held.ID); n != 1 {
		t.Fatalf("assignments of the superuser with an expired granted row = %d, want 1 (left as it is)", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE id = $1`, granted); n != 1 {
		t.Fatal("the backfill removed an expired assignment a person granted")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1`, held.ID); n != audits {
		t.Fatalf("the backfill wrote %d audit rows for an expired granted assignment", n-audits)
	}
	// Not a superuser: an expired granted row is not the backfill's and stays.
	plain := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfn"), Active: true}
	if _, err := e.store.BackfillUser(ctx, plain, false); err != nil {
		t.Fatal(err)
	}
	expired := plantExpiredRole(t, ctx, e, plain.ID, &granter.ID)
	if res, err := e.store.BackfillUser(ctx, plain, false); err != nil || res != (BackfillResult{}) {
		t.Fatalf("not a superuser, expired granted row: %+v %v", res, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE id = $1`, expired); n != 1 {
		t.Fatal("the backfill removed an expired assignment it did not grant")
	}
}

func TestBackfillNeverRevokesAnAssignmentWithAGranter(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	granter := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfg"), Active: true}
	target := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfh"), Active: true}
	for _, u := range []BackfillTarget{granter, target} {
		if _, err := e.store.BackfillUser(ctx, u, false); err != nil {
			t.Fatal(err)
		}
	}
	assignment := uuid.New()
	err := audit.Commit(ctx, e.store.pool, e.store.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `INSERT INTO `+e.store.q+`.platform_role_assignments (id, principal_id, role_key, granted_by)
VALUES ($1, $2, $3, $4)`, assignment, target.ID, PlatformAdminRole, granter.ID)
			return err
		},
		Audit:  roleAudit("auth.platform_role.granted", "role_granted", target.ID.String()),
		Events: []audit.OutboxEvent{roleEvent(assignment, "platform_role_assignment.granted", "active")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res, err := e.store.BackfillUser(ctx, target, false); err != nil || res.RoleRevoked || res.RoleGranted {
		t.Fatalf("touched a granted assignment: %+v %v", res, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1`, target.ID); n != 1 {
		t.Fatalf("granted assignment removed: %d", n)
	}
}
