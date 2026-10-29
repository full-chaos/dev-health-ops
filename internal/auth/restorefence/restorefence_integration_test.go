//go:build integration

package restorefence

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

const (
	fenceOrgs  = 3
	fenceCreds = 4
)

// fenceDB is one database with the legacy public schema and the auth lineage
// applied as the migration role (the test's admin pool).
type fenceDB struct {
	admin       *pgxpool.Pool
	runtime     *pgxpool.Pool
	runtimeRole string
	schema      authschema.ValidatedIdentifier
}

func newFenceDB(t *testing.T, ctx context.Context) *fenceDB {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("fence_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE ROLE %q", role)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		t.Fatalf("authschema.Apply: %v", err)
	}
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	// The runtime pool connects as the superuser and switches to the runtime
	// role at connection start, so the privileges under test are the role's.
	cfg := admin.Config().Copy()
	cfg.ConnConfig.RuntimeParams["options"] = "-c role=" + role
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	return &fenceDB{admin: admin, runtime: runtime, runtimeRole: role, schema: schema}
}

func fenceOrgID(i int) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("fence-org-%d", i)))
}

func fenceCredID(i int) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("fence-cred-%d", i)))
}

func (d *fenceDB) must(t *testing.T, ctx context.Context, sql string, args ...any) {
	t.Helper()
	if _, err := d.admin.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func (d *fenceDB) commit(t *testing.T, ctx context.Context, eventType string, orgIndex int, apply func(ctx context.Context, tx audit.TxOps) error) {
	t.Helper()
	err := audit.Commit(ctx, d.admin, d.schema, audit.Mutation{
		Apply: apply,
		Audit: audit.AuditEvent{EventType: eventType, Outcome: audit.OutcomeAllowed},
		Events: []audit.OutboxEvent{{AggregateType: "organization", AggregateID: "org_" + fenceOrgID(orgIndex).String(),
			EventType: "organization.updated"}},
	})
	if err != nil {
		t.Fatalf("%s: %v", eventType, err)
	}
}

func (d *fenceDB) orgUpdate(t *testing.T, ctx context.Context, i int, name string) {
	t.Helper()
	id := fenceOrgID(i)
	d.commit(t, ctx, "organization.updated", i, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `UPDATE auth.organizations SET name = $2 WHERE id = $1`, id, name)
		return err
	})
}

// stateT is the database as it is at the restore target T.
func (d *fenceDB) stateT(t *testing.T, ctx context.Context) {
	t.Helper()
	principal := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-principal"))
	d.must(t, ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'fence')`, principal)
	for i := 0; i < fenceOrgs; i++ {
		id := fenceOrgID(i)
		d.commit(t, ctx, "organization.created", i, func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `INSERT INTO auth.organizations (id, name, slug) VALUES ($1, $2, $2)`, id, "fence-"+id.String())
			return err
		})
		d.orgUpdate(t, ctx, i, fmt.Sprintf("t-%d", i))
	}
	for c := 0; c < fenceCreds; c++ {
		cred := fenceCredID(c)
		d.commit(t, ctx, "credential.created", 0, func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `INSERT INTO auth.credential_registry (id, principal_id, credential_class, secret_hash, hash_algorithm, fingerprint)
VALUES ($1, $2, 'workload_token', decode(md5($1::text), 'hex'), 'sha256', '0123456789abcdef')`, cred, principal)
			return err
		})
	}
	// Legacy rows.
	user := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-user"))
	org := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-legacy-org"))
	d.must(t, ctx, `INSERT INTO public.users (id, email) VALUES ($1, 'fence@example.invalid')`, user)
	d.must(t, ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, 'fence-legacy', 'fence legacy')`, org)
	d.must(t, ctx, `INSERT INTO public.refresh_tokens (id, user_id, token_hash, family_id, expires_at) VALUES ($1, $2, 'h1', $3, now() + interval '1 day')`,
		uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-rt")), user, uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-family")))
	d.must(t, ctx, `INSERT INTO public.password_reset_tokens (id, user_id, token_hash, expires_at) VALUES ($1, $2, 'p1', now() + interval '1 day')`,
		uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-prt")), user)
	d.must(t, ctx, `INSERT INTO public.password_reset_tokens (id, user_id, token_hash, expires_at) VALUES ($1, $2, 'p2', now() + interval '1 day')`,
		uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-prt-kept")), user)
	d.must(t, ctx, `INSERT INTO public.internal_service_credentials (id, service_name, token_hash, token_prefix, scopes, created_at) VALUES ($1, 'svc', 's1', 'svc_x', '[]', now())`,
		uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-isc")))
	d.must(t, ctx, `INSERT INTO public.org_invites (id, org_id, email, token_hash, expires_at) VALUES ($1, $2, 'inv@example.invalid', 'i1', now() + interval '1 day')`,
		uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-invite")), org)
}

// afterT is what the end of the WAL holds beyond T: six more revisions, two
// credentials revoked, one credential created and revoked, and the legacy
// revocations.
func (d *fenceDB) afterT(t *testing.T, ctx context.Context) {
	t.Helper()
	for i := 0; i < fenceOrgs; i++ {
		d.orgUpdate(t, ctx, i, fmt.Sprintf("after-%d-a", i))
		d.orgUpdate(t, ctx, i, fmt.Sprintf("after-%d-b", i))
	}
	revoke := func(cred uuid.UUID, reason string) {
		d.commit(t, ctx, "credential.revoked", 1, func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `UPDATE auth.credential_registry SET revoked_at = now(), revoked_reason = $2 WHERE id = $1`, cred, reason)
			return err
		})
	}
	revoke(fenceCredID(0), "compromised")
	revoke(fenceCredID(2), "owner_request")
	principal := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-principal"))
	ghost := fenceCredID(99)
	d.commit(t, ctx, "credential.created", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `INSERT INTO auth.credential_registry (id, principal_id, credential_class, secret_hash, hash_algorithm, fingerprint)
VALUES ($1, $2, 'workload_token', decode(md5($1::text), 'hex'), 'sha256', '0123456789abcdef')`, ghost, principal)
		return err
	})
	revoke(ghost, "compromised")
	user := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-user"))
	d.must(t, ctx, `UPDATE public.users SET is_active = false, token_version = 3 WHERE id = $1`, user)
	d.must(t, ctx, `UPDATE public.refresh_tokens SET revoked_at = now()`)
	d.must(t, ctx, `DELETE FROM public.password_reset_tokens WHERE token_hash = 'p1'`)
	d.must(t, ctx, `UPDATE public.internal_service_credentials SET revoked_at = now()`)
	d.must(t, ctx, `UPDATE public.org_invites SET status = 'accepted', accepted_at = now()`)
}

func (d *fenceDB) revisions(t *testing.T, ctx context.Context) map[string]int64 {
	t.Helper()
	rows, err := d.admin.Query(ctx, `SELECT aggregate_type || ':' || aggregate_id, revision FROM auth.aggregate_revisions`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var k string
		var r int64
		if err := rows.Scan(&k, &r); err != nil {
			t.Fatal(err)
		}
		out[k] = r
	}
	return out
}

func (d *fenceDB) keys(t *testing.T, ctx context.Context) map[string]bool {
	t.Helper()
	rows, err := d.admin.Query(ctx, `SELECT idempotency_key FROM auth.auth_outbox_events`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		out[k] = true
	}
	return out
}

func (d *fenceDB) count(t *testing.T, ctx context.Context, sql string) int64 {
	t.Helper()
	var n int64
	if err := d.admin.QueryRow(ctx, sql).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func fencePair(t *testing.T, ctx context.Context) (restored, end *fenceDB) {
	t.Helper()
	restored, end = newFenceDB(t, ctx), newFenceDB(t, ctx)
	restored.stateT(t, ctx)
	end.stateT(t, ctx)
	end.afterT(t, ctx)
	return restored, end
}

func exportEnd(t *testing.T, ctx context.Context, end *fenceDB) *Manifest {
	t.Helper()
	m, err := ExportManifest(ctx, end.admin, end.schema)
	if err != nil {
		t.Fatalf("ExportManifest: %v", err)
	}
	return &m
}

// The drill's GAP (c)=6 and (b)=2 on a fixture: the restored database is
// behind the end of the WAL, and one fence run closes both.
func TestTheFenceRaisesTheFloorsAndRevokesAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := fencePair(t, ctx)
	manifest := exportEnd(t, ctx, end)
	endRevs := end.revisions(t, ctx)

	dry, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if !dry.DryRun || dry.FloorsRaised != fenceOrgs {
		t.Fatalf("dry run report = %+v, want %d floors", dry, fenceOrgs)
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM auth.credential_registry WHERE revoked_at IS NOT NULL`); n != 0 {
		t.Fatalf("a dry run revoked %d credentials", n)
	}

	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if report.FloorsRaised != fenceOrgs {
		t.Errorf("floors raised = %d, want %d", report.FloorsRaised, fenceOrgs)
	}
	byName := map[string]SectionReport{}
	for _, s := range report.Sections {
		byName[s.Name] = s
	}
	if got := byName["auth.credential_registry"]; got.Changed != 2 || got.Absent != 1 {
		t.Errorf("credential_registry = %+v, want 2 revoked again and 1 absent (created and revoked after T)", got)
	}
	for _, name := range []string{"public.refresh_tokens", "public.internal_service_credentials", "public.org_invites", "public.users"} {
		if byName[name].Changed != 1 {
			t.Errorf("%s changed %d, want 1", name, byName[name].Changed)
		}
	}
	if byName["public.password_reset_tokens"].Changed != 1 {
		t.Errorf("password_reset_tokens removed %d, want the 1 consumed after T", byName["public.password_reset_tokens"].Changed)
	}
	for k, want := range endRevs {
		if got := restored.revisions(t, ctx)[k]; got < want {
			t.Errorf("%s = %d after the fence, want at least %d", k, got, want)
		}
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM public.password_reset_tokens`); n != 1 {
		t.Errorf("password_reset_tokens after the fence = %d, want only the one still live at the end of the WAL", n)
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM auth.credential_registry WHERE revoked_at IS NOT NULL`); n != 2 {
		t.Errorf("revoked credentials after the fence = %d, want 2", n)
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM public.users WHERE is_active = false AND token_version = 3`); n != 1 {
		t.Errorf("the legacy user is not deactivated with the raised token_version")
	}
	if err := Check(ctx, restored.runtime, restored.schema); err != nil {
		t.Errorf("readiness after the fence: %v", err)
	}

	// The next write continues past everything the end of the WAL published, so
	// no (type, id, revision) is issued twice.
	endKeys := end.keys(t, ctx)
	beforeKeys := restored.keys(t, ctx)
	for i := 0; i < fenceOrgs; i++ {
		restored.orgUpdate(t, ctx, i, fmt.Sprintf("post-%d", i))
	}
	reissued := 0
	for k := range restored.keys(t, ctx) {
		if !beforeKeys[k] && endKeys[k] {
			reissued++
		}
	}
	if reissued != 0 {
		t.Errorf("%d outbox keys issued after the restore were already published by the end of the WAL", reissued)
	}
}

// Without the fence the same write re-issues keys the end of the WAL had
// published: the problem the fence removes (the drill's GAP).
func TestWithoutTheFenceAPostRestoreWriteReissuesAPublishedKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := fencePair(t, ctx)
	endKeys := end.keys(t, ctx)
	atT := restored.keys(t, ctx)
	for i := 0; i < fenceOrgs; i++ {
		restored.orgUpdate(t, ctx, i, fmt.Sprintf("post-%d", i))
	}
	reissued := 0
	for k := range restored.keys(t, ctx) {
		if endKeys[k] && !atT[k] {
			reissued++
		}
	}
	if reissued != fenceOrgs {
		t.Fatalf("keys re-issued without the fence = %d, want %d", reissued, fenceOrgs)
	}
}

func TestApplyIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := fencePair(t, ctx)
	manifest := exportEnd(t, ctx, end)
	if _, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true}); err != nil {
		t.Fatal(err)
	}
	before := restored.revisions(t, ctx)
	again, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if !again.AlreadyApplied || again.FloorsRaised != 0 || again.Revocations != 0 || again.RowsRemoved != 0 {
		t.Fatalf("second run = %+v, want already applied with zero changes", again)
	}
	after := restored.revisions(t, ctx)
	for k, v := range before {
		if after[k] != v {
			t.Errorf("%s moved from %d to %d on the second run", k, v, after[k])
		}
	}
}

// A promote with no loss: a new timeline, the same data. apply changes no row
// and readiness opens.
func TestApplyOnANoLossTimelineChangesNothingAndClearsReadiness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	db.stateT(t, ctx)
	if err := Check(ctx, db.runtime, db.schema); err != nil {
		t.Fatalf("a migrated database is not ready: %v", err)
	}
	// Simulate the new timeline: the stamp of the old one is what a restored
	// database carries; remove the row for the live timeline.
	conn, err := db.admin.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `SET session_replication_role = replica`); err != nil {
		conn.Release()
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `DELETE FROM auth.restore_fence_state`); err != nil {
		conn.Release()
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `SET session_replication_role = DEFAULT`); err != nil {
		conn.Release()
		t.Fatal(err)
	}
	conn.Release()
	if err := Check(ctx, db.runtime, db.schema); !errors.Is(err, ErrNotFenced) {
		t.Fatalf("a timeline with no fence record = %v, want ErrNotFenced", err)
	}
	manifest := exportEnd(t, ctx, db)
	before := db.revisions(t, ctx)
	report, err := Apply(ctx, db.admin, db.schema, manifest, Options{Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if report.FloorsRaised != 0 || report.Revocations != 0 || report.RowsRemoved != 0 || report.MarginRows != 0 {
		t.Fatalf("a no-loss apply changed rows: %+v", report)
	}
	for k, v := range before {
		if db.revisions(t, ctx)[k] != v {
			t.Errorf("%s changed on a no-loss apply", k)
		}
	}
	if err := Check(ctx, db.runtime, db.schema); err != nil {
		t.Fatalf("readiness after a no-loss apply: %v", err)
	}
}

// A failure in a late section rolls back the floors raised before it.
func TestApplyIsAllOrNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := fencePair(t, ctx)
	manifest := exportEnd(t, ctx, end)
	for i := range manifest.Sections {
		if manifest.Sections[i].Name == "auth.credential_registry" {
			for _, row := range manifest.Sections[i].Rows {
				row["revoked_reason"] = "Not A Reason Code"
			}
		}
	}
	if err := manifest.Seal(); err != nil {
		t.Fatal(err)
	}
	before := restored.revisions(t, ctx)
	if _, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true}); err == nil {
		t.Fatal("a manifest the schema refuses was applied")
	}
	for k, v := range before {
		if restored.revisions(t, ctx)[k] != v {
			t.Errorf("%s changed although the apply failed", k)
		}
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM auth.restore_fence_state WHERE source <> 'migration'`); n != 0 {
		t.Errorf("a failed apply recorded %d fence rows", n)
	}
}

func TestTheBlindMarginRaisesEveryRevisionAndIsAppliedOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	db.stateT(t, ctx)
	before := db.revisions(t, ctx)
	report, err := Apply(ctx, db.admin, db.schema, nil, Options{Apply: true, BlindMargin: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if report.MarginRows != int64(len(before)) {
		t.Fatalf("margin rows = %d, want %d", report.MarginRows, len(before))
	}
	for k, v := range before {
		if got := db.revisions(t, ctx)[k]; got != v+1000 {
			t.Errorf("%s = %d, want %d", k, got, v+1000)
		}
	}
	if again, err := Apply(ctx, db.admin, db.schema, nil, Options{Apply: true, BlindMargin: 1000}); err != nil || !again.AlreadyApplied {
		t.Fatalf("a second blind run = %+v, %v; want already applied", again, err)
	}
	for k, v := range before {
		if got := db.revisions(t, ctx)[k]; got != v+1000 {
			t.Errorf("%s = %d after the second blind run, want %d", k, got, v+1000)
		}
	}
}

func TestTheRuntimeRoleCannotApplyOrWriteTheState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	db.stateT(t, ctx)
	manifest := exportEnd(t, ctx, db)
	if _, err := Apply(ctx, db.runtime, db.schema, manifest, Options{Apply: true}); !errors.Is(err, ErrPrivileges) {
		t.Fatalf("apply as the runtime role = %v, want ErrPrivileges", err)
	}
	if _, err := db.runtime.Exec(ctx, `INSERT INTO auth.restore_fence_state (timeline_id, manifest_sha256, source) VALUES (999, 'initial', 'migration')`); err == nil {
		t.Fatal("the runtime role wrote restore_fence_state")
	}
}

func TestTheTimelineProbePostureAndValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	var runtimeMay, publicMay bool
	if err := db.admin.QueryRow(ctx, `SELECT has_function_privilege($1, 'auth.restore_fence_timeline()', 'EXECUTE'), has_function_privilege('public', 'auth.restore_fence_timeline()', 'EXECUTE')`,
		db.runtimeRole).Scan(&runtimeMay, &publicMay); err != nil {
		t.Fatal(err)
	}
	if !runtimeMay || publicMay {
		t.Fatalf("EXECUTE: runtime=%v public=%v, want true and false", runtimeMay, publicMay)
	}
	var viaRuntime, viaControl int64
	if err := db.runtime.QueryRow(ctx, `SELECT auth.restore_fence_timeline()`).Scan(&viaRuntime); err != nil {
		t.Fatalf("the runtime role cannot read the timeline: %v", err)
	}
	if _, err := db.admin.Exec(ctx, `CHECKPOINT`); err != nil {
		t.Fatal(err)
	}
	if err := db.admin.QueryRow(ctx, `SELECT timeline_id FROM pg_control_checkpoint()`).Scan(&viaControl); err != nil {
		t.Fatal(err)
	}
	if viaRuntime < 1 || viaRuntime != viaControl {
		t.Fatalf("the probe says timeline %d, pg_control says %d", viaRuntime, viaControl)
	}
}

// The probe reads the WAL position; a definer that is not a superuser must be
// able to. If this fails the migration role needs a grant the deployment does
// not give.
func TestTheWalFunctionsAreCallableByAnOrdinaryRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	role := db.runtimeRole + "_plain"
	if _, err := db.admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q NOSUPERUSER`, role)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(db.admin, role, t.Logf) })
	conn, err := db.admin.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, fmt.Sprintf(`SET ROLE %q`, role)); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := conn.QueryRow(ctx, `SELECT pg_walfile_name(pg_current_wal_insert_lsn())`).Scan(&name); err != nil {
		t.Fatalf("an ordinary role cannot read the WAL position: %v", err)
	}
	if len(name) != 24 {
		t.Fatalf("WAL file name %q", name)
	}
}
