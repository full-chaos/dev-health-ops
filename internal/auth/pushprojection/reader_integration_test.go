//go:build integration

package pushprojection

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/outboxprojection"
	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

type env struct {
	admin    *pgxpool.Pool
	runtime  *pgxpool.Pool
	schema   authschema.ValidatedIdentifier
	identity *identity.Store
	engine   *outboxprojection.Engine
}

var (
	closers []func()
	envs    = map[bool]*env{}
	mu      sync.Mutex
)

func TestMain(m *testing.M) {
	code := m.Run()
	for i := len(closers) - 1; i >= 0; i-- {
		closers[i]()
	}
	os.Exit(code)
}

func getEnv(t *testing.T, fence bool) *env {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	if e := envs[fence]; e != nil {
		return e
	}
	e, err := newEnv(t, fence)
	if err != nil {
		t.Fatal(err)
	}
	envs[fence] = e
	return e
}

func newEnv(t *testing.T, fence bool) (*env, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		return nil, err
	}
	closers = append(closers, func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(closeCtx)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		return nil, err
	}
	closers = append(closers, admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("pushprojection_runtime", instance)
	if err != nil {
		return nil, err
	}
	const password = "pushprojection-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		return nil, err
	}
	closers = append(closers, func() { containers.DropRole(admin, role, func(string, ...any) {}) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
		return nil, err
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User, cfg.ConnConfig.Password = role, password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	closers = append(closers, runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		return nil, err
	}
	if fence {
		if _, err := restorefence.Init(ctx, admin, schema, restorefence.InitOptions{IfAbsent: true}); err != nil {
			return nil, err
		}
	}
	id, err := identity.New(identity.Config{Pool: runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		return nil, err
	}
	engine, err := outboxprojection.New(outboxprojection.Config{Pool: runtime, Schema: schema, ProjectOrganization: func(ctx context.Context, org uuid.UUID) (bool, error) {
		err := id.ProjectOrganization(ctx, org)
		if errors.Is(err, identity.ErrOrganizationNotFound) {
			return false, nil
		}
		return err == nil, err
	}})
	return &env{admin: admin, runtime: runtime, schema: schema, identity: id, engine: engine}, err
}

func (e *env) reader(t *testing.T, horizon time.Duration) *Reader {
	t.Helper()
	r, err := NewReader(Config{Pool: e.runtime, Schema: e.schema, Engine: e.engine, Registry: health.NewRegistry(time.Second), Horizon: horizon, BatchLimit: 50, PollInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (e *env) org(t *testing.T) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(context.Background(), `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'reader')`, id, "org-"+id.String()[:8]); err != nil {
		t.Fatal(err)
	}
	return id
}

func (e *env) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%q: %v", sql, err)
	}
	return n
}

func (e *env) begin(t *testing.T) pgx.Tx {
	t.Helper()
	tx, err := e.admin.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	return tx
}

// freshFingerprint is a new 16-hex fingerprint: the registry's secret_hash of a projected row is that fingerprint's 8
// bytes and is UNIQUE, so two credentials never share one.
func freshFingerprint() string { return strings.ReplaceAll(uuid.NewString(), "-", "")[:16] }

func created(org uuid.UUID, id string, scopes ...string) audit.PushLifecycleEvent {
	if scopes == nil {
		scopes = []string{"ingest:write"}
	}
	return audit.PushLifecycleEvent{Binding: externalpush.BindingSource, EventType: externalpush.Created, AggregateID: id, OrganizationID: "org_" + org.String(), SourceID: "src-r",
		Change: externalpush.Change{Status: "active", Fingerprint: freshFingerprint(), Scopes: scopes}}
}

func (e *env) append(t *testing.T, tx pgx.Tx, events ...audit.PushLifecycleEvent) {
	t.Helper()
	if err := audit.AppendPushLifecycle(context.Background(), tx, e.schema, events); err != nil {
		t.Fatalf("AppendPushLifecycle: %v", err)
	}
}

func (e *env) appendCommitted(t *testing.T, events ...audit.PushLifecycleEvent) {
	t.Helper()
	tx := e.begin(t)
	e.append(t, tx, events...)
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// seed runs one statement inside audit.Commit as the migration role: the guarded tables take no other write.
func (e *env) seed(t *testing.T, sql string, args ...any) {
	t.Helper()
	err := audit.Commit(context.Background(), e.admin, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, sql, args...)
			return err
		},
		Audit:     audit.AuditEvent{EventType: "credential.test", Outcome: audit.OutcomeAllowed},
		OffStream: audit.OffStreamWorkloadRegistry,
	})
	if err != nil {
		t.Fatalf("seed %q: %v", sql, err)
	}
}

// seedOutbox runs one statement against the guarded outbox inside audit.Commit. The guard (migration 0006) counts no
// outbox write as state, so audit.Commit refuses an outbox-only statement as "changed no state"; a cursor row written
// and deleted in the same transaction is the counted state that leaves nothing behind. (Arming the transaction by hand
// instead would name the commit-guard setting, which the audit writer guard forbids outside authschema.)
func (e *env) seedOutbox(t *testing.T, sql string, args ...any) {
	t.Helper()
	err := audit.Commit(context.Background(), e.admin, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if _, err := tx.Exec(ctx, sql, args...); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO auth.projection_cursor (stream) VALUES ('seed_marker')`); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `DELETE FROM auth.projection_cursor WHERE stream = 'seed_marker'`)
			return err
		},
		Audit:     audit.AuditEvent{EventType: "credential.test", Outcome: audit.OutcomeAllowed},
		OffStream: audit.OffStreamWorkloadRegistry,
	})
	if err != nil {
		t.Fatalf("seedOutbox %q: %v", sql, err)
	}
}

func (e *env) outboxID(t *testing.T, aggregate string, revision int) int64 {
	t.Helper()
	var id int64
	if err := e.admin.QueryRow(context.Background(), `SELECT id FROM auth.auth_outbox_events WHERE idempotency_key = $1`,
		fmt.Sprintf("external_push_credential:%s:%d", aggregate, revision)).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func scoped(c audit.PushLifecycleEvent, scopes ...string) audit.PushLifecycleEvent {
	snapshot := c.Change
	return audit.PushLifecycleEvent{Binding: externalpush.BindingSource, EventType: externalpush.Scoped, AggregateID: c.AggregateID, OrganizationID: c.OrganizationID, SourceID: c.SourceID,
		Change: externalpush.Change{Scopes: scopes}, Snapshot: &snapshot}
}

func TestReaderProjectsAnAppendedEventOnceAndReadsNothingTwice(t *testing.T) {
	e := getEnv(t, true)
	org, id := e.org(t), uuid.NewString()
	e.appendCommitted(t, created(org, id))
	r := e.reader(t, time.Hour)
	rep, err := r.Step(context.Background(), time.Now())
	if err != nil || rep.Applied != 1 {
		t.Fatalf("first step: %+v, %v; want one applied", rep, err)
	}
	if e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND domain_owner = 'external_push'`, id) != 1 {
		t.Fatal("no registry row")
	}
	rep, err = r.Step(context.Background(), time.Now())
	if err != nil || rep.Seen != 0 || rep.Applied != 0 {
		t.Fatalf("second step: %+v, %v; want nothing read", rep, err)
	}
}

func TestReaderProjectsThreeCredentialsOfOneSourceEachUnderItsOwnFingerprint(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	var ids []string
	for i := 0; i < 3; i++ {
		id := uuid.NewString()
		ids = append(ids, id)
		e.appendCommitted(t, created(org, id))
	}
	rep, err := e.reader(t, time.Hour).Step(context.Background(), time.Now())
	if err != nil || rep.Applied != 3 {
		t.Fatalf("step: %+v, %v; want all three credentials applied", rep, err)
	}
	if n := e.count(t, `SELECT count(DISTINCT secret_hash) FROM auth.credential_registry WHERE external_reference = ANY($1)`, ids); n != 3 {
		t.Fatalf("%d distinct secret hashes, want 3 (a projected row's secret hash is its UNIQUE fingerprint)", n)
	}
}

func TestReaderProjectsAnOrganizationBoundEventToTheOrganizationPrincipal(t *testing.T) {
	e := getEnv(t, true)
	org, id := e.org(t), uuid.NewString()
	e.appendCommitted(t, audit.PushLifecycleEvent{Binding: externalpush.BindingOrganization, EventType: externalpush.Created, AggregateID: id,
		OrganizationID: "org_" + org.String(), Change: externalpush.Change{Status: "active", Fingerprint: freshFingerprint(), Scopes: []string{"ingest:write"}}})
	rep, err := e.reader(t, time.Hour).Step(context.Background(), time.Now())
	if err != nil || rep.Applied != 1 {
		t.Fatalf("step: %+v, %v; want the organization-bound event applied", rep, err)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection p JOIN auth.push_sources s ON s.principal_id = p.principal_id WHERE p.external_reference = $1 AND p.binding = 'organization' AND p.source_id IS NULL AND s.source_id IS NULL AND s.organization_id = $2`, id, org); n != 1 {
		t.Fatal("the organization-bound credential is not held by the organization principal")
	}
}

func TestReaderReadsALateLowerIdThatCommitsAfterAHigherOne(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	x, y := uuid.NewString(), uuid.NewString()
	txA := e.begin(t)
	e.append(t, txA, created(org, x))
	e.appendCommitted(t, created(org, y))
	r := e.reader(t, time.Hour)
	rep, err := r.Step(context.Background(), time.Now())
	if err != nil || rep.Applied != 1 {
		t.Fatalf("step 1: %+v, %v; want the committed higher id applied", rep, err)
	}
	if err := txA.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	rep, err = r.Step(context.Background(), time.Now())
	if err != nil || rep.Applied != 1 {
		t.Fatalf("step 2: %+v, %v; want the late lower id applied (a pure id cursor skips it)", rep, err)
	}
	for _, id := range []string{x, y} {
		if e.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1`, id) != 1 {
			t.Fatalf("credential %s was not projected", id)
		}
	}
}

func TestReaderParksAGapAndOtherAggregatesProceedAndTheSameAggregateWaits(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	z, w := uuid.NewString(), uuid.NewString()
	first := created(org, z)
	e.appendCommitted(t, first)
	r := e.reader(t, time.Hour)
	now := time.Now()
	if rep, err := r.Step(context.Background(), now); err != nil || rep.Applied < 1 {
		t.Fatalf("setup step: %+v, %v", rep, err)
	}
	for i := 0; i < 4; i++ { // revisions 2..5: 4 is the gap, 5 waits behind it
		e.appendCommitted(t, scoped(first, "ingest:write"))
	}
	e.appendCommitted(t, created(org, w))
	gap, behind := e.outboxID(t, z, 4), e.outboxID(t, z, 5)
	// Revision 2 is marked decided without being applied: revision 3 then meets a gap.
	e.seed(t, `INSERT INTO auth.credential_projection_digest (domain_owner, external_reference, revision, digest, outcome) VALUES ('external_push', $1, 2, $2, 'applied')`, z, make([]byte, 32))
	e.seed(t, `INSERT INTO auth.credential_projection_digest (domain_owner, external_reference, revision, digest, outcome) VALUES ('external_push', $1, 3, $2, 'applied')`, z, make([]byte, 32))
	rep, err := r.Step(context.Background(), now)
	if err != nil || rep.Applied != 1 || rep.Parked != 1 {
		t.Fatalf("step: %+v, %v; want the other aggregate applied and the gap parked", rep, err)
	}
	if e.count(t, `SELECT count(*) FROM auth.projection_retry WHERE stream = 'external_push' AND outbox_id = $1 AND last_outcome = 'gap'`, gap) != 1 {
		t.Fatal("the gap row is not parked")
	}
	if e.count(t, `SELECT count(*) FROM auth.projection_retry WHERE outbox_id = $1`, behind) != 0 {
		t.Fatal("a later row of the aggregate was parked or read; it must wait behind the parked row")
	}
	rep, err = r.Step(context.Background(), now.Add(time.Second))
	if err != nil || rep.Seen != 0 || rep.Parked != 0 {
		t.Fatalf("a parked row was read again before it was due: %+v, %v", rep, err)
	}
	var attempts int
	if err := e.admin.QueryRow(context.Background(), `SELECT attempts FROM auth.projection_retry WHERE outbox_id = $1`, gap).Scan(&attempts); err != nil || attempts != 0 {
		t.Fatalf("attempts = %d (%v), want 0 before the row is due", attempts, err)
	}
	e.seed(t, `DELETE FROM auth.credential_projection_digest WHERE external_reference = $1 AND revision IN (2, 3)`, z)
	rep, err = r.Step(context.Background(), now.Add(time.Minute))
	if err != nil || rep.Released < 1 {
		t.Fatalf("after the missing revision was undecided: %+v, %v; want the parked row released", rep, err)
	}
	if e.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1 AND revision >= 4`, z) != 1 {
		t.Fatal("the parked revision was not applied after its predecessors")
	}
}

func TestReaderReleasesAParkedChildWhenItsParentApplies(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	parent, child := uuid.NewString(), uuid.NewString()
	e.appendCommitted(t, audit.PushLifecycleEvent{Binding: externalpush.BindingSource, EventType: externalpush.Rotated, AggregateID: child, OrganizationID: "org_" + org.String(), SourceID: "src-r",
		Change: externalpush.Change{Status: "active", Fingerprint: freshFingerprint(), Scopes: []string{"ingest:write"}, RotationParentID: parent}})
	r := e.reader(t, time.Hour)
	now := time.Now()
	rep, err := r.Step(context.Background(), now)
	if err != nil || rep.Parked != 1 {
		t.Fatalf("step: %+v, %v; want the child parked as parent_pending", rep, err)
	}
	if e.count(t, `SELECT count(*) FROM auth.projection_retry WHERE aggregate_id = $1 AND last_outcome = 'parent_pending'`, child) != 1 {
		t.Fatal("the child is not parked as parent_pending")
	}
	e.appendCommitted(t, created(org, parent))
	rep, err = r.Step(context.Background(), now.Add(time.Minute))
	if err != nil || rep.Applied < 1 || rep.Released != 1 {
		t.Fatalf("step: %+v, %v; want the parent applied and the child released", rep, err)
	}
	if e.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1 AND rotation_parent_reference = $2`, child, parent) != 1 {
		t.Fatal("the child was not projected with its lineage")
	}
}

func TestReaderParksAnUnknownOrganizationFinallyAndCountsIt(t *testing.T) {
	e := getEnv(t, true)
	id := uuid.NewString()
	e.appendCommitted(t, created(uuid.New(), id)) // an organization in neither projection source
	row := e.outboxID(t, id, 1)
	r := e.reader(t, time.Hour)
	rep, err := r.Step(context.Background(), time.Now())
	if err != nil || rep.Final != 1 {
		t.Fatalf("step: %+v, %v; want one final row", rep, err)
	}
	if e.count(t, `SELECT count(*) FROM auth.projection_retry WHERE outbox_id = $1 AND last_outcome = 'organization_unknown' AND next_at = 'infinity'`, row) != 1 {
		t.Fatal("the row is not parked for good")
	}
	rep, err = r.Step(context.Background(), time.Now().Add(24*time.Hour))
	if err != nil || rep.Seen != 0 || rep.Final != 0 {
		t.Fatalf("a final row was read again: %+v, %v", rep, err)
	}
	if e.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1`, id) != 0 {
		t.Fatal("an event of an unknown organization was applied")
	}
}

func TestReaderLowWaterMarkMovesOnlyOverRowsOlderThanTheHorizon(t *testing.T) {
	e := getEnv(t, true)
	e.drain(t, e.reader(t, time.Hour))
	org := e.org(t)
	oldID, recentID := uuid.NewString(), uuid.NewString()
	e.appendCommitted(t, created(org, oldID))
	e.appendCommitted(t, created(org, recentID))
	var oldRow, recentRow int64
	for id, dst := range map[string]*int64{oldID: &oldRow, recentID: &recentRow} {
		if err := e.admin.QueryRow(context.Background(), `SELECT id FROM auth.auth_outbox_events WHERE aggregate_id = $1`, id).Scan(dst); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	e.seedOutbox(t, `UPDATE auth.auth_outbox_events SET created_at = now() - interval '2 hours' WHERE aggregate_type = 'external_push_credential' AND id < $1`, recentRow)
	r := e.reader(t, time.Hour)
	rep, err := r.Step(ctx, time.Now().Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Mark < oldRow || rep.Mark >= recentRow {
		t.Fatalf("mark = %d, want it at or past the old row %d and below the recent row %d", rep.Mark, oldRow, recentRow)
	}
	var stored int64
	if err := e.admin.QueryRow(ctx, `SELECT low_water FROM auth.projection_cursor WHERE stream = 'external_push'`).Scan(&stored); err != nil || stored != rep.Mark {
		t.Fatalf("stored low_water = %d (%v), want %d", stored, err, rep.Mark)
	}
}

func TestReaderDoesNothingOnAnUnfencedTimeline(t *testing.T) {
	e := getEnv(t, false)
	org := e.org(t)
	e.appendCommitted(t, created(org, uuid.NewString()))
	r := e.reader(t, time.Hour)
	rep, err := r.Step(context.Background(), time.Now())
	if !errors.Is(err, ErrUnfenced) || rep.Applied != 0 {
		t.Fatalf("step: %+v, %v; want ErrUnfenced and nothing applied", rep, err)
	}
	if e.count(t, `SELECT count(*) FROM auth.credential_projection`) != 0 || e.count(t, `SELECT count(*) FROM auth.projection_retry`) != 0 ||
		e.count(t, `SELECT count(*) FROM auth.projection_cursor`) != 0 {
		t.Fatal("an unfenced step wrote state")
	}
}

func TestTwoReadersApplyEachEventOnce(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	var ids []string
	for i := 0; i < 6; i++ {
		id := uuid.NewString()
		ids = append(ids, id)
		e.appendCommitted(t, created(org, id))
	}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.reader(t, time.Hour).Step(context.Background(), time.Now()); err != nil {
				t.Errorf("Step: %v", err)
			}
		}()
	}
	wg.Wait()
	for _, id := range ids {
		if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1`, id); n != 1 {
			t.Fatalf("credential %s has %d registry rows, want 1", id, n)
		}
		if n := e.count(t, `SELECT count(*) FROM auth.credential_projection_digest WHERE external_reference = $1 AND revision = 1`, id); n != 1 {
			t.Fatalf("credential %s has %d digest rows, want 1", id, n)
		}
	}
}

func TestFloorInputCoversAppliedAndParkedRevisions(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	applied, parked := uuid.NewString(), uuid.NewString()
	e.appendCommitted(t, created(org, applied))
	e.appendCommitted(t, audit.PushLifecycleEvent{Binding: externalpush.BindingSource, EventType: externalpush.Rotated, AggregateID: parked, OrganizationID: "org_" + org.String(), SourceID: "src-r",
		Change: externalpush.Change{Status: "active", Fingerprint: freshFingerprint(), Scopes: []string{"ingest:write"}, RotationParentID: uuid.NewString()}})
	if _, err := e.reader(t, time.Hour).Step(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	in := NewFloorInput(e.runtime, e.schema)
	if err := in.Status(context.Background()); err != nil {
		t.Fatalf("Status: %v", err)
	}
	floors, err := in.Floors(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if floors[FloorKey(applied)] != 1 || floors[FloorKey(parked)] != 1 {
		t.Fatalf("floors %v: want the applied revision 1 and the parked revision 1", floors)
	}
}

func TestReaperKeepsAPushRowAboveTheCursorAndAStoppedReaderLosesNoRow(t *testing.T) {
	e := getEnv(t, true)
	ctx := context.Background()
	org := e.org(t)
	a, b := uuid.NewString(), uuid.NewString()
	e.appendCommitted(t, created(org, a))
	e.appendCommitted(t, created(org, b))
	var rowA, rowB int64
	for id, dst := range map[string]*int64{a: &rowA, b: &rowB} {
		if err := e.admin.QueryRow(ctx, `SELECT id FROM auth.auth_outbox_events WHERE aggregate_id = $1`, id).Scan(dst); err != nil {
			t.Fatal(err)
		}
	}
	guarded := e.seed
	e.seedOutbox(t, `UPDATE auth.auth_outbox_events SET published_at = now() - interval '9 days' WHERE id = ANY($1)`, []int64{rowA, rowB})
	guarded(t, `DELETE FROM auth.projection_cursor WHERE stream = 'external_push'`)
	cutoff := time.Now().Add(-7 * 24 * time.Hour)
	if _, err := audit.Reap(ctx, e.runtime, e.schema, cutoff, 1000); err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.auth_outbox_events WHERE id = ANY($1)`, []int64{rowA, rowB}); n != 2 {
		t.Fatalf("with no cursor row %d of 2 push rows survive the reaper, want both (a stopped reader loses no row)", n)
	}
	guarded(t, `INSERT INTO auth.projection_cursor (stream, low_water) VALUES ('external_push', $1)`, rowA)
	if _, err := audit.Reap(ctx, e.runtime, e.schema, cutoff, 1000); err != nil {
		t.Fatal(err)
	}
	if e.count(t, `SELECT count(*) FROM auth.auth_outbox_events WHERE id = $1`, rowA) != 0 {
		t.Fatal("a push row at or below the cursor was kept past retention")
	}
	if e.count(t, `SELECT count(*) FROM auth.auth_outbox_events WHERE id = $1`, rowB) != 1 {
		t.Fatal("a push row above the cursor was reaped")
	}
}

func TestReaderDecidesAStaleRowItReadsSoItIsNeverReadAgain(t *testing.T) {
	e := getEnv(t, true)
	ctx := context.Background()
	e.drain(t, e.reader(t, time.Hour))
	org, id := e.org(t), uuid.NewString()
	first := created(org, id)
	e.appendCommitted(t, first)
	snapshot := first.Change
	e.appendCommitted(t, audit.PushLifecycleEvent{Binding: externalpush.BindingSource, EventType: externalpush.Scoped, AggregateID: id, OrganizationID: first.OrganizationID, SourceID: first.SourceID,
		Change: externalpush.Change{Scopes: []string{"ingest:write"}}, Snapshot: &snapshot})
	r := e.reader(t, time.Hour)
	if rep, err := r.Step(ctx, time.Now()); err != nil || rep.Applied != 2 {
		t.Fatalf("step: %+v, %v; want both revisions applied", rep, err)
	}
	e.seed(t, `DELETE FROM auth.credential_projection_digest WHERE external_reference = $1 AND revision = 1`, id)
	if rep, err := r.Step(ctx, time.Now()); err != nil || rep.Seen != 1 || rep.Applied != 0 {
		t.Fatalf("step: %+v, %v; want the undecided old revision read once and not applied", rep, err)
	}
	if e.count(t, `SELECT count(*) FROM auth.credential_projection_digest WHERE external_reference = $1 AND revision = 1 AND outcome = 'stale'`, id) != 1 {
		t.Fatal("the stale delivery was not decided with a digest row")
	}
	if rep, err := r.Step(ctx, time.Now()); err != nil || rep.Seen != 0 {
		t.Fatalf("step: %+v, %v; want nothing left to read", rep, err)
	}
}

func (e *env) drain(t *testing.T, r *Reader) {
	t.Helper()
	for i := 0; i < 20; i++ {
		rep, err := r.Step(context.Background(), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if rep.Seen == 0 && rep.Retried == 0 {
			return
		}
	}
	t.Fatal("the reader did not drain the outbox in 20 steps")
}

func TestReaderMarkNeverPassesAnOldRowItDidNotScan(t *testing.T) {
	e := getEnv(t, true)
	ctx := context.Background()
	org := e.org(t)
	r := e.reader(t, time.Hour)
	e.drain(t, r)
	var ids []string
	var events []audit.PushLifecycleEvent
	for i := 0; i < 60; i++ {
		id := uuid.NewString()
		ids = append(ids, id)
		events = append(events, created(org, id))
	}
	e.appendCommitted(t, events...)
	// AppendPushLifecycle orders a batch by aggregate id, so the first and last outbox ids are not those of ids[0] and ids[59].
	var first, last int64
	if err := e.admin.QueryRow(ctx, `SELECT min(id), max(id) FROM auth.auth_outbox_events WHERE aggregate_id = ANY($1)`, ids).Scan(&first, &last); err != nil {
		t.Fatal(err)
	}
	e.seedOutbox(t, `UPDATE auth.auth_outbox_events SET created_at = now() - interval '2 hours' WHERE aggregate_type = 'external_push_credential' AND id <= $1`, last)
	undecided := `SELECT COALESCE(min(o.id), 0) FROM auth.auth_outbox_events o WHERE o.id BETWEEN $1 AND $2
		AND NOT EXISTS (SELECT 1 FROM auth.credential_projection_digest d WHERE d.domain_owner = 'external_push' AND d.external_reference = o.aggregate_id AND d.revision = 1)`
	rep, err := r.Step(ctx, time.Now().Add(2*time.Minute))
	if err != nil || rep.Applied != 50 {
		t.Fatalf("step 1: %+v, %v; want one batch of 50 applied", rep, err)
	}
	var unscanned int64
	if err := e.admin.QueryRow(ctx, undecided, first, last).Scan(&unscanned); err != nil || unscanned == 0 {
		t.Fatalf("unscanned row = %d (%v), want the first of the 10 rows past the batch", unscanned, err)
	}
	var stored int64
	if err := e.admin.QueryRow(ctx, `SELECT low_water FROM auth.projection_cursor WHERE stream = 'external_push'`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if rep.Mark >= unscanned || stored >= unscanned {
		t.Fatalf("mark = %d, stored = %d: the mark passed the old row %d the step never scanned (the reaper could delete it)", rep.Mark, stored, unscanned)
	}
	rep, err = r.Step(ctx, time.Now().Add(4*time.Minute))
	if err != nil || rep.Applied != 10 {
		t.Fatalf("step 2: %+v, %v; want the 10 rows left applied", rep, err)
	}
	if rep.Mark < last {
		t.Fatalf("mark = %d after every old row is decided, want it at or past %d", rep.Mark, last)
	}
}

func TestReaderReadsALaterRevisionThatWaitedBehindAParkedHeadOnceTheHeadResolves(t *testing.T) {
	e := getEnv(t, true)
	ctx := context.Background()
	org := e.org(t)
	r := e.reader(t, time.Hour)
	e.drain(t, r)
	parent, child := uuid.NewString(), uuid.NewString()
	head := audit.PushLifecycleEvent{Binding: externalpush.BindingSource, EventType: externalpush.Rotated, AggregateID: child, OrganizationID: "org_" + org.String(), SourceID: "src-r",
		Change: externalpush.Change{Status: "active", Fingerprint: freshFingerprint(), Scopes: []string{"ingest:write"}, RotationParentID: parent}}
	e.appendCommitted(t, head)
	snapshotOf := head
	snapshotOf.Change.RotationParentID = ""
	e.appendCommitted(t, scoped(snapshotOf, "ingest:write"))
	headRow, laterRow := e.outboxID(t, child, 1), e.outboxID(t, child, 2)
	e.seedOutbox(t, `UPDATE auth.auth_outbox_events SET created_at = now() - interval '2 hours' WHERE aggregate_type = 'external_push_credential' AND id <= $1`, laterRow)
	rep, err := r.Step(ctx, time.Now().Add(2*time.Minute))
	if err != nil || rep.Parked != 1 {
		t.Fatalf("step 1: %+v, %v; want the head parked as parent_pending", rep, err)
	}
	if e.count(t, `SELECT count(*) FROM auth.projection_retry WHERE outbox_id = $1`, headRow) != 1 {
		t.Fatal("the head is not parked")
	}
	var stored int64
	if err := e.admin.QueryRow(ctx, `SELECT low_water FROM auth.projection_cursor WHERE stream = 'external_push'`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if rep.Mark >= laterRow || stored >= laterRow {
		t.Fatalf("mark = %d, stored = %d: the mark passed the later revision %d that waits behind the parked head", rep.Mark, stored, laterRow)
	}
	if rep.Mark < headRow {
		t.Fatalf("mark = %d: the mark did not move over the parked head %d (a parked row is decided for the mark)", rep.Mark, headRow)
	}
	e.appendCommitted(t, created(org, parent))
	if _, err := r.Step(ctx, time.Now().Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Step(ctx, time.Now().Add(12*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if e.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1 AND revision >= 2`, child) != 1 {
		t.Fatal("the later revision was never read after its head resolved")
	}
}
