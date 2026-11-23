//go:build integration

package reconcilerservice

import (
	"context"
	"encoding/json"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	jobsv1 "github.com/full-chaos/dev-health-ops/contracts/jobs/v1"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

const (
	finalizePendingDomainPassword = "finalize_pending_domain_password"
	finalizePendingQueuePassword  = "finalize_pending_queue_password"
)

// finalizePendingVenue is the migrated schema with the real go-river-migrate
// grants for a fresh domain role and a fresh queue role. The coordinator pool
// is the admin pool: worker_job_routes is the only table the relay reads
// through it, and these cells are about the recovery, not the coordinator
// posture.
type finalizePendingVenue struct {
	admin      *pgxpool.Pool
	domain     *pgxpool.Pool
	queue      *pgxpool.Pool
	domainRole string
	queueRole  string
	domainSQL  *sqlRecorder
	queueSQL   *sqlRecorder
	registry   *jobruntime.Registry
	relay      joboutbox.RelayStepper
	store      *daily.PostgresStore
	pub        *daily.PostgresPublisher
}

// sqlRecorder keeps every statement one pool sent, so a cell can name the
// pool -- and so the role -- each half of the stage ran on.
type sqlRecorder struct {
	mu         sync.Mutex
	statements []string
}

func (recorder *sqlRecorder) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	recorder.mu.Lock()
	recorder.statements = append(recorder.statements, data.SQL)
	recorder.mu.Unlock()
	return ctx
}

func (*sqlRecorder) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (recorder *sqlRecorder) reset() {
	recorder.mu.Lock()
	recorder.statements = nil
	recorder.mu.Unlock()
}

// sawBefore reports whether a statement holding first was sent before the first statement holding then.
func (recorder *sqlRecorder) sawBefore(first, then string) bool {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	seenFirst := false
	for _, statement := range recorder.statements {
		if strings.Contains(statement, then) {
			return seenFirst
		}
		if strings.Contains(statement, first) {
			seenFirst = true
		}
	}
	return false
}

func (recorder *sqlRecorder) sawContaining(fragment string) bool {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	for _, statement := range recorder.statements {
		if strings.Contains(statement, fragment) {
			return true
		}
	}
	return false
}

func startFinalizePendingVenue(t *testing.T, ctx context.Context) *finalizePendingVenue {
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
	admin := openFinalizePendingPool(t, ctx, instance.URI)
	// The schema first: the queue role's SELECT grants on the daily tables are
	// to_regclass guarded, so a table created after the migration gets none.
	pgschema.Apply(ctx, t, admin)
	domainRole, err := containers.RoleName("fpr_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("fpr_queue", instance)
	if err != nil {
		t.Fatal(err)
	}
	for role, password := range map[string]string{
		domainRole: finalizePendingDomainPassword,
		queueRole:  finalizePendingQueuePassword,
	} {
		if _, err := admin.Exec(ctx, "CREATE ROLE "+role+
			" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '"+password+"'"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	}
	// The route rows go-river-migrate seeds in production (rivermigrate.go):
	// without them the relay resolves a native kind as unknown and every
	// Step fails before it reaches any repair.
	nativeRoutes, err := jobcontract.NativeRiverRouteKinds(jobsv1.MigrationState)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
		NativeRiverRoutes: nativeRoutes,
	}); err != nil {
		t.Fatal(err)
	}
	domainSQL, queueSQL := &sqlRecorder{}, &sqlRecorder{}
	domain := openTracedFinalizePendingPool(t, ctx, finalizePendingRoleURI(t, instance.URI, domainRole, finalizePendingDomainPassword), domainSQL)
	queue := openTracedFinalizePendingPool(t, ctx, finalizePendingRoleURI(t, instance.URI, queueRole, finalizePendingQueuePassword), queueSQL)
	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	// The production reconciler step list, as cmd builds it.
	relay, err := buildReconcilerRelay(admin, domain, queue, "river", registry)
	if err != nil {
		t.Fatal(err)
	}
	store, err := daily.NewPostgresStore(domain)
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := daily.NewPostgresPublisher(domain, registry)
	if err != nil {
		t.Fatal(err)
	}
	return &finalizePendingVenue{
		admin: admin, domain: domain, queue: queue, domainRole: domainRole, queueRole: queueRole,
		domainSQL: domainSQL, queueSQL: queueSQL, registry: registry,
		relay: relay, store: store, pub: publisher,
	}
}

func openFinalizePendingPool(t *testing.T, ctx context.Context, uri string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	return pool
}

func openTracedFinalizePendingPool(t *testing.T, ctx context.Context, uri string, recorder *sqlRecorder) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(uri)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Tracer = recorder
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	return pool
}

func finalizePendingRoleURI(t *testing.T, raw, role, password string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, password)
	return parsed.String()
}

// seedShapeD writes a run in shape D of CHAOS-14072: status running, every
// partition succeeded, finalization pending, no write for longer than the 24 h
// horizon. It writes no outbox row and no River job: both are gone.
func (venue *finalizePendingVenue) seedShapeD(t *testing.T, ctx context.Context, orgID, runID, targetDay string, at time.Time) {
	t.Helper()
	if _, err := venue.admin.Exec(ctx, `
INSERT INTO public.daily_metrics_runs
    (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3::date, $4, 'running', 'pending', $5, $5)`,
		runID, orgID, targetDay, "post-sync:"+runID, at); err != nil {
		t.Fatal(err)
	}
	if _, err := venue.admin.Exec(ctx, `
INSERT INTO public.daily_metrics_partitions
    (id, run_id, ordinal, repo_ids, status, attempt_count, completed_at, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, 0, '[]'::jsonb, 'succeeded', 1, $3, $3, $3)`,
		uuid.NewString(), runID, at); err != nil {
		t.Fatal(err)
	}
}

// seedRiverFinalize inserts a metrics.daily_finalize River job for the run with
// the real envelope as its arguments, and NO outbox row: the row was pruned by
// outbox retention while River still holds the job.
func (venue *finalizePendingVenue) seedRiverFinalize(t *testing.T, ctx context.Context, orgID, runID, state string, at time.Time) int64 {
	t.Helper()
	org := orgID
	args, err := jobcontract.MarshalCanonical(jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &org,
		CorrelationID:   "daily:" + runID,
		IdempotencyKey:  "metrics.daily_finalize:" + runID,
		Domain:          jobcontract.DomainLink{Type: "daily_metrics_run", ID: runID},
		Payload:         jobcontract.DailyMetricsFinalizePayload{RunID: runID},
	})
	if err != nil {
		t.Fatal(err)
	}
	var finalizedAt *time.Time
	if state == "completed" || state == "discarded" || state == "cancelled" {
		finalizedAt = &at
	}
	var jobID int64
	if err := venue.admin.QueryRow(ctx, `
INSERT INTO river.river_job (args, attempt, created_at, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at)
VALUES ($1::jsonb, 0, $2, $3, 'metrics.daily_finalize', 4, '{}'::jsonb, 2, 'metrics', $4::river.river_job_state, $2)
RETURNING id`, string(args), at, finalizedAt, state).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	return jobID
}

// finalizeTrace is what the queue holds for one run's finalize: its outbox rows
// and its River jobs, by the domain link both carry.
func (venue *finalizePendingVenue) finalizeTrace(t *testing.T, ctx context.Context, runID string) (outboxRows, riverJobs int) {
	t.Helper()
	if err := venue.admin.QueryRow(ctx, `
SELECT
  (SELECT count(*) FROM public.worker_job_outbox
    WHERE job_kind = 'metrics.daily_finalize' AND args #>> '{domain,id}' = $1),
  (SELECT count(*) FROM river.river_job
    WHERE kind = 'metrics.daily_finalize'
      AND args @> jsonb_build_object('domain', jsonb_build_object('id', $1::text)))`,
		runID).Scan(&outboxRows, &riverJobs); err != nil {
		t.Fatal(err)
	}
	return outboxRows, riverJobs
}

func (venue *finalizePendingVenue) runState(t *testing.T, ctx context.Context, runID string) (status, finalization string) {
	t.Helper()
	if err := venue.admin.QueryRow(ctx,
		`SELECT status, finalization_status FROM public.daily_metrics_runs WHERE id = $1::uuid`,
		runID).Scan(&status, &finalization); err != nil {
		t.Fatal(err)
	}
	return status, finalization
}

// countingFinalizeFamily stands in for one native finalize family. computes is
// shared by every family of one handler; icFinalize counts the ic_finalize
// family alone, which runs once per finalize compute.
type countingFinalizeFamily struct {
	computes *atomic.Int32
}

func (family countingFinalizeFamily) ComputeFinalizeFamily(context.Context, daily.Run) (int, error) {
	family.computes.Add(1)
	return 0, nil
}

// workRiverFinalizes runs every runnable metrics.daily_finalize River job of
// the run through the real FinalizeHandler, oldest first, and returns how many
// times the run's finalize computed. The ClaimFinalize CAS is what decides it.
func (venue *finalizePendingVenue) workRiverFinalizes(t *testing.T, ctx context.Context, runID string) int32 {
	t.Helper()
	var icFinalize, others atomic.Int32
	handler, err := daily.NewFinalizeHandler(venue.store)
	if err != nil {
		t.Fatal(err)
	}
	families := map[string]daily.NativeFinalizeFamilyExecutor{
		"ic_finalize": countingFinalizeFamily{computes: &icFinalize},
	}
	for _, name := range []string{
		daily.TeamCognitiveLoadFamilyName, daily.TeamComplexityFamilyName, daily.BenchmarkingFamilyName,
		daily.CompoundingRiskTeamFamilyName, daily.WorkItemMeasureCapabilityFamilyName,
	} {
		families[name] = countingFinalizeFamily{computes: &others}
	}
	if err := handler.SetNativeFinalizeFamilies(families); err != nil {
		t.Fatal(err)
	}
	descriptor, ok := venue.registry.Descriptor(jobcontract.KindDailyMetricsFinalize)
	if !ok {
		t.Fatalf("registry has no %s", jobcontract.KindDailyMetricsFinalize)
	}
	rows, err := venue.admin.Query(ctx, `
SELECT id, args::text FROM river.river_job
WHERE kind = 'metrics.daily_finalize'
  AND state::text IN ('available', 'scheduled', 'retryable', 'pending')
  AND args @> jsonb_build_object('domain', jsonb_build_object('id', $1::text))
ORDER BY id`, runID)
	if err != nil {
		t.Fatal(err)
	}
	// A Fatal inside the loop must not leave the connection busy: the pool's Close in the cleanup would wait forever.
	defer rows.Close()
	type job struct {
		id   int64
		args string
	}
	var jobs []job
	for rows.Next() {
		var found job
		if err := rows.Scan(&found.id, &found.args); err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, found)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, found := range jobs {
		var args jobruntime.DailyMetricsFinalizeArgs
		if err := json.Unmarshal([]byte(found.args), &args); err != nil {
			t.Fatalf("River job %d args: %v", found.id, err)
		}
		execution := &jobruntime.Execution[jobruntime.DailyMetricsFinalizeArgs]{
			JobID: found.id, Attempt: 1, Args: args, Envelope: args.ContractEnvelope(),
			OrganizationID: args.OrganizationID, Definition: descriptor,
		}
		if err := handler.Work(ctx, execution); err != nil {
			t.Fatalf("finalize River job %d: %v", found.id, err)
		}
		if _, err := venue.admin.Exec(ctx,
			`UPDATE river.river_job SET state = 'completed', finalized_at = now(), attempt = 1 WHERE id = $1`,
			found.id); err != nil {
			t.Fatal(err)
		}
	}
	return icFinalize.Load()
}

// RED on main (CHAOS-14090): a run in shape D -- finalize pending for more than
// 24 h, its River job cleaned and its outbox row pruned -- is reached by no
// reconciler stage. One Step of the production reconciler step list must
// publish exactly one finalize redrive and deliver it; working it computes the
// finalize once and the run leaves 'running'.
func TestReconcilerRedrivesAFinalizePendingRunWithNoJobAndNoOutboxRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startFinalizePendingVenue(t, ctx)
	now := time.Now().UTC().Truncate(time.Microsecond)
	orgID, runID := uuid.NewString(), uuid.NewString()
	venue.seedShapeD(t, ctx, orgID, runID, "2026-11-01", now.Add(-48*time.Hour))
	if outboxRows, riverJobs := venue.finalizeTrace(t, ctx, runID); outboxRows != 0 || riverJobs != 0 {
		t.Fatalf("fixture: %d outbox rows and %d River jobs, want none", outboxRows, riverJobs)
	}

	result, err := venue.relay.Step(ctx, now, 10)
	if err != nil {
		t.Fatalf("reconciler Step: %v", err)
	}

	// The stage is fail-open: a deadline is a counted failure, not a Step error, so name it on a miss.
	if outboxRows, _ := venue.finalizeTrace(t, ctx, runID); outboxRows != 1 {
		var levels joboutbox.StaleFinalizeLevels
		if result.StaleFinalizeLevels != nil {
			levels = *result.StaleFinalizeLevels
		}
		t.Fatalf("after one reconciler Step the shape-D run has %d finalize outbox rows, want 1: "+
			"no stage redrove the stranded finalize (stage redriven %v, failures %+v, deferred %d, levels read %t %+v)",
			outboxRows, result.StaleFinalizeRedriven, result.StaleFinalizeFailures, result.StaleFinalizeDeferred,
			result.StaleFinalizeLevels != nil, levels)
	}
	// The redrive is stamped with the publisher's own clock, after the Step's
	// now, so the relay delivers it on the next tick. That tick must deliver it
	// and must not publish a second one.
	if _, err := venue.relay.Step(ctx, time.Now().UTC().Add(time.Second), 10); err != nil {
		t.Fatalf("second reconciler Step: %v", err)
	}
	if outboxRows, riverJobs := venue.finalizeTrace(t, ctx, runID); outboxRows != 1 || riverJobs != 1 {
		t.Fatalf("after the next Step: %d finalize outbox rows and %d River finalize jobs, want 1 and 1",
			outboxRows, riverJobs)
	}
	if computes := venue.workRiverFinalizes(t, ctx, runID); computes != 1 {
		t.Fatalf("the redriven finalize computed %d times, want 1", computes)
	}
	if status, finalization := venue.runState(t, ctx, runID); status != "succeeded" || finalization != "succeeded" {
		t.Fatalf("run status=%s finalization=%s after the redrive, want succeeded/succeeded", status, finalization)
	}
}

// A run whose finalize River job is still live while outbox retention already
// pruned its row is skipped and counted as live. A redrive beside a live job
// adds a second finalize job (#6440 r3 finding 4). Each state steps one stage
// interval later, so every state is surveyed.
func TestReconcilerSkipsAFinalizePendingRunWhoseRiverJobIsLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startFinalizePendingVenue(t, ctx)
	now := time.Now().UTC().Truncate(time.Microsecond)
	for index, state := range []string{"available", "scheduled", "retryable", "running"} {
		t.Run(state, func(t *testing.T) {
			orgID, runID := uuid.NewString(), uuid.NewString()
			venue.seedShapeD(t, ctx, orgID, runID, time.Date(2026, 10, 1+index, 0, 0, 0, 0, time.UTC).Format("2006-01-02"), now.Add(-48*time.Hour))
			// A live job outside the horizon too: River can hold a scheduled
			// or retryable job past the day the outbox keeps its row.
			venue.seedRiverFinalize(t, ctx, orgID, runID, state, now.Add(-30*time.Hour))

			result, err := venue.relay.Step(ctx, now.Add(time.Duration(index)*11*time.Minute), 10)
			if err != nil {
				t.Fatalf("reconciler Step: %v", err)
			}
			if result.StaleFinalizeLevels == nil || result.StaleFinalizeLevels.Live != index+1 || len(result.StaleFinalizeRedriven) != 0 {
				t.Fatalf("live %s River job: levels %+v redriven %v, want %d live and nothing redriven",
					state, result.StaleFinalizeLevels, result.StaleFinalizeRedriven, index+1)
			}

			if outboxRows, riverJobs := venue.finalizeTrace(t, ctx, runID); outboxRows != 0 || riverJobs != 1 {
				t.Fatalf("live %s River job: %d outbox rows and %d River jobs after Step, want 0 and 1 "+
					"(no redrive beside a live job)", state, outboxRows, riverJobs)
			}
			if status, finalization := venue.runState(t, ctx, runID); status != "running" || finalization != "pending" {
				t.Fatalf("run status=%s finalization=%s, want running/pending", status, finalization)
			}
		})
	}
}

// Control of the ClaimFinalize CAS (green on main): an old finalize job that is
// still queued and the redrive the stage publishes compute the finalize exactly
// once together. The first job claims and completes the run; the second finds
// it ended.
func TestAQueuedOldFinalizeAndItsRedriveComputeOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startFinalizePendingVenue(t, ctx)
	now := time.Now().UTC().Truncate(time.Microsecond)
	orgID, runID := uuid.NewString(), uuid.NewString()
	venue.seedShapeD(t, ctx, orgID, runID, "2026-11-02", now.Add(-48*time.Hour))
	venue.seedRiverFinalize(t, ctx, orgID, runID, "available", now.Add(-47*time.Hour))

	outcome, err := venue.store.RedriveStrandedFinalize(ctx, venue.pub, []string{runID}, uuid.NewString(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(outcome.FinalizedRunIDs) != 1 {
		t.Fatalf("redrive published for %v, want the run", outcome.FinalizedRunIDs)
	}
	// After the publish: the redrive row is stamped with the publisher's clock.
	if _, err := venue.relay.Step(ctx, time.Now().UTC().Add(time.Second), 10); err != nil {
		t.Fatalf("reconciler Step: %v", err)
	}
	if outboxRows, riverJobs := venue.finalizeTrace(t, ctx, runID); outboxRows != 1 || riverJobs != 2 {
		t.Fatalf("%d outbox rows and %d River jobs, want 1 (the redrive) and 2 (old + redrive)", outboxRows, riverJobs)
	}
	if computes := venue.workRiverFinalizes(t, ctx, runID); computes != 1 {
		t.Fatalf("old queued finalize + redrive computed %d times, want exactly 1 (ClaimFinalize CAS)", computes)
	}
	if status, finalization := venue.runState(t, ctx, runID); status != "succeeded" || finalization != "succeeded" {
		t.Fatalf("run status=%s finalization=%s, want succeeded/succeeded", status, finalization)
	}
}

// OPEN BY DESIGN (pm ruling F4; follow-up CHAOS-14492): a run whose discovery
// found no repository is materialized empty by the real producer -- no
// partition, the finalize handed off in the same transaction -- and then loses
// that finalize (River cleaned the job, retention pruned the outbox row). After
// retention it looks exactly like a run whose dispatch died before it
// materialized, and a zero-partition finalize ends the run as no_repositories,
// so the stage never guesses: it counts the run as no_partitions and redrives
// nothing. The follow-up flips this WANT.
func TestAMaterializedEmptyRunWithALostFinalizeIsNotRecoveredByThisStage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startFinalizePendingVenue(t, ctx)
	orgID := uuid.NewString()
	tx, err := venue.domain.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run, err := venue.store.StartScheduledFanoutRunTx(ctx, tx, daily.ScheduledFanoutRequest{
		OrganizationID: orgID, TargetDay: time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC),
		Generation: daily.ScheduledFanoutGenerationPrefix + "2026-11-04T01:00:00Z",
	}, venue.pub)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	claimed, err := venue.store.ClaimDispatch(ctx, run.ID)
	if err != nil || claimed == nil {
		t.Fatalf("ClaimDispatch: %v %v", claimed, err)
	}
	if _, err := venue.store.MaterializeScheduledFanout(ctx, *claimed, nil, venue.pub); err != nil {
		t.Fatalf("MaterializeScheduledFanout with an empty snapshot: %v", err)
	}
	var partitions, handoffs int
	if err := venue.admin.QueryRow(ctx, `
SELECT (SELECT count(*) FROM public.daily_metrics_partitions WHERE run_id = $1::uuid),
       (SELECT count(*) FROM public.worker_job_outbox WHERE dedupe_key = $2)`,
		run.ID, daily.FinalizeHandoffKey(run.ID)).Scan(&partitions, &handoffs); err != nil {
		t.Fatal(err)
	}
	if partitions != 0 || handoffs != 1 {
		t.Fatalf("fixture: %d partitions and %d finalize handoffs, want the empty shape (0 and 1)", partitions, handoffs)
	}
	// The finalize is lost and the run is past the horizon.
	old := time.Now().UTC().Add(-48 * time.Hour)
	if _, err := venue.admin.Exec(ctx, `
DELETE FROM public.worker_job_outbox WHERE args #>> '{domain,id}' = $1`, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := venue.admin.Exec(ctx, `
UPDATE public.daily_metrics_runs SET created_at = $2, updated_at = $2 WHERE id = $1::uuid`, run.ID, old); err != nil {
		t.Fatal(err)
	}
	if status, finalization := venue.runState(t, ctx, run.ID); status != "running" || finalization != "pending" {
		t.Fatalf("fixture run status=%s finalization=%s, want running/pending", status, finalization)
	}

	result, err := venue.relay.Step(ctx, time.Now().UTC(), 10)
	if err != nil {
		t.Fatalf("reconciler Step: %v", err)
	}
	if result.StaleFinalizeLevels == nil || result.StaleFinalizeLevels.NoPartitions != 1 || len(result.StaleFinalizeRedriven) != 0 {
		t.Fatalf("levels %+v redriven %v, want one standing no_partitions run and nothing redriven",
			result.StaleFinalizeLevels, result.StaleFinalizeRedriven)
	}
	if _, err := venue.relay.Step(ctx, time.Now().UTC().Add(time.Second), 10); err != nil {
		t.Fatalf("second reconciler Step: %v", err)
	}
	if outboxRows, riverJobs := venue.finalizeTrace(t, ctx, run.ID); outboxRows != 0 || riverJobs != 0 {
		t.Fatalf("materialized-empty run with a lost finalize: %d finalize outbox rows and %d River jobs, want 0 and 0 "+
			"(not recovered by this stage, by design)", outboxRows, riverJobs)
	}
	if status, finalization := venue.runState(t, ctx, run.ID); status != "running" || finalization != "pending" {
		t.Fatalf("run status=%s finalization=%s, want it left running/pending", status, finalization)
	}
}

// redriveRows counts the run's finalize outbox rows written by a redrive.
func (venue *finalizePendingVenue) redriveRows(t *testing.T, ctx context.Context, runID string) int {
	t.Helper()
	var count int
	if err := venue.admin.QueryRow(ctx, `
SELECT count(*) FROM public.worker_job_outbox
WHERE job_kind = 'metrics.daily_finalize' AND args #>> '{domain,id}' = $1
  AND dedupe_key LIKE 'metrics.daily_finalize:redrive:%'`, runID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// cleanRiverFinalizes deletes the run's River finalize jobs, as River's
// cleaner does once a finished job is past its retention.
func (venue *finalizePendingVenue) cleanRiverFinalizes(t *testing.T, ctx context.Context, runID string) {
	t.Helper()
	if _, err := venue.admin.Exec(ctx, `
DELETE FROM river.river_job
WHERE kind = 'metrics.daily_finalize'
  AND args @> jsonb_build_object('domain', jsonb_build_object('id', $1::text))`, runID); err != nil {
		t.Fatal(err)
	}
}

// C1 (RED on main: nothing redrives): the stage redrives one run at most once
// per 24 h. After the first redrive the River job is gone before ClaimFinalize
// (the delivery died and River's cleaner removed it), so only the "recent"
// refusal holds the run back. Inside the horizon: no second publish. Past it:
// exactly one more. Dropping the recent fragment must turn the middle step red.
func TestStaleFinalizeRedrivesARunAtMostOncePer24Hours(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startFinalizePendingVenue(t, ctx)
	start := time.Now().UTC()
	orgID, runID := uuid.NewString(), uuid.NewString()
	venue.seedShapeD(t, ctx, orgID, runID, "2026-11-05", start.Add(-48*time.Hour))

	if _, err := venue.relay.Step(ctx, start, 10); err != nil {
		t.Fatalf("Step 1: %v", err)
	}
	if got := venue.redriveRows(t, ctx, runID); got != 1 {
		t.Fatalf("Step 1: %d redrive rows, want 1: no stage redrove the stranded finalize", got)
	}
	if _, err := venue.relay.Step(ctx, time.Now().UTC().Add(time.Second), 10); err != nil {
		t.Fatalf("delivery Step: %v", err)
	}
	if _, riverJobs := venue.finalizeTrace(t, ctx, runID); riverJobs != 1 {
		t.Fatalf("the redrive was not delivered: %d River jobs, want 1", riverJobs)
	}
	venue.cleanRiverFinalizes(t, ctx, runID)

	inside, err := venue.relay.Step(ctx, start.Add(11*time.Minute), 10)
	if err != nil {
		t.Fatalf("Step inside 24 h: %v", err)
	}
	if inside.StaleFinalizeLevels == nil || inside.StaleFinalizeLevels.Recent != 1 {
		t.Fatalf("inside 24 h: levels %+v, want the run standing as recent", inside.StaleFinalizeLevels)
	}
	if got := venue.redriveRows(t, ctx, runID); got != 1 {
		t.Fatalf("inside 24 h of the first redrive: %d redrive rows, want 1 (at most one per run per 24 h)", got)
	}
	if _, err := venue.relay.Step(ctx, start.Add(24*time.Hour+11*time.Minute), 10); err != nil {
		t.Fatalf("Step past 24 h: %v", err)
	}
	if got := venue.redriveRows(t, ctx, runID); got != 2 {
		t.Fatalf("past 24 h of the first redrive: %d redrive rows, want 2", got)
	}
}

// C2 (RED on main): a delivered finalize outbox row older than 24 h whose River
// job is gone (River cleaned it; outbox retention has not pruned the row yet).
// By the fragments it is a candidate: no live job (no River row, the row is not
// pending or claimed), not the strand shape (that shape JOINs the River row),
// not recent (the row is older than the horizon). WANT: one redrive row beside
// the old delivered row, which stays as it is, and the redrive is delivered.
func TestStaleFinalizeRedrivesARunWhoseDeliveredRowOutlivedItsRiverJob(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startFinalizePendingVenue(t, ctx)
	orgID, runID := uuid.NewString(), uuid.NewString()
	venue.seedShapeD(t, ctx, orgID, runID, "2026-11-06", time.Now().UTC().Add(-48*time.Hour))
	tx, err := venue.domain.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := venue.pub.PublishFinalizeTx(ctx, tx, daily.Run{ID: runID, OrganizationID: orgID, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := venue.relay.Step(ctx, time.Now().UTC().Add(time.Second), 10); err != nil {
		t.Fatalf("delivery Step: %v", err)
	}
	var handoffStatus string
	if err := venue.admin.QueryRow(ctx, `SELECT status FROM public.worker_job_outbox WHERE dedupe_key = $1`,
		daily.FinalizeHandoffKey(runID)).Scan(&handoffStatus); err != nil {
		t.Fatal(err)
	}
	if handoffStatus != "delivered" {
		t.Fatalf("fixture: the canonical finalize row is %q, want delivered", handoffStatus)
	}
	venue.cleanRiverFinalizes(t, ctx, runID)
	old := time.Now().UTC().Add(-30 * time.Hour)
	if _, err := venue.admin.Exec(ctx, `
UPDATE public.worker_job_outbox SET created_at = $2, delivered_at = $2, updated_at = $2 WHERE dedupe_key = $1`,
		daily.FinalizeHandoffKey(runID), old); err != nil {
		t.Fatal(err)
	}

	// One stage interval after the delivery Step, which already ran a pass.
	later := time.Now().UTC().Add(11 * time.Minute)
	if _, err := venue.relay.Step(ctx, later, 10); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if got := venue.redriveRows(t, ctx, runID); got != 1 {
		t.Fatalf("delivered row older than 24 h, River job gone: %d redrive rows, want 1", got)
	}
	if err := venue.admin.QueryRow(ctx, `SELECT status FROM public.worker_job_outbox WHERE dedupe_key = $1`,
		daily.FinalizeHandoffKey(runID)).Scan(&handoffStatus); err != nil {
		t.Fatal(err)
	}
	if handoffStatus != "delivered" {
		t.Fatalf("the old delivered row is %q, want it left delivered", handoffStatus)
	}
	if _, err := venue.relay.Step(ctx, later.Add(time.Second), 10); err != nil {
		t.Fatalf("second Step: %v", err)
	}
	if _, riverJobs := venue.finalizeTrace(t, ctx, runID); riverJobs != 1 {
		t.Fatalf("the redrive was not delivered: %d River jobs, want 1", riverJobs)
	}
}

// seedRunWithoutPartitions writes a running, finalize-pending run with no
// partition at all.
func (venue *finalizePendingVenue) seedRunWithoutPartitions(t *testing.T, ctx context.Context, orgID, runID, targetDay string, at time.Time) {
	t.Helper()
	if _, err := venue.admin.Exec(ctx, `
INSERT INTO public.daily_metrics_runs
    (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at, full_org)
VALUES ($1::uuid, $2::uuid, $3::date, $4, 'running', 'pending', $5, $5, true)`,
		runID, orgID, targetDay, "post-sync:"+runID, at); err != nil {
		t.Fatal(err)
	}
}

func stepDay(base time.Time, offset int) string {
	return base.AddDate(0, 0, offset).Format("2006-01-02")
}

// F1: standing refusals older than a candidate never hide it. The Step limit
// is 10; 18 refusals are older than the one candidate: 11 with no partition
// (the oldest, more than the limit), 4 recent and 3 live. The candidate is
// redriven in the first pass, nothing else is, and the refusals are counted by
// class. Each refusal fragment dropped from statement A turns this red: no
// partition fills the limit ahead of the candidate, recent and live runs get
// redriven.
func TestStaleFinalizeStandingRefusalsDoNotHideACandidate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startFinalizePendingVenue(t, ctx)
	now := time.Now().UTC().Truncate(time.Microsecond)
	orgID := uuid.NewString()
	days := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for index := 0; index < 18; index++ {
		runID := uuid.NewString()
		at := now.Add(-60*time.Hour - time.Duration(index)*time.Minute)
		switch {
		case index < 11:
			venue.seedRunWithoutPartitions(t, ctx, orgID, runID, stepDay(days, index), at.Add(-10*time.Hour))
		case index < 15:
			venue.seedShapeD(t, ctx, orgID, runID, stepDay(days, index), at)
			venue.seedRiverFinalize(t, ctx, orgID, runID, "completed", now.Add(-time.Hour))
		default:
			venue.seedShapeD(t, ctx, orgID, runID, stepDay(days, index), at)
			venue.seedRiverFinalize(t, ctx, orgID, runID, "scheduled", now.Add(-30*time.Hour))
		}
	}
	candidate := uuid.NewString()
	venue.seedShapeD(t, ctx, orgID, candidate, stepDay(days, 30), now.Add(-30*time.Hour))

	result, err := venue.relay.Step(ctx, now, 10)
	if err != nil {
		t.Fatalf("reconciler Step: %v", err)
	}
	if len(result.StaleFinalizeRedriven) != 1 || result.StaleFinalizeRedriven[0] != candidate || venue.redriveRows(t, ctx, candidate) != 1 {
		t.Fatalf("redriven %v (rows %d), want the candidate behind 18 older refusals", result.StaleFinalizeRedriven, venue.redriveRows(t, ctx, candidate))
	}
	want := joboutbox.StaleFinalizeLevels{Live: 3, Recent: 4, NoPartitions: 11}
	if result.StaleFinalizeLevels == nil || *result.StaleFinalizeLevels != want {
		t.Fatalf("levels %+v, want %+v", result.StaleFinalizeLevels, want)
	}
}

// The candidate matrix of one pass, against the fragments:
//   - shape D -> redriven;
//   - a partition written inside the horizon, a run written inside it, a
//     'failed' finalization -> not in the stage at all (base);
//   - an undelivered canonical finalize row -> live;
//   - a delivered row whose River job is still there (terminal) -> the
//     EXISTING finalize strand shape rearms it in the same Step, and the stage
//     then sees the rearmed row as live: the two never both act;
//   - the same, but the finalize's idempotency claim is still live, so the
//     strand shape refuses it -> the stage refuses it too, as strand_shape
//     (a redrive here would run beside a live claim).
//
// A and B agree: the base runs are the redriven ones plus the counted refusals.
func TestStaleFinalizeCandidateMatrix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startFinalizePendingVenue(t, ctx)
	now := time.Now().UTC().Truncate(time.Microsecond)
	old := now.Add(-48 * time.Hour)
	orgID := uuid.NewString()
	days := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	shapeD, freshPartition, freshRun, failedFinalize, undelivered, strandShape, claimLive :=
		uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	for index, runID := range []string{freshPartition, freshRun, failedFinalize, undelivered, strandShape, claimLive} {
		at := old
		if runID == freshRun {
			at = now.Add(-time.Hour)
		}
		venue.seedShapeD(t, ctx, orgID, runID, stepDay(days, index), at)
	}
	if _, err := venue.admin.Exec(ctx, `UPDATE public.daily_metrics_partitions SET updated_at = $2 WHERE run_id = $1::uuid`,
		freshPartition, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	// freshRun is excluded by the run clause ALONE: its partitions are old, so
	// dropping "run.updated_at <= $1" from statement A gets it redriven.
	if _, err := venue.admin.Exec(ctx, `UPDATE public.daily_metrics_partitions SET updated_at = $2, completed_at = $2, created_at = $2 WHERE run_id = $1::uuid`,
		freshRun, old); err != nil {
		t.Fatal(err)
	}
	if _, err := venue.admin.Exec(ctx, `UPDATE public.daily_metrics_runs SET finalization_status = 'failed' WHERE id = $1::uuid`,
		failedFinalize); err != nil {
		t.Fatal(err)
	}
	for _, runID := range []string{undelivered, strandShape, claimLive} {
		tx, err := venue.domain.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := venue.pub.PublishFinalizeTx(ctx, tx, daily.Run{ID: runID, OrganizationID: orgID, Status: "running"}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Deliver both canonical rows, then hold the first one back as undelivered
	// and end the second one's River job as completed, all aged past 24 h.
	if _, err := venue.relay.Step(ctx, time.Now().UTC().Add(time.Second), 10); err != nil {
		t.Fatalf("delivery Step: %v", err)
	}
	for _, runID := range []string{undelivered, strandShape, claimLive} {
		if _, riverJobs := venue.finalizeTrace(t, ctx, runID); riverJobs != 1 {
			t.Fatalf("fixture: run %s has %d River jobs after the delivery Step, want 1", runID, riverJobs)
		}
	}
	if _, err := venue.admin.Exec(ctx, `
UPDATE public.worker_job_outbox SET status = 'pending', river_job_id = NULL, delivered_at = NULL,
    created_at = $2, updated_at = $2, scheduled_at = $3, next_attempt_at = $3
WHERE dedupe_key = $1`, daily.FinalizeHandoffKey(undelivered), old, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	venue.cleanRiverFinalizes(t, ctx, undelivered)
	for _, runID := range []string{strandShape, claimLive} {
		if _, err := venue.admin.Exec(ctx, `
UPDATE public.worker_job_outbox SET created_at = $2, delivered_at = $2, updated_at = $2 WHERE dedupe_key = $1`,
			daily.FinalizeHandoffKey(runID), old); err != nil {
			t.Fatal(err)
		}
		if _, err := venue.admin.Exec(ctx, `
UPDATE river.river_job SET state = 'completed', finalized_at = $2, created_at = $2, attempt = 1
WHERE kind = 'metrics.daily_finalize' AND args @> jsonb_build_object('domain', jsonb_build_object('id', $1::text))`,
			runID, old); err != nil {
			t.Fatal(err)
		}
	}
	// A live idempotency claim on claimLive's finalize: the strand shape refuses it.
	if _, err := venue.admin.Exec(ctx, `
INSERT INTO public.worker_job_runs (
    id, job_kind, idempotency_key, org_id, domain_type, domain_id,
    status, claim_token, lease_expires_at, attempt_count,
    started_at, finished_at, created_at, updated_at
) VALUES ($1::uuid, 'metrics.daily_finalize', $2, $3, 'daily_metrics_run', $4, 'running', $5::uuid, $6, 1,
    statement_timestamp(), NULL, statement_timestamp(), statement_timestamp())`,
		uuid.NewString(), daily.FinalizeHandoffKey(claimLive), orgID, claimLive, uuid.NewString(), now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	// Seeded after the delivery Step, whose stage pass would have redriven it.
	venue.seedShapeD(t, ctx, orgID, shapeD, stepDay(days, 10), old)

	result, err := venue.relay.Step(ctx, time.Now().UTC().Add(11*time.Minute), 10)
	if err != nil {
		t.Fatalf("reconciler Step: %v", err)
	}
	if len(result.StaleFinalizeRedriven) != 1 || result.StaleFinalizeRedriven[0] != shapeD {
		t.Fatalf("redriven %v, want shape D only", result.StaleFinalizeRedriven)
	}
	if result.StrandsRearmed != 1 || result.StrandClaimsLive != 1 {
		t.Fatalf("the finalize strand shape rearmed %d rows and refused %d on a live claim, want 1 and 1", result.StrandsRearmed, result.StrandClaimsLive)
	}
	for _, runID := range []string{freshPartition, freshRun, failedFinalize, undelivered, strandShape, claimLive} {
		if got := venue.redriveRows(t, ctx, runID); got != 0 {
			t.Fatalf("run %s got %d redrive rows, want 0", runID, got)
		}
	}
	want := joboutbox.StaleFinalizeLevels{Live: 2, StrandShape: 1}
	if result.StaleFinalizeLevels == nil || *result.StaleFinalizeLevels != want {
		t.Fatalf("levels %+v, want %+v (live: the undelivered row and the rearmed row; strand_shape: the live claim)", result.StaleFinalizeLevels, want)
	}
}

// holdRunLock takes the run row lock in an admin transaction and returns its
// release.
func (venue *finalizePendingVenue) holdRunLock(t *testing.T, ctx context.Context, runID string) func() {
	t.Helper()
	tx, err := venue.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM public.daily_metrics_runs WHERE id = $1::uuid FOR UPDATE`, runID); err != nil {
		t.Fatal(err)
	}
	released := false
	release := func() {
		if !released {
			released = true
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}
	t.Cleanup(release)
	return release
}

// publishOrdinaryFinalize gives the relay one ordinary due row: the canonical
// finalize of a fresh run, from the real publisher.
func (venue *finalizePendingVenue) publishOrdinaryFinalize(t *testing.T, ctx context.Context, orgID, targetDay string) string {
	t.Helper()
	runID := uuid.NewString()
	venue.seedShapeD(t, ctx, orgID, runID, targetDay, time.Now().UTC())
	tx, err := venue.domain.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := venue.pub.PublishFinalizeTx(ctx, tx, daily.Run{ID: runID, OrganizationID: orgID, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return runID
}

// F2 + F5: a redrive blocked on a held run row lock ends at its per-run
// deadline. The Step returns while the lock is STILL held (so the deadline,
// not the lock release, ended the wait), with no error, one {redrive, deadline}
// failure, the newer candidate redriven in the same Step and the ordinary due
// row delivered. After the lock is gone, the next pass redrives the run.
func TestStaleFinalizeRedriveBlockedOnARunLockEndsAtTheDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startFinalizePendingVenue(t, ctx)
	now := time.Now().UTC().Truncate(time.Microsecond)
	orgID := uuid.NewString()
	locked, newer := uuid.NewString(), uuid.NewString()
	venue.seedShapeD(t, ctx, orgID, locked, "2026-07-01", now.Add(-60*time.Hour))
	venue.seedShapeD(t, ctx, orgID, newer, "2026-07-02", now.Add(-30*time.Hour))
	ordinary := venue.publishOrdinaryFinalize(t, ctx, orgID, "2026-07-03")

	// The lock holder releases only after Step has returned, and says so in an
	// ordered log. A redrive with no per-run deadline waits here until the pass
	// deadline (the newer run is then deferred, not redriven) or, with no
	// deadline at all, for ever: the holder never releases before Step returns.
	var orderMu sync.Mutex
	var order []string
	mark := func(event string) {
		orderMu.Lock()
		order = append(order, event)
		orderMu.Unlock()
	}
	lockTx, err := venue.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockTx.Exec(ctx, `SELECT 1 FROM public.daily_metrics_runs WHERE id = $1::uuid FOR UPDATE`, locked); err != nil {
		t.Fatal(err)
	}
	stepReturned, holderDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(holderDone)
		<-stepReturned
		mark("lock released")
		_ = lockTx.Rollback(context.WithoutCancel(ctx))
	}()

	result, err := venue.relay.Step(ctx, time.Now().UTC().Add(time.Second), 10)
	mark("step returned")
	close(stepReturned)
	<-holderDone
	if err != nil {
		t.Fatalf("Step returned the stage failure: %v", err)
	}
	if strings.Join(order, ", ") != "step returned, lock released" {
		t.Fatalf("order %v, want Step to return while the lock was still held", order)
	}
	wantFailures := []joboutbox.StaleFinalizeFailure{{Step: joboutbox.StaleFinalizeStepRedrive, Class: joboutbox.StaleFinalizeClassDeadline}}
	if !equalFailures(result.StaleFinalizeFailures, wantFailures) {
		t.Fatalf("failures %+v, want %+v", result.StaleFinalizeFailures, wantFailures)
	}
	if len(result.StaleFinalizeRedriven) != 1 || result.StaleFinalizeRedriven[0] != newer {
		t.Fatalf("redriven %v, want the newer candidate in the same Step", result.StaleFinalizeRedriven)
	}
	if _, riverJobs := venue.finalizeTrace(t, ctx, ordinary); riverJobs != 1 || result.Delivered < 1 {
		t.Fatalf("ordinary row: %d River jobs, %d delivered, want it delivered in the same Step", riverJobs, result.Delivered)
	}
	if got := venue.redriveRows(t, ctx, locked); got != 0 {
		t.Fatalf("the locked run got %d redrive rows during the lock, want 0", got)
	}

	next, err := venue.relay.Step(ctx, time.Now().UTC().Add(11*time.Minute), 10)
	if err != nil {
		t.Fatalf("next Step: %v", err)
	}
	if len(next.StaleFinalizeRedriven) != 1 || next.StaleFinalizeRedriven[0] != locked || venue.redriveRows(t, ctx, locked) != 1 {
		t.Fatalf("after the lock: redriven %v, want the formerly locked run", next.StaleFinalizeRedriven)
	}
}

func equalFailures(got, want []joboutbox.StaleFinalizeFailure) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

// F3: the redrive's backend is ended while it waits on the lock, so its
// transaction and its rollback fail on a dead connection. The stage reports
// one {redrive, unavailable} failure and nothing redriven; never "redriven 0
// with no failure".
func TestStaleFinalizeRedriveWhoseRollbackFailsIsOneFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startFinalizePendingVenue(t, ctx)
	now := time.Now().UTC().Truncate(time.Microsecond)
	orgID, runID := uuid.NewString(), uuid.NewString()
	venue.seedShapeD(t, ctx, orgID, runID, "2026-06-01", now.Add(-48*time.Hour))
	release := venue.holdRunLock(t, ctx, runID)

	stop := make(chan struct{})
	killed := make(chan bool, 1)
	go func() {
		for {
			select {
			case <-stop:
				killed <- false
				return
			default:
			}
			var terminated bool
			err := venue.admin.QueryRow(ctx, `
SELECT coalesce(bool_or(pg_terminate_backend(pid)), false) FROM pg_stat_activity
WHERE usename = $1 AND wait_event_type = 'Lock'`, venue.domainRole).Scan(&terminated)
			if err == nil && terminated {
				killed <- true
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	result, err := venue.relay.Step(ctx, now, 10)
	close(stop)
	wasKilled := <-killed
	release()
	if err != nil {
		t.Fatalf("Step returned the stage failure: %v", err)
	}
	if !wasKilled {
		t.Fatal("fixture: the redrive's backend was never seen waiting on the lock")
	}
	want := []joboutbox.StaleFinalizeFailure{{Step: joboutbox.StaleFinalizeStepRedrive, Class: joboutbox.StaleFinalizeClassUnavailable}}
	if !equalFailures(result.StaleFinalizeFailures, want) || len(result.StaleFinalizeRedriven) != 0 {
		t.Fatalf("failures %+v redriven %v, want %+v and nothing redriven", result.StaleFinalizeFailures, result.StaleFinalizeRedriven, want)
	}
	if got := venue.redriveRows(t, ctx, runID); got != 0 {
		t.Fatalf("%d redrive rows after the failed redrive, want 0", got)
	}
}

// acp's pool split: the survey runs on the queue pool (the queue role, the only
// one with River access) and the redrive on the domain pool (the only one with
// an outbox INSERT and a runs UPDATE).
func TestStaleFinalizeSurveyIsQueueRoleAndRedriveIsDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startFinalizePendingVenue(t, ctx)
	now := time.Now().UTC().Truncate(time.Microsecond)
	orgID, runID := uuid.NewString(), uuid.NewString()
	venue.seedShapeD(t, ctx, orgID, runID, "2026-05-01", now.Add(-48*time.Hour))
	for pool, role := range map[*pgxpool.Pool]string{venue.queue: venue.queueRole, venue.domain: venue.domainRole} {
		var current string
		if err := pool.QueryRow(ctx, `SELECT current_user`).Scan(&current); err != nil || current != role {
			t.Fatalf("pool role %q (%v), want %q", current, err, role)
		}
	}

	result, err := venue.relay.Step(ctx, now, 10)
	if err != nil || len(result.StaleFinalizeRedriven) != 1 {
		t.Fatalf("Step: %v, redriven %v, want the run", err, result.StaleFinalizeRedriven)
	}
	const survey, redrive = "AND ($3::timestamptz IS NULL OR (run.updated_at, run.id) > ($3::timestamptz, $4::uuid))", "FOR UPDATE OF run"
	if !venue.queueSQL.sawContaining(survey) || venue.domainSQL.sawContaining(survey) {
		t.Fatal("the candidate read did not run on the queue pool only")
	}
	if !venue.domainSQL.sawContaining(redrive) || venue.queueSQL.sawContaining(redrive) {
		t.Fatal("the redrive did not run on the domain pool only")
	}
}

// I1: statement A's keyset arm "(run.updated_at, run.id) > ($3, $4::uuid)" on
// real Postgres, with a cursor that made the round trip through the driver.
// Step limit 3; three shape-D runs; the two oldest are locked from another
// connection. Pass 1: two deadline failures end the pass, the third run is
// deferred, the cursor is the second run. Pass 2 (past the interval, locks
// still held): the read starts AFTER the cursor, so the third run is redriven
// and no locked run is tried. Then the locks are released: after the wrap the
// two runs are redriven.
func TestStaleFinalizeCursorReadsPastTheLastTriedRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startFinalizePendingVenue(t, ctx)
	now := time.Now().UTC().Truncate(time.Microsecond)
	orgID := uuid.NewString()
	oldest, second, third := uuid.NewString(), uuid.NewString(), uuid.NewString()
	// Sub-second, distinct stamps: a lost microsecond in the round trip
	// would re-read or skip the cursor row.
	venue.seedShapeD(t, ctx, orgID, oldest, "2026-04-01", now.Add(-50*time.Hour+123456*time.Microsecond))
	venue.seedShapeD(t, ctx, orgID, second, "2026-04-02", now.Add(-40*time.Hour+654321*time.Microsecond))
	venue.seedShapeD(t, ctx, orgID, third, "2026-04-03", now.Add(-30*time.Hour+111111*time.Microsecond))
	releaseOldest := venue.holdRunLock(t, ctx, oldest)
	releaseSecond := venue.holdRunLock(t, ctx, second)

	deadline := joboutbox.StaleFinalizeFailure{Step: joboutbox.StaleFinalizeStepRedrive, Class: joboutbox.StaleFinalizeClassDeadline}
	first, err := venue.relay.Step(ctx, now, 3)
	if err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if !equalFailures(first.StaleFinalizeFailures, []joboutbox.StaleFinalizeFailure{deadline, deadline}) ||
		first.StaleFinalizeDeferred != 1 || len(first.StaleFinalizeRedriven) != 0 {
		t.Fatalf("pass 1: failures %+v deferred %d redriven %v, want 2 deadlines, 1 deferred, nothing redriven",
			first.StaleFinalizeFailures, first.StaleFinalizeDeferred, first.StaleFinalizeRedriven)
	}

	secondPass, err := venue.relay.Step(ctx, now.Add(11*time.Minute), 3)
	if err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if len(secondPass.StaleFinalizeFailures) != 0 || len(secondPass.StaleFinalizeRedriven) != 1 || secondPass.StaleFinalizeRedriven[0] != third {
		t.Fatalf("pass 2 (cursor at the second run): failures %+v redriven %v, want the third run redriven and no locked run tried",
			secondPass.StaleFinalizeFailures, secondPass.StaleFinalizeRedriven)
	}

	releaseOldest()
	releaseSecond()
	wrapped, err := venue.relay.Step(ctx, now.Add(22*time.Minute), 3)
	if err != nil {
		t.Fatalf("pass 3: %v", err)
	}
	if len(wrapped.StaleFinalizeFailures) != 0 || len(wrapped.StaleFinalizeRedriven) != 2 ||
		wrapped.StaleFinalizeRedriven[0] != oldest || wrapped.StaleFinalizeRedriven[1] != second {
		t.Fatalf("pass 3 (after the wrap, locks released): failures %+v redriven %v, want the two oldest runs in order",
			wrapped.StaleFinalizeFailures, wrapped.StaleFinalizeRedriven)
	}
}

// F-3: a run whose finalize job ended discarded or cancelled (its outbox row
// pruned, the River row kept by River's retention) is a candidate: the job can
// never run again. Dropping either state from liveJob's terminal list counts the
// run as live and turns this red.
func TestStaleFinalizeRedrivesARunWhoseFinalizeJobEndedDiscardedOrCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startFinalizePendingVenue(t, ctx)
	now := time.Now().UTC().Truncate(time.Microsecond)
	orgID := uuid.NewString()
	discarded, cancelled := uuid.NewString(), uuid.NewString()
	venue.seedShapeD(t, ctx, orgID, discarded, "2026-03-01", now.Add(-50*time.Hour))
	venue.seedShapeD(t, ctx, orgID, cancelled, "2026-03-02", now.Add(-49*time.Hour))
	venue.seedRiverFinalize(t, ctx, orgID, discarded, "discarded", now.Add(-30*time.Hour))
	venue.seedRiverFinalize(t, ctx, orgID, cancelled, "cancelled", now.Add(-30*time.Hour))

	result, err := venue.relay.Step(ctx, now, 10)
	if err != nil {
		t.Fatalf("reconciler Step: %v", err)
	}
	if len(result.StaleFinalizeRedriven) != 2 || result.StaleFinalizeRedriven[0] != discarded || result.StaleFinalizeRedriven[1] != cancelled {
		t.Fatalf("redriven %v, want the discarded run and the cancelled run", result.StaleFinalizeRedriven)
	}
	if result.StaleFinalizeLevels == nil || result.StaleFinalizeLevels.Live != 0 {
		t.Fatalf("levels %+v, want no live refusal", result.StaleFinalizeLevels)
	}
}

// (a) base first (acp ruling on SIZING-14090-2): when no run is in the base,
// the pass sends statement 0 only. It never sends statement A or B, which read
// worker_job_outbox and river_job (a full outbox scan on prod sizes), and it
// reports every level as an exact zero. The runs seeded here are NOT base: a
// fresh run, a failed finalization, a run with an unfinished partition, each
// with finalize rows in the outbox and River.
func TestStaleFinalizeBaseZeroReadsNoOutboxAndNoRiver(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startFinalizePendingVenue(t, ctx)
	now := time.Now().UTC().Truncate(time.Microsecond)
	orgID := uuid.NewString()
	fresh, failed, unfinished := uuid.NewString(), uuid.NewString(), uuid.NewString()
	venue.seedShapeD(t, ctx, orgID, fresh, "2026-02-01", now.Add(-time.Hour))
	venue.seedShapeD(t, ctx, orgID, failed, "2026-02-02", now.Add(-48*time.Hour))
	venue.seedShapeD(t, ctx, orgID, unfinished, "2026-02-03", now.Add(-48*time.Hour))
	if _, err := venue.admin.Exec(ctx, `UPDATE public.daily_metrics_runs SET finalization_status = 'failed' WHERE id = $1::uuid`, failed); err != nil {
		t.Fatal(err)
	}
	if _, err := venue.admin.Exec(ctx, `UPDATE public.daily_metrics_partitions SET status = 'pending', completed_at = NULL WHERE run_id = $1::uuid`, unfinished); err != nil {
		t.Fatal(err)
	}
	for _, runID := range []string{fresh, failed, unfinished} {
		venue.seedRiverFinalize(t, ctx, orgID, runID, "discarded", now.Add(-30*time.Hour))
	}
	venue.publishOrdinaryFinalize(t, ctx, orgID, "2026-02-04")

	result, err := venue.relay.Step(ctx, now, 10)
	if err != nil {
		t.Fatalf("reconciler Step: %v", err)
	}
	if result.StaleFinalizeLevels == nil || *result.StaleFinalizeLevels != (joboutbox.StaleFinalizeLevels{}) ||
		len(result.StaleFinalizeRedriven) != 0 || len(result.StaleFinalizeFailures) != 0 {
		t.Fatalf("base 0: levels %+v redriven %v failures %+v, want exact zero levels and nothing else",
			result.StaleFinalizeLevels, result.StaleFinalizeRedriven, result.StaleFinalizeFailures)
	}
	const statement0 = "SELECT EXISTS (SELECT 1 FROM public.daily_metrics_runs AS run WHERE"
	const statementA = "AND ($3::timestamptz IS NULL OR (run.updated_at, run.id) > ($3::timestamptz, $4::uuid))"
	const statementB = "ELSE 'redrive'"
	if !venue.queueSQL.sawContaining(statement0) {
		t.Fatal("statement 0 did not run on the queue pool")
	}
	for name, marker := range map[string]string{"A": statementA, "B": statementB} {
		if venue.queueSQL.sawContaining(marker) || venue.domainSQL.sawContaining(marker) {
			t.Fatalf("statement %s ran with an empty base: it reads worker_job_outbox and river_job for nothing", name)
		}
	}
}

// F2 (fcc PLAN-READ-14090-PERF-1): the TRUE arm of statement 0. One base row of each class -- a candidate, a live job,
// the finalize strand shape (its idempotency claim is live, so that shape refuses it too), a recent finalize job, no
// partition -- makes statement 0 true: the pass sends A and B, redrives the candidate and counts each refusal class once.
// A statement 0 that answers false for a base it has (a constant false, a narrowed copy) turns this red; one that
// answers true for an empty base turns TestStaleFinalizeBaseZeroReadsNoOutboxAndNoRiver red.
func TestStaleFinalizeBaseWithOneRunOfEachClassSendsAAndB(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startFinalizePendingVenue(t, ctx)
	now := time.Now().UTC().Truncate(time.Microsecond)
	old := now.Add(-48 * time.Hour)
	orgID := uuid.NewString()

	// strand_shape: a delivered canonical finalize whose River job ended completed and whose idempotency claim is live
	// (the claimLive run of TestStaleFinalizeCandidateMatrix).
	strandShape := uuid.NewString()
	venue.seedShapeD(t, ctx, orgID, strandShape, "2026-01-01", old)
	tx, err := venue.domain.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := venue.pub.PublishFinalizeTx(ctx, tx, daily.Run{ID: strandShape, OrganizationID: orgID, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := venue.relay.Step(ctx, time.Now().UTC().Add(time.Second), 10); err != nil {
		t.Fatalf("delivery Step: %v", err)
	}
	if _, err := venue.admin.Exec(ctx, `
UPDATE public.worker_job_outbox SET created_at = $2, delivered_at = $2, updated_at = $2 WHERE dedupe_key = $1`,
		daily.FinalizeHandoffKey(strandShape), old); err != nil {
		t.Fatal(err)
	}
	if _, err := venue.admin.Exec(ctx, `
UPDATE river.river_job SET state = 'completed', finalized_at = $2, created_at = $2, attempt = 1
WHERE kind = 'metrics.daily_finalize' AND args @> jsonb_build_object('domain', jsonb_build_object('id', $1::text))`,
		strandShape, old); err != nil {
		t.Fatal(err)
	}
	if _, err := venue.admin.Exec(ctx, `
INSERT INTO public.worker_job_runs (
    id, job_kind, idempotency_key, org_id, domain_type, domain_id,
    status, claim_token, lease_expires_at, attempt_count,
    started_at, finished_at, created_at, updated_at
) VALUES ($1::uuid, 'metrics.daily_finalize', $2, $3, 'daily_metrics_run', $4, 'running', $5::uuid, $6, 1,
    statement_timestamp(), NULL, statement_timestamp(), statement_timestamp())`,
		uuid.NewString(), daily.FinalizeHandoffKey(strandShape), orgID, strandShape, uuid.NewString(), now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}

	candidate, live, recent, noPartitions := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	venue.seedShapeD(t, ctx, orgID, candidate, "2026-01-02", old)
	venue.seedShapeD(t, ctx, orgID, live, "2026-01-03", old)
	venue.seedRiverFinalize(t, ctx, orgID, live, "scheduled", now.Add(-30*time.Hour))
	venue.seedShapeD(t, ctx, orgID, recent, "2026-01-04", old)
	venue.seedRiverFinalize(t, ctx, orgID, recent, "completed", now.Add(-time.Hour))
	venue.seedRunWithoutPartitions(t, ctx, orgID, noPartitions, "2026-01-05", old)

	venue.queueSQL.reset()
	result, err := venue.relay.Step(ctx, time.Now().UTC().Add(11*time.Minute), 10)
	if err != nil {
		t.Fatalf("reconciler Step: %v", err)
	}
	if len(result.StaleFinalizeRedriven) != 1 || result.StaleFinalizeRedriven[0] != candidate || len(result.StaleFinalizeFailures) != 0 {
		t.Fatalf("redriven %v failures %+v, want the candidate redriven and no failure", result.StaleFinalizeRedriven, result.StaleFinalizeFailures)
	}
	want := joboutbox.StaleFinalizeLevels{Live: 1, StrandShape: 1, Recent: 1, NoPartitions: 1}
	if result.StaleFinalizeLevels == nil || *result.StaleFinalizeLevels != want {
		t.Fatalf("levels %+v, want %+v", result.StaleFinalizeLevels, want)
	}
	// JIT is off in the survey transaction before statement 0.
	if !venue.queueSQL.sawBefore("SET LOCAL jit = off", "SELECT EXISTS (SELECT 1 FROM public.daily_metrics_runs AS run WHERE") {
		t.Fatal("the survey sent statement 0 without SET LOCAL jit = off before it in its transaction")
	}
	for name, marker := range map[string]string{
		"0": "SELECT EXISTS (SELECT 1 FROM public.daily_metrics_runs AS run WHERE",
		"A": "AND ($3::timestamptz IS NULL OR (run.updated_at, run.id) > ($3::timestamptz, $4::uuid))",
		"B": "ELSE 'redrive'",
	} {
		if !venue.queueSQL.sawContaining(marker) {
			t.Fatalf("statement %s was not sent with a base of one run per class", name)
		}
	}
}

// Round 1 P1 (pm/acp ruling): two reconcilers read the same run as a candidate and both redrive it. Under the run lock
// the redrive re-checks the outbox (READ COMMITTED: a new snapshot per statement), so the one that waited on the lock
// sees the other's committed pending row and publishes nothing: exactly one redrive row. The admin transaction holds
// the run lock until both redrives wait on it, so the order is fixed by the lock, not by timing.
func TestTwoConcurrentStaleFinalizeRedrivesOfOneRunPublishOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startFinalizePendingVenue(t, ctx)
	orgID, runID := uuid.NewString(), uuid.NewString()
	venue.seedShapeD(t, ctx, orgID, runID, "2026-03-01", time.Now().UTC().Add(-48*time.Hour))
	redriver, err := newStaleFinalizeRedriver(venue.domain, venue.registry)
	if err != nil {
		t.Fatal(err)
	}
	release := venue.holdRunLock(t, ctx, runID)
	type outcome struct {
		redriven bool
		err      error
	}
	results := make(chan outcome, 2)
	for range 2 {
		go func() {
			redriven, err := redriver.RedriveStaleFinalize(ctx, runID, uuid.NewString(), time.Now().UTC().Add(-joboutbox.StaleFinalizeHorizon))
			results <- outcome{redriven, err}
		}()
	}
	for {
		var waiting int
		if err := venue.admin.QueryRow(ctx, `
SELECT count(*) FROM pg_catalog.pg_stat_activity
WHERE wait_event_type = 'Lock' AND query LIKE '%FOR UPDATE OF run%'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting == 2 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("%d of 2 redrives wait on the run lock", waiting)
		}
		time.Sleep(20 * time.Millisecond)
	}
	release()
	published := 0
	for range 2 {
		got := <-results
		if got.err != nil {
			t.Fatalf("redrive: %v", got.err)
		}
		if got.redriven {
			published++
		}
	}
	if rows := venue.redriveRows(t, ctx, runID); published != 1 || rows != 1 {
		t.Fatalf("two concurrent redrives: %d reported a publish, %d redrive rows, want 1 and 1", published, rows)
	}
	// JIT is off in the re-check transaction before the re-check runs.
	if !venue.domainSQL.sawBefore("SET LOCAL jit = off", "OR outbox.created_at > $2)") {
		t.Fatal("the redrive re-check ran without SET LOCAL jit = off before it in its transaction")
	}
}

// seedLiveRuns writes n base runs (shape D, one succeeded partition) under one org, each with a scheduled River
// finalize job, so statement B classes every one of them live.
func (venue *finalizePendingVenue) seedLiveRuns(t *testing.T, ctx context.Context, orgID string, firstDay, n int, at time.Time) {
	t.Helper()
	if _, err := venue.admin.Exec(ctx, `
WITH run AS (
    INSERT INTO public.daily_metrics_runs (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at)
    SELECT gen_random_uuid(), $1::uuid, DATE '2020-01-01' + n, 'live-level:' || n, 'running', 'pending', $4, $4
    FROM generate_series($2::int, $2::int + $3::int - 1) AS n
    RETURNING id
), partition AS (
    INSERT INTO public.daily_metrics_partitions (id, run_id, ordinal, repo_ids, status, attempt_count, completed_at, created_at, updated_at)
    SELECT gen_random_uuid(), run.id, 0, '[]'::jsonb, 'succeeded', 1, $4, $4, $4 FROM run
)
INSERT INTO river.river_job (args, attempt, created_at, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at)
SELECT jsonb_build_object('domain', jsonb_build_object('type', 'daily_metrics_run', 'id', run.id::text)),
       0, $4, NULL, 'metrics.daily_finalize', 4, '{}'::jsonb, 2, 'metrics', 'scheduled', $4 + interval '30 days'
FROM run`, orgID, firstDay, n, at); err != nil {
		t.Fatal(err)
	}
}

// Round 1 P2 (pm ruling): statement B reads cap + 1 rows only to learn that the base is larger than the cap; the
// row past the cap is not counted into a level. At exactly 1000 live runs: Live 1000, not truncated. At 1001 (the
// newest is live too): Live still 1000, truncated.
func TestStaleFinalizeLevelsCountTheCapAndNotTheLookaheadRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startFinalizePendingVenue(t, ctx)
	orgID := uuid.NewString()
	old := time.Now().UTC().Add(-72 * time.Hour)
	venue.seedLiveRuns(t, ctx, orgID, 1, 1000, old)
	start := time.Now().UTC()
	result, err := venue.relay.Step(ctx, start, 10)
	if err != nil {
		t.Fatalf("reconciler Step: %v", err)
	}
	if result.StaleFinalizeLevels == nil || *result.StaleFinalizeLevels != (joboutbox.StaleFinalizeLevels{Live: 1000}) {
		t.Fatalf("1000 live runs: levels %+v failures %+v, want Live 1000, not truncated", result.StaleFinalizeLevels, result.StaleFinalizeFailures)
	}
	venue.seedLiveRuns(t, ctx, orgID, 1001, 1, old.Add(time.Second))
	result, err = venue.relay.Step(ctx, start.Add(11*time.Minute), 10)
	if err != nil {
		t.Fatalf("reconciler Step: %v", err)
	}
	if result.StaleFinalizeLevels == nil || *result.StaleFinalizeLevels != (joboutbox.StaleFinalizeLevels{Live: 1000, Truncated: true}) {
		t.Fatalf("1001 live runs: levels %+v failures %+v, want Live 1000, truncated", result.StaleFinalizeLevels, result.StaleFinalizeFailures)
	}
}
