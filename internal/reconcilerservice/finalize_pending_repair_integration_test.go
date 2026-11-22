//go:build integration

package reconcilerservice

import (
	"context"
	"encoding/json"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
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
	admin    *pgxpool.Pool
	domain   *pgxpool.Pool
	queue    *pgxpool.Pool
	registry *jobruntime.Registry
	relay    joboutbox.RelayStepper
	store    *daily.PostgresStore
	pub      *daily.PostgresPublisher
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
	domain := openFinalizePendingPool(t, ctx, finalizePendingRoleURI(t, instance.URI, domainRole, finalizePendingDomainPassword))
	queue := openFinalizePendingPool(t, ctx, finalizePendingRoleURI(t, instance.URI, queueRole, finalizePendingQueuePassword))
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
		admin: admin, domain: domain, queue: queue, registry: registry,
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

	if _, err := venue.relay.Step(ctx, now, 10); err != nil {
		t.Fatalf("reconciler Step: %v", err)
	}

	outboxRows, riverJobs := venue.finalizeTrace(t, ctx, runID)
	if outboxRows != 1 || riverJobs != 1 {
		t.Fatalf("after one reconciler Step the shape-D run has %d finalize outbox rows and %d River "+
			"finalize jobs, want 1 and 1: no stage redrove the stranded finalize", outboxRows, riverJobs)
	}
	if computes := venue.workRiverFinalizes(t, ctx, runID); computes != 1 {
		t.Fatalf("the redriven finalize computed %d times, want 1", computes)
	}
	if status, finalization := venue.runState(t, ctx, runID); status != "succeeded" || finalization != "succeeded" {
		t.Fatalf("run status=%s finalization=%s after the redrive, want succeeded/succeeded", status, finalization)
	}
}

// Control (passes on main for the trivial reason that nothing redrives; it is
// the guard for the stage): a run whose finalize River job is still live while
// outbox retention already pruned its row must be skipped. A redrive beside a
// live job adds a second finalize job (#6440 r3 finding 4).
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

			if _, err := venue.relay.Step(ctx, now, 10); err != nil {
				t.Fatalf("reconciler Step: %v", err)
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
	if _, err := venue.relay.Step(ctx, now, 10); err != nil {
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
