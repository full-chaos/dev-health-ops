//go:build integration

package syncreconciler

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type postSyncOutcomeDailyWriter struct{ store *daily.PostgresStore }

type postSyncOutcomeNopDailyPublisher struct{}

func (postSyncOutcomeNopDailyPublisher) PublishDispatchTx(context.Context, pgx.Tx, daily.Run, string) error {
	return nil
}

func (w postSyncOutcomeDailyWriter) StartRunTx(ctx context.Context, tx pgx.Tx, plan syncdispatchruntime.PostSyncPlan, prereq string) (string, string, error) {
	run, err := w.store.StartRunTx(ctx, tx, daily.StartRunRequest{
		OrganizationID: plan.OrganizationID, TargetDay: plan.TargetDay,
		Generation: "post-sync:" + plan.SyncRunID, PrerequisiteCompletionKey: prereq,
	}, postSyncOutcomeNopDailyPublisher{})
	if err != nil {
		return "", "", err
	}
	return run.ID, "daily-key", nil
}

type postSyncOutcomeWorker struct {
	river.WorkerDefaults[syncdispatchruntime.PostSyncArgs]
	service *syncdispatchruntime.NativePostSyncService
}

func (w *postSyncOutcomeWorker) Work(ctx context.Context, job *river.Job[syncdispatchruntime.PostSyncArgs]) error {
	return w.service.Fanout(ctx, job.Args)
}

// TestPostSyncOutcomeLedgerTellsNoOpFromNeverRan: once River's JobCleaner has removed the job rows, a fan-out that ran
// with a product (A), one that never ran (B, exhausted) and one that ran as a no-op plan (C) still differ in the ledger
// row sync_run_post_dispatches: A reads published, C no_repositories, B nothing. A redelivery keeps the first outcome and
// a delivery that is not current writes none.
func TestPostSyncOutcomeLedgerTellsNoOpFromNeverRan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h := startFinalizeBackstopHarness(t, ctx)
	resetMaterializerIntegrationTables(t, ctx, h.admin)
	if _, err := h.admin.Exec(ctx, `TRUNCATE river.river_job`); err != nil {
		t.Fatal(err)
	}
	backstopRoutes(t, ctx, h.admin)
	var generation int64
	if err := h.admin.QueryRow(ctx, `SELECT generation FROM public.sync_dispatch_transport_routes WHERE kind = 'post_sync'`).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	const (
		orgID         = "14183000-0000-4000-8000-000000000001"
		integrationID = "14183000-0000-4000-8000-000000000002"
		sourceID      = "14183000-0000-4000-8000-000000000003"
	)
	pgseed.EnsureSyncIntegration(ctx, t, h.admin, orgID, integrationID, sourceID)
	now := time.Now().UTC()

	type seedRow struct {
		name, runID, outboxID, unitID, runStatus, unitStatus string
		createdAt                                            time.Time
		exhausted                                            bool
		wantOutcome                                          string
		wantProducts                                         int
	}
	ran := &seedRow{"A ran, product", "14183000-0000-4000-8000-0000000000a1", "14183000-0000-4000-8000-0000000000a2",
		"14183000-0000-4000-8000-0000000000a3", "success", "success", now.Add(-2 * time.Hour), false, "published", 1}
	neverRan := &seedRow{"B never ran (exhausted)", "14183000-0000-4000-8000-0000000000b1", "14183000-0000-4000-8000-0000000000b2",
		"14183000-0000-4000-8000-0000000000b3", "success", "success", now.Add(-10 * 24 * time.Hour), true, "", 0}
	noOp := &seedRow{"C ran, no-op plan", "14183000-0000-4000-8000-0000000000c1", "14183000-0000-4000-8000-0000000000c2",
		"14183000-0000-4000-8000-0000000000c3", "failed", "failed", now.Add(-3 * time.Hour), false, "no_repositories", 0}
	seeds := []*seedRow{ran, neverRan, noOp}
	args := func(s *seedRow, attempt int64) syncdispatchruntime.PostSyncArgs {
		return syncdispatchruntime.PostSyncArgs{TransportArgs: syncdispatchruntime.TransportArgs{
			Version: syncdispatchruntime.ContractVersionV1, OrgID: orgID, RunID: s.runID,
			DispatchOutbox: s.outboxID, DeliveryAttempt: attempt, RouteGeneration: generation,
		}}
	}
	for _, s := range seeds {
		pgseed.EnsureSyncRun(ctx, t, h.admin, pgseed.SyncRun{ID: s.runID, OrgID: orgID, IntegrationID: integrationID})
		pgseed.InsertSyncRunUnit(ctx, t, h.admin, pgseed.SyncRunUnit{
			ID: s.unitID, RunID: s.runID, OrgID: orgID, IntegrationID: integrationID, SourceID: sourceID,
			DatasetKey: "commits", Status: s.unitStatus,
		})
		if _, err := h.admin.Exec(ctx, `UPDATE public.sync_runs SET status = $2, started_at = $3, completed_at = $4 WHERE id = $1`,
			s.runID, s.runStatus, s.createdAt.Add(-time.Hour), s.createdAt); err != nil {
			t.Fatal(err)
		}
		if _, err := h.admin.Exec(ctx, `INSERT INTO public.sync_run_post_dispatches (id, org_id, sync_run_id, kind, dispatched_at)
			VALUES (gen_random_uuid(), $1, $2, 'post_sync', $3)`, orgID, s.runID, s.createdAt); err != nil {
			t.Fatal(err)
		}
		inserted, err := h.river.Insert(ctx, args(s, 1), &river.InsertOpts{Queue: syncdispatchcontract.RiverQueue})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.admin.Exec(ctx, `INSERT INTO public.sync_dispatch_outbox
			(id,org_id,sync_run_id,kind,status,available_at,attempts,dispatched_at,dispatched_transport,dispatched_route_generation,transport_job_id,created_at,updated_at)
			VALUES ($1,$2,$3,'post_sync','dispatched',$4,1,$4,'river',$5,$6,$4,$4)`,
			s.outboxID, orgID, s.runID, s.createdAt, generation, strconv.FormatInt(inserted.Job.ID, 10)); err != nil {
			t.Fatal(err)
		}
		if s.exhausted {
			discardRiverJob(t, ctx, h.admin, inserted.Job.ID, now, 5, 5, "post_sync fanout unavailable")
		}
	}

	type ledgerState struct {
		outcome     *string
		committedAt *time.Time
		products    int
	}
	read := func(s *seedRow) ledgerState {
		t.Helper()
		var got ledgerState
		if err := h.admin.QueryRow(ctx, `
			SELECT ledger.post_sync_outcome, ledger.post_sync_committed_at,
			       (SELECT count(*) FROM public.daily_metrics_runs WHERE generation = 'post-sync:' || ledger.sync_run_id::text)
			FROM public.sync_run_post_dispatches AS ledger
			WHERE ledger.sync_run_id = $1 AND ledger.kind = 'post_sync'`, s.runID).Scan(&got.outcome, &got.committedAt, &got.products); err != nil {
			t.Fatalf("%s: ledger read: %v", s.name, err)
		}
		return got
	}

	store, err := daily.NewPostgresStore(h.admin)
	if err != nil {
		t.Fatal(err)
	}
	writers := &recordingPostSyncWriters{}
	service, err := syncdispatchruntime.NewNativePostSyncService(h.admin, postSyncOutcomeDailyWriter{store}, recordingRemaining{writers},
		recordingWorkGraph{writers}, recordingPublish{}, recordingPublish{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, &postSyncOutcomeWorker{service: service}); err != nil {
		t.Fatal(err)
	}
	// Retention 1 ms stands in for River's 24 h (completed) and 7 d (discarded) defaults, so the real JobCleaner removes the
	// job rows inside the test.
	client, err := river.NewClient(riverpgxv5.New(h.admin), &river.Config{
		Schema:                      "river",
		Queues:                      map[string]river.QueueConfig{syncdispatchcontract.RiverQueue: {MaxWorkers: 1}},
		Workers:                     workers,
		CompletedJobRetentionPeriod: time.Millisecond,
		DiscardedJobRetentionPeriod: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		_ = client.Stop(stopCtx)
	}()
	waitFor := func(label string, limit time.Duration, done func() bool) {
		t.Helper()
		deadline := time.Now().Add(limit)
		for !done() {
			if time.Now().After(deadline) {
				t.Fatalf("%s: not reached in %s", label, limit)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	waitFor("job rows removed by the JobCleaner", 120*time.Second, func() bool {
		var left int
		if err := h.admin.QueryRow(ctx, `SELECT count(*) FROM river.river_job`).Scan(&left); err != nil {
			t.Fatal(err)
		}
		return left == 0
	})

	for _, s := range seeds {
		got := read(s)
		if got.products != s.wantProducts {
			t.Errorf("%s: post-sync product rows = %d, want %d", s.name, got.products, s.wantProducts)
		}
		switch {
		case s.wantOutcome == "" && (got.outcome != nil || got.committedAt != nil):
			t.Errorf("%s: outcome = %v at %v, want none (the fan-out never committed)", s.name, got.outcome, got.committedAt)
		case s.wantOutcome != "" && (got.outcome == nil || *got.outcome != s.wantOutcome || got.committedAt == nil):
			t.Errorf("%s: outcome = %v at %v, want %q with a commit time", s.name, got.outcome, got.committedAt, s.wantOutcome)
		}
	}
	if t.Failed() {
		return
	}

	first := read(ran)
	if err := service.Fanout(ctx, args(ran, 1)); err != nil {
		t.Fatalf("redelivery of A: %v", err)
	}
	again := read(ran)
	if again.outcome == nil || *again.outcome != "published" || again.committedAt == nil || !again.committedAt.Equal(*first.committedAt) {
		t.Fatalf("redelivery of A: outcome = %v at %v, want published at %v (the first outcome is kept)", again.outcome, again.committedAt, first.committedAt)
	}

	if err := service.Fanout(ctx, args(neverRan, 9)); err != nil {
		t.Fatalf("not-current delivery of B: %v", err)
	}
	if got := read(neverRan); got.outcome != nil || got.committedAt != nil || got.products != 0 {
		t.Fatalf("not-current delivery of B: outcome = %v at %v, products %d, want none", got.outcome, got.committedAt, got.products)
	}
}
