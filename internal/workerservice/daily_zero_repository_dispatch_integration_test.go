//go:build integration

package workerservice

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// A daily run of the whole organization whose discovery finds no repository
// reaches the finalize of PR-A through a finalize handoff that the dispatch
// publishes, on each trigger path. A retried dispatch of such a run cuts no
// partition: the handoff, or its abandonment record, says the run already
// decided that it is empty.

type zeroRepositoryDispatchRig struct {
	*zeroPartitionRig
	publisher  *daily.PostgresPublisher
	dispatcher *daily.Dispatcher
}

func newZeroRepositoryDispatchRig(t *testing.T, ctx context.Context) *zeroRepositoryDispatchRig {
	t.Helper()
	rig := newZeroPartitionRig(t, ctx)
	registry, err := jobruntime.Load(filepath.Join("contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := daily.NewPostgresPublisher(rig.pool, registry)
	if err != nil {
		t.Fatal(err)
	}
	discoverer, err := daily.NewClickHouseRepositoryDiscoverer(rig.conn)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := newDrainingDailyDispatcher(rig.store, publisher, discoverer, &zeroPartitionDrain{})
	if err != nil {
		t.Fatal(err)
	}
	return &zeroRepositoryDispatchRig{zeroPartitionRig: rig, publisher: publisher, dispatcher: dispatcher}
}

func (rig *zeroRepositoryDispatchRig) dispatch(t *testing.T, ctx context.Context, org, runID string) {
	t.Helper()
	execution := &jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs]{
		OrganizationID: &org,
		Envelope:       jobcontract.Envelope{OrganizationID: &org, Domain: jobcontract.DomainLink{Type: "daily_metrics_run", ID: runID}},
		Args: jobruntime.DailyMetricsDispatchArgs{EnvelopeArgs: func() jobruntime.EnvelopeArgs[jobcontract.DailyMetricsDispatchPayload] {
			args := nilPartitionExecution[jobcontract.DailyMetricsDispatchPayload](org, "daily_metrics_run", runID)
			args.Payload = jobcontract.DailyMetricsDispatchPayload{RunID: runID}
			return args
		}()},
	}
	if err := rig.dispatcher.Work(ctx, execution); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
}

// handoffs counts the finalize handoffs of the run (by its outbox key) and its
// partitions.
func (rig *zeroRepositoryDispatchRig) handoffs(t *testing.T, ctx context.Context, runID string) (finalize, partitions int) {
	t.Helper()
	if err := rig.pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE dedupe_key = $1`,
		"metrics.daily_finalize:"+runID).Scan(&finalize); err != nil {
		t.Fatal(err)
	}
	if err := rig.pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partitions WHERE run_id = $1::uuid`, runID).Scan(&partitions); err != nil {
		t.Fatal(err)
	}
	return finalize, partitions
}

func (rig *zeroRepositoryDispatchRig) startDeferred(t *testing.T, ctx context.Context, org string, day time.Time, generation string) string {
	t.Helper()
	run := rig.startRun(t, ctx, func(tx pgx.Tx) (daily.Run, error) {
		return rig.store.StartRunTx(ctx, tx, daily.StartRunRequest{OrganizationID: org, TargetDay: day, Generation: generation}, rig.publisher)
	})
	return run.ID
}

func TestADispatchOfAWholeOrganizationRunWithNoRepositoryHandsTheRunToFinalize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newZeroRepositoryDispatchRig(t, ctx)
	day := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	before := day.AddDate(0, 0, -1)
	removed := uuid.MustParse("00000000-0000-4000-8000-00000014037e")
	cases := []struct {
		name  string
		start func(t *testing.T, org string) string
	}{
		{"scheduled", func(t *testing.T, org string) string {
			return rig.startRun(t, ctx, func(tx pgx.Tx) (daily.Run, error) {
				return rig.store.StartScheduledFanoutRunTx(ctx, tx, daily.ScheduledFanoutRequest{
					OrganizationID: org, TargetDay: day, Generation: daily.ScheduledFanoutGenerationPrefix + "2026-09-15T01:00:00Z",
				}, rig.publisher)
			}).ID
		}},
		{"post-sync", func(t *testing.T, org string) string {
			return rig.startDeferred(t, ctx, org, day, "post-sync:zero-repos")
		}},
		{"manual", func(t *testing.T, org string) string {
			outcome, err := rig.store.StartManualDailyRun(ctx, org, day.Format("2006-01-02"),
				daily.ManualDailyGenerationPrefix+"zero-repos", nil, rig.publisher)
			if err != nil {
				t.Fatal(err)
			}
			return outcome.RunID
		}},
		{"external-recompute", func(t *testing.T, org string) string {
			return rig.startDeferred(t, ctx, org, day, daily.ExternalRecomputeGenerationPrefix+"zero-repos")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			org := uuid.NewSHA1(uuid.NameSpaceURL, []byte("chaos-14037-pr-b:"+tc.name)).String()
			other := uuid.NewSHA1(uuid.NameSpaceURL, []byte("chaos-14037-pr-b-control:"+tc.name)).String()
			seedZeroPartitionDay(t, ctx, rig.conn, org, day, removed)
			seedZeroPartitionDay(t, ctx, rig.conn, org, before, removed)
			seedZeroPartitionDay(t, ctx, rig.conn, other, day, removed)
			controls := []struct {
				org string
				day time.Time
			}{{other, day}, {org, before}}
			heldBefore := map[string]float64{}
			for _, measure := range zeroPartitionMeasures {
				for _, control := range controls {
					heldBefore[measure.table+control.org+control.day.String()] = zeroPartitionHeld(t, ctx, rig.conn, measure, control.org, control.day)
				}
			}
			runID := tc.start(t, org)

			rig.dispatch(t, ctx, org, runID)

			finalizeHandoffs, partitions := rig.handoffs(t, ctx, runID)
			status, finalization, fences := rig.runState(t, ctx, runID)
			t.Logf("after dispatch: run=%s status=%s finalization=%s fences=%d finalize_handoffs=%d partitions=%d",
				runID, status, finalization, fences, finalizeHandoffs, partitions)
			if finalizeHandoffs != 1 || partitions != 0 || status != "running" || finalization != "pending" || fences != 0 {
				t.Fatalf("after dispatch: %d handoff(s), %d partition(s), status=%s finalization=%s, %d fence(s); want 1, 0, running, pending, 0",
					finalizeHandoffs, partitions, status, finalization, fences)
			}
			if err := rig.runFinalize(ctx, org, runID); err != nil {
				t.Fatalf("finalize: %v", err)
			}
			status, finalization, fences = rig.runState(t, ctx, runID)
			t.Logf("after finalize: status=%s finalization=%s fences=%d", status, finalization, fences)
			if status != "no_repositories" || finalization != "succeeded" || fences != 1 {
				t.Errorf("after finalize: status=%s finalization=%s with %d fence(s), want no_repositories, succeeded, 1", status, finalization, fences)
			}
			for _, measure := range zeroPartitionMeasures {
				held := zeroPartitionHeld(t, ctx, rig.conn, measure, org, day)
				t.Logf("%s: the row of the day holds %v after the run", measure.table, held)
				if held != 0 {
					t.Errorf("%s: the row of the removed repository still holds %v after the run", measure.table, held)
				}
				for _, control := range controls {
					want := heldBefore[measure.table+control.org+control.day.String()]
					if got := zeroPartitionHeld(t, ctx, rig.conn, measure, control.org, control.day); got != want {
						t.Errorf("%s: the control row of org %s day %s holds %v, want the stored %v",
							measure.table, control.org, control.day.Format("2006-01-02"), got, want)
					}
				}
			}
		})
	}
}

func TestARetriedDispatchOfARunThatDecidedEmptyCutsNoPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newZeroRepositoryDispatchRig(t, ctx)
	day := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		// abandon moves the finalize handoff of the run from the outbox to its
		// abandonment record, the path of a dead outbox row.
		abandon bool
	}{{"outbox row", false}, {"abandonment record", true}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			org := uuid.NewSHA1(uuid.NameSpaceURL, []byte("chaos-14037-pr-b-retry:"+tc.name)).String()
			runID := rig.startRun(t, ctx, func(tx pgx.Tx) (daily.Run, error) {
				return rig.store.StartScheduledFanoutRunTx(ctx, tx, daily.ScheduledFanoutRequest{
					OrganizationID: org, TargetDay: day, Generation: daily.ScheduledFanoutGenerationPrefix + "2026-09-15T01:00:00Z",
				}, rig.publisher)
			}).ID
			rig.dispatch(t, ctx, org, runID)
			first, _ := rig.handoffs(t, ctx, runID)
			key := "metrics.daily_finalize:" + runID
			if tc.abandon {
				if _, err := rig.pool.Exec(ctx, `
INSERT INTO worker_job_delivery_abandonments (dedupe_key, job_kind, abandoned_at, attempt_count, last_error_code)
SELECT dedupe_key, job_kind, now(), attempt_count, 'test_dead' FROM worker_job_outbox WHERE dedupe_key = $1`, key); err != nil {
					t.Fatal(err)
				}
				if _, err := rig.pool.Exec(ctx, `DELETE FROM worker_job_outbox WHERE dedupe_key = $1`, key); err != nil {
					t.Fatal(err)
				}
			}
			// A repository of the organization comes back before the retry.
			if err := rig.conn.Exec(ctx, `INSERT INTO repos (id, org_id, repo, provider, created_at, last_synced) VALUES (?, ?, 'acme/back', 'github', ?, ?)`,
				uuid.New(), org, day, day); err != nil {
				t.Fatal(err)
			}

			rig.dispatch(t, ctx, org, runID)

			finalizeHandoffs, partitions := rig.handoffs(t, ctx, runID)
			var abandoned int
			if err := rig.pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_delivery_abandonments WHERE dedupe_key = $1`, key).Scan(&abandoned); err != nil {
				t.Fatal(err)
			}
			status, finalization, fences := rig.runState(t, ctx, runID)
			t.Logf("first dispatch: %d handoff(s); after the retry: status=%s finalization=%s fences=%d finalize_handoffs=%d abandoned=%d partitions=%d",
				first, status, finalization, fences, finalizeHandoffs, abandoned, partitions)
			if first != 1 {
				t.Errorf("the first dispatch published %d finalize handoff(s), want 1", first)
			}
			if partitions != 0 {
				t.Errorf("the retried dispatch cut %d partition(s) for a run that already decided it is empty, want 0", partitions)
			}
			wantHandoffs, wantAbandoned := 1, 0
			if tc.abandon {
				wantHandoffs, wantAbandoned = 0, 1
			}
			if finalizeHandoffs != wantHandoffs || abandoned != wantAbandoned {
				t.Errorf("after the retry: %d handoff(s) and %d abandonment record(s), want %d and %d",
					finalizeHandoffs, abandoned, wantHandoffs, wantAbandoned)
			}
			if tc.abandon {
				return
			}
			if err := rig.runFinalize(ctx, org, runID); err != nil {
				t.Fatalf("finalize: %v", err)
			}
			status, finalization, fences = rig.runState(t, ctx, runID)
			t.Logf("after finalize: status=%s finalization=%s fences=%d", status, finalization, fences)
			if status != "no_repositories" || finalization != "succeeded" || fences != 1 {
				t.Errorf("after finalize: status=%s finalization=%s with %d fence(s), want no_repositories, succeeded, 1", status, finalization, fences)
			}
		})
	}
}
