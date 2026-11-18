//go:build integration

package syncreconciler

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
)

// recordingPostSyncWriters records what a fan-out stages, in memory: the plan the daily writer got and every
// remaining-metrics family.
type recordingPostSyncWriters struct {
	mu        sync.Mutex
	daily     []syncdispatchruntime.PostSyncPlan
	families  []string
	workGraph []string
}

func (w *recordingPostSyncWriters) StartRunTx(_ context.Context, _ pgx.Tx, plan syncdispatchruntime.PostSyncPlan, _ string) (string, string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.daily = append(w.daily, plan)
	return "daily-dispatch", "daily-key", nil
}

type recordingRemaining struct{ w *recordingPostSyncWriters }

func (r recordingRemaining) StartRunTx(_ context.Context, _ pgx.Tx, family string, _ syncdispatchruntime.PostSyncPlan, _ string) (string, error) {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	r.w.families = append(r.w.families, family)
	return family + "-key", nil
}

type recordingWorkGraph struct{ w *recordingPostSyncWriters }

func (r recordingWorkGraph) StartRequestTx(_ context.Context, _ pgx.Tx, consumer string, _ syncdispatchruntime.PostSyncPlan, _ string) (string, error) {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	r.w.workGraph = append(r.w.workGraph, consumer)
	return consumer + "-key", nil
}

type recordingPublish struct{}

func (recordingPublish) PublishTx(context.Context, pgx.Tx, syncdispatchruntime.PostSyncPlan) error {
	return nil
}

// TestRedrivenPostSyncFansOutTheRunsDay (N6): a post_sync delivery of a run that finished yesterday exhausts its
// River budget; the repair re-arms it, the next claim moves the attempt, the exhausted attempt's late pass writes
// nothing, and the re-driven pass plans the run's own day (its completed_at) with no complexity.
func TestRedrivenPostSyncFansOutTheRunsDay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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
		orgID         = "60000000-0000-4000-8000-000000000001"
		runID         = "60000000-0000-4000-8000-000000000002"
		outboxID      = "60000000-0000-4000-8000-000000000003"
		integrationID = "60000000-0000-4000-8000-000000000004"
		sourceID      = "60000000-0000-4000-8000-000000000005"
		unitID        = "60000000-0000-4000-8000-000000000006"
	)
	// The fan-out reads its own wall clock, so the run's day is yesterday on that clock.
	now := time.Now().UTC()
	yesterday := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	completedAt := yesterday.Add(10 * time.Hour)
	pgseed.EnsureSyncIntegration(ctx, t, h.admin, orgID, integrationID, sourceID)
	pgseed.EnsureSyncRun(ctx, t, h.admin, pgseed.SyncRun{ID: runID, OrgID: orgID, IntegrationID: integrationID})
	pgseed.InsertSyncRunUnit(ctx, t, h.admin, pgseed.SyncRunUnit{
		ID: unitID, RunID: runID, OrgID: orgID, IntegrationID: integrationID, SourceID: sourceID,
		DatasetKey: "commits", Status: "success",
	})
	if _, err := h.admin.Exec(ctx, `UPDATE public.sync_runs SET status = 'success', started_at = $2, completed_at = $3 WHERE id = $1`,
		runID, yesterday.Add(8*time.Hour), completedAt); err != nil {
		t.Fatal(err)
	}
	args := func(attempt int64) syncdispatchruntime.PostSyncArgs {
		return syncdispatchruntime.PostSyncArgs{TransportArgs: syncdispatchruntime.TransportArgs{
			Version: syncdispatchruntime.ContractVersionV1, OrgID: orgID, RunID: runID,
			DispatchOutbox: outboxID, DeliveryAttempt: attempt, RouteGeneration: generation,
		}}
	}
	inserted, err := h.river.Insert(ctx, args(1), &river.InsertOpts{Queue: "sync"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.admin.Exec(ctx, `INSERT INTO public.sync_dispatch_outbox
		(id,org_id,sync_run_id,kind,status,available_at,attempts,dispatched_at,dispatched_transport,dispatched_route_generation,transport_job_id,created_at,updated_at)
		VALUES ($1,$2,$3,'post_sync','dispatched',$4,1,$4,'river',$5,$6,$4,$4)`,
		outboxID, orgID, runID, completedAt, generation, strconv.FormatInt(inserted.Job.ID, 10)); err != nil {
		t.Fatal(err)
	}
	discardRiverJob(t, ctx, h.admin, inserted.Job.ID, now, 5, 5, "post_sync fanout unavailable")

	if result, err := h.repair.Step(ctx, now, 10); err != nil || result.PostSyncRearmed != 1 {
		t.Fatalf("repair = %#v, %v; want one post_sync re-arm", result, err)
	}
	// The Kernel's claim and River dispatch of the re-armed row (TestReclaimedPostSyncDeliveryRepublishesThroughTheKernel
	// runs the real Kernel).
	if _, err := h.admin.Exec(ctx, `
		UPDATE public.sync_dispatch_outbox
		SET status = 'dispatched', attempts = attempts + 1, dispatched_at = $2, dispatched_transport = 'river',
			dispatched_route_generation = $3, transport_job_id = '987654', updated_at = $2
		WHERE id = $1`, outboxID, now, generation); err != nil {
		t.Fatal(err)
	}

	writers := &recordingPostSyncWriters{}
	service, err := syncdispatchruntime.NewNativePostSyncService(h.admin, writers, recordingRemaining{writers},
		recordingWorkGraph{writers}, recordingPublish{}, recordingPublish{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Fanout(ctx, args(1)); err != nil || len(writers.daily) != 0 || len(writers.families) != 0 {
		t.Fatalf("exhausted attempt 1 after the re-drive: err=%v daily=%d families=%v, want nil and nothing staged",
			err, len(writers.daily), writers.families)
	}
	if err := service.Fanout(ctx, args(2)); err != nil {
		t.Fatalf("re-driven attempt 2: %v", err)
	}
	if len(writers.daily) != 1 || !writers.daily[0].TargetDay.Equal(completedAt) || writers.daily[0].Complexity {
		t.Fatalf("re-driven daily plans = %+v, want one plan on the run's completion %s with no complexity", writers.daily, completedAt)
	}
	for _, family := range writers.families {
		if family == "complexity" {
			t.Fatalf("re-driven pass staged complexity for a past day: families %v", writers.families)
		}
	}
	var status string
	var attempts int64
	if err := h.admin.QueryRow(ctx, `SELECT status, attempts FROM public.sync_dispatch_outbox WHERE id = $1`, outboxID).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "dispatched" || attempts != 2 {
		t.Fatalf("outbox status=%s attempts=%d after the re-driven pass, want dispatched/2", status, attempts)
	}
}
