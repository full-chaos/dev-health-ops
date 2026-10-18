//go:build integration

package syncreconciler

import (
	"context"
	"testing"
	"time"
)

func (h *finalizeBackstopHarness) bumpFinalizeRoute(t *testing.T, ctx context.Context) {
	t.Helper()
	if _, err := h.admin.Exec(ctx, `UPDATE public.sync_dispatch_transport_routes SET generation=generation+1 WHERE kind='finalize_sync_run'`); err != nil {
		t.Fatal(err)
	}
}

func (h *finalizeBackstopHarness) outboxState(t *testing.T, ctx context.Context) (status string, generation *int64) {
	t.Helper()
	if err := h.admin.QueryRow(ctx, `SELECT status, dispatched_route_generation FROM public.sync_dispatch_outbox WHERE id=$1`, backstopOutboxID).Scan(&status, &generation); err != nil {
		t.Fatal(err)
	}
	return status, generation
}

// CHAOS-9964: a finalize delivery dispatched at route generation 3 while the route is at 4 (the shape of four bigboy
// runs that sat dispatching for seven weeks) is re-armed by the route command's reconcile when its River job is gone or
// completed and the run is finalize-ready. The scheduled repair keeps its generation fence (CHAOS-5462). A live job, a
// terminal run, an unfinished unit and a paused route are left alone. Other kinds are counted, not re-armed.
func TestRouteCommandReconcilesFinalizeDeliveriesLeftAtAnotherGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	h := startFinalizeBackstopHarness(t, ctx)
	now := time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)

	for _, state := range []string{"missing", "completed"} {
		t.Run("rearmed when the job is "+state, func(t *testing.T) {
			h.seed(t, ctx, now, state)
			h.bumpFinalizeRoute(t, ctx)
			step, err := h.repair.Step(ctx, now, 20)
			if err != nil {
				t.Fatal(err)
			}
			if status, _ := h.outboxState(t, ctx); step.Recovered != 0 || status != "dispatched" {
				t.Fatalf("the scheduled repair crossed the generation fence: %+v status=%s", step, status)
			}
			report, err := h.repair.ReconcileOtherGenerations(ctx, now, "finalize_sync_run")
			if err != nil {
				t.Fatal(err)
			}
			status, generation := h.outboxState(t, ctx)
			if report.Rearmed != 1 || status != "pending" || generation != nil {
				t.Fatalf("reconcile = %+v, outbox status=%s generation=%v; want 1 re-armed, pending, no generation", report, status, generation)
			}
			again, err := h.repair.ReconcileOtherGenerations(ctx, now, "finalize_sync_run")
			if err != nil || again.Rearmed != 0 {
				t.Fatalf("second reconcile = %+v err=%v, want nothing left", again, err)
			}
		})
	}

	t.Run("rows ahead of an unchanged route (the bigboy shape): one apply with no generation change re-arms", func(t *testing.T) {
		h.seed(t, ctx, now, "missing")
		if _, err := h.admin.Exec(ctx, `UPDATE public.sync_dispatch_outbox SET dispatched_route_generation=4 WHERE id=$1`, backstopOutboxID); err != nil {
			t.Fatal(err)
		}
		var routeGeneration int64
		if err := h.admin.QueryRow(ctx, `SELECT generation FROM public.sync_dispatch_transport_routes WHERE kind='finalize_sync_run'`).Scan(&routeGeneration); err != nil || routeGeneration != 3 {
			t.Fatalf("route generation = %d err=%v, want 3 and unchanged", routeGeneration, err)
		}
		report, err := h.repair.ReconcileOtherGenerations(ctx, now, "finalize_sync_run")
		if err != nil {
			t.Fatal(err)
		}
		if status, generation := h.outboxState(t, ctx); report.Rearmed != 1 || status != "pending" || generation != nil {
			t.Fatalf("reconcile = %+v status=%s generation=%v; want 1 re-armed under a route that did not change", report, status, generation)
		}
	})

	for _, tc := range []struct {
		name, state, sql       string
		wantLive, wantNotReady int
	}{
		{name: "available job", state: "available", wantLive: 1},
		{name: "running job", state: "running", wantLive: 1},
		{name: "retryable job", state: "retryable", wantLive: 1},
		{name: "terminal run", state: "missing", sql: `UPDATE sync_runs SET status='success'`},
		{name: "unfinished unit", state: "missing", sql: `UPDATE sync_run_units SET status='running'`, wantNotReady: 1},
		{name: "paused route", state: "missing", sql: `UPDATE sync_dispatch_transport_routes SET paused=true,paused_at=now() WHERE kind='finalize_sync_run'`},
		{name: "live claim", state: "completed", sql: `UPDATE sync_dispatch_outbox SET claim_token='live',claim_expires_at=now()+interval '1 hour',claim_transport='river',claim_route_generation=3`},
	} {
		t.Run("left alone: "+tc.name, func(t *testing.T) {
			h.seed(t, ctx, now, tc.state)
			h.bumpFinalizeRoute(t, ctx)
			if tc.sql != "" {
				if _, err := h.admin.Exec(ctx, tc.sql); err != nil {
					t.Fatal(err)
				}
			}
			report, err := h.repair.ReconcileOtherGenerations(ctx, now, "finalize_sync_run")
			if err != nil {
				t.Fatal(err)
			}
			if status, _ := h.outboxState(t, ctx); report.Rearmed != 0 || status != "dispatched" ||
				report.SkippedLiveJob != tc.wantLive || report.SkippedNotReady != tc.wantNotReady {
				t.Fatalf("reconcile = %+v status=%s, want nothing re-armed, live=%d notReady=%d", report, status, tc.wantLive, tc.wantNotReady)
			}
		})
	}

	t.Run("other kinds are counted and not re-armed", func(t *testing.T) {
		h.seed(t, ctx, now, "missing")
		const postSyncOutbox = "00000000-0000-4000-8000-000000005460"
		if _, err := h.admin.Exec(ctx, `INSERT INTO public.sync_dispatch_outbox
            (id,org_id,sync_run_id,kind,status,available_at,attempts,dispatched_at,dispatched_transport,dispatched_route_generation,transport_job_id,created_at,updated_at)
            VALUES ($1,'org-materializer',$2,'post_sync','dispatched',$3,1,$3,'river',3,'999999',$3,$3)`,
			postSyncOutbox, materializerFinalize, now.Add(-30*time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := h.admin.Exec(ctx, `UPDATE public.sync_dispatch_transport_routes SET generation=generation+1 WHERE kind='post_sync'`); err != nil {
			t.Fatal(err)
		}
		report, err := h.repair.ReconcileOtherGenerations(ctx, now, "post_sync")
		if err != nil {
			t.Fatal(err)
		}
		var status string
		if err := h.admin.QueryRow(ctx, `SELECT status FROM public.sync_dispatch_outbox WHERE id=$1`, postSyncOutbox).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "dispatched" || report.Rearmed != 0 || len(report.OtherKinds) != 1 || report.OtherKinds[0].Kind != "post_sync" || report.OtherKinds[0].Stranded != 1 {
			t.Fatalf("report = %+v post_sync status=%s, want post_sync counted once and still dispatched", report, status)
		}
	})
}
