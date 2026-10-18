//go:build integration

package daily

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"testing"
	"time"
)

// CHAOS-10072: runs the pre-CHAOS-4442 range finalize redrive rewrote to
// "redrive:<nonce>" lost the producer prefix the day-coverage readers match.
// The repair restores the fan-out generation only where the run id proves it.
func TestRepairLegacyRedriveGenerationsRestoresOnlyProvenFanoutRuns(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newFinalizeRedriveTestStack(t)

	const otherOrg = "00000000-0000-4000-8000-000000010073"
	day := func(d int) time.Time { return time.Date(2026, 10, d, 0, 0, 0, 0, time.UTC) }
	insert := func(org string, d int, original, stored, status, finalization string, createdAt time.Time) string {
		t.Helper()
		run := newRun(org, day(d), original)
		if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at,full_org)
VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$7,false)`,
			run.ID, org, day(d), stored, status, finalization, createdAt); err != nil {
			t.Fatal(err)
		}
		return run.ID
	}
	fanout := func(d int) string {
		return ScheduledFanoutGenerationPrefix + time.Date(2026, 10, d, 1, 0, 0, 0, time.UTC).Format(time.RFC3339)
	}
	created := func(d int) time.Time { return time.Date(2026, 10, d, 1, 0, 4, 0, time.UTC) }

	damaged := insert(repairTestOrg, 1, fanout(1), "redrive:n1", "succeeded", "succeeded", created(1))
	postSync := insert(repairTestOrg, 2, "post-sync:5a1f3d52-7a2f-4a53-9b0e-5d3c3a8b6b11", "redrive:n2", "succeeded", "succeeded", created(2))
	inFlight := insert(repairTestOrg, 3, fanout(3), "redrive:n3", "running", "pending", created(3))
	openEvent := insert(repairTestOrg, 4, fanout(4), "redrive:n4", "succeeded", "succeeded", created(4))
	healthy := insert(repairTestOrg, 5, fanout(5), fanout(5), "succeeded", "succeeded", created(5))
	drained := insert(repairTestOrg, 6, TouchedDrainGenerationPrefix+"e:"+healthy, "redrive:n6", "succeeded", "succeeded", created(6))
	otherOrgRun := insert(otherOrg, 1, fanout(1), "redrive:n5", "succeeded", "succeeded", created(1))
	if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_finalize_redrive_events (id, run_id, org_id, target_day, prior_status, prior_finalization_status, actor, reason, nonce)
VALUES (gen_random_uuid(), $1::uuid, $2::uuid, $3, 'succeeded', 'succeeded', 'finalize-redrive', 'test', 'n4')`,
		openEvent, repairTestOrg, day(4)); err != nil {
		t.Fatal(err)
	}

	state := func(runID string) (generation string, fullOrg bool) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT generation, full_org FROM daily_metrics_runs WHERE id = $1::uuid`, runID).Scan(&generation, &fullOrg); err != nil {
			t.Fatal(err)
		}
		return generation, fullOrg
	}
	covering := func(d int) string {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		id, err := store.coveringRunForDay(ctx, tx, repairTestOrg, day(d).Format(dailyTargetDayLayout), "manual-daily:probe")
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	if got := covering(1); got != "" {
		t.Fatalf("before the repair coveringRunForDay found %q, want nothing", got)
	}

	dry, err := store.RepairLegacyRedriveGenerations(ctx, repairTestOrg, day(1), day(31), true, "dry")
	if err != nil {
		t.Fatal(err)
	}
	if dry.Examined != 5 || dry.WouldRepair != 2 || dry.NotProvable != 1 || dry.NotSettled != 2 || dry.Repaired != 0 || dry.FullOrgFlipped != 1 {
		t.Fatalf("dry run = %+v, want examined 5, would_repair 2, not_provable 1, not_settled 2, full_org_flipped 1", dry)
	}
	if generation, _ := state(damaged); generation != "redrive:n1" {
		t.Fatalf("a dry run changed the generation to %q", generation)
	}

	outcome, err := store.RepairLegacyRedriveGenerations(ctx, repairTestOrg, day(1), day(31), false, "CHAOS-10072 test")
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Examined != 5 || outcome.Repaired != 2 || outcome.NotProvable != 1 || outcome.NotSettled != 2 || outcome.Raced != 0 || outcome.FullOrgFlipped != 1 {
		t.Fatalf("repair = %+v, want examined 5, repaired 2, not_provable 1, not_settled 2, full_org_flipped 1", outcome)
	}
	if generation, fullOrg := state(damaged); generation != fanout(1) || !fullOrg {
		t.Fatalf("repaired run = %q full_org=%v, want %q true", generation, fullOrg, fanout(1))
	}
	for name, id := range map[string]string{"post-sync": postSync, "in flight": inFlight, "open event": openEvent, "other org": otherOrgRun} {
		if generation, fullOrg := state(id); len(generation) < 8 || generation[:8] != "redrive:" || fullOrg {
			t.Errorf("%s run was touched: generation %q full_org=%v", name, generation, fullOrg)
		}
	}
	if generation, fullOrg := state(drained); generation != TouchedDrainGenerationPrefix+"e:"+healthy || fullOrg {
		t.Errorf("touched-drain run = %q full_org=%v, want its generation back and full_org untouched", generation, fullOrg)
	}
	if outcome.ByFamily["fixed-schedule"] != 1 || outcome.ByFamily["touched-drain"] != 1 {
		t.Errorf("by_family = %v, want one fixed-schedule and one touched-drain", outcome.ByFamily)
	}
	if generation, _ := state(healthy); generation != fanout(5) {
		t.Errorf("a healthy run changed to %q", generation)
	}
	if got := covering(1); got != damaged {
		t.Fatalf("after the repair coveringRunForDay found %q, want %q", got, damaged)
	}

	again, err := store.RepairLegacyRedriveGenerations(ctx, repairTestOrg, day(1), day(31), false, "CHAOS-10072 test")
	if err != nil {
		t.Fatal(err)
	}
	if again.Repaired != 0 || again.WouldRepair != 0 || again.Examined != 3 {
		t.Fatalf("second pass = %+v, want nothing repaired and the 3 unrepairable rows only", again)
	}
}

type cancelAfterRowsHandler struct {
	slog.Handler
	cancel context.CancelFunc
	after  int
	seen   *int
}

func (h cancelAfterRowsHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "daily metrics legacy redrive generation" {
		*h.seen++
		if *h.seen == h.after {
			h.cancel()
		}
	}
	return h.Handler.Handle(ctx, record)
}

// CHAOS-10139: a pass whose budget ends mid-range is a resumable partial
// result, not an error; a rerun from resume_from finishes the range.
func TestRepairLegacyRedriveGenerationsPartialOnBudgetResumes(t *testing.T) {
	pool, store, _ := newFinalizeRedriveTestStack(t)
	day := func(d int) time.Time { return time.Date(2026, 10, d, 0, 0, 0, 0, time.UTC) }
	for d := 1; d <= 6; d++ {
		generation := ScheduledFanoutGenerationPrefix + time.Date(2026, 10, d, 1, 0, 0, 0, time.UTC).Format(time.RFC3339)
		run := newRun(repairTestOrg, day(d), generation)
		if _, err := pool.Exec(context.Background(), `
INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at,full_org)
VALUES ($1::uuid,$2::uuid,$3,$4,'succeeded','succeeded',$5,$5,false)`,
			run.ID, repairTestOrg, day(d), "redrive:n"+strconv.Itoa(d), time.Date(2026, 10, d, 1, 0, 4, 0, time.UTC)); err != nil {
			t.Fatal(err)
		}
	}
	asMap := func(outcome GenerationRepairOutcome) map[string]any {
		raw, err := json.Marshal(outcome)
		if err != nil {
			t.Fatal(err)
		}
		fields := map[string]any{}
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatal(err)
		}
		return fields
	}

	budget, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := 0
	previous := slog.Default()
	slog.SetDefault(slog.New(cancelAfterRowsHandler{Handler: previous.Handler(), cancel: cancel, after: 2, seen: &seen}))
	first, err := store.RepairLegacyRedriveGenerations(budget, repairTestOrg, day(1), day(31), false, "CHAOS-10139 test")
	slog.SetDefault(previous)
	if err != nil {
		t.Fatalf("a budget that ends mid-range returned %v, want a partial outcome", err)
	}
	fields := asMap(first)
	if fields["partial"] != true || fields["resume_from"] != "2026-10-03" || first.Repaired != 2 {
		t.Fatalf("first pass = %+v, want partial=true, resume_from 2026-10-03, repaired 2", fields)
	}

	resume, err := time.Parse("2006-01-02", fields["resume_from"].(string))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.RepairLegacyRedriveGenerations(context.Background(), repairTestOrg, resume, day(31), false, "CHAOS-10139 test")
	if err != nil {
		t.Fatal(err)
	}
	if asMap(second)["partial"] == true || second.Repaired != 4 {
		t.Fatalf("resumed pass = %+v, want complete with 4 repaired", asMap(second))
	}
	check, err := store.RepairLegacyRedriveGenerations(context.Background(), repairTestOrg, day(1), day(31), true, "check")
	if err != nil {
		t.Fatal(err)
	}
	if check.WouldRepair != 0 || check.Examined != 0 {
		t.Fatalf("check after resume = %+v, want nothing left", check)
	}
}
