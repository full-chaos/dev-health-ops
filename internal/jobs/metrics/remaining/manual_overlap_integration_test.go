//go:build integration

package remaining

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/aftercommit"
)

type recordingOverlapObserver struct {
	mu       sync.Mutex
	families []string
}

func (observer *recordingOverlapObserver) ObserveRemainingMetricsManualOverlap(family string) error {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.families = append(observer.families, family)
	return nil
}

func (observer *recordingOverlapObserver) count() int {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return len(observer.families)
}

// overlapLines returns the overlap log lines the store wrote.
func overlapLines(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if raw == "" {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("log line is not JSON: %v: %q", err, raw)
		}
		if line["msg"] == "remaining metrics automatic start overlaps a manual run" {
			lines = append(lines, line)
		}
	}
	return lines
}

// startAutomaticDora starts an automatic dora run the way post-sync does: one
// StartRunTx for an anchor day and a window of backfillDays days.
func startAutomaticDora(
	t *testing.T, ctx context.Context, store *PostgresStore, pool *pgxpool.Pool,
	generation, anchorDay string, backfillDays int,
) Run {
	t.Helper()
	run, err := tryStartAutomaticDora(ctx, store, pool, nopPartitionPublisher{}, generation, anchorDay, backfillDays)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

// tryStartAutomaticDora is startAutomaticDora returning the start's error. A
// failed start rolls its transaction back, as post-sync does.
func tryStartAutomaticDora(
	ctx context.Context, store *PostgresStore, pool *pgxpool.Pool, publisher PartitionPublisher,
	generation, anchorDay string, backfillDays int,
) (Run, error) {
	scope := json.RawMessage(`{"version":1,"day":"` + anchorDay + `","sink":"auto","interval":"daily","backfill_days":` + strconv.Itoa(backfillDays) + `}`)
	tx, err := pool.Begin(ctx)
	if err != nil {
		return Run{}, err
	}
	run, err := store.StartRunTx(ctx, tx, StartRunRequest{
		OrganizationID: recomputeTestOrg, Family: "dora", Generation: generation,
		ScopeKey: anchorDay, Scopes: []json.RawMessage{scope},
	}, publisher)
	if err != nil {
		_ = tx.Rollback(ctx)
		return Run{}, err
	}
	return run, tx.Commit(ctx)
}

type failingPartitionPublisher struct{}

func (failingPartitionPublisher) PublishPartitionTx(context.Context, pgx.Tx, Run, Partition, string) error {
	return errors.New("publisher unavailable")
}

func overlapTestStore(t *testing.T, ctx context.Context) (*PostgresStore, *pgxpool.Pool, *bytes.Buffer, *recordingOverlapObserver) {
	t.Helper()
	store, pool := recomputeTestStore(t, ctx)
	logs := &bytes.Buffer{}
	store.SetLogger(slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	observer := &recordingOverlapObserver{}
	store.SetManualOverlapObserver(observer)
	return store, pool, logs, observer
}

// An automatic dora start whose window holds a day a pending manual recompute
// run will rewrite is logged and counted, and is still started: both runs
// compute the day (TestARecomputeAndAWiderPostSyncWindowThatOverlapsItLandTheSameValue
// shows the values are identical).
func TestAnAutomaticDoraStartOverlappingAPendingRecomputeIsLoggedAndCounted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	store, pool, logs, observer := overlapTestStore(t, ctx)

	day := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)
	dayString := day.Format("2006-01-02")
	nextDay := day.AddDate(0, 0, 1).Format("2006-01-02")
	seedSucceededDoraPartition(t, ctx, store, pool, recomputeTestOrg, dayString, 1, "post-sync:old-basis")
	recompute, err := store.StartManualRecomputeRun(ctx, "dora", recomputeTestOrg, dayString, "cfr-9692", nopPartitionPublisher{})
	if err != nil || recompute.Disposition != RecomputeStarted {
		t.Fatalf("recompute = %+v, %v", recompute, err)
	}

	// A pending run whose handoff can never be delivered computes nothing, so
	// it overlaps nothing: no line, no count.
	startAutomaticDora(t, ctx, store, pool, "post-sync:dead-handoff", nextDay, 2)
	if lines := overlapLines(t, logs); len(lines) != 0 || observer.count() != 0 {
		t.Fatalf("a manual run with a dead handoff was reported: %d lines, %d observations\n%s", len(lines), observer.count(), logs.String())
	}

	seedOutboxRow(t, ctx, pool, recompute.PartitionID, "pending", "")
	wide := startAutomaticDora(t, ctx, store, pool, "post-sync:wide", nextDay, 2)
	if wide.ID == recompute.RunID {
		t.Fatal("the automatic start was folded into the recompute run: the start must not change")
	}
	lines := overlapLines(t, logs)
	if len(lines) != 1 {
		t.Fatalf("overlap log lines = %d, want 1:\n%s", len(lines), logs.String())
	}
	line := lines[0]
	for key, want := range map[string]any{
		"level": "INFO", "family": "dora", "organization_id": recomputeTestOrg,
		"run_id": wide.ID, "manual_run_id": recompute.RunID, "manual_kind": "manual_recompute",
		"anchor_day": nextDay, "backfill_days": float64(2),
		"overlap_from": dayString, "overlap_to": dayString, "overlap_days": float64(1),
	} {
		if line[key] != want {
			t.Errorf("log attribute %s = %v, want %v (line %v)", key, line[key], want, line)
		}
	}
	if observer.count() != 1 {
		t.Fatalf("overlap counter observations = %d, want 1", observer.count())
	}
}

// A pending manual backfill run is seen the same way, and a window that shares
// no day with any manual run is not reported.
func TestAnAutomaticDoraStartIsReportedOnlyWhenItsWindowSharesADayWithAManualRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	store, pool, logs, observer := overlapTestStore(t, ctx)

	manualDay := time.Now().UTC().AddDate(0, 0, -10).Truncate(24 * time.Hour)
	manualDayString := manualDay.Format("2006-01-02")
	generation := "manual-backfill:dora:" + recomputeTestOrg + ":" + manualDayString + ".." + manualDayString
	backfill, err := store.StartManualBackfillRun(ctx, "dora", recomputeTestOrg, manualDayString, generation, nopPartitionPublisher{})
	if err != nil {
		t.Fatalf("manual backfill: %v", err)
	}
	seedOutboxRow(t, ctx, pool, deterministicPartitionID(backfill.RunID, 1), "pending", "")

	// Disjoint: a window of two days that ends three days before the manual day.
	startAutomaticDora(t, ctx, store, pool, "post-sync:disjoint", manualDay.AddDate(0, 0, -3).Format("2006-01-02"), 2)
	// Disjoint: a window that starts after the manual day.
	startAutomaticDora(t, ctx, store, pool, "post-sync:after", manualDay.AddDate(0, 0, 3).Format("2006-01-02"), 2)
	if lines := overlapLines(t, logs); len(lines) != 0 || observer.count() != 0 {
		t.Fatalf("a window sharing no day with the manual run was reported: %d lines, %d observations\n%s", len(lines), observer.count(), logs.String())
	}

	// Overlapping: a window of five days anchored two days after the manual day holds it.
	wide := startAutomaticDora(t, ctx, store, pool, "post-sync:holds", manualDay.AddDate(0, 0, 2).Format("2006-01-02"), 5)
	lines := overlapLines(t, logs)
	if len(lines) != 1 || observer.count() != 1 {
		t.Fatalf("overlap reports = %d lines, %d observations, want 1 and 1\n%s", len(lines), observer.count(), logs.String())
	}
	if lines[0]["manual_kind"] != "manual_backfill" || lines[0]["manual_run_id"] != backfill.RunID || lines[0]["run_id"] != wide.ID ||
		lines[0]["overlap_from"] != manualDayString || lines[0]["overlap_to"] != manualDayString || lines[0]["overlap_days"] != float64(1) {
		t.Fatalf("overlap line = %v", lines[0])
	}
}

// A start that fails after the overlap is known (the publisher cannot hand the
// partition off) rolls back and leaves no run, so it leaves no log line and no
// count either; the same start retried and committed is reported once, and its
// replay, which only finds the run again, is not reported a second time.
func TestAnAutomaticDoraStartThatRollsBackIsNotReportedAndAReplayIsReportedOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	store, pool, logs, observer := overlapTestStore(t, ctx)

	day := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)
	dayString := day.Format("2006-01-02")
	nextDay := day.AddDate(0, 0, 1).Format("2006-01-02")
	seedSucceededDoraPartition(t, ctx, store, pool, recomputeTestOrg, dayString, 1, "post-sync:old-basis")
	recompute, err := store.StartManualRecomputeRun(ctx, "dora", recomputeTestOrg, dayString, "cfr-9841", nopPartitionPublisher{})
	if err != nil || recompute.Disposition != RecomputeStarted {
		t.Fatalf("recompute = %+v, %v", recompute, err)
	}
	seedOutboxRow(t, ctx, pool, recompute.PartitionID, "pending", "")

	runsBefore := countRemainingRuns(t, ctx, pool)
	if _, err := tryStartAutomaticDora(ctx, store, pool, failingPartitionPublisher{}, "post-sync:rolls-back", nextDay, 2); err == nil {
		t.Fatal("the start with a failing publisher succeeded: the test does not exercise a rollback")
	}
	if got := countRemainingRuns(t, ctx, pool); got != runsBefore {
		t.Fatalf("the failed start left a run: %d -> %d", runsBefore, got)
	}
	if lines := overlapLines(t, logs); len(lines) != 0 || observer.count() != 0 {
		t.Fatalf("a start that rolled back was reported: %d lines, %d observations\n%s", len(lines), observer.count(), logs.String())
	}

	first := startAutomaticDora(t, ctx, store, pool, "post-sync:rolls-back", nextDay, 2)
	if lines := overlapLines(t, logs); len(lines) != 1 || observer.count() != 1 || lines[0]["run_id"] != first.ID {
		t.Fatalf("the committed start was not reported once with its run id: %d lines, %d observations\n%s", len(lines), observer.count(), logs.String())
	}
	replay := startAutomaticDora(t, ctx, store, pool, "post-sync:rolls-back", nextDay, 2)
	if replay.ID != first.ID {
		t.Fatalf("the replay started another run: %s then %s", first.ID, replay.ID)
	}
	if lines := overlapLines(t, logs); len(lines) != 1 || observer.count() != 1 {
		t.Fatalf("the replay of a start was reported again: %d lines, %d observations\n%s", len(lines), observer.count(), logs.String())
	}
}

// The owner of the transaction decides when the report is true: with hooks on
// the context, a start whose outer transaction rolls back after StartRunTx
// returned (a later stage failed, or the commit did) leaves no log line and no
// count, and the same start committed reports once when the owner runs the hooks.
func TestAnOverlapIsReportedOnlyWhenTheOwnerOfTheTransactionCommits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	store, pool, logs, observer := overlapTestStore(t, ctx)

	day := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)
	dayString := day.Format("2006-01-02")
	nextDay := day.AddDate(0, 0, 1).Format("2006-01-02")
	seedSucceededDoraPartition(t, ctx, store, pool, recomputeTestOrg, dayString, 1, "post-sync:old-basis")
	recompute, err := store.StartManualRecomputeRun(ctx, "dora", recomputeTestOrg, dayString, "cfr-9841b", nopPartitionPublisher{})
	if err != nil || recompute.Disposition != RecomputeStarted {
		t.Fatalf("recompute = %+v, %v", recompute, err)
	}
	seedOutboxRow(t, ctx, pool, recompute.PartitionID, "pending", "")
	scope := json.RawMessage(`{"version":1,"day":"` + nextDay + `","sink":"auto","interval":"daily","backfill_days":2}`)
	start := func(hookCtx context.Context) (Run, pgx.Tx) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		run, err := store.StartRunTx(hookCtx, tx, StartRunRequest{
			OrganizationID: recomputeTestOrg, Family: "dora", Generation: "post-sync:owner",
			ScopeKey: nextDay, Scopes: []json.RawMessage{scope},
		}, nopPartitionPublisher{})
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		return run, tx
	}

	runsBefore := countRemainingRuns(t, ctx, pool)
	rolledCtx, rolledHooks := aftercommit.WithHooks(ctx)
	_, rolled := start(rolledCtx)
	if lines := overlapLines(t, logs); len(lines) != 0 || observer.count() != 0 {
		t.Fatalf("the overlap was reported before the owner committed: %d lines, %d observations", len(lines), observer.count())
	}
	if err := rolled.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if got := countRemainingRuns(t, ctx, pool); got != runsBefore {
		t.Fatalf("the rolled back start left a run: %d -> %d", runsBefore, got)
	}
	_ = rolledHooks // the owner did not commit: it never runs the hooks
	if lines := overlapLines(t, logs); len(lines) != 0 || observer.count() != 0 {
		t.Fatalf("a start whose outer transaction rolled back was reported: %d lines, %d observations\n%s", len(lines), observer.count(), logs.String())
	}

	committedCtx, committedHooks := aftercommit.WithHooks(ctx)
	run, committed := start(committedCtx)
	if err := committed.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if lines := overlapLines(t, logs); len(lines) != 0 {
		t.Fatalf("the overlap was reported before the owner ran the hooks: %d lines", len(lines))
	}
	committedHooks.Run()
	lines := overlapLines(t, logs)
	if len(lines) != 1 || observer.count() != 1 || lines[0]["run_id"] != run.ID || lines[0]["manual_run_id"] != recompute.RunID {
		t.Fatalf("the committed start was not reported once after the commit: %d lines, %d observations\n%s", len(lines), observer.count(), logs.String())
	}
}
