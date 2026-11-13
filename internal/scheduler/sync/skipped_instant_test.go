package sync

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/jackc/pgx/v5/pgconn"
)

// runSkipWindowThroughLoop runs one handoff window through a Loop whose stepper
// is the real HandoffDueResult over transaction, and returns the decoded WARN
// log lines the window wrote.
func runSkipWindowThroughLoop(
	t *testing.T,
	transaction *fakeSchedulerTransaction,
	ledger *ledgerCoordinator,
	observedAt time.Time,
) (HandoffResult, []map[string]any) {
	t.Helper()
	var logs bytes.Buffer
	var result HandoffResult
	loop, err := newLoop(
		loopStepFunc(func(ctx context.Context, now time.Time, limit int, coordinator Coordinator) (HandoffResult, error) {
			var stepErr error
			result, stepErr = mutationRepository(transaction).HandoffDueResult(ctx, now, limit, coordinator)
			return result, stepErr
		}),
		ledger,
		LoopConfig{
			PollInterval: minLoopPollInterval,
			StepTimeout:  time.Second,
			MaxBackoff:   80 * time.Millisecond,
			Limit:        4,
			Registry:     health.NewRegistry(time.Second),
			Occurrences:  &stubOccurrences{},
			Logger:       slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})),
		},
		&testLoopClock{now: observedAt},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	var lines []map[string]any
	scanner := bufio.NewScanner(&logs)
	for scanner.Scan() {
		line := map[string]any{}
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", scanner.Text(), err)
		}
		lines = append(lines, line)
	}
	return result, lines
}

// CHAOS-3954 red cell. The 22:00 scheduled run of an hourly configuration is
// still open at the 23:00 tick and ends at 23:12. The 23:00 tick is skipped
// (one scheduled run per configuration at a time, CHAOS-8459) and the 00:00
// tick rebases onto the 23:12 completion, so 23:00 never reaches the
// occurrence ledger. The skip itself is kept; what this cell requires is that
// the skip line names the cron instant it skipped. Today it names only the
// next marker (00:00), so nothing records that 23:00 was lost.
func TestSyncRunSpanningACronInstantNamesTheSkippedInstant(t *testing.T) {
	createdAt := at("2026-08-01T00:00:00Z")
	previousSync := at("2026-08-19T21:10:00Z")
	minted := at("2026-08-19T22:00:00Z")
	skipped := at("2026-08-19T23:00:00Z")
	marker := skipped
	ledger := newLedgerCoordinator()
	ledger.minted[minted] = true

	spanning := &fakeSchedulerTransaction{
		rows: &fakeLockedRows{rows: [][]any{lockedLedgerRow(
			"config-span", "org-a", "job-a", "0 * * * *",
			createdAt, previousSync, &marker, ledger.latest(),
		)}},
		execTag: pgconn.NewCommandTag("UPDATE 1"),
		openRuns: openScheduledRuns{
			blocking: 1, blockingOccurrenceID: "sha256:run-2200", blockingSyncRunID: "run-2200",
			blockingOpenSeconds: 3605, lastScheduledRunEndedAt: &previousSync,
		},
	}
	result, lines := runSkipWindowThroughLoop(t, spanning, ledger, at("2026-08-19T23:00:05Z"))
	if len(result.SkippedOpenRun) != 1 || result.Minted() != 0 {
		t.Fatalf("23:00 window skipped=%d minted=%d, want the tick skipped", len(result.SkippedOpenRun), result.Minted())
	}

	completed := at("2026-08-19T23:12:00Z")
	nextMarker := result.SkippedOpenRun[0].NextRunAt
	after := &fakeSchedulerTransaction{
		rows: &fakeLockedRows{rows: [][]any{lockedLedgerRow(
			"config-span", "org-a", "job-a", "0 * * * *",
			createdAt, completed, &nextMarker, ledger.latest(),
		)}},
		execTag:  pgconn.NewCommandTag("UPDATE 1"),
		openRuns: openScheduledRuns{lastScheduledRunEndedAt: &completed},
	}
	if _, err := mutationRepository(after).HandoffDueResult(
		context.Background(), at("2026-08-20T00:00:05Z"), 4, ledger,
	); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(ledger.instants(), ","); got != "2026-08-19T22:00:00Z,2026-08-20T00:00:00Z" {
		t.Fatalf("ledger instants = %s, want 22:00 and 00:00 with 23:00 skipped", got)
	}

	want := skipped.Format(time.RFC3339)
	var skipLines []map[string]any
	for _, line := range lines {
		if line["reason"] == "skipped_open_run" && line["config_id"] == "config-span" {
			skipLines = append(skipLines, line)
		}
	}
	if len(skipLines) != 1 {
		t.Fatalf("skip lines = %d, want 1; lines = %v", len(skipLines), lines)
	}
	named := false
	for key, value := range skipLines[0] {
		if key != "time" && value == want {
			named = true
		}
	}
	if !named {
		t.Errorf("CHAOS-3954: the skip line does not name the skipped cron instant %s; line = %v", want, skipLines[0])
	}
}

// CHAOS-3954 control cell. The 22:00 run ended at 22:20, before the next
// instant, so the 23:00 tick mints 23:00 and writes no skip line. Passes on
// main and must keep passing after the fix.
func TestSyncRunEndingBeforeTheNextInstantMintsIt(t *testing.T) {
	createdAt := at("2026-08-01T00:00:00Z")
	minted := at("2026-08-19T22:00:00Z")
	ended := at("2026-08-19T22:20:00Z")
	marker := at("2026-08-19T23:00:00Z")
	ledger := newLedgerCoordinator()
	ledger.minted[minted] = true

	transaction := &fakeSchedulerTransaction{
		rows: &fakeLockedRows{rows: [][]any{lockedLedgerRow(
			"config-short", "org-a", "job-a", "0 * * * *",
			createdAt, ended, &marker, ledger.latest(),
		)}},
		execTag:  pgconn.NewCommandTag("UPDATE 1"),
		openRuns: openScheduledRuns{lastScheduledRunEndedAt: &ended},
	}
	result, lines := runSkipWindowThroughLoop(t, transaction, ledger, at("2026-08-19T23:00:05Z"))
	if result.Minted() != 1 || len(result.SkippedOpenRun) != 0 {
		t.Fatalf("minted=%d skipped=%d, want 23:00 minted", result.Minted(), len(result.SkippedOpenRun))
	}
	if !result.HandedOff[0].ScheduledFor.Equal(marker) {
		t.Fatalf("minted %s, want %s", result.HandedOff[0].ScheduledFor, marker)
	}
	if !result.HandedOff[0].NextRunAt.Equal(at("2026-08-20T00:00:00Z")) {
		t.Fatalf("next marker = %s, want 00:00", result.HandedOff[0].NextRunAt)
	}
	for _, line := range lines {
		if line["reason"] == "skipped_open_run" {
			t.Fatalf("a run that ended before the next instant wrote a skip line: %v", line)
		}
	}
	if got := strings.Join(ledger.instants(), ","); got != "2026-08-19T22:00:00Z,2026-08-19T23:00:00Z" {
		t.Fatalf("ledger instants = %s", got)
	}
}
