package sync

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
) (HandoffResult, []map[string]any, string) {
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
	var metrics strings.Builder
	if err := loop.WritePrometheus(&metrics); err != nil {
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
	return result, lines, metrics.String()
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
	result, lines, _ := runSkipWindowThroughLoop(t, spanning, ledger, at("2026-08-19T23:00:05Z"))
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
	result, lines, _ := runSkipWindowThroughLoop(t, transaction, ledger, at("2026-08-19T23:00:05Z"))
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

// CHAOS-3954 red cell for the late-poll path. The scheduler is down from 22:30
// to 02:30. The 02:30 window mints 23:00 late and moves the marker to the next
// instant after now (03:00), and the 03:00 window rebases onto the 23:00 run's
// completion (02:45). 00:00, 01:00 and 02:00 never reach the ledger. The window
// that passes over them must count them and name them in one WARN line.
func TestSchedulerDownAcrossInstantsCountsAndNamesTheUnmintedInstants(t *testing.T) {
	createdAt := at("2026-08-01T00:00:00Z")
	previousEnd := at("2026-08-19T22:20:00Z")
	marker := at("2026-08-19T23:00:00Z")
	ledger := newLedgerCoordinator()
	ledger.minted[at("2026-08-19T22:00:00Z")] = true

	late := &fakeSchedulerTransaction{
		rows: &fakeLockedRows{rows: [][]any{lockedLedgerRow(
			"config-down", "org-a", "job-a", "0 * * * *",
			createdAt, previousEnd, &marker, ledger.latest(),
		)}},
		execTag:  pgconn.NewCommandTag("UPDATE 1"),
		openRuns: openScheduledRuns{lastScheduledRunEndedAt: &previousEnd},
	}
	first, _, _ := runSkipWindowThroughLoop(t, late, ledger, at("2026-08-20T02:30:05Z"))
	if first.Minted() != 1 || !first.HandedOff[0].ScheduledFor.Equal(marker) {
		t.Fatalf("02:30 window minted %d, handed off %v, want 23:00 minted late", first.Minted(), first.HandedOff)
	}

	lateEnd := at("2026-08-20T02:45:00Z")
	nextMarker := first.HandedOff[0].NextRunAt
	resumed := &fakeSchedulerTransaction{
		rows: &fakeLockedRows{rows: [][]any{lockedLedgerRow(
			"config-down", "org-a", "job-a", "0 * * * *",
			createdAt, lateEnd, &nextMarker, ledger.latest(),
		)}},
		execTag:  pgconn.NewCommandTag("UPDATE 1"),
		openRuns: openScheduledRuns{lastScheduledRunEndedAt: &lateEnd},
	}
	second, lines, metrics := runSkipWindowThroughLoop(t, resumed, ledger, at("2026-08-20T03:00:05Z"))
	if second.Minted() != 1 || !second.HandedOff[0].ScheduledFor.Equal(at("2026-08-20T03:00:00Z")) {
		t.Fatalf("03:00 window minted %d, handed off %v, want 03:00", second.Minted(), second.HandedOff)
	}
	if got := strings.Join(ledger.instants(), ","); got != "2026-08-19T22:00:00Z,2026-08-19T23:00:00Z,2026-08-20T03:00:00Z" {
		t.Fatalf("ledger instants = %s", got)
	}

	var named []map[string]any
	for _, line := range lines {
		if line["reason"] == "unminted_instants" && line["config_id"] == "config-down" {
			named = append(named, line)
		}
	}
	if len(named) != 1 {
		t.Fatalf("CHAOS-3954: unminted-instant lines = %d, want 1; lines = %v", len(named), lines)
	}
	if named[0]["unminted_count"] != float64(3) ||
		named[0]["unminted_instants"] != "2026-08-20T00:00:00Z,2026-08-20T01:00:00Z,2026-08-20T02:00:00Z" ||
		named[0]["scheduled_for"] != "2026-08-20T03:00:00Z" {
		t.Errorf("CHAOS-3954: unminted-instant line = %v", named[0])
	}
	if !strings.Contains(metrics, "sync_scheduler_unminted_instants_total 3\n") {
		t.Errorf("CHAOS-3954: metrics omit sync_scheduler_unminted_instants_total 3; metrics = %s", metrics)
	}
}

// CHAOS-3954 bound cell, skip side. The 22:00 run stays open for the whole
// 23:00 hour and the scheduler polls every second. The skip moves the marker
// to the next instant, so the hour writes one skip, not one per poll.
func TestOpenRunSkipIsOneLinePerSkippedInstantNotPerPoll(t *testing.T) {
	createdAt := at("2026-08-01T00:00:00Z")
	previousSync := at("2026-08-19T21:10:00Z")
	start := at("2026-08-19T23:00:00Z")
	marker := start
	ledger := newLedgerCoordinator()
	ledger.minted[at("2026-08-19T22:00:00Z")] = true
	skips := 0
	for second := range 3600 {
		transaction := &fakeSchedulerTransaction{
			rows: &fakeLockedRows{rows: [][]any{lockedLedgerRow(
				"config-open", "org-a", "job-a", "0 * * * *",
				createdAt, previousSync, &marker, ledger.latest(),
			)}},
			execTag:  pgconn.NewCommandTag("UPDATE 1"),
			openRuns: openScheduledRuns{blocking: 1, blockingOccurrenceID: "sha256:run-2200", blockingOpenSeconds: 3600},
		}
		result, err := mutationRepository(transaction).HandoffDueResult(
			context.Background(), start.Add(time.Duration(second)*time.Second), 4, ledger,
		)
		if err != nil {
			t.Fatal(err)
		}
		skips += len(result.SkippedOpenRun)
		// Feed the marker the window wrote, not the one it reported: a skip
		// that reports a moved marker but does not write one must still fail.
		if len(transaction.execArgs) == 1 {
			marker = transaction.execArgs[0][0].(time.Time)
		}
		if result.Minted() != 0 {
			t.Fatalf("poll %d minted while the run is open", second)
		}
	}
	if skips != 1 {
		t.Fatalf("skips over one hour of 1 s polls = %d, want 1", skips)
	}
}

// CHAOS-3954 bound cell, coalescing side. One minted occurrence that passes
// over many instants writes one line naming at most unmintedNamedLimit
// instants (first and last) with the full count, and the scan that counts them
// stops at unmintedScanLimit.
func TestUnmintedInstantLineIsBounded(t *testing.T) {
	createdAt := at("2026-08-01T00:00:00Z")
	for _, tc := range []struct {
		name       string
		newest     string
		ended      string
		observedAt string
		wantCount  float64
		wantCapped bool
		wantNamed  string
	}{
		{"two hours of a minutely cron", "2026-08-20T10:01:00Z", "2026-08-20T12:00:50Z", "2026-08-20T12:01:05Z", 119, false, "2026-08-20T10:02:00Z,2026-08-20T12:00:00Z"},
		{"one day of a minutely cron hits the scan cap", "2026-08-19T00:00:00Z", "2026-08-19T23:59:50Z", "2026-08-20T00:00:05Z", unmintedScanLimit, true, "2026-08-19T00:01:00Z,2026-08-19T17:04:00Z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ledger := newLedgerCoordinator()
			ledger.minted[at(tc.newest)] = true
			ended := at(tc.ended)
			observedAt := at(tc.observedAt)
			marker := observedAt.Truncate(time.Minute)
			transaction := &fakeSchedulerTransaction{
				rows: &fakeLockedRows{rows: [][]any{lockedLedgerRow(
					"config-minutely", "org-a", "job-a", "* * * * *",
					createdAt, ended, &marker, ledger.latest(),
				)}},
				execTag:  pgconn.NewCommandTag("UPDATE 1"),
				openRuns: openScheduledRuns{lastScheduledRunEndedAt: &ended},
			}
			result, lines, metrics := runSkipWindowThroughLoop(t, transaction, ledger, observedAt)
			if result.Minted() != 1 || !result.HandedOff[0].ScheduledFor.Equal(marker) {
				t.Fatalf("minted %d, handed off %v, want %s", result.Minted(), result.HandedOff, marker)
			}
			var named []map[string]any
			for _, line := range lines {
				if line["reason"] == "unminted_instants" {
					named = append(named, line)
				}
			}
			if len(named) != 1 {
				t.Fatalf("unminted-instant lines = %d, want 1", len(named))
			}
			instants := strings.Split(named[0]["unminted_instants"].(string), ",")
			if len(instants) > unmintedNamedLimit {
				t.Fatalf("one line names %d instants, want at most %d", len(instants), unmintedNamedLimit)
			}
			if named[0]["unminted_instants"] != tc.wantNamed ||
				named[0]["unminted_count"] != tc.wantCount ||
				named[0]["unminted_count_capped"] != tc.wantCapped {
				t.Fatalf("unminted-instant line = %v", named[0])
			}
			if want := fmt.Sprintf("sync_scheduler_unminted_instants_total %d\n", int(tc.wantCount)); !strings.Contains(metrics, want) {
				t.Fatalf("metrics omit %q", want)
			}
		})
	}
}

// CHAOS-3954 cell: the unminted-instant count only reports. When it fails, the
// window still mints, advances the marker and commits, exactly as before the
// count existed, and names the config whose count it lost.
func TestUnmintedCountFailureStillMints(t *testing.T) {
	createdAt := at("2026-08-01T00:00:00Z")
	lateEnd := at("2026-08-20T02:45:00Z")
	marker := at("2026-08-20T03:00:00Z")
	ledger := newLedgerCoordinator()
	ledger.minted[at("2026-08-19T23:00:00Z")] = true
	transaction := &fakeSchedulerTransaction{
		rows: &fakeLockedRows{rows: [][]any{lockedLedgerRow(
			"config-count", "org-a", "job-a", "0 * * * *",
			createdAt, lateEnd, &marker, ledger.latest(),
		)}},
		execTag:  pgconn.NewCommandTag("UPDATE 1"),
		openRuns: openScheduledRuns{lastScheduledRunEndedAt: &lateEnd},
	}
	repository := mutationRepository(transaction)
	repository.countUnminted = func(context.Context, *Job, Occurrence, time.Time) (UnmintedInstants, error) {
		return UnmintedInstants{}, errors.New("forced count failure")
	}
	var logs bytes.Buffer
	var result HandoffResult
	var stepErr error
	loop, err := newLoop(
		loopStepFunc(func(ctx context.Context, now time.Time, limit int, coordinator Coordinator) (HandoffResult, error) {
			result, stepErr = repository.HandoffDueResult(ctx, now, limit, coordinator)
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
		&testLoopClock{now: at("2026-08-20T03:00:05Z")},
	)
	if err != nil {
		t.Fatal(err)
	}
	startErr := loop.Start(context.Background())
	_ = loop.Shutdown(context.Background())
	if stepErr != nil || startErr != nil {
		t.Fatalf("a count failure failed the window: step err = %v, start err = %v", stepErr, startErr)
	}
	if result.Minted() != 1 || !result.HandedOff[0].ScheduledFor.Equal(marker) || !transaction.committed {
		t.Fatalf("minted=%d committed=%v handed off=%v, want 03:00 minted and committed",
			result.Minted(), transaction.committed, result.HandedOff)
	}
	if len(transaction.execArgs) != 1 {
		t.Fatalf("marker writes = %d, want 1", len(transaction.execArgs))
	}
	if len(result.Unminted) != 0 || len(result.UnmintedCountFailed) != 1 ||
		result.UnmintedCountFailed[0].ConfigID != "config-count" {
		t.Fatalf("unminted = %v, count failures = %v", result.Unminted, result.UnmintedCountFailed)
	}
	if !strings.Contains(logs.String(), `"reason":"unminted_count_failed"`) ||
		!strings.Contains(logs.String(), `"config_id":"config-count"`) {
		t.Fatalf("no count-failure line; logs = %s", logs.String())
	}
}
