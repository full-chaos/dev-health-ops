package sync

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/jackc/pgx/v5/pgconn"
)

// ledgerCoordinator models scheduled_sync_occurrences' unique
// (sync_config_id, scheduled_for) constraint: the second insert of an instant
// writes nothing and is reported as a repeat, exactly like the production
// ON CONFLICT DO NOTHING path in OccurrenceCoordinator.Handoff.
type ledgerCoordinator struct{ minted map[time.Time]bool }

func newLedgerCoordinator() *ledgerCoordinator {
	return &ledgerCoordinator{minted: map[time.Time]bool{}}
}

func (ledger *ledgerCoordinator) Handoff(
	_ context.Context,
	_ HandoffTransaction,
	occurrence Occurrence,
) (HandoffOutcome, error) {
	instant := occurrence.ScheduledFor.UTC()
	if ledger.minted[instant] {
		return OccurrenceRepeated, nil
	}
	ledger.minted[instant] = true
	return OccurrenceMinted, nil
}

// latest mirrors the MAX(scheduled_for) sub-select the handoff query reads.
func (ledger *ledgerCoordinator) latest() *time.Time {
	var newest time.Time
	for instant := range ledger.minted {
		if instant.After(newest) {
			newest = instant
		}
	}
	if newest.IsZero() {
		return nil
	}
	return &newest
}

func (ledger *ledgerCoordinator) instants() []string {
	instants := make([]string, 0, len(ledger.minted))
	for instant := range ledger.minted {
		instants = append(instants, instant.Format(time.RFC3339))
	}
	sort.Strings(instants)
	return instants
}

// lockedLedgerRow is lockedRow plus the marker and occurrence-ledger columns
// the handoff query now reads, so a test can replay consecutive windows the
// way production does rather than one isolated window.
func lockedLedgerRow(
	configID, orgID, jobID, cron string,
	createdAt, lastSyncAt time.Time,
	nextRunAt, lastOccurrenceAt *time.Time,
) []any {
	row := lockedRow(configID, orgID, jobID, cron, createdAt, lastSyncAt)
	if nextRunAt != nil {
		row[14] = nextRunAt.UTC()
	}
	row = append(row, nil)
	if lastOccurrenceAt != nil {
		row[15] = lastOccurrenceAt.UTC()
	}
	return row
}

// TestFailedRunStillMintsAnOccurrenceEveryWindow is the CHAOS-3936 regression.
// The config's sync never completes, so last_sync_at stays frozen at 20:38 for
// the whole test -- the exact production state on 2026-08-19. Every hourly
// window must still durably add one NEW instant. Before the fix this holds one
// instant (21:00) forever while every window reports a successful handoff.
func TestFailedRunStillMintsAnOccurrenceEveryWindow(t *testing.T) {
	createdAt := at("2026-08-01T00:00:00Z")
	frozenLastSync := at("2026-08-19T20:38:00Z")
	ledger := newLedgerCoordinator()
	var nextRunAt *time.Time

	for tick := range 5 {
		observedAt := at("2026-08-19T21:00:00Z").Add(time.Duration(tick) * time.Hour)
		transaction := &fakeSchedulerTransaction{
			rows: &fakeLockedRows{rows: [][]any{lockedLedgerRow(
				"config-frozen", "org-a", "job-a", "0 * * * *",
				createdAt, frozenLastSync, nextRunAt, ledger.latest(),
			)}},
			execTag: pgconn.NewCommandTag("UPDATE 1"),
		}
		result, err := mutationRepository(transaction).HandoffDueResult(
			context.Background(), observedAt, 4, ledger,
		)
		if err != nil {
			t.Fatalf("window %d HandoffDueResult() err = %v", tick, err)
		}
		if len(ledger.minted) != tick+1 {
			t.Fatalf(
				"after %d windows the occurrence ledger holds %d instants, want %d: %v",
				tick+1, len(ledger.minted), tick+1, ledger.instants(),
			)
		}
		if result.Minted() != 1 || len(result.Repeated) != 0 {
			t.Fatalf("window %d minted=%d repeated=%d", tick, result.Minted(), len(result.Repeated))
		}
		advanced := transaction.execArgs[0][0].(time.Time)
		nextRunAt = &advanced
	}
	want := []string{
		"2026-08-19T21:00:00Z", "2026-08-19T22:00:00Z", "2026-08-19T23:00:00Z",
		"2026-08-20T00:00:00Z", "2026-08-20T01:00:00Z",
	}
	if strings.Join(ledger.instants(), ",") != strings.Join(want, ",") {
		t.Fatalf("ledger instants = %v, want %v", ledger.instants(), want)
	}
}

// A completed sync must keep its Python-parity base: the ledger only ever
// pushes the base forward, it never drags a config back to an older instant.
func TestCompletedSyncKeepsLastSyncBaseWhenItLeadsTheLedger(t *testing.T) {
	lastOccurrence := at("2026-08-19T21:00:00Z")
	lastSync := at("2026-08-19T21:40:00Z")
	evaluation := Evaluate(Candidate{
		ConfigID:         "config-a",
		Active:           true,
		ScheduleCron:     "0 * * * *",
		CreatedAt:        at("2026-08-01T00:00:00Z"),
		LastSyncAt:       &lastSync,
		LastOccurrenceAt: &lastOccurrence,
	}, at("2026-08-19T22:00:00Z"))
	if !evaluation.Base.Equal(lastSync) {
		t.Fatalf("base = %s, want the later completed sync %s", evaluation.Base, lastSync)
	}
	if evaluation.NextOccurrence == nil || !evaluation.NextOccurrence.Equal(at("2026-08-19T22:00:00Z")) {
		t.Fatalf("next occurrence = %v", evaluation.NextOccurrence)
	}
}

// The kernel must distinguish an occurrence it created from one that was
// already there. Both are successes; only the first is scheduler productivity.
func TestHandoffWindowReportsAnAlreadyPresentOccurrenceAsRepeated(t *testing.T) {
	observedAt := at("2026-08-19T21:00:00Z")
	ledger := newLedgerCoordinator()
	ledger.minted[observedAt] = true
	transaction := &fakeSchedulerTransaction{
		rows: &fakeLockedRows{rows: [][]any{lockedLedgerRow(
			"config-frozen", "org-a", "job-a", "0 * * * *",
			at("2026-08-01T00:00:00Z"), at("2026-08-19T20:38:00Z"), nil, nil,
		)}},
		execTag: pgconn.NewCommandTag("UPDATE 1"),
	}
	result, err := mutationRepository(transaction).HandoffDueResult(
		context.Background(), observedAt, 4, ledger,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Minted() != 0 || len(result.Repeated) != 1 {
		t.Fatalf("minted=%d repeated=%d", result.Minted(), len(result.Repeated))
	}
	if result.Repeated[0].ConfigID != "config-frozen" ||
		!result.Repeated[0].ScheduledFor.Equal(observedAt) {
		t.Fatalf("repeated occurrence = %#v", result.Repeated[0])
	}
	if !transaction.committed {
		t.Fatal("a repeated occurrence must still commit and advance the marker")
	}
}

// The 2026-08-19 outage emitted no error, no log line, and no metric: every
// counter measured whether the scheduler RAN. A window that re-confirms an
// existing instant must now name the config and the instant, and must move
// minted and repeated apart so a dashboard can see a frozen schedule.
func TestLoopNamesAndCountsWindowsThatProduceNoNewOccurrence(t *testing.T) {
	repeated := Occurrence{
		ConfigID:     "config-frozen",
		OrgID:        "org-a",
		JobID:        "job-a",
		ScheduledFor: at("2026-08-19T21:00:00Z"),
		ObservedAt:   at("2026-08-19T23:00:00Z"),
	}
	logs := &strings.Builder{}
	clock := &testLoopClock{now: at("2026-08-19T23:00:00Z")}
	registry := health.NewRegistry(time.Second)
	loop, err := newLoop(
		schedulerHandoffStepper(func() (HandoffResult, error) {
			return HandoffResult{
				Candidates:     1,
				TimingEligible: 1,
				HandedOff:      []Occurrence{repeated},
				Repeated:       []Occurrence{repeated},
			}, nil
		}),
		CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
			return OccurrenceMinted, nil
		}),
		LoopConfig{
			PollInterval: minLoopPollInterval,
			StepTimeout:  time.Second,
			MaxBackoff:   80 * time.Millisecond,
			Limit:        3,
			Registry:     registry,
			Occurrences:  &stubOccurrences{},
			Logger:       slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})),
		},
		clock,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = loop.Shutdown(context.Background()) }()

	for _, want := range []string{
		"config_id=config-frozen",
		"scheduled_for=2026-08-19T21:00:00Z",
		"found due candidates but minted no occurrence",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("scheduler logs omit %q; logs = %s", want, logs.String())
		}
	}
	var metrics strings.Builder
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"sync_scheduler_occurrences_minted_total 0",
		"sync_scheduler_occurrences_repeated_total 1",
		"sync_scheduler_idle_due_windows_total 1",
		"sync_scheduler_handoffs_total 1",
	} {
		if !strings.Contains(metrics.String(), want) {
			t.Errorf("metrics omit %q; metrics = %s", want, metrics.String())
		}
	}
}

// A ledger row dated after observedAt must not advance the base. The scheduler
// never mints a future instant itself, so such a row means clock skew between
// replicas or a hand-edited/restored row -- and letting it through would push
// the next occurrence past now and make the config silently not-due until real
// time caught up. That is the CHAOS-3936 freeze again in a different hat, so
// the fix must not introduce it while removing the original.
func TestFutureDatedLedgerRowCannotSuppressADueOccurrence(t *testing.T) {
	observedAt := at("2026-08-19T12:00:00Z")
	lastSync := at("2026-08-19T10:00:00Z")
	future := at("2026-08-19T20:00:00Z")
	evaluation := Evaluate(Candidate{
		ConfigID:         "config-skewed",
		Active:           true,
		ScheduleCron:     "0 * * * *",
		CreatedAt:        at("2026-08-01T00:00:00Z"),
		LastSyncAt:       &lastSync,
		LastOccurrenceAt: &future,
	}, observedAt)
	if !evaluation.Due || evaluation.Decision != DecisionScheduleDue {
		t.Fatalf("a future-dated ledger row froze the schedule: due=%v decision=%s base=%s",
			evaluation.Due, evaluation.Decision, evaluation.Base)
	}
	if !evaluation.Base.Equal(lastSync) {
		t.Fatalf("base = %s, want the last completed sync %s", evaluation.Base, lastSync)
	}
	if evaluation.NextOccurrence == nil || !evaluation.NextOccurrence.Equal(at("2026-08-19T11:00:00Z")) {
		t.Fatalf("next occurrence = %v, want 11:00", evaluation.NextOccurrence)
	}
}
