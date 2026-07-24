package fixed

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// memoryLedger is a shared, transactional stand-in for the PostgreSQL ledger.
// Claims are only visible to other engines after Complete commits, which is
// what lets one process model two racing replicas.
type memoryLedger struct {
	mu       sync.Mutex
	byKey    map[string]Occurrence
	byTime   map[string]Anchor
	statuses map[string]string
	failNext error
}

func newMemoryLedger() *memoryLedger {
	return &memoryLedger{
		byKey:    map[string]Occurrence{},
		byTime:   map[string]Anchor{},
		statuses: map[string]string{},
	}
}

func (ledger *memoryLedger) Claim(_ context.Context, _ pgx.Tx, occurrence Occurrence) (ClaimResult, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.failNext != nil {
		err := ledger.failNext
		ledger.failNext = nil
		return "", err
	}
	if existing, ok := ledger.byKey[occurrence.Key]; ok {
		if existing.ScheduleID != occurrence.ScheduleID ||
			!existing.ScheduledFor.Equal(occurrence.ScheduledFor) {
			return "", ErrOccurrenceConflict
		}
		return ClaimDuplicate, nil
	}
	ledger.byKey[occurrence.Key] = occurrence
	ledger.statuses[occurrence.Key] = "claimed"
	last := ledger.byTime[occurrence.ScheduleID]
	if occurrence.ScheduledFor.After(last.ScheduledFor) {
		ledger.byTime[occurrence.ScheduleID] = Anchor{
			ScheduledFor: occurrence.ScheduledFor,
			ObservedAt:   occurrence.ObservedAt,
		}
	}
	return ClaimInserted, nil
}

func (ledger *memoryLedger) Complete(
	_ context.Context, _ pgx.Tx, occurrence Occurrence, status string, handoffs int, reason string,
) error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if _, ok := ledger.byKey[occurrence.Key]; !ok {
		return ErrLedgerUnavailable
	}
	_ = handoffs
	_ = reason
	ledger.statuses[occurrence.Key] = status
	return nil
}

func (ledger *memoryLedger) LastOccurrence(
	_ context.Context, _ pgx.Tx, scheduleID string,
) (Anchor, bool, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	value, ok := ledger.byTime[scheduleID]
	return value, ok, nil
}

// rollback discards an uncommitted claim, modelling a crash between the claim
// and the commit.
func (ledger *memoryLedger) rollback(key string) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	occurrence, ok := ledger.byKey[key]
	if !ok {
		return
	}
	delete(ledger.byKey, key)
	delete(ledger.statuses, key)
	delete(ledger.byTime, occurrence.ScheduleID)
}

func (ledger *memoryLedger) count() int {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return len(ledger.byKey)
}

func (ledger *memoryLedger) status(key string) string {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return ledger.statuses[key]
}

// stubTx satisfies pgx.Tx for producers and ledgers that never touch the
// connection. Any unexpected use panics rather than silently succeeding.
type stubTx struct {
	mu           sync.Mutex
	committed    int
	rolledBack   int
	failCommit   bool
	onRollback   func()
	queryHandler func(string, ...any) (pgx.Rows, error)
}

func (tx *stubTx) Begin(context.Context) (pgx.Tx, error) { return tx, nil }
func (tx *stubTx) Commit(context.Context) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.failCommit {
		return errors.New("commit failed")
	}
	tx.committed++
	return nil
}

func (tx *stubTx) Rollback(context.Context) error {
	tx.mu.Lock()
	handler := tx.onRollback
	tx.rolledBack++
	tx.mu.Unlock()
	if handler != nil {
		handler()
	}
	return nil
}

func (tx *stubTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	panic("unexpected CopyFrom")
}
func (tx *stubTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	panic("unexpected SendBatch")
}
func (tx *stubTx) LargeObjects() pgx.LargeObjects { panic("unexpected LargeObjects") }
func (tx *stubTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	panic("unexpected Prepare")
}
func (tx *stubTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (tx *stubTx) Query(_ context.Context, statement string, args ...any) (pgx.Rows, error) {
	if tx.queryHandler != nil {
		return tx.queryHandler(statement, args...)
	}
	panic("unexpected Query")
}
func (tx *stubTx) QueryRow(context.Context, string, ...any) pgx.Row { panic("unexpected QueryRow") }
func (tx *stubTx) Conn() *pgx.Conn                                  { return nil }

type stubBeginner struct{ tx *stubTx }

func (beginner stubBeginner) Begin(context.Context) (pgx.Tx, error) { return beginner.tx, nil }

// recordingPublisher captures published handoffs.
type recordingPublisher struct {
	mu       sync.Mutex
	requests []JobRequest
	deferred []bool
	err      error
}

func (publisher *recordingPublisher) Publish(
	_ context.Context, _ pgx.Tx, request JobRequest, executable bool,
) error {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if publisher.err != nil {
		return publisher.err
	}
	publisher.requests = append(publisher.requests, request)
	publisher.deferred = append(publisher.deferred, !executable)
	return nil
}

func (publisher *recordingPublisher) count() int {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	return len(publisher.requests)
}

func testRegistry(t *testing.T) *jobruntime.Registry {
	t.Helper()
	registry, err := jobruntime.Load(
		filepath.Join(repositoryRoot(t), "contracts", "jobs", "v1"),
	)
	if err != nil {
		t.Fatalf("load job registry: %v", err)
	}
	return registry
}

func heartbeatSchedule(t *testing.T) Schedule {
	t.Helper()
	schedules, err := Schedules()
	if err != nil {
		t.Fatalf("Schedules() = %v", err)
	}
	for _, schedule := range schedules {
		if schedule.ID == "phone_home_heartbeat" {
			return schedule
		}
	}
	t.Fatal("phone_home_heartbeat is not declared")
	return Schedule{}
}

// seedAnchor records a prior boundary so a test exercises steady state instead
// of the cold-start baseline path.
func seedAnchor(ledger *memoryLedger, schedule Schedule, scheduledFor time.Time) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	ledger.byTime[schedule.ID] = Anchor{
		ScheduledFor: scheduledFor.UTC(),
		ObservedAt:   scheduledFor.UTC(),
	}
}

func newTestEngine(
	t *testing.T,
	schedule Schedule,
	ledger Ledger,
	publisher Publisher,
	tx *stubTx,
) *Engine {
	t.Helper()
	producers, err := NewProducerSet(NewHeartbeatProducer(), NewRetentionProducer())
	if err != nil {
		t.Fatalf("NewProducerSet() = %v", err)
	}
	engine, err := newEngine(EngineConfig{
		Schedules: []Schedule{schedule},
		Producers: producers,
		Ledger:    ledger,
		Publisher: publisher,
		Registry:  testRegistry(t),
	}, stubBeginner{tx: tx})
	if err != nil {
		t.Fatalf("newEngine() = %v", err)
	}
	return engine
}

func TestTwoReplicasProduceExactlyOneOccurrencePerDueTime(t *testing.T) {
	ctx := context.Background()
	schedule := heartbeatSchedule(t)
	ledger := newMemoryLedger()
	seedAnchor(ledger, schedule, mustTime(t, "2026-07-23T00:00:00Z"))
	firstPublisher := &recordingPublisher{}
	secondPublisher := &recordingPublisher{}
	first := newTestEngine(t, schedule, ledger, firstPublisher, &stubTx{})
	second := newTestEngine(t, schedule, ledger, secondPublisher, &stubTx{})

	observedAt := mustTime(t, "2026-07-24T00:30:00Z")
	var waiter sync.WaitGroup
	results := make([]WindowResult, 2)
	errs := make([]error, 2)
	waiter.Add(2)
	for index, engine := range []*Engine{first, second} {
		go func(index int, engine *Engine) {
			defer waiter.Done()
			results[index], errs[index] = engine.Step(ctx, observedAt)
		}(index, engine)
	}
	waiter.Wait()

	for index, err := range errs {
		if err != nil {
			t.Fatalf("replica %d Step() = %v", index, err)
		}
		if results[index].Failed() {
			t.Fatalf("replica %d window failed: %v", index, results[index].Err())
		}
	}
	if ledger.count() != 1 {
		t.Fatalf("ledger holds %d occurrences, want exactly one per due time", ledger.count())
	}
	published := firstPublisher.count() + secondPublisher.count()
	if published != 1 {
		t.Fatalf("replicas published %d handoffs, want exactly one", published)
	}
	claimed := results[0].Schedules[0].Claimed + results[1].Schedules[0].Claimed
	duplicate := results[0].Schedules[0].Duplicate + results[1].Schedules[0].Duplicate
	if claimed != 1 || duplicate != 1 {
		t.Fatalf("claimed=%d duplicate=%d, want one winner and one loser", claimed, duplicate)
	}
}

func TestRepeatedWindowsInTheSameBucketDoNotDuplicateWork(t *testing.T) {
	ctx := context.Background()
	schedule := heartbeatSchedule(t)
	ledger := newMemoryLedger()
	seedAnchor(ledger, schedule, mustTime(t, "2026-07-23T00:00:00Z"))
	publisher := &recordingPublisher{}
	engine := newTestEngine(t, schedule, ledger, publisher, &stubTx{})

	for _, observed := range []string{
		"2026-07-24T00:00:00Z", "2026-07-24T00:00:15Z", "2026-07-24T06:00:00Z",
	} {
		result, err := engine.Step(ctx, mustTime(t, observed))
		if err != nil || result.Failed() {
			t.Fatalf("Step(%s) = %v / %v", observed, err, result.Err())
		}
	}
	if ledger.count() != 1 {
		t.Fatalf("ledger holds %d occurrences after repeated windows, want 1", ledger.count())
	}
	if publisher.count() != 1 {
		t.Fatalf("published %d handoffs after repeated windows, want 1", publisher.count())
	}
}

func TestRestartAfterCommitDoesNotReproduceTheOccurrence(t *testing.T) {
	ctx := context.Background()
	schedule := heartbeatSchedule(t)
	ledger := newMemoryLedger()
	seedAnchor(ledger, schedule, mustTime(t, "2026-07-23T00:00:00Z"))
	publisher := &recordingPublisher{}
	observedAt := mustTime(t, "2026-07-24T00:05:00Z")

	first := newTestEngine(t, schedule, ledger, publisher, &stubTx{})
	if _, err := first.Step(ctx, observedAt); err != nil {
		t.Fatalf("Step() = %v", err)
	}
	// A fresh process shares only the durable ledger, exactly like a restart.
	restarted := newTestEngine(t, schedule, ledger, publisher, &stubTx{})
	result, err := restarted.Step(ctx, observedAt.Add(time.Hour))
	if err != nil || result.Failed() {
		t.Fatalf("restarted Step() = %v / %v", err, result.Err())
	}
	if publisher.count() != 1 {
		t.Fatalf("restart published %d handoffs, want 1", publisher.count())
	}
	// The durable anchor means a restart does not even attempt the boundary it
	// already owns. The duplicate-claim path still matters for two replicas
	// racing the same window and is covered separately.
	if result.Schedules[0].Due != 0 || result.Schedules[0].Claimed != 0 {
		t.Fatalf("restart re-attempted an owned occurrence: %+v", result.Schedules[0])
	}
}

func TestCrashBeforeCommitLeavesTheOccurrenceEligible(t *testing.T) {
	ctx := context.Background()
	schedule := heartbeatSchedule(t)
	ledger := newMemoryLedger()
	seedAnchor(ledger, schedule, mustTime(t, "2026-07-23T00:00:00Z"))
	publisher := &recordingPublisher{}
	observedAt := mustTime(t, "2026-07-24T00:05:00Z")

	// A failing commit models a crash after the producer wrote its rows but
	// before the transaction became durable. The rollback must discard the
	// claim so the next window can retry the same due time.
	crashing := &stubTx{failCommit: true}
	occurrence := NewOccurrence(schedule, mustTime(t, "2026-07-24T00:00:00Z"), observedAt)
	crashing.onRollback = func() {
		ledger.rollback(occurrence.Key)
		seedAnchor(ledger, schedule, mustTime(t, "2026-07-23T00:00:00Z"))
	}
	engine := newTestEngine(t, schedule, ledger, publisher, crashing)
	result, err := engine.Step(ctx, observedAt)
	if err != nil {
		t.Fatalf("Step() = %v", err)
	}
	if !result.Failed() {
		t.Fatal("a failed commit was reported as a successful window")
	}
	if ledger.count() != 0 {
		t.Fatalf("ledger retained %d occurrences after rollback", ledger.count())
	}

	recovered := newTestEngine(t, schedule, ledger, publisher, &stubTx{})
	retry, err := recovered.Step(ctx, observedAt.Add(time.Minute))
	if err != nil || retry.Failed() {
		t.Fatalf("recovery Step() = %v / %v", err, retry.Err())
	}
	if retry.Schedules[0].Claimed != 1 {
		t.Fatalf("recovery did not re-claim the occurrence: %+v", retry.Schedules[0])
	}
	if ledger.status(occurrence.Key) != OccurrenceMaterialized {
		t.Fatalf("recovered occurrence status = %q", ledger.status(occurrence.Key))
	}
}

func TestSkipPolicyDoesNotReplayOccurrencesMissedDuringAnOutage(t *testing.T) {
	ctx := context.Background()
	schedule := heartbeatSchedule(t)
	if schedule.CatchUp != CatchUpSkip {
		t.Fatalf("heartbeat catch-up policy changed to %q", schedule.CatchUp)
	}
	ledger := newMemoryLedger()
	publisher := &recordingPublisher{}
	engine := newTestEngine(t, schedule, ledger, publisher, &stubTx{})

	// The scheduler was down for three days. Skip policy resumes rather than
	// catching up: a heartbeat for a day the process was absent would misstate
	// that day, so the boundary is re-baselined and no telemetry is emitted.
	seedAnchor(ledger, schedule, mustTime(t, "2026-07-21T00:00:00Z"))
	result, err := engine.Step(ctx, mustTime(t, "2026-07-24T09:00:00Z"))
	if err != nil || result.Failed() {
		t.Fatalf("Step() = %v / %v", err, result.Err())
	}
	if !result.Schedules[0].StaleSkipped || publisher.count() != 0 {
		t.Fatalf("skip policy after a long gap published %d handoffs (%+v)",
			publisher.count(), result.Schedules[0])
	}

	// One missed period is not a gap: the schedule runs normally.
	seedAnchor(ledger, schedule, mustTime(t, "2026-07-24T00:00:00Z"))
	next, err := engine.Step(ctx, mustTime(t, "2026-07-25T09:00:00Z"))
	if err != nil || next.Failed() {
		t.Fatalf("Step() = %v / %v", err, next.Err())
	}
	if publisher.count() != 1 {
		t.Fatalf("published %d handoffs after a single period, want 1", publisher.count())
	}
	payload, ok := publisher.requests[0].Envelope.Payload.(jobcontract.HeartbeatPayload)
	if !ok {
		t.Fatalf("unexpected payload %T", publisher.requests[0].Envelope.Payload)
	}
	if payload.ScheduledFor != "2026-07-25T00:00:00Z" {
		t.Fatalf("emitted %s, want the newest due time", payload.ScheduledFor)
	}
}

func anchor(t *testing.T, value string) *Anchor {
	t.Helper()
	parsed := mustTime(t, value)
	return &Anchor{ScheduledFor: parsed, ObservedAt: parsed}
}

func TestBoundedCatchUpEmitsOneOverdueOccurrenceNotTheWholeGap(t *testing.T) {
	schedule := Schedule{
		ID:               "catch_up_probe",
		LegacyBeatEntry:  "run-daily-metrics",
		Cadence:          DailyAt(1, 0),
		Timezone:         "UTC",
		CatchUp:          CatchUpBounded,
		UniquenessWindow: 72 * time.Hour,
		TargetKind:       jobcontract.KindHeartbeat,
		ProducerID:       ProducerHeartbeat,
		MaxAttempts:      1,
		AlertThreshold:   72 * time.Hour,
		Rationale:        "probe",
	}
	// Three nights were missed. Celery Beat fires one overdue task and resumes;
	// replaying all three would turn a restart into a burst of backdated
	// nightly fan-outs.
	decision, err := DueOccurrence(
		schedule, mustTime(t, "2026-07-24T09:00:00Z"), anchor(t, "2026-07-21T01:00:00Z"),
	)
	if err != nil {
		t.Fatalf("DueOccurrence() = %v", err)
	}
	if decision.Occurrence == nil {
		t.Fatal("a missed nightly safety net produced no occurrence")
	}
	if !decision.Occurrence.ScheduledFor.Equal(mustTime(t, "2026-07-24T01:00:00Z")) {
		t.Fatalf("emitted %s, want only the newest owed boundary", decision.Occurrence.ScheduledFor)
	}
	if decision.ColdStart {
		t.Fatal("an anchored schedule reported a cold start")
	}
}

func TestColdStartRecordsABaselineInsteadOfFiring(t *testing.T) {
	schedule := heartbeatSchedule(t)
	decision, err := DueOccurrence(schedule, mustTime(t, "2026-07-24T09:00:00Z"), nil)
	if err != nil {
		t.Fatalf("DueOccurrence() = %v", err)
	}
	if decision.Occurrence == nil || !decision.ColdStart {
		t.Fatalf("cold start = %+v, want a baseline occurrence", decision)
	}
}

func TestAnchoredScheduleDoesNotRefireAnOwnedBoundary(t *testing.T) {
	schedule := heartbeatSchedule(t)
	decision, err := DueOccurrence(
		schedule, mustTime(t, "2026-07-24T09:00:00Z"), anchor(t, "2026-07-24T00:00:00Z"),
	)
	if err != nil {
		t.Fatalf("DueOccurrence() = %v", err)
	}
	if decision.Occurrence != nil {
		t.Fatalf("re-fired an already recorded boundary: %+v", decision.Occurrence)
	}
}

// A 300 second schedule started mid-bucket must wait a full period rather than
// immediately firing the bucket it started inside, which is Beat's behavior.
// An interval cadence's grid points carry no meaning of their own, so after a
// cold start the guarantee owed is elapsed time, not the next grid point.
// Anchoring on the grid alone fired a 300 second schedule three minutes after a
// 10:02 start; measuring from the baseline's observation instant restores
// Beat's "one full interval after you started" behavior.
func TestIntervalScheduleWaitsAFullPeriodAfterItsBaseline(t *testing.T) {
	schedule := Schedule{
		ID:               "interval_probe",
		LegacyBeatEntry:  "dispatch-scheduled-metrics",
		Cadence:          EveryInterval(300 * time.Second),
		Timezone:         "UTC",
		CatchUp:          CatchUpSkip,
		UniquenessWindow: time.Hour,
		TargetKind:       jobcontract.KindHeartbeat,
		ProducerID:       ProducerHeartbeat,
		MaxAttempts:      1,
		AlertThreshold:   30 * time.Minute,
		Rationale:        "probe",
	}
	startedAt := mustTime(t, "2026-07-24T10:02:00Z")
	baseline, err := DueOccurrence(schedule, startedAt, nil)
	if err != nil || !baseline.ColdStart {
		t.Fatalf("cold start = %+v err=%v", baseline, err)
	}
	recorded := Anchor{
		ScheduledFor: baseline.Occurrence.ScheduledFor,
		ObservedAt:   baseline.Occurrence.ObservedAt,
	}
	if !recorded.ObservedAt.Equal(startedAt) {
		t.Fatalf("baseline recorded observation %s, want the start instant", recorded.ObservedAt)
	}

	// 10:05 is the next grid point but only three minutes after activation.
	// Firing there is the exact regression this guards.
	early, err := DueOccurrence(schedule, mustTime(t, "2026-07-24T10:05:01Z"), &recorded)
	if err != nil {
		t.Fatal(err)
	}
	if early.Occurrence != nil {
		t.Fatalf("fired at %s, only %s after activation; a full period is owed",
			early.Occurrence.ScheduledFor, early.Occurrence.ScheduledFor.Sub(startedAt))
	}

	// The first eligible boundary is the first grid point at or after
	// start + period, and it must be at least a full period out.
	fired, err := DueOccurrence(schedule, mustTime(t, "2026-07-24T10:11:00Z"), &recorded)
	if err != nil {
		t.Fatal(err)
	}
	if fired.Occurrence == nil {
		t.Fatal("no occurrence after a full period elapsed")
	}
	if elapsed := fired.Occurrence.ScheduledFor.Sub(startedAt); elapsed < schedule.Cadence.Period() {
		t.Fatalf("fired %s after activation, want at least one %s period",
			elapsed, schedule.Cadence.Period())
	}

	// Steady state resumes the plain grid cadence once anchored on real runs.
	steady := Anchor{
		ScheduledFor: fired.Occurrence.ScheduledFor,
		ObservedAt:   fired.Occurrence.ScheduledFor,
	}
	next, err := DueOccurrence(schedule, fired.Occurrence.ScheduledFor.Add(6*time.Minute), &steady)
	if err != nil {
		t.Fatal(err)
	}
	if next.Occurrence == nil ||
		!next.Occurrence.ScheduledFor.Equal(steady.ScheduledFor.Add(5*time.Minute)) {
		t.Fatalf("steady-state cadence drifted: %+v", next.Occurrence)
	}
}

// A wall-clock cadence must NOT wait a period past its own startup: 01:00 means
// 01:00, so a daily schedule baselined at 09:00 still owes tomorrow's 01:00.
func TestWallClockScheduleIsNotDelayedByItsBaselineObservation(t *testing.T) {
	schedule := scheduleByID(t, "daily_metrics_fanout")
	startedAt := mustTime(t, "2026-07-24T09:00:00Z")
	baseline, err := DueOccurrence(schedule, startedAt, nil)
	if err != nil || !baseline.ColdStart {
		t.Fatalf("cold start = %+v err=%v", baseline, err)
	}
	recorded := Anchor{
		ScheduledFor: baseline.Occurrence.ScheduledFor,
		ObservedAt:   baseline.Occurrence.ObservedAt,
	}
	next, err := DueOccurrence(schedule, mustTime(t, "2026-07-25T01:30:00Z"), &recorded)
	if err != nil {
		t.Fatal(err)
	}
	if next.Occurrence == nil {
		t.Fatal("a daily schedule baselined at 09:00 skipped the next 01:00 boundary")
	}
	if !next.Occurrence.ScheduledFor.Equal(mustTime(t, "2026-07-25T01:00:00Z")) {
		t.Fatalf("fired %s, want the next 01:00 boundary", next.Occurrence.ScheduledFor)
	}
}

// Skip-policy schedules must refuse a boundary older than their staleness
// horizon: a heartbeat reported a day late misstates the day it describes.
func TestSkipPolicyRefusesAStaleBoundary(t *testing.T) {
	schedule := heartbeatSchedule(t)
	decision, err := DueOccurrence(
		schedule, mustTime(t, "2026-08-01T09:00:00Z"), anchor(t, "2026-07-01T00:00:00Z"),
	)
	if err != nil {
		t.Fatalf("DueOccurrence() = %v", err)
	}
	if !decision.SkippedStale {
		t.Fatalf("skip policy caught up across a month-long gap: %+v", decision)
	}
	if decision.Occurrence == nil {
		t.Fatal("a resumed schedule must still record where it resumed")
	}
}

// Bounded catch-up has no staleness horizon: a nightly safety net must run
// however late, which is the whole point of a backstop.
func TestBoundedCatchUpRunsHoweverLate(t *testing.T) {
	schedule := Schedule{
		ID:               "late_probe",
		LegacyBeatEntry:  "run-membership-backfill-daily",
		Cadence:          DailyAt(3, 30),
		Timezone:         "UTC",
		CatchUp:          CatchUpBounded,
		UniquenessWindow: 25 * time.Hour,
		TargetKind:       jobcontract.KindHeartbeat,
		ProducerID:       ProducerHeartbeat,
		MaxAttempts:      1,
		AlertThreshold:   25 * time.Hour,
		Rationale:        "probe",
	}
	decision, err := DueOccurrence(
		schedule, mustTime(t, "2026-08-01T09:00:00Z"), anchor(t, "2026-07-01T03:30:00Z"),
	)
	if err != nil {
		t.Fatalf("DueOccurrence() = %v", err)
	}
	if decision.Occurrence == nil || decision.SkippedStale {
		t.Fatalf("bounded catch-up refused a late safety net: %+v", decision)
	}
}

func TestOccurrenceKeysAreDeterministicAndScheduleScoped(t *testing.T) {
	schedule := heartbeatSchedule(t)
	dueTime := mustTime(t, "2026-07-24T00:00:00Z")
	first := NewOccurrence(schedule, dueTime, mustTime(t, "2026-07-24T00:00:03Z"))
	second := NewOccurrence(schedule, dueTime, mustTime(t, "2026-07-24T00:04:59Z"))
	if first.Key != second.Key {
		t.Fatalf("observation instant leaked into the occurrence key: %s vs %s", first.Key, second.Key)
	}
	other := schedule
	other.ID = "prune_rate_limit_observations"
	if NewOccurrence(other, dueTime, dueTime).Key == first.Key {
		t.Fatal("two schedules share an occurrence key at the same due time")
	}
	if NewOccurrence(schedule, dueTime.Add(time.Second), dueTime).Key == first.Key {
		t.Fatal("two due times share an occurrence key")
	}
	if first.IdentityVersion != OccurrenceIdentityVersion {
		t.Fatalf("identity version = %s", first.IdentityVersion)
	}
}

func TestEngineRejectsAScheduleWithoutAConstructedProducer(t *testing.T) {
	schedules, err := Schedules()
	if err != nil {
		t.Fatalf("Schedules() = %v", err)
	}
	producers, err := NewProducerSet(NewHeartbeatProducer())
	if err != nil {
		t.Fatalf("NewProducerSet() = %v", err)
	}
	if _, err := newEngine(EngineConfig{
		Schedules: schedules,
		Producers: producers,
		Ledger:    newMemoryLedger(),
		Publisher: &recordingPublisher{},
		Registry:  testRegistry(t),
	}, stubBeginner{tx: &stubTx{}}); !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("newEngine() = %v, want ErrProducerUnavailable", err)
	}
	if missing := producers.Missing(schedules); len(missing) == 0 {
		t.Fatal("Missing() reported full coverage with an incomplete producer set")
	}
}

func TestNotImplementedProducerFailsLoudlyRatherThanSilently(t *testing.T) {
	ctx := context.Background()
	schedule := heartbeatSchedule(t)
	schedule.ProducerID = "unbuilt"
	producers, err := NewProducerSet(NewNotImplementedProducer("unbuilt", "owned by another lane"))
	if err != nil {
		t.Fatalf("NewProducerSet() = %v", err)
	}
	ledger := newMemoryLedger()
	seedAnchor(ledger, schedule, mustTime(t, "2026-07-23T00:00:00Z"))
	engine, err := newEngine(EngineConfig{
		Schedules: []Schedule{schedule},
		Producers: producers,
		Ledger:    ledger,
		Publisher: &recordingPublisher{},
		Registry:  testRegistry(t),
	}, stubBeginner{tx: &stubTx{}})
	if err != nil {
		t.Fatalf("newEngine() = %v", err)
	}
	result, err := engine.Step(ctx, mustTime(t, "2026-07-24T00:05:00Z"))
	if err != nil {
		t.Fatalf("Step() = %v", err)
	}
	if !result.Failed() || !errors.Is(result.Err(), ErrProducerNotImplemented) {
		t.Fatalf("unbuilt producer did not fail the window: %+v", result)
	}
}

func TestDeferredRouteIsChosenFromTheRegistryNotTheProducer(t *testing.T) {
	ctx := context.Background()
	schedule := heartbeatSchedule(t)
	publisher := &recordingPublisher{}
	ledger := newMemoryLedger()
	seedAnchor(ledger, schedule, mustTime(t, "2026-07-23T00:00:00Z"))
	engine := newTestEngine(t, schedule, ledger, publisher, &stubTx{})
	if _, err := engine.Step(ctx, mustTime(t, "2026-07-24T00:05:00Z")); err != nil {
		t.Fatalf("Step() = %v", err)
	}
	registry := testRegistry(t)
	descriptor, ok := registry.Descriptor(jobcontract.KindHeartbeat)
	if !ok {
		t.Fatal("system.heartbeat is not registered")
	}
	if publisher.count() != 1 {
		t.Fatalf("published %d handoffs", publisher.count())
	}
	if publisher.deferred[0] != !descriptor.Executable() {
		t.Fatalf(
			"publication deferred=%t but the checked route says executable=%t",
			publisher.deferred[0], descriptor.Executable(),
		)
	}
}
