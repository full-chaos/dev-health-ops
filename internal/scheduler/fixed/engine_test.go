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
	byTime   map[string]time.Time
	statuses map[string]string
	failNext error
}

func newMemoryLedger() *memoryLedger {
	return &memoryLedger{
		byKey:    map[string]Occurrence{},
		byTime:   map[string]time.Time{},
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
	if occurrence.ScheduledFor.After(last) {
		ledger.byTime[occurrence.ScheduleID] = occurrence.ScheduledFor
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

func (ledger *memoryLedger) LastScheduledFor(
	_ context.Context, _ pgx.Tx, scheduleID string,
) (time.Time, bool, error) {
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
	if result.Schedules[0].Duplicate != 1 {
		t.Fatalf("restart claimed a new occurrence: %+v", result.Schedules[0])
	}
}

func TestCrashBeforeCommitLeavesTheOccurrenceEligible(t *testing.T) {
	ctx := context.Background()
	schedule := heartbeatSchedule(t)
	ledger := newMemoryLedger()
	publisher := &recordingPublisher{}
	observedAt := mustTime(t, "2026-07-24T00:05:00Z")

	// A failing commit models a crash after the producer wrote its rows but
	// before the transaction became durable. The rollback must discard the
	// claim so the next window can retry the same due time.
	crashing := &stubTx{failCommit: true}
	occurrence := NewOccurrence(schedule, mustTime(t, "2026-07-24T00:00:00Z"), observedAt)
	crashing.onRollback = func() { ledger.rollback(occurrence.Key) }
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

	// The scheduler was down for three days and starts back up mid-morning.
	// Skip policy emits only the newest due time, never three days of
	// backdated telemetry.
	result, err := engine.Step(ctx, mustTime(t, "2026-07-24T09:00:00Z"))
	if err != nil || result.Failed() {
		t.Fatalf("Step() = %v / %v", err, result.Err())
	}
	if result.Schedules[0].Due != 1 || publisher.count() != 1 {
		t.Fatalf("skip policy produced due=%d published=%d, want 1/1",
			result.Schedules[0].Due, publisher.count())
	}
	envelope := publisher.requests[0].Envelope
	payload, ok := envelope.Payload.(jobcontract.HeartbeatPayload)
	if !ok {
		t.Fatalf("unexpected payload %T", envelope.Payload)
	}
	if payload.ScheduledFor != "2026-07-24T00:00:00Z" {
		t.Fatalf("skip policy emitted %s, want the newest due time", payload.ScheduledFor)
	}
}

func TestBoundedCatchUpReplaysMissedNightlyOccurrences(t *testing.T) {
	schedule := Schedule{
		ID:               "catch_up_probe",
		LegacyBeatEntry:  "run-daily-metrics",
		Cadence:          DailyAt(1, 0),
		Timezone:         "UTC",
		CatchUp:          CatchUpBounded,
		UniquenessWindow: 72 * time.Hour,
		TargetKind:       jobcontract.KindHeartbeat,
		ProducerID:       ProducerHeartbeat,
		MaxAttempts:      3,
		AlertThreshold:   72 * time.Hour,
		Rationale:        "probe",
	}
	occurrences, err := DueOccurrences(schedule, mustTime(t, "2026-07-24T09:00:00Z"))
	if err != nil {
		t.Fatalf("DueOccurrences() = %v", err)
	}
	if len(occurrences) != 3 {
		t.Fatalf("bounded catch-up returned %d occurrences, want the 3 inside the window", len(occurrences))
	}
	// Ascending order matters: a catch-up that ran newest-first would leave the
	// oldest safety net racing the next tick.
	for index := 1; index < len(occurrences); index++ {
		if !occurrences[index].ScheduledFor.After(occurrences[index-1].ScheduledFor) {
			t.Fatalf("catch-up occurrences are not ascending: %v", occurrences)
		}
	}
}

func TestOccurrencesOlderThanTheUniquenessWindowAreNotEmitted(t *testing.T) {
	schedule := heartbeatSchedule(t)
	// Four days after the last observed due time the newest occurrence is
	// still within one day, so this probes the boundary directly instead.
	occurrences, err := DueOccurrences(Schedule{
		ID:               "narrow_window_probe",
		LegacyBeatEntry:  schedule.LegacyBeatEntry,
		Cadence:          DailyAt(0, 0),
		Timezone:         "UTC",
		CatchUp:          CatchUpSkip,
		UniquenessWindow: 25 * time.Hour,
		TargetKind:       schedule.TargetKind,
		ProducerID:       schedule.ProducerID,
		MaxAttempts:      schedule.MaxAttempts,
		AlertThreshold:   25 * time.Hour,
		Rationale:        "probe",
	}, mustTime(t, "2026-07-25T02:00:00Z"))
	if err != nil {
		t.Fatalf("DueOccurrences() = %v", err)
	}
	if len(occurrences) != 1 {
		t.Fatalf("expected the in-window occurrence only, got %d", len(occurrences))
	}
	if !occurrences[0].ScheduledFor.Equal(mustTime(t, "2026-07-25T00:00:00Z")) {
		t.Fatalf("emitted %s, want the newest in-window due time", occurrences[0].ScheduledFor)
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
	engine, err := newEngine(EngineConfig{
		Schedules: []Schedule{schedule},
		Producers: producers,
		Ledger:    newMemoryLedger(),
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
	engine := newTestEngine(t, schedule, newMemoryLedger(), publisher, &stubTx{})
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
