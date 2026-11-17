package fixed

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/jackc/pgx/v5"
)

// freshRemainingStore is the recording store plus RemainingMetricsFreshCoverageStore (CHAOS-9587).
type freshRemainingStore struct {
	recordingRemainingStore
	state map[string]remaining.FreshCoverageState
	calls []freshCall
}

type freshCall struct {
	org, family   string
	scope         json.RawMessage
	cutoff, since time.Time
}

func (store *freshRemainingStore) FreshCoverage(
	_ context.Context, _ pgx.Tx, organizationID, family string, scope json.RawMessage, cutoff, inFlightSince time.Time,
) (remaining.FreshCoverageState, error) {
	store.calls = append(store.calls, freshCall{organizationID, family, scope, cutoff, inFlightSince})
	return store.state[organizationID], nil
}

func freshProducer(t *testing.T, state map[string]remaining.FreshCoverageState) (*RemainingMetricsFanoutProducer, *freshRemainingStore) {
	t.Helper()
	store := &freshRemainingStore{state: state}
	producer, err := NewRemainingMetricsFanoutProducer(store, nopPartitionPublisher{},
		fixedOrganizationLister{identifiers: []string{testOrgA, testOrgB}}, &recordingGraphWriter{})
	if err != nil {
		t.Fatal(err)
	}
	return producer.(*RemainingMetricsFanoutProducer), store
}

// One test per family: the cutoff each family hands the store, and the four cells of the ruling.
func TestFreshCoverageRuleCells(t *testing.T) {
	for _, tc := range []struct {
		schedule   string
		due        string
		wantCutoff string
	}{
		{"recommendations_daily_fanout", "2026-11-18T02:00:00Z", "2026-11-18T00:00:00Z"},
		{"capacity_forecast_weekly_fanout", "2026-11-23T01:00:00Z", "2026-11-23T01:00:00Z"},
	} {
		t.Run(tc.schedule, func(t *testing.T) {
			schedule := scheduleByID(t, tc.schedule)
			due := mustTime(t, tc.due)
			at := func(offset time.Duration) Occurrence { return NewOccurrence(schedule, due, due.Add(offset)) }

			t.Run("covered org is skipped, the other still runs", func(t *testing.T) {
				producer, store := freshProducer(t, map[string]remaining.FreshCoverageState{testOrgA: remaining.FreshCoverageCovered})
				outcome, err := producer.Produce(context.Background(), &stubTx{}, schedule, at(5*time.Second))
				if err != nil || outcome.Handoffs != 1 || len(store.requests) != 1 || store.requests[0].OrganizationID != testOrgB {
					t.Fatalf("outcome=%+v err=%v requests=%+v", outcome, err, store.requests)
				}
				if got := store.calls[0].cutoff; !got.Equal(mustTime(t, tc.wantCutoff)) {
					t.Fatalf("cutoff = %s, want %s", got, tc.wantCutoff)
				}
				if got, want := store.calls[0].since, due.Add(5*time.Second).Add(-remainingStageBudget); !got.Equal(want) {
					t.Fatalf("inFlightSince = %s, want %s", got, want)
				}
			})

			t.Run("nothing covers: runs as today", func(t *testing.T) {
				producer, store := freshProducer(t, nil)
				outcome, err := producer.Produce(context.Background(), &stubTx{}, schedule, at(5*time.Second))
				if err != nil || outcome.Handoffs != 2 || len(store.requests) != 2 {
					t.Fatalf("outcome=%+v err=%v requests=%d", outcome, err, len(store.requests))
				}
			})

			t.Run("in flight and younger than the bound: defer, nothing started", func(t *testing.T) {
				producer, store := freshProducer(t, map[string]remaining.FreshCoverageState{testOrgB: remaining.FreshCoverageInFlight})
				_, err := producer.Produce(context.Background(), &stubTx{}, schedule, at(5*time.Minute))
				if !errors.Is(err, ErrOccurrenceDeferred) {
					t.Fatalf("err = %v, want ErrOccurrenceDeferred", err)
				}
				_ = store
			})

			t.Run("in flight past the bound: runs anyway", func(t *testing.T) {
				producer, store := freshProducer(t, map[string]remaining.FreshCoverageState{testOrgB: remaining.FreshCoverageInFlight})
				outcome, err := producer.Produce(context.Background(), &stubTx{}, schedule, at(remainingStageBudget+time.Second))
				if err != nil || outcome.Handoffs != 2 || len(store.requests) != 2 {
					t.Fatalf("outcome=%+v err=%v requests=%d", outcome, err, len(store.requests))
				}
			})
		})
	}
}

// The other bindings keep the day-label check: a store with FreshCoverage is never asked for them.
func TestFreshCoverageIsOnlyForRecommendationsAndCapacity(t *testing.T) {
	for _, id := range []string{"dora_daily_fanout", "release_impact_daily_fanout"} {
		schedule := scheduleByID(t, id)
		producer, store := freshProducer(t, nil)
		due := mustTime(t, "2026-11-18T02:00:00Z")
		if _, err := producer.Produce(context.Background(), &stubTx{}, schedule, NewOccurrence(schedule, due, due)); err != nil {
			t.Fatal(err)
		}
		if len(store.calls) != 0 {
			t.Fatalf("%s: FreshCoverage called %d times", id, len(store.calls))
		}
	}
}

// remainingStageBudget is the kind timeout in the job registry, not a number of its own.
func TestRemainingStageBudgetIsTheKindTimeout(t *testing.T) {
	registry := testRegistry(t)
	for _, kind := range []string{jobcontract.KindRemainingRecommendations, jobcontract.KindRemainingCapacity} {
		descriptor, ok := registry.Descriptor(kind)
		if !ok {
			t.Fatalf("%s is not in the job registry", kind)
		}
		if descriptor.Timeout != remainingStageBudget {
			t.Fatalf("%s timeout = %s, remainingStageBudget = %s", kind, descriptor.Timeout, remainingStageBudget)
		}
	}
}

type deferringProducer struct {
	Producer
	deferring *bool
}

func (producer deferringProducer) Produce(ctx context.Context, tx pgx.Tx, schedule Schedule, occurrence Occurrence) (Outcome, error) {
	if *producer.deferring {
		return Outcome{}, ErrOccurrenceDeferred
	}
	return producer.Producer.Produce(ctx, tx, schedule, occurrence)
}

// A deferred occurrence is a state of the schedule, not a failed window: it rolls back, and the same due time is produced by a later window once nothing is in flight.
func TestDeferredOccurrenceIsNotAWindowFailureAndRunsLater(t *testing.T) {
	ctx := context.Background()
	schedule := heartbeatSchedule(t)
	ledger := newMemoryLedger()
	seedAnchor(ledger, schedule, mustTime(t, "2026-07-23T00:00:00Z"))
	observedAt := mustTime(t, "2026-07-24T00:05:00Z")
	occurrence := NewOccurrence(schedule, mustTime(t, "2026-07-24T00:00:00Z"), observedAt)

	deferring := true
	tx := &stubTx{}
	tx.onRollback = func() {
		ledger.rollback(occurrence.Key)
		seedAnchor(ledger, schedule, mustTime(t, "2026-07-23T00:00:00Z"))
	}
	build := func(tx *stubTx) *Engine {
		engine := newTestEngine(t, schedule, ledger, &recordingPublisher{}, tx)
		inner, _ := engine.producers.Producer(schedule.ProducerID)
		wrapped, err := NewProducerSet(deferringProducer{Producer: inner, deferring: &deferring})
		if err != nil {
			t.Fatal(err)
		}
		engine.producers = wrapped
		return engine
	}

	first, err := build(tx).Step(ctx, observedAt)
	if err != nil {
		t.Fatalf("Step() = %v", err)
	}
	if first.Failed() {
		t.Fatalf("a deferred occurrence failed the window: %v", first.Err())
	}
	if got := first.Schedules[0]; got.Evaluated || got.Claimed != 0 || got.Err != nil {
		t.Fatalf("deferred schedule result = %+v", got)
	}
	if ledger.count() != 0 {
		t.Fatalf("ledger kept %d occurrences of a deferred window", ledger.count())
	}

	deferring = false
	later, err := build(&stubTx{}).Step(ctx, observedAt.Add(time.Minute))
	if err != nil || later.Failed() || later.Schedules[0].Claimed != 1 {
		t.Fatalf("later window = %+v / %v", later.Schedules, err)
	}
	if ledger.status(occurrence.Key) != OccurrenceMaterialized {
		t.Fatalf("status = %q", ledger.status(occurrence.Key))
	}
}
