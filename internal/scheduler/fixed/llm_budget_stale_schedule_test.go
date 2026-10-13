package fixed

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

func TestLLMBudgetStaleProducerEnvelope(t *testing.T) {
	schedules, err := Schedules()
	if err != nil {
		t.Fatal(err)
	}
	var schedule Schedule
	for _, candidate := range schedules {
		if candidate.ID == "llm_budget_stale_reservations" {
			schedule = candidate
		}
	}
	if schedule.TargetKind != jobcontract.KindLLMBudgetStaleReservations || schedule.CatchUp != CatchUpSkip ||
		!schedule.Native || schedule.ProducerID != ProducerLLMBudgetStale {
		t.Fatalf("schedule=%+v", schedule)
	}
	due := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	produce := func(at time.Time) JobRequest {
		t.Helper()
		outcome, err := NewLLMBudgetStaleProducer().Produce(context.Background(), nil, schedule, NewOccurrence(schedule, at, at))
		if err != nil || len(outcome.Requests) != 1 {
			t.Fatalf("outcome=%+v err=%v", outcome, err)
		}
		return outcome.Requests[0]
	}
	first, replica, next := produce(due), produce(due), produce(due.Add(time.Hour))
	payload, ok := first.Envelope.Payload.(jobcontract.LLMBudgetStaleReservationsPayload)
	if !ok || payload.ScheduledFor != "2026-10-13T12:00:00Z" {
		t.Fatalf("payload=%#v", first.Envelope.Payload)
	}
	if first.Envelope.IdempotencyKey != replica.Envelope.IdempotencyKey || first.Envelope.IdempotencyKey == next.Envelope.IdempotencyKey {
		t.Fatal("idempotency key must repeat for one occurrence and differ for the next")
	}
	encoded, err := jobcontract.MarshalCanonical(first.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jobcontract.Decode(jobcontract.KindLLMBudgetStaleReservations, encoded); err != nil {
		t.Fatal(err)
	}
}
