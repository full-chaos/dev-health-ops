package fixed

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

func providerRevocationsSchedule(t *testing.T) Schedule {
	t.Helper()
	schedules, err := Schedules()
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range schedules {
		if candidate.ID == "provider_oauth_revocations" {
			return candidate
		}
	}
	t.Fatal("provider_oauth_revocations schedule is not in the inventory")
	return Schedule{}
}

func TestProviderRevocationsProducerEnvelope(t *testing.T) {
	schedule := providerRevocationsSchedule(t)
	if schedule.TargetKind != jobcontract.KindProviderOAuthRevocations || schedule.CatchUp != CatchUpSkip ||
		!schedule.Native || schedule.ProducerID != ProducerProviderRevocations {
		t.Fatalf("schedule=%+v", schedule)
	}
	due := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	produce := func(at time.Time) JobRequest {
		t.Helper()
		outcome, err := NewProviderRevocationsProducer().Produce(context.Background(), nil, schedule, NewOccurrence(schedule, at, at))
		if err != nil || len(outcome.Requests) != 1 {
			t.Fatalf("outcome=%+v err=%v", outcome, err)
		}
		return outcome.Requests[0]
	}
	first, replica, next := produce(due), produce(due), produce(due.Add(5*time.Minute))
	payload, ok := first.Envelope.Payload.(jobcontract.ProviderOAuthRevocationsPayload)
	if !ok || payload.ScheduledFor != "2026-10-30T12:00:00Z" {
		t.Fatalf("payload=%#v", first.Envelope.Payload)
	}
	if first.Envelope.IdempotencyKey != replica.Envelope.IdempotencyKey || first.Envelope.IdempotencyKey == next.Envelope.IdempotencyKey {
		t.Fatal("idempotency key must repeat for one occurrence and differ for the next")
	}
	encoded, err := jobcontract.MarshalCanonical(first.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jobcontract.Decode(jobcontract.KindProviderOAuthRevocations, encoded); err != nil {
		t.Fatal(err)
	}
}

// A 5 minute interval emits every 5 to 10 minutes: a requeued row waits at most
// one backoff step plus 10 minutes for its first send.
func TestProviderRevocationsEmitsEveryTenMinutesAtMost(t *testing.T) {
	schedule := providerRevocationsSchedule(t)
	now := time.Date(2026, 10, 30, 10, 7, 13, 0, time.UTC)
	var anchor *Anchor
	var emitted []time.Time
	for end := now.Add(3 * time.Hour); now.Before(end); now = now.Add(7 * time.Second) {
		decision, err := DueOccurrence(schedule, now, anchor)
		if err != nil {
			t.Fatal(err)
		}
		if decision.Occurrence == nil {
			continue
		}
		anchor = &Anchor{ScheduledFor: decision.Occurrence.ScheduledFor, ObservedAt: now}
		if !decision.ColdStart && !decision.SkippedStale {
			emitted = append(emitted, now)
		}
	}
	if len(emitted) < 17 {
		t.Fatalf("emitted %d occurrences in 3h, want at least 17", len(emitted))
	}
	for index := 1; index < len(emitted); index++ {
		if gap := emitted[index].Sub(emitted[index-1]); gap > 10*time.Minute+time.Minute {
			t.Fatalf("gap %s between occurrences %d and %d exceeds ten minutes", gap, index-1, index)
		}
	}
}
