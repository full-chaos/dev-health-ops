package fixed

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

func pushExpirySchedule(t *testing.T) Schedule {
	t.Helper()
	schedules, err := Schedules()
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range schedules {
		if candidate.ID == "external_push_expiry_sweep" {
			return candidate
		}
	}
	t.Fatal("external_push_expiry_sweep schedule is not in the inventory")
	return Schedule{}
}

func TestPushExpiryScheduleRow(t *testing.T) {
	schedule := pushExpirySchedule(t)
	if schedule.TargetKind != jobcontract.KindExternalPushExpirySweep || schedule.CatchUp != CatchUpSkip ||
		!schedule.Native || schedule.ProducerID != ProducerPushExpirySweep || schedule.MaxAttempts != 2 ||
		schedule.Cadence != EveryInterval(15*time.Minute) {
		t.Fatalf("schedule=%+v", schedule)
	}
}

func TestPushExpiryProducerEnvelope(t *testing.T) {
	schedule := pushExpirySchedule(t)
	due := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	produce := func(at time.Time) JobRequest {
		t.Helper()
		outcome, err := NewPushExpirySweepProducer().Produce(context.Background(), nil, schedule, NewOccurrence(schedule, at, at))
		if err != nil || len(outcome.Requests) != 1 {
			t.Fatalf("outcome=%+v err=%v", outcome, err)
		}
		return outcome.Requests[0]
	}
	first, replica, next := produce(due), produce(due), produce(due.Add(15*time.Minute))
	if first.Kind != jobcontract.KindExternalPushExpirySweep {
		t.Fatalf("kind=%s", first.Kind)
	}
	payload, ok := first.Envelope.Payload.(jobcontract.ExternalPushExpirySweepPayload)
	if !ok || payload.ScheduledFor != "2026-11-03T12:00:00Z" {
		t.Fatalf("payload=%#v", first.Envelope.Payload)
	}
	if first.Envelope.IdempotencyKey != "external-push-expiry-sweep:2026-11-03T12:00:00Z" ||
		first.Envelope.CorrelationID != "fixed-schedule:external_push_expiry_sweep:2026-11-03T12:00:00Z" {
		t.Fatalf("envelope=%+v", first.Envelope)
	}
	if first.Envelope.IdempotencyKey != replica.Envelope.IdempotencyKey || first.Envelope.IdempotencyKey == next.Envelope.IdempotencyKey {
		t.Fatal("idempotency key must repeat for one occurrence and differ for the next")
	}
	encoded, err := jobcontract.MarshalCanonical(first.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jobcontract.Decode(jobcontract.KindExternalPushExpirySweep, encoded); err != nil {
		t.Fatal(err)
	}
}

// A 15 minute interval emits every 15 to 30 minutes.
func TestPushExpiryEmitsEveryThirtyMinutesAtMost(t *testing.T) {
	schedule := pushExpirySchedule(t)
	now := time.Date(2026, 11, 3, 10, 7, 13, 0, time.UTC)
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
	if len(emitted) < 5 {
		t.Fatalf("emitted %d occurrences in 3h, want at least 5", len(emitted))
	}
	for index := 1; index < len(emitted); index++ {
		if gap := emitted[index].Sub(emitted[index-1]); gap > 30*time.Minute+time.Minute {
			t.Fatalf("gap %s between occurrences %d and %d exceeds thirty minutes", gap, index-1, index)
		}
	}
}
