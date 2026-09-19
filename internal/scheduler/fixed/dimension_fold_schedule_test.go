package fixed

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

func dimensionFoldSchedule(t *testing.T) Schedule {
	t.Helper()
	schedules, err := Schedules()
	if err != nil {
		t.Fatal(err)
	}
	for _, schedule := range schedules {
		if schedule.ID == dimensionFoldScheduleID {
			return schedule
		}
	}
	t.Fatal("dimension_fold schedule is not declared")
	return Schedule{}
}

// TestDimensionFoldIntervalInputDomain pins the cadence override: only a
// whole number of seconds inside the accepted range changes the schedule,
// absent or blank keeps the default, and anything else refuses the table.
func TestDimensionFoldIntervalInputDomain(t *testing.T) {
	for _, tc := range []struct {
		raw   string
		set   bool
		want  time.Duration
		valid bool
	}{
		{"", false, time.Minute, true},
		{"", true, time.Minute, true},
		{"   ", true, time.Minute, true},
		{"60", true, time.Minute, true},
		{"10", true, 10 * time.Second, true},
		{"3600", true, time.Hour, true},
		{"9", true, 0, false},
		{"3601", true, 0, false},
		{"0", true, 0, false},
		{"-60", true, 0, false},
		{"60.5", true, 0, false},
		{"1m", true, 0, false},
		{"sixty", true, 0, false},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			if tc.set {
				t.Setenv(DimensionFoldIntervalEnv, tc.raw)
			}
			schedules, err := Schedules()
			if !tc.valid {
				if !errors.Is(err, ErrInvalidSchedule) {
					t.Fatalf("err=%v, want ErrInvalidSchedule", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, schedule := range schedules {
				if schedule.ID != dimensionFoldScheduleID {
					continue
				}
				if schedule.Cadence.Period() != tc.want {
					t.Fatalf("period=%s want %s", schedule.Cadence.Period(), tc.want)
				}
				if err := schedule.Validate(); err != nil {
					t.Fatalf("overridden schedule does not validate: %v", err)
				}
				return
			}
			t.Fatal("dimension_fold schedule missing")
		})
	}
}

// TestDimensionFoldProducerEnvelope pins the job the scheduler emits: the
// registered Go-only kind, a payload carrying only the due time, and an
// idempotency key that is the same for two replicas producing one occurrence
// and different for the next occurrence.
func TestDimensionFoldProducerEnvelope(t *testing.T) {
	schedule := dimensionFoldSchedule(t)
	if schedule.TargetKind != jobcontract.KindDimensionFold || schedule.CatchUp != CatchUpSkip ||
		!schedule.Native || schedule.ProducerID != ProducerDimensionFold {
		t.Fatalf("schedule=%+v", schedule)
	}
	due := time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC)
	produce := func(at time.Time) JobRequest {
		t.Helper()
		outcome, err := NewDimensionFoldProducer().Produce(
			context.Background(), nil, schedule, NewOccurrence(schedule, at, at),
		)
		if err != nil || len(outcome.Requests) != 1 {
			t.Fatalf("outcome=%+v err=%v", outcome, err)
		}
		return outcome.Requests[0]
	}
	first, replica, next := produce(due), produce(due), produce(due.Add(time.Minute))
	if first.Kind != jobcontract.KindDimensionFold {
		t.Fatalf("kind=%s", first.Kind)
	}
	payload, ok := first.Envelope.Payload.(jobcontract.DimensionFoldPayload)
	if !ok || payload.ScheduledFor != "2026-01-02T03:04:00Z" {
		t.Fatalf("payload=%#v", first.Envelope.Payload)
	}
	if first.Envelope.IdempotencyKey != replica.Envelope.IdempotencyKey ||
		first.Envelope.Domain != replica.Envelope.Domain {
		t.Fatal("two replicas produced different identities for one occurrence")
	}
	if first.Envelope.IdempotencyKey == next.Envelope.IdempotencyKey {
		t.Fatal("the next occurrence reused the idempotency key")
	}
	encoded, err := jobcontract.MarshalCanonical(first.Envelope)
	if err != nil {
		t.Fatalf("envelope does not encode against the contract: %v", err)
	}
	if _, err := jobcontract.Decode(jobcontract.KindDimensionFold, encoded); err != nil {
		t.Fatalf("encoded envelope does not decode: %v", err)
	}
}
