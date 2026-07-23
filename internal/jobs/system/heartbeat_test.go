package system

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

func TestHeartbeatHandlerDispatchesExactScheduleOccurrence(t *testing.T) {
	t.Parallel()
	dispatcher := &heartbeatDispatcher{}
	handler, err := NewHeartbeatHandler(dispatcher)
	if err != nil {
		t.Fatal(err)
	}
	execution := &jobruntime.Execution[jobruntime.HeartbeatArgs]{
		Args: jobruntime.HeartbeatArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.HeartbeatPayload]{
			Payload: jobcontract.HeartbeatPayload{ScheduledFor: "2026-07-21T12:00:00Z"},
		}},
	}
	if err := handler.Work(context.Background(), execution); err != nil {
		t.Fatalf("Work: %v", err)
	}
	want := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	if !dispatcher.scheduledFor.Equal(want) || dispatcher.calls != 1 {
		t.Fatalf("dispatch = (%s, %d)", dispatcher.scheduledFor, dispatcher.calls)
	}
}

func TestHeartbeatHandlerRejectsInvalidOccurrenceWithoutDispatch(t *testing.T) {
	t.Parallel()
	dispatcher := &heartbeatDispatcher{}
	handler, err := NewHeartbeatHandler(dispatcher)
	if err != nil {
		t.Fatal(err)
	}
	execution := &jobruntime.Execution[jobruntime.HeartbeatArgs]{
		Args: jobruntime.HeartbeatArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.HeartbeatPayload]{
			Payload: jobcontract.HeartbeatPayload{ScheduledFor: "not-a-time"},
		}},
	}
	if err := handler.Work(context.Background(), execution); err == nil || dispatcher.calls != 0 {
		t.Fatalf("error = %v, calls = %d", err, dispatcher.calls)
	}
}

func TestHeartbeatHandlerDoesNotRetryPermanentBridgeRejection(t *testing.T) {
	t.Parallel()
	dispatcher := &heartbeatDispatcher{err: errors.New("telemetry rejected")}
	handler, err := NewHeartbeatHandler(dispatcher)
	if err != nil {
		t.Fatal(err)
	}
	execution := &jobruntime.Execution[jobruntime.HeartbeatArgs]{
		Args: jobruntime.HeartbeatArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.HeartbeatPayload]{
			Payload: jobcontract.HeartbeatPayload{ScheduledFor: "2026-07-21T12:00:00Z"},
		}},
	}
	if err := handler.Work(context.Background(), execution); err == nil ||
		err.Error() != "job error category: permanent" {
		t.Fatalf("error = %v", err)
	}
}

type heartbeatDispatcher struct {
	scheduledFor time.Time
	calls        int
	err          error
}

func (dispatcher *heartbeatDispatcher) DispatchHeartbeat(_ context.Context, scheduledFor time.Time) error {
	dispatcher.scheduledFor = scheduledFor
	dispatcher.calls++
	return dispatcher.err
}
