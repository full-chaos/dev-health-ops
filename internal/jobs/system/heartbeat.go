package system

import (
	"context"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/operational"
)

// HeartbeatDispatcher performs the bounded compatibility side effect while
// the existing Python telemetry implementation remains authoritative.
type HeartbeatDispatcher interface {
	DispatchHeartbeat(context.Context, time.Time) error
}

type HeartbeatHandler struct {
	dispatcher HeartbeatDispatcher
}

func NewHeartbeatHandler(dispatcher HeartbeatDispatcher) (*HeartbeatHandler, error) {
	if dispatcher == nil {
		return nil, errors.New("heartbeat dispatcher is required")
	}
	return &HeartbeatHandler{dispatcher: dispatcher}, nil
}

func (handler *HeartbeatHandler) Work(
	ctx context.Context,
	execution *jobruntime.Execution[jobruntime.HeartbeatArgs],
) error {
	if handler == nil || handler.dispatcher == nil || execution == nil {
		return jobruntime.Permanent(errors.New("heartbeat handler is not configured"))
	}
	scheduledFor, err := time.Parse(time.RFC3339, execution.Args.Payload.ScheduledFor)
	if err != nil || scheduledFor.Location() != time.UTC {
		return jobruntime.Permanent(errors.New("heartbeat schedule occurrence is invalid"))
	}
	if err := handler.dispatcher.DispatchHeartbeat(ctx, scheduledFor); err != nil {
		if errors.Is(err, operational.ErrDispatchPermanent) {
			return jobruntime.Permanent(err)
		}
		return jobruntime.Retryable(err)
	}
	return nil
}
