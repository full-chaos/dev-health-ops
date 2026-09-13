package system

import (
	"context"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/operational"
)

// HeartbeatDispatcher performs the heartbeat's phone-home side effect. This
// handler owns the occurrence contract, the River attempt, retry
// classification, and cancellation; the effect itself -- organization and
// user counts, the single OrgLicense tier, a truncated licence-key digest,
// an audit_logs row, and a POST to TELEMETRY_ENDPOINT -- is native Go
// (NativeHeartbeatDispatcher, heartbeat_native.go). No HTTP call into the
// Python API remains on this path.
//
// The payload's version/uptime_seconds/request_metadata.source fields carry
// this worker's OWN identity now, not a value translated from the Python
// interpreter that used to compute them (see HeartbeatSnapshot's doc
// comment) -- a telemetry receiver keyed on the old Python-shaped values for
// those three fields would need to be told about this change; every other
// field keeps its name and meaning unchanged.
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
