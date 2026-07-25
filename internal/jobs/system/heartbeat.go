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
//
// CLASSIFICATION: python_compatibility, not native Go. This handler owns the
// occurrence contract, the River attempt, retry classification, and
// cancellation, but the effect itself is an HTTP call to
// /api/internal/worker-operational/heartbeat, which runs the Celery
// phone_home_heartbeat body.
//
// The compute is small (organization and user counts, the single OrgLicense
// tier, a truncated licence-key digest, an audit_logs row, and a POST to
// TELEMETRY_ENDPOINT) but three payload fields carry Python identity that a
// telemetry receiver may key on and that a Go process cannot reproduce:
// "version" is the dev_health_ops package version, "uptime_seconds" is that
// interpreter's time.monotonic() origin, and request_metadata.source is
// hardcoded "celery". Porting the body without first agreeing the new values
// for those three fields would silently change what the receiver records.
//
// DELETION: CUT-20 removes this relay. The native replacement needs (1) an
// agreed payload identity for version/uptime/source, (2) config for
// TELEMETRY_ENDPOINT and INSTANCE_ID on the Go worker, and (3) a Go writer for
// audit_logs. Until then this pair must never be reported as native_go.
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
