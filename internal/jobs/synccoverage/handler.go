// Package synccoverage binds the native projection builder to the generic
// River job runtime.
package synccoverage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	coverage "github.com/full-chaos/dev-health-ops/internal/synccoverage"
)

// Projector is the bounded native projection sweep.
type Projector interface {
	RefreshDue(context.Context, int) (coverage.RefreshResult, error)
}

// Handler executes one scheduled bounded refresh. Individual configuration
// failures are collected by the projector so one tenant cannot prevent the
// other selected configurations from rebuilding; the aggregate attempt still
// fails retryably so a partial sweep is never reported as success.
type Handler struct {
	projector Projector
	logger    *slog.Logger
}

// maxLoggedFailures bounds the per-sweep WARN lines; the limit cap is 1000.
const maxLoggedFailures = 50

// NewHandler constructs the executable sync-coverage job owner.
func NewHandler(projector Projector, logger *slog.Logger) (*Handler, error) {
	if projector == nil || logger == nil {
		return nil, errors.New("sync coverage projector and logger are required")
	}
	return &Handler{projector: projector, logger: logger}, nil
}

// Work rebuilds the selected cold, invalidated, and oldest projections.
func (handler *Handler) Work(
	ctx context.Context,
	execution *jobruntime.Execution[jobruntime.SyncCoverageRefreshArgs],
) error {
	if handler == nil || handler.projector == nil || handler.logger == nil || execution == nil {
		return jobruntime.Permanent(errors.New("sync coverage handler is not configured"))
	}
	payload := execution.Args.Payload
	scheduledFor, err := time.Parse(time.RFC3339, payload.ScheduledFor)
	if err != nil || scheduledFor.Location() != time.UTC || payload.Limit < 1 || payload.Limit > 1000 {
		return jobruntime.Permanent(errors.New("sync coverage refresh request is invalid"))
	}
	result, err := handler.projector.RefreshDue(ctx, payload.Limit)
	if err != nil {
		return jobruntime.Retryable(err)
	}
	if result.Failed > 0 || len(result.Failures) > 0 {
		for index, failure := range result.Failures {
			if index == maxLoggedFailures {
				break
			}
			handler.logger.WarnContext(ctx, "sync_coverage_refresh.config_failed",
				slog.String("org_id", failure.OrgID),
				slog.String("config_id", failure.ConfigID.String()),
				slog.String("cause_class", coverage.FailureClass(failure.Err)))
		}
		if omitted := len(result.Failures) - maxLoggedFailures; omitted > 0 {
			handler.logger.WarnContext(ctx, "sync_coverage_refresh.config_failures_omitted",
				slog.Int("omitted", omitted))
		}
		return jobruntime.Retryable(jobruntime.WithSafeCauseText(
			errors.New("one or more sync coverage projections failed"),
			fmt.Sprintf("sync coverage refresh: failed=%d refreshed=%d", max(result.Failed, len(result.Failures)), result.Refreshed)))
	}
	return nil
}
