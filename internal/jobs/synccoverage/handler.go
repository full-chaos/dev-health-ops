// Package synccoverage binds the native projection builder to the generic
// River job runtime.
package synccoverage

import (
	"context"
	"errors"
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
type Handler struct{ projector Projector }

// NewHandler constructs the executable sync-coverage job owner.
func NewHandler(projector Projector) (*Handler, error) {
	if projector == nil {
		return nil, errors.New("sync coverage projector is required")
	}
	return &Handler{projector: projector}, nil
}

// Work rebuilds the selected cold, invalidated, and oldest projections.
func (handler *Handler) Work(
	ctx context.Context,
	execution *jobruntime.Execution[jobruntime.SyncCoverageRefreshArgs],
) error {
	if handler == nil || handler.projector == nil || execution == nil {
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
		return jobruntime.Retryable(errors.New("one or more sync coverage projections failed"))
	}
	return nil
}
