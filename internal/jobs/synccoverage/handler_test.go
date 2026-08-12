package synccoverage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	coverage "github.com/full-chaos/dev-health-ops/internal/synccoverage"
)

type recordingProjector struct {
	limit  int
	result coverage.RefreshResult
	err    error
}

func (projector *recordingProjector) RefreshDue(
	_ context.Context,
	limit int,
) (coverage.RefreshResult, error) {
	projector.limit = limit
	return projector.result, projector.err
}

func coverageExecution(limit int) *jobruntime.Execution[jobruntime.SyncCoverageRefreshArgs] {
	return &jobruntime.Execution[jobruntime.SyncCoverageRefreshArgs]{
		Args: jobruntime.SyncCoverageRefreshArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.SyncCoverageRefreshPayload]{
			Payload: jobcontract.SyncCoverageRefreshPayload{
				ScheduledFor: "2026-08-12T12:00:00Z",
				Limit:        limit,
			},
		}},
	}
}

func TestHandlerExecutesTheBoundedNativeSweep(t *testing.T) {
	t.Parallel()
	projector := &recordingProjector{result: coverage.RefreshResult{Refreshed: 3}}
	handler, err := NewHandler(projector)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), coverageExecution(100)); err != nil {
		t.Fatalf("Work() = %v", err)
	}
	if projector.limit != 100 {
		t.Fatalf("RefreshDue limit = %d, want 100", projector.limit)
	}
}

func TestHandlerDoesNotReportAPartialSweepAsSuccess(t *testing.T) {
	t.Parallel()
	for name, projector := range map[string]*recordingProjector{
		"query failure":                {err: errors.New("postgres unavailable")},
		"failed count":                 {result: coverage.RefreshResult{Refreshed: 2, Failed: 1}},
		"failure detail without count": {result: coverage.RefreshResult{Refreshed: 2, Failures: []coverage.RefreshFailure{{Err: errors.New("config failed")}}}},
	} {
		t.Run(name, func(t *testing.T) {
			handler, err := NewHandler(projector)
			if err != nil {
				t.Fatal(err)
			}
			err = handler.Work(context.Background(), coverageExecution(100))
			if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
				t.Fatalf("Work() = %v, want retryable", err)
			}
		})
	}
}

func TestHandlerRejectsAnInvalidOrUnconfiguredRequest(t *testing.T) {
	t.Parallel()
	if _, err := NewHandler(nil); err == nil {
		t.Fatal("NewHandler(nil) error = nil")
	}
	handler, _ := NewHandler(&recordingProjector{})
	execution := coverageExecution(0)
	execution.Args.Payload.ScheduledFor = "not-a-time"
	err := handler.Work(context.Background(), execution)
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf("Work() = %v, want permanent", err)
	}
}
