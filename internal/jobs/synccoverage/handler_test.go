package synccoverage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"

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
	handler, err := NewHandler(projector, slog.New(slog.DiscardHandler))
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
			handler, err := NewHandler(projector, slog.New(slog.DiscardHandler))
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
	if _, err := NewHandler(nil, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("NewHandler(nil, slog.New(slog.DiscardHandler)) error = nil")
	}
	handler, _ := NewHandler(&recordingProjector{}, slog.New(slog.DiscardHandler))
	execution := coverageExecution(0)
	execution.Args.Payload.ScheduledFor = "not-a-time"
	err := handler.Work(context.Background(), execution)
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf("Work() = %v, want permanent", err)
	}
}

func logRecords(t *testing.T, projector *recordingProjector) (string, error) {
	t.Helper()
	var logs bytes.Buffer
	handler, err := NewHandler(projector, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(context.Background(), coverageExecution(100))
	return logs.String(), err
}

func TestHandlerNamesEachFailingConfigAndItsCauseClass(t *testing.T) {
	t.Parallel()
	first, second := uuid.New(), uuid.New()
	flagsErr := fmt.Errorf("%w: processor_flags of work-items is ['sentinel-stored-value'], not a dict", coverage.ErrInvalidProcessorFlags)
	projector := &recordingProjector{result: coverage.RefreshResult{
		Refreshed: 1, Failed: 2,
		Failures: []coverage.RefreshFailure{
			{OrgID: "org-a", ConfigID: first, Err: flagsErr},
			{OrgID: "org-b", ConfigID: second, Err: fmt.Errorf("lock: %w", coverage.ErrConfigNotFound)},
		},
	}}
	logs, err := logRecords(t, projector)
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("Work() = %v, want retryable", err)
	}
	for _, want := range []string{
		"org_id=org-a", "config_id=" + first.String(), "cause_class=invalid_processor_flags",
		"org_id=org-b", "config_id=" + second.String(), "cause_class=config_not_found",
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("log lacks %q:\n%s", want, logs)
		}
	}
	if strings.Contains(logs, "sentinel-stored-value") {
		t.Fatalf("log carries a stored value:\n%s", logs)
	}
	if got := strings.Count(logs, "sync_coverage_refresh.config_failed"); got != 2 {
		t.Fatalf("config_failed lines = %d, want 2", got)
	}
	cause, ok := jobruntime.SafeCause(err)
	if !ok || cause != "sync coverage refresh: failed=2 refreshed=1" {
		t.Fatalf("SafeCause = %q, %v", cause, ok)
	}
}

func TestHandlerBoundsTheFailureLogLines(t *testing.T) {
	t.Parallel()
	failures := make([]coverage.RefreshFailure, 0, 60)
	for range 60 {
		failures = append(failures, coverage.RefreshFailure{OrgID: "org", ConfigID: uuid.New(), Err: errors.New("x")})
	}
	logs, err := logRecords(t, &recordingProjector{result: coverage.RefreshResult{Failed: 60, Failures: failures}})
	if err == nil {
		t.Fatal("Work() = nil")
	}
	if got := strings.Count(logs, "sync_coverage_refresh.config_failed"); got != maxLoggedFailures {
		t.Fatalf("config_failed lines = %d, want %d", got, maxLoggedFailures)
	}
	if !strings.Contains(logs, "omitted=10") {
		t.Fatalf("log lacks omitted=10:\n%s", logs)
	}
}
