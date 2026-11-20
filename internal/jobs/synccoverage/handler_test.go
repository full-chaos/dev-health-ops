package synccoverage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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

func TestHandlerRetriesUnlessEveryFailureIsDeterministic(t *testing.T) {
	t.Parallel()
	limit := &coverage.LimitError{Bound: "backfill_pair", Count: 20001, Max: 20000}
	fail := func(errs ...error) coverage.RefreshResult {
		result := coverage.RefreshResult{Refreshed: 2, Failed: len(errs)}
		for _, err := range errs {
			result.Failures = append(result.Failures, coverage.RefreshFailure{OrgID: "org-a", ConfigID: uuid.New(), Err: err})
		}
		return result
	}
	countOnly := coverage.RefreshResult{Refreshed: 2, Failed: 2, Failures: fail(limit).Failures}
	for name, tc := range map[string]struct {
		result coverage.RefreshResult
		want   jobruntime.ErrorCategory
	}{
		"limit alone":                 {fail(limit), jobruntime.CategoryPermanent},
		"flags alone":                 {fail(coverage.ErrInvalidProcessorFlags), jobruntime.CategoryPermanent},
		"not found alone":             {fail(coverage.ErrConfigNotFound), jobruntime.CategoryPermanent},
		"wrapped limit":               {fail(fmt.Errorf("build: %w", limit)), jobruntime.CategoryPermanent},
		"joined deterministic":        {fail(errors.Join(coverage.ErrConfigNotFound, limit)), jobruntime.CategoryPermanent},
		"limit and connection":        {fail(limit, io.EOF), jobruntime.CategoryRetryable},
		"limit and postgres":          {fail(limit, &pgconn.PgError{Code: "40001"}), jobruntime.CategoryRetryable},
		"limit and deadline":          {fail(limit, context.DeadlineExceeded), jobruntime.CategoryRetryable},
		"limit and unknown":           {fail(limit, errors.New("other")), jobruntime.CategoryRetryable},
		"limit and nil error":         {fail(limit, nil), jobruntime.CategoryRetryable},
		"joined with transient":       {fail(errors.Join(coverage.ErrConfigNotFound, io.EOF)), jobruntime.CategoryRetryable},
		"failed count above failures": {countOnly, jobruntime.CategoryRetryable},
		"count without failures":      {coverage.RefreshResult{Refreshed: 2, Failed: 1}, jobruntime.CategoryRetryable},
	} {
		t.Run(name, func(t *testing.T) {
			logs, err := logRecords(t, &recordingProjector{result: tc.result})
			if err == nil || !strings.Contains(err.Error(), string(tc.want)) {
				t.Fatalf("Work() = %v, want %s", err, tc.want)
			}
			if got := strings.Count(logs, "sync_coverage_refresh.config_failed"); got != len(tc.result.Failures) {
				t.Fatalf("config_failed lines = %d, want %d", got, len(tc.result.Failures))
			}
			cause, ok := jobruntime.SafeCause(err)
			if want := fmt.Sprintf("sync coverage refresh: failed=%d refreshed=2", max(tc.result.Failed, len(tc.result.Failures))); !ok || cause != want {
				t.Fatalf("SafeCause = %q, %v, want %q", cause, ok, want)
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
	first, second, third := uuid.New(), uuid.New(), uuid.New()
	flagsErr := fmt.Errorf("%w: processor_flags of work-items is ['sentinel-stored-value'], not a dict", coverage.ErrInvalidProcessorFlags)
	projector := &recordingProjector{result: coverage.RefreshResult{
		Refreshed: 1, Failed: 3,
		Failures: []coverage.RefreshFailure{
			{OrgID: "org-a", ConfigID: first, Err: flagsErr},
			{OrgID: "org-b", ConfigID: second, Err: fmt.Errorf("lock: %w", coverage.ErrConfigNotFound)},
			{OrgID: "org-c", ConfigID: third, Err: fmt.Errorf("build: %w", &coverage.LimitError{Bound: "compact_window", Count: 30002, Max: 30000})},
		},
	}}
	logs, err := logRecords(t, projector)
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf("Work() = %v, want permanent: every failure is deterministic", err)
	}
	for _, want := range []string{
		"org_id=org-a", "config_id=" + first.String(), "cause_class=invalid_processor_flags",
		"org_id=org-b", "config_id=" + second.String(), "cause_class=config_not_found",
		"org_id=org-c", "config_id=" + third.String(), "cause_class=compact_window_limit",
		`cause_detail="sync coverage compact window limit exceeded: 30002 (limit 30000)"`,
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("log lacks %q:\n%s", want, logs)
		}
	}
	if strings.Contains(logs, "sentinel-stored-value") {
		t.Fatalf("log carries a stored value:\n%s", logs)
	}
	if got := strings.Count(logs, "sync_coverage_refresh.config_failed"); got != 3 {
		t.Fatalf("config_failed lines = %d, want 3", got)
	}
	cause, ok := jobruntime.SafeCause(err)
	if !ok || cause != "sync coverage refresh: failed=3 refreshed=1" {
		t.Fatalf("SafeCause = %q, %v", cause, ok)
	}
}

// TestHandlerLogsNoErrorText fails one config per coarse class, each error carrying a marker, and requires the
// config_failed lines to name the classes and carry no marker: only a limit's counts reach cause_detail.
func TestHandlerLogsNoErrorText(t *testing.T) {
	t.Parallel()
	const marker = "sentinel-stored-value"
	failures := []struct {
		err   error
		class string
	}{
		{&pgconn.PgError{Code: "57014", Message: marker, Detail: marker}, "postgres_57014"},
		{fmt.Errorf("query %s: %w", marker, context.DeadlineExceeded), "deadline_exceeded"},
		{fmt.Errorf("query %s: %w", marker, context.Canceled), "context_canceled"},
		{fmt.Errorf("query %s: %w", marker, &net.OpError{Op: "read", Err: errors.New(marker)}), "connection"},
		{fmt.Errorf("scan %s: %w", marker, pgx.ScanArgError{ColumnIndex: 2, Err: errors.New(marker)}), "decode"},
		{fmt.Errorf("%w: %s", coverage.ErrInvalidProcessorFlags, marker), "invalid_processor_flags"},
		{fmt.Errorf("fold %s: %w", marker, &coverage.LimitError{Bound: "compact_window", Count: 30002, Max: 30000}), "compact_window_limit"},
		{errors.New(marker), "other"},
	}
	result := coverage.RefreshResult{Failed: len(failures)}
	for _, failure := range failures {
		result.Failures = append(result.Failures, coverage.RefreshFailure{OrgID: "org-a", ConfigID: uuid.New(), Err: failure.err})
	}
	logs, err := logRecords(t, &recordingProjector{result: result})
	if err == nil {
		t.Fatal("Work() = nil, want retryable")
	}
	for _, failure := range failures {
		if !strings.Contains(logs, "cause_class="+failure.class+" ") {
			t.Errorf("log lacks cause_class=%s:\n%s", failure.class, logs)
		}
	}
	if strings.Contains(logs, marker) {
		t.Fatalf("an error text reached the log:\n%s", logs)
	}
	if got := strings.Count(logs, `cause_detail=""`); got != len(failures)-1 {
		t.Fatalf("empty cause_detail lines = %d, want %d:\n%s", got, len(failures)-1, logs)
	}
}

func TestHandlerNamesEveryFailingConfigAtTheJobLimit(t *testing.T) {
	t.Parallel()
	failures := make([]coverage.RefreshFailure, 0, 1000)
	ids := make([]string, 0, 1000)
	for range 1000 {
		id := uuid.New()
		ids = append(ids, id.String())
		failures = append(failures, coverage.RefreshFailure{OrgID: "org", ConfigID: id, Err: errors.New("x")})
	}
	logs, err := logRecords(t, &recordingProjector{result: coverage.RefreshResult{Failed: 1000, Failures: failures}})
	if err == nil {
		t.Fatal("Work() = nil")
	}
	for _, id := range ids {
		if !strings.Contains(logs, "config_id="+id) {
			t.Fatalf("config %s is not named in the log", id)
		}
	}
}
