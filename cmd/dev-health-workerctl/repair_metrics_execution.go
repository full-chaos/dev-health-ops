package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/repair"
)

// dispatchMetricsExecutionRepair handles `metrics execution-repair`
// (CHAOS-5042): the per-execution twin of the bulk `metrics daily-redrive`,
// resolving ONE metric_compatibility_executions row an operator has already
// reviewed, in the operator's own database transaction
// (repair.RepairMetricExecution). It exists because the bulk path only ever
// authorizes retry_safe -- executions whose output already exists need
// confirm_succeeded specifically (retry_safe would SUM-duplicate their
// already-written rows). The same assertion applied twice is idempotent
// (already_applied); a row whose original claim is still live is refused. With
// --dry-run the transaction runs and is rolled back.
func dispatchMetricsExecutionRepair(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	flags := quietFlags("metrics execution-repair")
	execution := flags.String("execution", "", "metric_compatibility_executions id (uuid)")
	expectedState := flags.String("expected-state", "", "executing or ambiguous -- the row's CURRENT state, read via `metrics list-ambiguous-executions` just before this call")
	expectedAttemptCount := flags.Int("expected-attempt-count", 0, "REQUIRED: the row's attempt_count, read via `metrics list-ambiguous-executions` just before this call")
	resolution := flags.String("resolution", "", "confirm_succeeded or retry_safe")
	reviewEvidence := flags.String("review-evidence", "", "REQUIRED: what you verified before authorizing this resolution")
	outputEvidence := flags.String("output-evidence", "", "REQUIRED only when --resolution=confirm_succeeded: a JSON object describing the real output this execution already produced; refused for retry_safe")
	dryRun := flags.Bool("dry-run", false, "run the repair in a transaction that is rolled back, and print what it would have done")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return writeError(stderr, "invalid_request")
	}
	executionID, err := uuid.Parse(*execution)
	if err != nil {
		return writeError(stderr, "invalid_request")
	}
	if *expectedState != "executing" && *expectedState != "ambiguous" {
		return writeError(stderr, "invalid_request")
	}
	if *resolution != repair.ResolutionConfirmSucceeded && *resolution != repair.ResolutionRetrySafe {
		return writeError(stderr, "invalid_request")
	}
	if *expectedAttemptCount < 1 {
		return writeError(stderr, "invalid_request")
	}
	if !validateReviewEvidence(*reviewEvidence) {
		return writeError(stderr, "invalid_request")
	}
	repairRequest := repair.MetricRequest{
		ExecutionID:          executionID,
		ExpectedState:        *expectedState,
		ExpectedAttemptCount: *expectedAttemptCount,
		Resolution:           *resolution,
		ReviewEvidence:       *reviewEvidence,
	}
	trimmedOutputEvidence := strings.TrimSpace(*outputEvidence)
	if *resolution == repair.ResolutionConfirmSucceeded {
		if trimmedOutputEvidence == "" {
			return writeError(stderr, "invalid_request")
		}
		evidence, err := parseOutputEvidence(trimmedOutputEvidence)
		if err != nil {
			return writeError(stderr, "invalid_request")
		}
		repairRequest.OutputEvidence = evidence
	} else if trimmedOutputEvidence != "" {
		return writeError(stderr, "invalid_request")
	}
	pool, err := coordinatorPoolOf(ctx, runtime)
	if err != nil {
		return writeRepairSetupError(stderr, err)
	}
	result, err := repair.RepairMetricExecution(ctx, pool, repairRequest, *dryRun)
	return writeRepairOutcome(stdout, stderr, result, err, *dryRun)
}

type metricsAmbiguousExecution struct {
	ExecutionID   string `json:"execution_id"`
	OrgID         string `json:"org_id"`
	WorkerKind    string `json:"worker_kind"`
	Operation     string `json:"operation"`
	RunID         string `json:"run_id"`
	Family        string `json:"family"`
	Generation    string `json:"generation"`
	State         string `json:"state"`
	AttemptCount  int    `json:"attempt_count"`
	FailureDetail string `json:"failure_detail"`
	AgeSeconds    int64  `json:"age_seconds"`
	// RepairCommand (team-lead addition, CHAOS-5042) is a ready-to-copy
	// `metrics execution-repair` invocation with this row's --execution/
	// --expected-state/--expected-attempt-count already filled in from THIS
	// read -- mirrors workgraphRepairCommandHint's shape.
	RepairCommand string `json:"repair_command"`
}

const metricsExecutionFailureDetailPreviewLimit = 200

// dispatchMetricsListAmbiguousExecutions is read-only: no bridge call, no
// ledger write. Scoped to state='ambiguous' -- the shape `execution-repair`
// can act on (a 'retry_authorized'/'succeeded' row needs no repair; an
// 'executing' row may still resolve on its own, see worker_metrics.py's
// _original_claim_is_active handling -- this listing intentionally does not
// try to reproduce that liveness check locally, `execution-repair`'s
// --expected-state accepts 'executing' too for exactly that already-decided
// case).
//
// Also excludes a row whose owning run (daily_metrics_runs or
// remaining_metric_runs, by worker_kind) has already reached a terminal
// status (succeeded/failed/canceled): repairing this ledger can never
// change anything about a run nothing will ever revisit again, and
// surfacing it here would read as "still needs repair" for a run that is
// already done, one way or the other. This matters specifically now that a
// remaining_metric_run can reach status='failed' (the automatic
// last-partition terminalize, and `metrics remaining redrive
// --terminalize`) -- a schema-legal value the ledger listing previously had
// no reason to treat specially, because nothing ever wrote it.
func dispatchMetricsListAmbiguousExecutions(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	flags := quietFlags("metrics list-ambiguous-executions")
	org := flags.String("org", "", "optional organization id (uuid) to scope the listing")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return writeError(stderr, "invalid_request")
	}
	var orgFilter *string
	if strings.TrimSpace(*org) != "" {
		if _, err := uuid.Parse(*org); err != nil {
			return writeError(stderr, "invalid_request")
		}
		orgFilter = org
	}
	if runtime.pools == nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	rows, err := runtime.pools.Domain.Query(ctx, `
SELECT execution.id, execution.worker_kind, execution.operation, execution.run_id,
       execution.family, execution.generation, execution.state, execution.attempt_count,
       COALESCE(execution.failure_detail, ''), execution.last_attempt_at,
       COALESCE(daily_run.org_id, remaining_run.org_id) AS org_id
FROM metric_compatibility_executions AS execution
LEFT JOIN daily_metrics_runs AS daily_run
  ON execution.worker_kind = 'daily' AND daily_run.id = execution.run_id
LEFT JOIN remaining_metric_runs AS remaining_run
  ON execution.worker_kind = 'remaining' AND remaining_run.id = execution.run_id
WHERE execution.state = 'ambiguous'
  AND ($1::uuid IS NULL OR COALESCE(daily_run.org_id, remaining_run.org_id) = $1::uuid)
  AND COALESCE(daily_run.status, remaining_run.status) NOT IN ('succeeded', 'failed', 'canceled')
ORDER BY execution.last_attempt_at`, orgFilter)
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	defer rows.Close()
	results := []metricsAmbiguousExecution{}
	for rows.Next() {
		var (
			executionID, workerKind, operation, runID, family, generation, state, failureDetail string
			attemptCount                                                                        int
			lastAttemptAt                                                                       time.Time
			orgID                                                                               *string
		)
		if err := rows.Scan(
			&executionID, &workerKind, &operation, &runID, &family, &generation, &state,
			&attemptCount, &failureDetail, &lastAttemptAt, &orgID,
		); err != nil {
			return writeError(stderr, "operator_backend_unavailable")
		}
		resolvedOrgID := ""
		if orgID != nil {
			resolvedOrgID = *orgID
		}
		results = append(results, metricsAmbiguousExecution{
			ExecutionID:   executionID,
			OrgID:         resolvedOrgID,
			WorkerKind:    workerKind,
			Operation:     operation,
			RunID:         runID,
			Family:        family,
			Generation:    generation,
			State:         state,
			AttemptCount:  attemptCount,
			FailureDetail: truncateForDisplay(failureDetail, metricsExecutionFailureDetailPreviewLimit),
			AgeSeconds:    int64(time.Since(lastAttemptAt).Seconds()),
			RepairCommand: metricsExecutionRepairCommandHint(executionID, state, attemptCount),
		})
	}
	if rows.Err() != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	return writeResult(stdout, stderr, results)
}

// metricsExecutionRepairCommandHint builds the ready-to-copy `metrics
// execution-repair` invocation for one `list-ambiguous-executions` row
// (team-lead addition, CHAOS-5042): --execution/--expected-state/
// --expected-attempt-count come straight from this read.
func metricsExecutionRepairCommandHint(executionID, state string, attemptCount int) string {
	return fmt.Sprintf(
		`dev-health-workerctl metrics execution-repair --execution %s --expected-state %s --expected-attempt-count %d --resolution <confirm_succeeded|retry_safe> --review-evidence "<what you verified>" [--output-evidence '{"...":"..."}']`,
		executionID, state, attemptCount,
	)
}
