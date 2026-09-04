package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// dispatchMetricsExecutionRepair handles `metrics execution-repair`
// (CHAOS-5042): the per-execution twin of the existing bulk `metrics
// daily-redrive` -- POST
// /internal/worker/metric-executions/v1/{execution_id}/repair
// (worker_metrics.py:2946-2954) for ONE metric_compatibility_executions row
// an operator has already reviewed, using the SAME WORKER_METRIC_REPAIR_TOKEN
// daily-redrive already requires (worker_auth.py:19-28 --
// authorize_metric_repair, not the workgraph repair's distinct token).
// Exists because the bulk endpoint only ever authorizes retry_safe
// (main.go:797-811's "a bulk path cannot inspect per-row evidence" note) --
// the 4 daily compat executions this ticket found whose output already
// exists in file_hotspot_daily need confirm_succeeded specifically, which
// only this single-execution endpoint can grant (retry_safe would
// SUM-duplicate their already-written rows).
func dispatchMetricsExecutionRepair(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := quietFlags("metrics execution-repair")
	execution := flags.String("execution", "", "metric_compatibility_executions id (uuid)")
	expectedState := flags.String("expected-state", "", "executing or ambiguous -- the row's CURRENT state, read via `metrics list-ambiguous-executions` just before this call")
	expectedAttemptCount := flags.Int("expected-attempt-count", 0, "REQUIRED: the row's attempt_count, read via `metrics list-ambiguous-executions` just before this call -- the bridge refuses a stale value (CAS guard, worker_metrics.py:1337-1343)")
	resolution := flags.String("resolution", "", "confirm_succeeded or retry_safe")
	reviewEvidence := flags.String("review-evidence", "", "REQUIRED: what you verified before authorizing this resolution for this ONE execution (e.g. \"file_hotspot_daily already has rows for this run/family -- retry would SUM-duplicate them, confirming succeeded instead\")")
	outputEvidence := flags.String("output-evidence", "", "REQUIRED only when --resolution=confirm_succeeded: a JSON object describing the real output this execution already produced (MetricExecutionRepairRequest.output_evidence); refused for retry_safe")
	dryRun := flags.Bool("dry-run", false, "validate flags and print the request that WOULD be sent (token redacted), without calling the bridge")
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
	if *resolution != "confirm_succeeded" && *resolution != "retry_safe" {
		return writeError(stderr, "invalid_request")
	}
	if *expectedAttemptCount < 1 {
		return writeError(stderr, "invalid_request")
	}
	// Same friction-by-design bar as `metrics daily-redrive` and `workgraph
	// repair`: no default, no generic hardcoded string.
	if strings.TrimSpace(*reviewEvidence) == "" {
		return writeError(stderr, "invalid_request")
	}
	payload := map[string]any{
		"expected_state":         *expectedState,
		"expected_attempt_count": *expectedAttemptCount,
		"resolution":             *resolution,
		"review_evidence":        *reviewEvidence,
	}
	trimmedOutputEvidence := strings.TrimSpace(*outputEvidence)
	if *resolution == "confirm_succeeded" {
		if trimmedOutputEvidence == "" {
			return writeError(stderr, "invalid_request")
		}
		var evidence map[string]any
		if err := json.Unmarshal([]byte(trimmedOutputEvidence), &evidence); err != nil {
			return writeError(stderr, "invalid_request")
		}
		payload["output_evidence"] = evidence
	} else if trimmedOutputEvidence != "" {
		// worker_metrics.py:681-684 refuses retry_safe with output_evidence
		// present just as hard as it refuses confirm_succeeded without it --
		// reject locally rather than spend a round trip on a guaranteed 422.
		return writeError(stderr, "invalid_request")
	}
	path := "/internal/worker/metric-executions/v1/" + executionID.String() + "/repair"
	if *dryRun {
		// Team-lead addition (CHAOS-5042): same preview shape as `workgraph
		// repair --dry-run` -- full validation already ran, token never
		// resolved or printed.
		return writeResult(stdout, stderr, map[string]any{
			"dry_run":       true,
			"method":        http.MethodPost,
			"path":          path,
			"authorization": "Bearer [REDACTED]",
			"payload":       payload,
		})
	}
	status, body, err := postWorkerBridge(
		ctx, "WORKER_METRIC_REPAIR_TOKEN",
		path,
		payload,
	)
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	// Print the bridge's own response verbatim on every outcome, same as
	// `workgraph repair` -- a 409 ("Execution state or attempt changed",
	// "Original execution claim is still active") or 422 names the exact
	// reason.
	writeResult(stdout, stderr, body)
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		return 1
	}
	return 0
}

// metricsAmbiguousExecution is one row of `metrics
// list-ambiguous-executions`'s read-only output. metric_compatibility_executions
// carries no org_id column of its own (alembic 0059) -- org is resolved by
// joining run_id against whichever run table its worker_kind implies
// (daily_metrics_runs for 'daily', remaining_metric_runs for 'remaining';
// alembic 0059's own CHECK bounds worker_kind to exactly those two values).
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
