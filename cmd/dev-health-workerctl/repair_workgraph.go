package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
	"github.com/full-chaos/dev-health-ops/internal/jobs/repair"
)

// dispatchWorkgraph handles `workerctl workgraph ...`: the operator entry
// point for work_graph_execution_requests rows the Go worker can never resolve
// on its own -- `workgraph repair` is the ONLY way to move an unleased
// 'ambiguous' row (state AND ledger.state both 'ambiguous', claim_token/
// lease_expires_at both NULL) forward; without it the row sits there until
// repaired by hand against the database. It runs the repair as one Postgres
// transaction on the coordinator role (internal/jobs/repair), after the
// operator credential is authorized for `workers:operate`. Mirrors
// `metrics daily-redrive`'s CLI/auth/JSON-result conventions (main.go's
// dispatchMetrics).
func dispatchWorkgraph(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return writeError(stderr, "invalid_request")
	}
	switch args[0] {
	case "list-ambiguous":
		return dispatchWorkgraphListAmbiguous(ctx, runtime, args[1:], stdout, stderr)
	case "list-undelivered":
		return dispatchWorkgraphListUndelivered(ctx, runtime, args[1:], stdout, stderr)
	case "repair":
		return dispatchWorkgraphRepair(ctx, runtime, args[1:], stdout, stderr)
	case "trigger":
		return dispatchWorkgraphTrigger(ctx, runtime, args[1:], stdout, stderr)
	default:
		return writeError(stderr, "invalid_request")
	}
}

// workgraphAmbiguousRequest is one row of `workgraph list-ambiguous`'s
// read-only output: enough for an operator to decide a resolution and quote
// --expected-attempt-count back to `workgraph repair` without a second,
// separate lookup (the repair endpoint's own optimistic-concurrency check --
// worker_workgraph.py:472 -- refuses a stale attempt_count with a 409).
type workgraphAmbiguousRequest struct {
	RequestID     string `json:"request_id"`
	OrgID         string `json:"org_id"`
	Kind          string `json:"kind"`
	AttemptCount  int    `json:"attempt_count"`
	FailureDetail string `json:"failure_detail"`
	AgeSeconds    int64  `json:"age_seconds"`
	LeaseState    string `json:"lease_state"`
	// RepairCommand (team-lead addition, CHAOS-5042) is a ready-to-copy
	// `workgraph repair` invocation with this row's --request/
	// --expected-attempt-count already filled in from THIS read -- an
	// operator only has to choose --resolution and write --review-evidence
	// (and --output-evidence for confirm_succeeded) in their own words, never
	// retype an id or attempt count by hand.
	RepairCommand string `json:"repair_command"`
}

const workgraphFailureDetailPreviewLimit = 200

// dispatchWorkgraphListAmbiguous is read-only: no bridge call, no ledger
// write, just the same PostgreSQL domain pool every other workerctl read
// (e.g. `jobs list`) already uses. It scopes to exactly the rows `workgraph
// repair` can act on -- state='ambiguous' on BOTH the request and its
// ledger row, unleased (claim_token IS NULL AND lease_expires_at IS NULL) --
// matching worker_workgraph.py's own repair-eligibility predicate
// (worker_workgraph.py:468-478) so a listed row is never rejected by the
// repair call that follows it for a reason this listing could have shown
// up front.
func dispatchWorkgraphListAmbiguous(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	flags := quietFlags("workgraph list-ambiguous")
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
SELECT request.id, request.org_id, request.kind, request.attempt_count,
       COALESCE(ledger.failure_detail, ''), request.updated_at
FROM work_graph_execution_requests AS request
JOIN work_graph_execution_ledger AS ledger ON ledger.request_id = request.id
WHERE request.state = 'ambiguous' AND ledger.state = 'ambiguous'
  AND request.claim_token IS NULL AND request.lease_expires_at IS NULL
  AND ($1::uuid IS NULL OR request.org_id = $1::uuid)
ORDER BY request.updated_at`, orgFilter)
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	defer rows.Close()
	results := []workgraphAmbiguousRequest{}
	for rows.Next() {
		var (
			requestID, orgID, kind, failureDetail string
			attemptCount                          int
			updatedAt                             time.Time
		)
		if err := rows.Scan(&requestID, &orgID, &kind, &attemptCount, &failureDetail, &updatedAt); err != nil {
			return writeError(stderr, "operator_backend_unavailable")
		}
		results = append(results, workgraphAmbiguousRequest{
			RequestID:     requestID,
			OrgID:         orgID,
			Kind:          kind,
			AttemptCount:  attemptCount,
			FailureDetail: truncateForDisplay(failureDetail, workgraphFailureDetailPreviewLimit),
			AgeSeconds:    int64(time.Since(updatedAt).Seconds()),
			LeaseState:    "unleased",
			RepairCommand: workgraphRepairCommandHint(requestID, attemptCount),
		})
	}
	if rows.Err() != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	return writeResult(stdout, stderr, results)
}

// dispatchWorkgraphRepair resolves one unleased ambiguous work-graph execution
// request in the operator's own database transaction (repair.RepairWorkgraph):
// the row must be ambiguous on both the request and its ledger, hold no lease,
// and carry the --expected-attempt-count the operator read from
// `workgraph list-ambiguous`, otherwise the repair is refused and nothing
// changes. It runs on the coordinator pool and deliberately does NOT go through
// joboperator.Service's Action/audit pipeline, the same scope choice
// `metrics daily-redrive` made. With --dry-run the whole transaction runs and is
// rolled back, so an operator can prove the repair against a live row without
// changing it.
func dispatchWorkgraphRepair(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	flags := quietFlags("workgraph repair")
	request := flags.String("request", "", "work_graph_execution_requests id (uuid)")
	resolution := flags.String("resolution", "", "confirm_succeeded or retry_safe")
	expectedAttemptCount := flags.Int("expected-attempt-count", 0, "REQUIRED: the row's ledger attempt_count, read via `workgraph list-ambiguous` just before this call -- the repair refuses a stale value")
	reviewEvidence := flags.String("review-evidence", "", "REQUIRED: what you verified before authorizing this resolution (e.g. \"confirmed ClickHouse has zero rows for this request's target -- safe to retry\")")
	outputEvidence := flags.String("output-evidence", "", "REQUIRED only when --resolution=confirm_succeeded: a JSON object describing the real output this execution already produced; refused for retry_safe")
	dryRun := flags.Bool("dry-run", false, "run the repair in a transaction that is rolled back, and print what it would have done")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return writeError(stderr, "invalid_request")
	}
	requestID, err := uuid.Parse(*request)
	if err != nil {
		return writeError(stderr, "invalid_request")
	}
	if *resolution != repair.ResolutionConfirmSucceeded && *resolution != repair.ResolutionRetrySafe {
		return writeError(stderr, "invalid_request")
	}
	if *expectedAttemptCount < 1 {
		return writeError(stderr, "invalid_request")
	}
	// Friction by design: no default, no generic hardcoded string; the operator
	// states in their own words what they verified.
	if !validateReviewEvidence(*reviewEvidence) {
		return writeError(stderr, "invalid_request")
	}
	repairRequest := repair.WorkgraphRequest{
		RequestID:            requestID,
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
	result, err := repair.RepairWorkgraph(ctx, pool, repairRequest, *dryRun)
	return writeRepairOutcome(stdout, stderr, result, err, *dryRun)
}

// coordinatorPoolOf is the pool every repair verb runs on: the coordinator
// role, which alone holds the ledger repair grants. The caller's credential is
// authorized for the repair before the pool is handed out, so no repair path
// reaches a transaction on authentication alone.
func coordinatorPoolOf(ctx context.Context, runtime *operatorRuntime) (*pgxpool.Pool, error) {
	if runtime == nil || runtime.service == nil {
		return nil, errors.New("operator backend unavailable")
	}
	if err := runtime.service.AuthorizeLedgerRepair(ctx, runtime.principal, "*"); err != nil {
		return nil, err
	}
	if runtime.pools == nil {
		return nil, errors.New("operator backend unavailable")
	}
	return runtime.pools.CoordinatorPool()
}

// writeRepairSetupError answers a failure to obtain the repair pool: a denial
// keeps its bounded service code, anything else is a backend error.
func writeRepairSetupError(stderr io.Writer, err error) int {
	var serviceError *joboperator.ServiceError
	if errors.As(err, &serviceError) {
		return writeServiceError(stderr, err)
	}
	return writeError(stderr, "operator_backend_unavailable")
}

// writeRepairOutcome prints a repair's result as JSON. A refusal prints the
// reason the ledger gave and exits 1; any other failure is a backend error.
func writeRepairOutcome(stdout, stderr io.Writer, result any, err error, dryRun bool) int {
	if err != nil {
		if refusal, ok := repair.AsRefusal(err); ok {
			writeResult(stdout, stderr, map[string]any{"detail": refusal.Detail, "status_code": refusal.Status})
			return 1
		}
		return writeError(stderr, "operator_backend_unavailable")
	}
	encoded, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	var body map[string]any
	if json.Unmarshal(encoded, &body) != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	if dryRun {
		body["dry_run"] = true
	}
	return writeResult(stdout, stderr, body)
}

// workgraphRepairCommandHint builds the ready-to-copy `workgraph repair`
// invocation for one `list-ambiguous` row (team-lead addition, CHAOS-5042):
// --request/--expected-attempt-count come straight from this read, so an
// operator only fills in --resolution/--review-evidence (and
// --output-evidence for confirm_succeeded) themselves.
func workgraphRepairCommandHint(requestID string, attemptCount int) string {
	return fmt.Sprintf(
		`dev-health-workerctl workgraph repair --request %s --expected-attempt-count %d --resolution <confirm_succeeded|retry_safe> --review-evidence "<what you verified>" [--output-evidence '{"...":"..."}']`,
		requestID, attemptCount,
	)
}

// truncateForDisplay bounds a free-form DB text field (failure_detail can be
// up to 1024 chars, metric_compatibility_executions' CHECK bound) to a
// listing-friendly preview, matching the ticket's "failure_detail
// (truncated)" column.
func truncateForDisplay(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit]) + "…"
}
