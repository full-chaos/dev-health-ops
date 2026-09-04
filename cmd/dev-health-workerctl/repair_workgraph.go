package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// dispatchWorkgraph handles `workerctl workgraph ...` (CHAOS-5042): the
// operator entry point for work_graph_execution_requests rows the Go worker
// can never resolve on its own -- POST
// /internal/worker/workgraph/v1/executions/{request_id}/repair
// (worker_workgraph.py:429-523) is the ONLY way to move an unleased
// 'ambiguous' row (state AND ledger.state both 'ambiguous', claim_token/
// lease_expires_at both NULL) forward; before this file, no Go caller ever
// reached it and the row sat there until repaired by hand against the
// database. Mirrors `metrics daily-redrive`'s CLI/auth/JSON-result
// conventions (main.go's dispatchMetrics), and authenticates with the SAME
// WORKER_METRIC_REPAIR_TOKEN -- chris ruling, CHAOS-5042 ("over-engineering"):
// workgraph and metric-execution repair share ONE operator repair token,
// not two distinct ones. worker_auth.py's authorize_workgraph_repair
// delegates to authorize_metric_repair for exactly this reason.
func dispatchWorkgraph(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return writeError(stderr, "invalid_request")
	}
	switch args[0] {
	case "list-ambiguous":
		return dispatchWorkgraphListAmbiguous(ctx, runtime, args[1:], stdout, stderr)
	case "repair":
		return dispatchWorkgraphRepair(ctx, args[1:], stdout, stderr)
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

// dispatchWorkgraphRepair calls the Python bridge's repair endpoint
// directly -- it deliberately does NOT go through joboperator.Service's
// Action/audit pipeline, the same scope choice `metrics daily-redrive`
// already made (main.go:773-777), gated only by WORKER_METRIC_REPAIR_TOKEN
// (the SAME token `metrics daily-redrive`/`metrics execution-repair` use --
// chris ruling, CHAOS-5042: one shared operator repair token, not a
// distinct one per repair endpoint). It takes no *operatorRuntime/DB connection
// at all: the repair endpoint is the sole source of truth for whether
// --request/--expected-attempt-count still name a live, unleased ambiguous
// row (worker_workgraph.py:468-478's FOR UPDATE read), so there is nothing
// for a second local check to add except a race with the bridge's own.
func dispatchWorkgraphRepair(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := quietFlags("workgraph repair")
	request := flags.String("request", "", "work_graph_execution_requests id (uuid)")
	resolution := flags.String("resolution", "", "confirm_succeeded or retry_safe")
	expectedAttemptCount := flags.Int("expected-attempt-count", 0, "REQUIRED: the row's ledger attempt_count, read via `workgraph list-ambiguous` just before this call -- the bridge refuses a stale value (CAS guard, worker_workgraph.py:472)")
	reviewEvidence := flags.String("review-evidence", "", "REQUIRED: what you verified before authorizing this resolution (e.g. \"confirmed ClickHouse has zero rows for this request's target -- safe to retry\")")
	outputEvidence := flags.String("output-evidence", "", "REQUIRED only when --resolution=confirm_succeeded: a JSON object describing the real output this execution already produced (worker_workgraph.py's RepairRequest.output_evidence); refused for retry_safe")
	dryRun := flags.Bool("dry-run", false, "validate flags and print the request that WOULD be sent (token redacted), without calling the bridge")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return writeError(stderr, "invalid_request")
	}
	requestID, err := uuid.Parse(*request)
	if err != nil {
		return writeError(stderr, "invalid_request")
	}
	if *resolution != "confirm_succeeded" && *resolution != "retry_safe" {
		return writeError(stderr, "invalid_request")
	}
	if *expectedAttemptCount < 1 {
		return writeError(stderr, "invalid_request")
	}
	// Same friction-by-design bar as `metrics daily-redrive`
	// (main.go:797-814): no default, no generic hardcoded string, the
	// operator states in their own words what they verified. codex round 1,
	// P2: also bounded to the bridge's own 2048-byte limit (worker_workgraph.py
	// RepairRequest.review_evidence) so an overlong value fails locally
	// instead of reaching the bridge only to 422.
	if !validateReviewEvidence(*reviewEvidence) {
		return writeError(stderr, "invalid_request")
	}
	payload := map[string]any{
		"expected_attempt_count": *expectedAttemptCount,
		"resolution":             *resolution,
		"review_evidence":        *reviewEvidence,
	}
	trimmedOutputEvidence := strings.TrimSpace(*outputEvidence)
	if *resolution == "confirm_succeeded" {
		if trimmedOutputEvidence == "" {
			return writeError(stderr, "invalid_request")
		}
		evidence, err := parseOutputEvidence(trimmedOutputEvidence)
		if err != nil {
			return writeError(stderr, "invalid_request")
		}
		payload["output_evidence"] = evidence
	} else if trimmedOutputEvidence != "" {
		// worker_workgraph.py:439-442 refuses retry_safe with output_evidence
		// present just as hard as it refuses confirm_succeeded without it --
		// reject locally rather than spend a round trip on a guaranteed 422.
		return writeError(stderr, "invalid_request")
	}
	path := "/internal/worker/workgraph/v1/executions/" + requestID.String() + "/repair"
	if *dryRun {
		// Team-lead addition (CHAOS-5042): preview the exact request a real
		// call would send -- full validation above already ran, so this is
		// the real payload, not a guess -- with the token kept out of the
		// output entirely (never resolved, never printed) rather than
		// resolved-then-redacted, which would need the env var configured
		// just to preview.
		return writeResult(stdout, stderr, map[string]any{
			"dry_run":       true,
			"method":        http.MethodPost,
			"path":          path,
			"authorization": "Bearer [REDACTED]",
			"payload":       payload,
		})
	}
	status, body, err := postWorkerBridge[workgraphRepairBridgeResponse](
		ctx, "WORKER_METRIC_REPAIR_TOKEN",
		path,
		payload,
	)
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	// Print the bridge's own response verbatim on every outcome -- a
	// 401/409/422/500 carries the exact reason (e.g. "Only unleased
	// ambiguous executions can be repaired") an operator needs to see, not
	// just a generic workerctl error code.
	writeResult(stdout, stderr, body)
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		return 1
	}
	return 0
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
