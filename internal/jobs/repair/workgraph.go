package repair

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// WorkgraphRequest names the ambiguous work-graph execution request to
// resolve and what the operator verified about it.
type WorkgraphRequest struct {
	RequestID            uuid.UUID
	ExpectedAttemptCount int
	Resolution           string
	ReviewEvidence       string
	// OutputEvidence is the decoded JSON object a confirm_succeeded resolution
	// records; nil means absent. Numbers are json.Number.
	OutputEvidence map[string]any
}

// WorkgraphResult is what a repair answers.
type WorkgraphResult struct {
	Status    string `json:"status"`
	RequestID string `json:"request_id"`
}

// RepairWorkgraph resolves one unleased ambiguous work-graph execution
// request: confirm_succeeded settles the request and its ledger row as
// succeeded with the operator's output evidence; retry_safe returns the
// request to pending and marks the ledger row repaired. The row must be
// ambiguous on both the request and its ledger, hold no claim or lease, and
// carry the attempt count the operator read; otherwise the repair is refused
// and nothing changes. With dryRun the whole transaction runs and is rolled
// back, so an operator can prove a repair against production data.
func RepairWorkgraph(ctx context.Context, db DB, request WorkgraphRequest, dryRun bool) (result WorkgraphResult, err error) {
	if request.ExpectedAttemptCount < 1 || !validResolution(request.Resolution) || !validReviewEvidence(request.ReviewEvidence) {
		return result, refuse(422, "Repair request is invalid")
	}
	if (request.Resolution == ResolutionConfirmSucceeded) != (request.OutputEvidence != nil) {
		return result, refuse(422, "Resolution evidence is invalid")
	}
	var evidence *string
	if request.OutputEvidence != nil {
		encoded, encodeErr := canonicalJSON(request.OutputEvidence)
		if encodeErr != nil || len(encoded) > OutputEvidenceMaxBytes {
			return result, refuse(422, "execution evidence exceeds durable bound")
		}
		evidence = &encoded
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return result, fmt.Errorf("repair: begin: %w", err)
	}
	defer func() {
		if err != nil || dryRun {
			_ = tx.Rollback(ctx)
		}
	}()

	var (
		state, ledgerState string
		claimToken         *string
		leaseExpires       *string
		attemptCount       int
	)
	scanErr := tx.QueryRow(ctx, `
SELECT request.state, request.claim_token::text, request.lease_expires_at::text,
       ledger.state, ledger.attempt_count
FROM work_graph_execution_requests AS request
JOIN work_graph_execution_ledger AS ledger ON ledger.request_id = request.id
WHERE request.id = $1::uuid
FOR UPDATE OF request, ledger`, request.RequestID.String()).Scan(&state, &claimToken, &leaseExpires, &ledgerState, &attemptCount)
	found := scanErr == nil
	if scanErr != nil && !isNoRows(scanErr) {
		return result, fmt.Errorf("repair: read execution: %w", scanErr)
	}
	if !found || state != "ambiguous" || ledgerState != "ambiguous" || attemptCount != request.ExpectedAttemptCount ||
		claimToken != nil || leaseExpires != nil {
		return result, refuse(409, "Only unleased ambiguous executions can be repaired")
	}

	requestState, ledgerTarget := "pending", "repaired"
	if request.Resolution == ResolutionConfirmSucceeded {
		requestState, ledgerTarget = "succeeded", "succeeded"
	}
	if _, err = tx.Exec(ctx, `
INSERT INTO work_graph_execution_repairs (
    id, request_id, expected_attempt_count, resolution, review_evidence, output_evidence
) VALUES (
    $1::uuid, $2::uuid, $3, $4, $5, $6::jsonb
)`, uuid.New().String(), request.RequestID.String(), request.ExpectedAttemptCount, request.Resolution, request.ReviewEvidence, evidence); err != nil {
		return result, fmt.Errorf("repair: record repair: %w", err)
	}
	if _, err = tx.Exec(ctx, `
UPDATE work_graph_execution_requests
SET state = $1, updated_at = statement_timestamp()
WHERE id = $2::uuid AND state = 'ambiguous'`, requestState, request.RequestID.String()); err != nil {
		return result, fmt.Errorf("repair: settle request: %w", err)
	}
	if _, err = tx.Exec(ctx, `
UPDATE work_graph_execution_ledger
SET state = $1, output_evidence = $2::jsonb, failure_detail = NULL,
    completed_at = CASE WHEN $1::text = 'succeeded' THEN statement_timestamp() ELSE NULL END
WHERE request_id = $3::uuid AND state = 'ambiguous'`, ledgerTarget, evidence, request.RequestID.String()); err != nil {
		return result, fmt.Errorf("repair: settle ledger: %w", err)
	}
	if !dryRun {
		if err = tx.Commit(ctx); err != nil {
			return result, fmt.Errorf("repair: commit: %w", err)
		}
	}
	return WorkgraphResult{Status: "repaired", RequestID: request.RequestID.String()}, nil
}
