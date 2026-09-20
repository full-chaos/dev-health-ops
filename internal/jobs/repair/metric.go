package repair

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// executionNamespace derives the identity of a repair from what the operator
// asserted, so the same assertion applied twice is recognised as one repair.
var executionNamespace = uuid.MustParse("e6678cc4-a4e9-55c5-9354-9c6202a1834e")

// MetricRequest names the ambiguous (or stuck-executing) metric compatibility
// execution to resolve and what the operator verified about it.
type MetricRequest struct {
	ExecutionID          uuid.UUID
	ExpectedState        string // "executing" or "ambiguous"
	ExpectedAttemptCount int
	Resolution           string
	ReviewEvidence       string
	// OutputEvidence is the decoded JSON object a confirm_succeeded resolution
	// records; nil means absent. Numbers are json.Number.
	OutputEvidence map[string]any
}

// MetricResult is what a metric execution repair answers.
type MetricResult struct {
	Status      string `json:"status"`
	ExecutionID string `json:"execution_id"`
	State       string `json:"state"`
}

func (r MetricRequest) validate() (encodedOutput *string, err error) {
	if r.ExpectedAttemptCount < 1 || !validResolution(r.Resolution) || !validReviewEvidence(r.ReviewEvidence) ||
		(r.ExpectedState != "executing" && r.ExpectedState != "ambiguous") {
		return nil, refuse(422, "Repair request is invalid")
	}
	if (r.Resolution == ResolutionConfirmSucceeded) != (r.OutputEvidence != nil) {
		return nil, refuse(422, "output_evidence is required only when confirming success")
	}
	if r.OutputEvidence != nil {
		encoded, encodeErr := canonicalJSON(r.OutputEvidence)
		if encodeErr != nil || len(encoded) > OutputEvidenceMaxBytes {
			return nil, refuse(422, "output_evidence exceeds the durable bound")
		}
		return &encoded, nil
	}
	return nil, nil
}

// repairID is the deterministic identity of one repair assertion.
func repairID(executionID uuid.UUID, r MetricRequest) (uuid.UUID, error) {
	identity, err := canonicalJSON([]any{
		"metric-compatibility-execution-repair",
		executionID.String(),
		r.ExpectedState,
		r.ExpectedAttemptCount,
		r.Resolution,
	})
	if err != nil {
		return uuid.Nil, err
	}
	return uuid.NewSHA1(executionNamespace, []byte(identity)), nil
}

type executionRow struct {
	ID           string
	WorkerKind   string
	Operation    string
	RunID        string
	PartitionID  *string
	ClaimToken   *string
	State        string
	AttemptCount int
}

// originalClaimIsActive reports whether the claim that started the execution
// still holds a live lease on its run or partition: a repair must never move
// a row out from under work that is still in flight.
func originalClaimIsActive(ctx context.Context, tx pgx.Tx, row executionRow) (bool, error) {
	if row.ClaimToken == nil {
		return false, errors.New("execution claim token is missing")
	}
	var (
		query string
		args  []any
	)
	switch {
	case row.WorkerKind == "remaining":
		query = `
SELECT EXISTS (
    SELECT 1
    FROM remaining_metric_runs AS r
    JOIN remaining_metric_partitions AS p ON p.run_id = r.id
    WHERE r.id = $1::uuid
      AND p.id = $2::uuid
      AND r.status = 'running'
      AND r.canceled_at IS NULL
      AND p.status = 'running'
      AND p.claim_token = $3::uuid
      AND p.lease_expires_at > statement_timestamp()
)`
		args = []any{row.RunID, row.PartitionID, *row.ClaimToken}
	case row.Operation == "partition":
		query = `
SELECT EXISTS (
    SELECT 1
    FROM daily_metrics_runs AS r
    JOIN daily_metrics_partitions AS p ON p.run_id = r.id
    WHERE r.id = $1::uuid
      AND p.id = $2::uuid
      AND r.status = 'running'
      AND p.status = 'running'
      AND p.claim_token = $3::uuid
      AND p.lease_expires_at > statement_timestamp()
)`
		args = []any{row.RunID, row.PartitionID, *row.ClaimToken}
	default:
		query = `
SELECT EXISTS (
    SELECT 1
    FROM daily_metrics_runs AS r
    WHERE r.id = $1::uuid
      AND r.status = 'running'
      AND r.finalization_status = 'running'
      AND r.finalization_claim_token = $2::uuid
      AND r.finalization_lease_expires_at > statement_timestamp()
)`
		args = []any{row.RunID, *row.ClaimToken}
	}
	var active bool
	if err := tx.QueryRow(ctx, query, args...).Scan(&active); err != nil {
		return false, fmt.Errorf("repair: read claim liveness: %w", err)
	}
	return active, nil
}

// RepairMetricExecution resolves one metric compatibility execution. The same
// assertion applied twice is idempotent (already_applied); a different
// resolution or evidence under the same assertion is a conflict. The row must
// still be in the state and attempt the operator read, and its original claim
// must no longer hold a live lease. retry_safe authorises a retry;
// confirm_succeeded settles the execution as succeeded with the operator's
// output evidence. With dryRun the transaction runs and is rolled back.
func RepairMetricExecution(ctx context.Context, db DB, request MetricRequest, dryRun bool) (MetricResult, error) {
	encodedOutput, err := request.validate()
	if err != nil {
		return MetricResult{}, err
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return MetricResult{}, fmt.Errorf("repair: begin: %w", err)
	}
	result, err := repairMetricExecutionTx(ctx, tx, request, encodedOutput)
	if err != nil || dryRun {
		_ = tx.Rollback(ctx)
		return result, err
	}
	if err := tx.Commit(ctx); err != nil {
		return MetricResult{}, fmt.Errorf("repair: commit: %w", err)
	}
	return result, nil
}

func repairMetricExecutionTx(ctx context.Context, tx pgx.Tx, request MetricRequest, encodedOutput *string) (MetricResult, error) {
	var row executionRow
	err := tx.QueryRow(ctx, `
SELECT id::text, worker_kind, operation, run_id::text, partition_id::text, claim_token::text,
       state, attempt_count
FROM metric_compatibility_executions
WHERE id = $1::uuid
FOR UPDATE`, request.ExecutionID.String()).Scan(
		&row.ID, &row.WorkerKind, &row.Operation, &row.RunID, &row.PartitionID, &row.ClaimToken, &row.State, &row.AttemptCount)
	if isNoRows(err) {
		return MetricResult{}, refuse(404, "Execution not found")
	}
	if err != nil {
		return MetricResult{}, fmt.Errorf("repair: read execution: %w", err)
	}

	id, err := repairID(request.ExecutionID, request)
	if err != nil {
		return MetricResult{}, fmt.Errorf("repair: identity: %w", err)
	}
	var (
		priorResolution, priorReview string
		priorOutput                  []byte
	)
	priorErr := tx.QueryRow(ctx, `
SELECT resolution, review_evidence, output_evidence
FROM metric_compatibility_execution_repairs
WHERE id = $1::uuid`, id.String()).Scan(&priorResolution, &priorReview, &priorOutput)
	if priorErr != nil && !isNoRows(priorErr) {
		return MetricResult{}, fmt.Errorf("repair: read prior repair: %w", priorErr)
	}
	if priorErr == nil {
		conflict := priorResolution != request.Resolution || priorReview != request.ReviewEvidence
		if !conflict && priorOutput != nil {
			decoded, decodeErr := decodeJSONB(priorOutput)
			if decodeErr != nil {
				return MetricResult{}, fmt.Errorf("repair: decode prior evidence: %w", decodeErr)
			}
			priorCanonical, canonErr := canonicalJSON(decoded)
			if canonErr != nil {
				return MetricResult{}, fmt.Errorf("repair: canonicalize prior evidence: %w", canonErr)
			}
			conflict = encodedOutput == nil || priorCanonical != *encodedOutput
		}
		if conflict {
			return MetricResult{}, refuse(409, "Repair identity conflict")
		}
		return MetricResult{Status: "already_applied", ExecutionID: request.ExecutionID.String(), State: row.State}, nil
	}

	if row.State != request.ExpectedState || row.AttemptCount != request.ExpectedAttemptCount {
		return MetricResult{}, refuse(409, "Execution state or attempt changed")
	}
	active, err := originalClaimIsActive(ctx, tx, row)
	if err != nil {
		return MetricResult{}, err
	}
	if active {
		return MetricResult{}, refuse(409, "Original execution claim is still active")
	}

	var (
		update      string
		targetState string
	)
	if request.Resolution == ResolutionRetrySafe {
		update = `
UPDATE metric_compatibility_executions
SET state = 'retry_authorized',
    last_attempt_at = statement_timestamp()
WHERE id = $1::uuid
  AND state = $2
  AND attempt_count = $3
RETURNING id`
		targetState = "retry_authorized"
	} else {
		update = `
UPDATE metric_compatibility_executions
SET state = 'succeeded',
    output_evidence = $4::jsonb,
    completed_at = statement_timestamp(),
    last_attempt_at = statement_timestamp()
WHERE id = $1::uuid
  AND state = $2
  AND attempt_count = $3
RETURNING id`
		targetState = "succeeded"
	}
	args := []any{request.ExecutionID.String(), request.ExpectedState, request.ExpectedAttemptCount}
	if request.Resolution != ResolutionRetrySafe {
		args = append(args, encodedOutput)
	}
	var updatedID string
	if err := tx.QueryRow(ctx, update, args...).Scan(&updatedID); err != nil {
		if isNoRows(err) {
			return MetricResult{}, refuse(409, "Execution repair CAS failed")
		}
		return MetricResult{}, fmt.Errorf("repair: settle execution: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO metric_compatibility_execution_repairs (
    id, execution_id, expected_state, expected_attempt_count,
    resolution, review_evidence, output_evidence
)
VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7::jsonb)`,
		id.String(), request.ExecutionID.String(), request.ExpectedState, request.ExpectedAttemptCount,
		request.Resolution, request.ReviewEvidence, encodedOutput); err != nil {
		return MetricResult{}, fmt.Errorf("repair: record repair: %w", err)
	}
	return MetricResult{Status: "repaired", ExecutionID: request.ExecutionID.String(), State: targetState}, nil
}
