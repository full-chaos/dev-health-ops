package repair

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// RedriveRequest scopes a bulk retry authorisation to the daily runs an
// operator has already identified as stranded.
type RedriveRequest struct {
	RunIDs         []uuid.UUID
	ReviewEvidence string
	// Operations selects which ledger rows of a run the call may advance:
	// "partition" and/or "finalize". Empty means partition only.
	Operations []string
}

// RedriveResult counts the rows moved and the rows left because their
// original claim is still live or their state changed under the call.
type RedriveResult struct {
	Repaired           int `json:"repaired"`
	SkippedClaimActive int `json:"skipped_claim_active"`
}

// MaxRedriveRuns bounds the runs one call names.
const MaxRedriveRuns = 200

// RedriveDaily authorises a retry for every ambiguous or stuck-executing daily
// ledger row of the named runs whose original claim is no longer live. It never
// confirms success (that needs per-row output evidence a bulk call cannot
// supply). A row that is refused on state, attempt or claim grounds is counted
// and skipped; the rest still advance. Each row is its own transaction, so a
// refusal never undoes a neighbour's repair. With dryRun every row's
// transaction is rolled back and the counts show what would have moved.
func RedriveDaily(ctx context.Context, db DB, request RedriveRequest, dryRun bool) (RedriveResult, error) {
	if len(request.RunIDs) == 0 {
		return RedriveResult{}, nil
	}
	operations := request.Operations
	if len(operations) == 0 {
		operations = []string{"partition"}
	}
	seen := map[string]bool{}
	for _, op := range operations {
		if (op != "partition" && op != "finalize") || seen[op] {
			return RedriveResult{}, refuse(422, "operations are invalid")
		}
		seen[op] = true
	}
	if len(request.RunIDs) > MaxRedriveRuns || !validReviewEvidence(request.ReviewEvidence) {
		return RedriveResult{}, refuse(422, "Redrive request is invalid")
	}

	ids := make([]string, 0, len(request.RunIDs))
	for _, id := range request.RunIDs {
		ids = append(ids, id.String())
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return RedriveResult{}, fmt.Errorf("repair: begin: %w", err)
	}
	rows, err := tx.Query(ctx, `
SELECT id::text, state, attempt_count
FROM metric_compatibility_executions
WHERE run_id = ANY($1::uuid[])
  AND worker_kind = 'daily' AND operation = ANY($2::text[])
  AND state IN ('ambiguous', 'executing')`, ids, operations)
	if err != nil {
		_ = tx.Rollback(ctx)
		return RedriveResult{}, fmt.Errorf("repair: read stranded executions: %w", err)
	}
	type candidate struct {
		id      uuid.UUID
		state   string
		attempt int
	}
	var candidates []candidate
	for rows.Next() {
		var (
			idText string
			c      candidate
		)
		if err := rows.Scan(&idText, &c.state, &c.attempt); err != nil {
			rows.Close()
			_ = tx.Rollback(ctx)
			return RedriveResult{}, fmt.Errorf("repair: scan stranded execution: %w", err)
		}
		parsed, err := uuid.Parse(idText)
		if err != nil {
			rows.Close()
			_ = tx.Rollback(ctx)
			return RedriveResult{}, fmt.Errorf("repair: execution id: %w", err)
		}
		c.id = parsed
		candidates = append(candidates, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		_ = tx.Rollback(ctx)
		return RedriveResult{}, fmt.Errorf("repair: read stranded executions: %w", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		return RedriveResult{}, fmt.Errorf("repair: end read: %w", err)
	}

	var result RedriveResult
	for _, c := range candidates {
		_, err := RepairMetricExecution(ctx, db, MetricRequest{
			ExecutionID:          c.id,
			ExpectedState:        c.state,
			ExpectedAttemptCount: c.attempt,
			Resolution:           ResolutionRetrySafe,
			ReviewEvidence:       request.ReviewEvidence,
		}, dryRun)
		if err != nil {
			if refusal, ok := AsRefusal(err); ok && refusal.Status == 409 {
				result.SkippedClaimActive++
				continue
			}
			return result, err
		}
		result.Repaired++
	}
	return result, nil
}
