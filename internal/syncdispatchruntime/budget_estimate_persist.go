package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// persistedBudgetEstimate is the part of a dispatch estimate the unit keeps:
// what the finish needs to compare it with what the unit drew
// (result.observations.budget_estimate; Python kept the same observation).
// Bucket identity beyond the dimension (host, credential fingerprint) stays out.
type persistedBudgetEstimate struct {
	RouteFamily    string `json:"route_family"`
	Dimension      string `json:"dimension"`
	EstimatedUnits int    `json:"estimated_units"`
	Confidence     string `json:"confidence"`
}

// persistBudgetEstimates merges, into each claimed unit's result, the estimates
// the dispatch pass admitted it on, in the claim's own transaction. A unit with
// no estimates (an estimator outage fails open) is left as it is. Every other
// key of the result, and every other key of result.observations, is kept.
func persistBudgetEstimates(ctx context.Context, tx pgx.Tx, claimed []budgetUnit, estimatesByUnit map[string][]budgetEstimate) (int, error) {
	var ids []string
	var documents []string
	for _, unit := range claimed {
		estimates := estimatesByUnit[unit.id]
		if len(estimates) == 0 {
			continue
		}
		kept := make([]persistedBudgetEstimate, 0, len(estimates))
		for _, estimate := range estimates {
			kept = append(kept, persistedBudgetEstimate{
				RouteFamily: estimate.RouteFamily, Dimension: estimate.Bucket.Dimension,
				EstimatedUnits: estimate.EstimatedUnits, Confidence: estimate.Confidence,
			})
		}
		document, err := json.Marshal(kept)
		if err != nil {
			return 0, fmt.Errorf("encode budget estimates of unit %s: %w", unit.id, err)
		}
		ids = append(ids, unit.id)
		documents = append(documents, string(document))
	}
	if len(ids) == 0 {
		return 0, nil
	}
	command, err := tx.Exec(ctx, `
UPDATE public.sync_run_units AS unit
SET result = jsonb_set(
      COALESCE(unit.result::jsonb, '{}'::jsonb),
      '{observations}',
      COALESCE(unit.result::jsonb -> 'observations', '{}'::jsonb) || jsonb_build_object('budget_estimate', batch.estimate)
    )::json
FROM unnest($1::uuid[], $2::jsonb[]) AS batch(id, estimate)
WHERE unit.id = batch.id`, ids, documents)
	if err != nil {
		return 0, fmt.Errorf("persist budget estimates: %w", err)
	}
	return int(command.RowsAffected()), nil
}
