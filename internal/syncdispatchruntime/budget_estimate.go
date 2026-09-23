package syncdispatchruntime

// dispatchBudgetEstimateMaxUnitIDs bounds one estimate batch. It was the
// bridge endpoint's request ceiling (unit_ids max_length=500); the
// in-process estimator keeps it as the batch size, so one batch still
// bootstraps at most 500 units and the chunked callers keep their
// per-chunk failure isolation.
const dispatchBudgetEstimateMaxUnitIDs = 500

// chunkUnitIDs splits unitIDs into contiguous slices no larger than
// dispatchBudgetEstimateMaxUnitIDs, preserving order. A nil/empty input
// yields no chunks, matching every caller's own "nothing to estimate"
// short-circuit.
func chunkUnitIDs(unitIDs []string) [][]string {
	if len(unitIDs) == 0 {
		return nil
	}
	chunks := make([][]string, 0, (len(unitIDs)+dispatchBudgetEstimateMaxUnitIDs-1)/dispatchBudgetEstimateMaxUnitIDs)
	for start := 0; start < len(unitIDs); start += dispatchBudgetEstimateMaxUnitIDs {
		end := start + dispatchBudgetEstimateMaxUnitIDs
		if end > len(unitIDs) {
			end = len(unitIDs)
		}
		chunks = append(chunks, unitIDs[start:end])
	}
	return chunks
}

// budgetEstimateBucket is BudgetBucketKey (sync/budget_types.py), as the
// estimator returns it: identifiers and a credential fingerprint, never
// credential material.
type budgetEstimateBucket struct {
	Provider              string `json:"provider"`
	OrgID                 string `json:"org_id"`
	Host                  string `json:"host"`
	CredentialFingerprint string `json:"credential_fingerprint"`
	Dimension             string `json:"dimension"`
}

// budgetEstimate is BudgetEstimate (sync/budget_types.py).
type budgetEstimate struct {
	Bucket         budgetEstimateBucket `json:"bucket"`
	EstimatedUnits int                  `json:"estimated_units"`
	Confidence     string               `json:"confidence"`
	RouteFamily    string               `json:"route_family"`
	Notes          []string             `json:"notes"`
}
