package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
)

// dispatchBudgetEstimateMaxUnitIDs is the estimate bridge's own documented
// request-size ceiling -- src/dev_health_ops/api/internal/worker_sync.py's
// DispatchBudgetEstimateReference.unit_ids: Field(min_length=1,
// max_length=500). Pinned equal by
// tests/api/test_worker_sync_bridge.py::test_dispatch_budget_estimate_request_unit_ids_cap_matches_go
// (Python is the source of truth: Pydantic enforces it at the schema
// level; this constant exists only so every Go caller chunks BELOW that
// ceiling instead of ever discovering it via a 422 -- codex round 2,
// CHAOS-4175).
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

// budgetEstimateBucket mirrors worker_sync.py's BudgetEstimateBucketPayload
// field-for-field -- the closed schema half of the estimate-only bridge
// contract (CHAOS-4175 family 3 BudgetGuard ruling): identifiers in, this
// numeric-estimate schema out, no credential material either direction.
type budgetEstimateBucket struct {
	Provider              string `json:"provider"`
	OrgID                 string `json:"org_id"`
	Host                  string `json:"host"`
	CredentialFingerprint string `json:"credential_fingerprint"`
	Dimension             string `json:"dimension"`
}

// budgetEstimate mirrors worker_sync.py's BudgetEstimatePayload field-for-
// field, itself a mirror of budget_types.py's BudgetEstimate.to_dict().
type budgetEstimate struct {
	Bucket         budgetEstimateBucket `json:"bucket"`
	EstimatedUnits int                  `json:"estimated_units"`
	Confidence     string               `json:"confidence"`
	RouteFamily    string               `json:"route_family"`
	Notes          []string             `json:"notes"`
}

// dispatchBudgetEstimateRequest mirrors worker_sync.py's
// DispatchBudgetEstimateReference field-for-field: identifiers only.
type dispatchBudgetEstimateRequest struct {
	OrganizationID string   `json:"organization_id"`
	SyncRunID      string   `json:"sync_run_id"`
	UnitIDs        []string `json:"unit_ids"`
}

// dispatchBudgetEstimateResponse mirrors worker_sync.py's
// DispatchBudgetEstimateResponse field-for-field. A unit id present in the
// request but ABSENT from this map's keys, or mapped to an empty/missing
// slice, both mean "no budget constraint for this unit" -- see that Python
// model's own docstring; the Go caller must treat both the same way
// enforce_run does (no estimate to check against any bucket, not a hard
// failure).
type dispatchBudgetEstimateResponse struct {
	Estimates map[string][]budgetEstimate `json:"estimates"`
}

// DispatchBudgetEstimate calls the narrow, identifiers-only
// /dispatch-budget-estimate bridge endpoint (CHAOS-4175 BudgetGuard
// ruling): unit_ids in, the closed BudgetEstimate schema out. Credential
// decryption (SyncTaskBootstrap.load) and the six per-provider budget
// estimator classes run entirely on the Python side of this call; nothing
// this method sends or receives ever carries credential material.
//
// Decoding is strict (DisallowUnknownFields) on this side to match the
// ruling's "strict decode on the Go side" -- a field the Python response
// model doesn't declare must fail closed here, not be silently ignored,
// mirroring extra="forbid" on the Python request model's own enforcement.
func (bridge *HTTPBridge) DispatchBudgetEstimate(ctx context.Context, orgID, runID string, unitIDs []string) (map[string][]budgetEstimate, error) {
	if bridge == nil || len(unitIDs) == 0 {
		return nil, ErrInvalidBridge
	}
	response, err := bridge.do(ctx, "/api/internal/worker-sync/dispatch-budget-estimate", dispatchBudgetEstimateRequest{
		OrganizationID: orgID,
		SyncRunID:      runID,
		UnitIDs:        unitIDs,
	})
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	decoder.DisallowUnknownFields()
	var decoded dispatchBudgetEstimateResponse
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("%w: decode budget estimate response: %v", ErrBridgeRequest, err)
	}
	return decoded.Estimates, nil
}
