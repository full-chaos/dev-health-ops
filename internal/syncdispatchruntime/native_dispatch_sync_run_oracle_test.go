package syncdispatchruntime

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// dispatchAdmissionOracleProofFile is this test's own proof marker, a
// sibling to livePythonOracleProofFile (native_finalize_sync_run_oracle_test.go)
// -- see that file's doc comment for why ci/check_go.sh's live-python-oracles
// verb needs one per oracle-bearing test, not just per package.
const dispatchAdmissionOracleProofFile = "sync-dispatch-admission"

// Live-Python oracle for dispatch_sync_run's BudgetGuard admission math
// (CHAOS-4198): executes testdata/dispatch_admission_oracle.py, which calls
// the REAL, unmodified dev_health_ops.sync.budget_guard._observe_estimate/
// _baseline_unfitness/_cooldown_expiry/_matching_cooldown_expiry, and diffs
// Go's observeEstimate/baselineUnfitness/cooldownExpiry/matchingCooldownExpiry
// against every case the script produced. pythonExecutable/
// requireLivePythonOracles/assertPythonProducerIsThisWorktree are shared
// with native_finalize_sync_run_oracle_test.go (same package, same gate) --
// not redefined here.

type oracleBucketPayload struct {
	Provider              string `json:"provider"`
	OrgID                 string `json:"org_id"`
	Host                  string `json:"host"`
	CredentialFingerprint string `json:"credential_fingerprint"`
	Dimension             string `json:"dimension"`
}

func (payload oracleBucketPayload) toBucket() budgetEstimateBucket {
	return budgetEstimateBucket{
		Provider:              payload.Provider,
		OrgID:                 payload.OrgID,
		Host:                  payload.Host,
		CredentialFingerprint: payload.CredentialFingerprint,
		Dimension:             payload.Dimension,
	}
}

type observeEstimateOracleCase struct {
	Name                 string              `json:"name"`
	EstimatedUnits       int                 `json:"estimated_units"`
	RouteFamily          string              `json:"route_family"`
	Bucket               oracleBucketPayload `json:"bucket"`
	ConsumedByBucket     map[string]int      `json:"consumed_by_bucket"`
	Limits               map[string]int      `json:"limits"`
	DefaultLimit         int                 `json:"default_limit"`
	DeferralSeconds      int                 `json:"deferral_seconds"`
	BudgetKey            string              `json:"budget_key"`
	BudgetLimit          int                 `json:"budget_limit"`
	ProjectedUnits       int                 `json:"projected_units"`
	Decision             string              `json:"decision"`
	SuggestedAvailableAt *string             `json:"suggested_available_at"`
}

type unfitnessResultPayload struct {
	BudgetKey      string `json:"budget_key"`
	EstimatedUnits int    `json:"estimated_units"`
	BudgetLimit    int    `json:"budget_limit"`
	DurableUnits   int    `json:"durable_units"`
	Permanent      bool   `json:"permanent"`
}

type baselineUnfitnessOracleCase struct {
	Name      string `json:"name"`
	Estimates []struct {
		EstimatedUnits int                 `json:"estimated_units"`
		RouteFamily    string              `json:"route_family"`
		Bucket         oracleBucketPayload `json:"bucket"`
	} `json:"estimates"`
	BaselineConsumption map[string]int          `json:"baseline_consumption"`
	Limits              map[string]int          `json:"limits"`
	DefaultLimit        int                     `json:"default_limit"`
	Result              *unfitnessResultPayload `json:"result"`
}

type cooldownExpiryOracleCase struct {
	Name              string   `json:"name"`
	ResetAt           *string  `json:"reset_at"`
	RetryAfterSeconds *float64 `json:"retry_after_seconds"`
	ObservedAt        string   `json:"observed_at"`
	Expiry            string   `json:"expiry"`
}

type matchingCooldownExpiryOracleCase struct {
	Name               string            `json:"name"`
	OrgID              string            `json:"org_id"`
	Provider           string            `json:"provider"`
	IntegrationID      string            `json:"integration_id"`
	FamilyCooldowns    map[string]string `json:"family_cooldowns"`
	DimensionCooldowns map[string]string `json:"dimension_cooldowns"`
	Estimates          []struct {
		RouteFamily string `json:"route_family"`
		Dimension   string `json:"dimension"`
	} `json:"estimates"`
	Expiry *string `json:"expiry"`
}

type dispatchAdmissionOracle struct {
	ObserveEstimate        []observeEstimateOracleCase        `json:"observe_estimate"`
	BaselineUnfitness      []baselineUnfitnessOracleCase      `json:"baseline_unfitness"`
	CooldownExpiry         []cooldownExpiryOracleCase         `json:"cooldown_expiry"`
	MatchingCooldownExpiry []matchingCooldownExpiryOracleCase `json:"matching_cooldown_expiry"`
}

func mustParseOracleTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("parse oracle timestamp %q: %v", value, err)
	}
	return parsed
}

// TestBudgetAdmissionMathMatchesLivePython is the CHAOS-4198 live-Python
// oracle for BudgetGuard's admission math.
func TestBudgetAdmissionMathMatchesLivePython(t *testing.T) {
	python := pythonExecutable(t)
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	oracleScript := filepath.Join(packageDir, "testdata", "dispatch_admission_oracle.py")
	output, err := exec.Command(python, oracleScript).CombinedOutput()
	if err != nil {
		t.Fatalf("execute Python dispatch admission oracle: %v: %s", err, output)
	}
	var want dispatchAdmissionOracle
	if err := json.Unmarshal(output, &want); err != nil {
		t.Fatalf("decode Python dispatch admission oracle: %v: %s", err, output)
	}
	if len(want.ObserveEstimate) == 0 || len(want.BaselineUnfitness) == 0 ||
		len(want.CooldownExpiry) == 0 || len(want.MatchingCooldownExpiry) == 0 {
		t.Fatalf("oracle produced no cases in at least one category: %s", output)
	}

	for _, oracleCase := range want.ObserveEstimate {
		t.Run("observe_estimate/"+oracleCase.Name, func(t *testing.T) {
			estimate := budgetEstimate{
				Bucket:         oracleCase.Bucket.toBucket(),
				EstimatedUnits: oracleCase.EstimatedUnits,
				RouteFamily:    oracleCase.RouteFamily,
			}
			consumed := map[string]int{}
			for key, value := range oracleCase.ConsumedByBucket {
				consumed[key] = value
			}
			observedAt := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
			got := observeEstimate(estimate, map[string]any{}, consumed, oracleCase.Limits,
				oracleCase.DefaultLimit, observedAt, oracleCase.DeferralSeconds, true)

			if got["budget_key"] != oracleCase.BudgetKey {
				t.Errorf("budget_key = %v, want %v", got["budget_key"], oracleCase.BudgetKey)
			}
			if got["budget_limit"] != oracleCase.BudgetLimit {
				t.Errorf("budget_limit = %v, want %v", got["budget_limit"], oracleCase.BudgetLimit)
			}
			if got["projected_units"] != oracleCase.ProjectedUnits {
				t.Errorf("projected_units = %v, want %v", got["projected_units"], oracleCase.ProjectedUnits)
			}
			if got["decision"] != oracleCase.Decision {
				t.Errorf("decision = %v, want %v", got["decision"], oracleCase.Decision)
			}
			gotSuggested, _ := got["suggested_available_at"].(string)
			if oracleCase.SuggestedAvailableAt == nil {
				if gotSuggested != "" {
					t.Errorf("suggested_available_at = %q, want none", gotSuggested)
				}
			} else {
				gotTime, err := time.Parse(time.RFC3339Nano, gotSuggested)
				if err != nil {
					t.Fatalf("parse Go suggested_available_at %q: %v", gotSuggested, err)
				}
				wantTime := mustParseOracleTime(t, *oracleCase.SuggestedAvailableAt)
				if !gotTime.Equal(wantTime) {
					t.Errorf("suggested_available_at = %v, want %v", gotTime, wantTime)
				}
			}
		})
	}

	for _, oracleCase := range want.BaselineUnfitness {
		t.Run("baseline_unfitness/"+oracleCase.Name, func(t *testing.T) {
			estimates := make([]budgetEstimate, len(oracleCase.Estimates))
			for i, estimatePayload := range oracleCase.Estimates {
				estimates[i] = budgetEstimate{
					Bucket:         estimatePayload.Bucket.toBucket(),
					EstimatedUnits: estimatePayload.EstimatedUnits,
					RouteFamily:    estimatePayload.RouteFamily,
				}
			}
			got := baselineUnfitness(estimates, oracleCase.BaselineConsumption, oracleCase.Limits, oracleCase.DefaultLimit)
			if oracleCase.Result == nil {
				if got != nil {
					t.Errorf("baselineUnfitness = %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("baselineUnfitness = nil, want %+v", oracleCase.Result)
			}
			want := oracleCase.Result
			if got.budgetKey != want.BudgetKey || got.estimatedUnits != want.EstimatedUnits ||
				got.budgetLimit != want.BudgetLimit || got.durableUnits != want.DurableUnits ||
				got.permanent != want.Permanent {
				t.Errorf("baselineUnfitness = %+v, want %+v", got, want)
			}
		})
	}

	for _, oracleCase := range want.CooldownExpiry {
		t.Run("cooldown_expiry/"+oracleCase.Name, func(t *testing.T) {
			observedAt := mustParseOracleTime(t, oracleCase.ObservedAt)
			observation := rateLimitObservation{observedAt: observedAt, retryAfterSeconds: oracleCase.RetryAfterSeconds}
			if oracleCase.ResetAt != nil {
				resetAt := mustParseOracleTime(t, *oracleCase.ResetAt)
				observation.resetAt = &resetAt
			}
			got := cooldownExpiry(observation)
			want := mustParseOracleTime(t, oracleCase.Expiry)
			if !got.Equal(want) {
				t.Errorf("cooldownExpiry = %v, want %v", got, want)
			}
		})
	}

	for _, oracleCase := range want.MatchingCooldownExpiry {
		t.Run("matching_cooldown_expiry/"+oracleCase.Name, func(t *testing.T) {
			estimates := make([]budgetEstimate, len(oracleCase.Estimates))
			for i, estimatePayload := range oracleCase.Estimates {
				estimates[i] = budgetEstimate{
					RouteFamily: estimatePayload.RouteFamily,
					Bucket:      budgetEstimateBucket{Dimension: estimatePayload.Dimension},
				}
			}
			familyCooldowns := map[cooldownKey]time.Time{}
			for key, value := range oracleCase.FamilyCooldowns {
				familyCooldowns[oracleCooldownKey(t, oracleCase.OrgID, oracleCase.Provider, key)] = mustParseOracleTime(t, value)
			}
			dimensionCooldowns := map[cooldownKey]time.Time{}
			for key, value := range oracleCase.DimensionCooldowns {
				dimensionCooldowns[oracleCooldownKey(t, oracleCase.OrgID, oracleCase.Provider, key)] = mustParseOracleTime(t, value)
			}
			gotExpiry, gotFound := matchingCooldownExpiry(estimates, oracleCase.OrgID, oracleCase.Provider, oracleCase.IntegrationID, familyCooldowns, dimensionCooldowns)
			if oracleCase.Expiry == nil {
				if gotFound {
					t.Errorf("matchingCooldownExpiry found=%v expiry=%v, want not found", gotFound, gotExpiry)
				}
				return
			}
			if !gotFound {
				t.Fatalf("matchingCooldownExpiry found=false, want found with expiry %v", *oracleCase.Expiry)
			}
			want := mustParseOracleTime(t, *oracleCase.Expiry)
			if !gotExpiry.Equal(want) {
				t.Errorf("matchingCooldownExpiry = %v, want %v", gotExpiry, want)
			}
		})
	}

	proofDir := os.Getenv(livePythonOracleProofDir)
	proof := filepath.Join(proofDir, dispatchAdmissionOracleProofFile)
	if err := os.WriteFile(proof, []byte("executed\n"), 0o600); err != nil {
		t.Fatalf("write live Python dispatch admission oracle proof: %v", err)
	}
}

// oracleCooldownKey parses the pipe-joined "org|provider|integration|family"
// key the Python oracle script emits for its family_cooldowns/
// dimension_cooldowns maps back into a cooldownKey. org/provider are passed
// separately (redundant with the key's own leading two fields) only as a
// sanity cross-check that the harness constructed the key the way the test
// case's own org_id/provider fields say it should have.
func oracleCooldownKey(t *testing.T, orgID, provider, joinedKey string) cooldownKey {
	t.Helper()
	var integrationID, familyOrDimension string
	parts := splitOracleKey(joinedKey)
	if len(parts) != 4 {
		t.Fatalf("malformed oracle cooldown key %q", joinedKey)
	}
	if parts[0] != orgID || parts[1] != provider {
		t.Fatalf("oracle cooldown key %q does not match case org_id=%q provider=%q", joinedKey, orgID, provider)
	}
	integrationID = parts[2]
	familyOrDimension = parts[3]
	return cooldownKey{orgID: orgID, provider: provider, integrationID: integrationID, familyOrDimension: familyOrDimension}
}

func splitOracleKey(joined string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(joined); i++ {
		if joined[i] == '|' {
			parts = append(parts, joined[start:i])
			start = i + 1
		}
	}
	parts = append(parts, joined[start:])
	return parts
}
