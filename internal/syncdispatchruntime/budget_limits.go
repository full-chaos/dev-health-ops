package syncdispatchruntime

import (
	"encoding/json"
	"os"
	"strconv"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// enforcedBudgetLimits ports _enforced_budget_limits/_parse_budget_limits
// verbatim: a JSON object of budget-key -> limit overrides from
// SYNC_BUDGET_BUCKET_LIMITS, silently {} on any malformed input (missing,
// invalid JSON, not an object) -- Python's own precedent for this env var,
// not a Go-side relaxation. A per-key value that fails to coerce to a
// non-negative int is skipped individually, matching Python's per-key
// try/except continuing past one bad entry rather than discarding the
// whole map.
func enforcedBudgetLimits() map[string]int {
	return parseBudgetLimits(os.Getenv("SYNC_BUDGET_BUCKET_LIMITS"))
}

// budgetLimits ports _budget_limits verbatim (SYNC_BUDGET_DRY_RUN_BUCKET_LIMITS,
// same parsing as enforcedBudgetLimits -- Python duplicates this function
// rather than sharing it; ported as two thin callers over one shared parser
// instead of duplicating the parser itself, since the parsing logic itself
// is byte-identical between the two Python functions).
func budgetLimits() map[string]int {
	return parseBudgetLimits(os.Getenv("SYNC_BUDGET_DRY_RUN_BUCKET_LIMITS"))
}

func parseBudgetLimits(raw string) map[string]int {
	limits := map[string]int{}
	if raw == "" {
		return limits
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return limits
	}
	for key, value := range parsed {
		coerced, ok := coerceLimitValue(value)
		if !ok {
			continue
		}
		if coerced < 0 {
			coerced = 0
		}
		limits[key] = coerced
	}
	return limits
}

// coerceLimitValue ports Python's int(value) coercion for the common cases
// a JSON-decoded value can actually take here (a number or a numeric
// string) -- int(None)/int([...])/int({...}) all raise TypeError in Python
// and are skipped there too; encoding/json can only ever decode a JSON
// value into float64/string/bool/nil/map/slice, so those are exactly the
// shapes this needs to handle.
func coerceLimitValue(value any) (int, bool) {
	switch typed := value.(type) {
	case float64:
		return int(typed), true
	case string:
		parsed, err := strconv.Atoi(typed)
		if err != nil {
			return 0, false
		}
		return parsed, true
	case bool:
		// Python: bool is an int subclass, so int(True)==1, int(False)==0.
		if typed {
			return 1, true
		}
		return 0, true
	default:
		return 0, false
	}
}

// limitCandidateKeys is _limit_for_bucket's fallback chain, most-specific
// first, ending in the wildcard "*".
func limitCandidateKeys(bucket budgetEstimateBucket, routeFamily string) []string {
	return []string{
		bucket.Provider + ":" + bucket.OrgID + ":" + bucket.Host + ":" + bucket.CredentialFingerprint + ":" + bucket.Dimension + ":" + routeFamily,
		bucket.Provider + ":" + bucket.Host + ":" + bucket.Dimension + ":" + routeFamily,
		bucket.Provider + ":" + bucket.Dimension + ":" + routeFamily,
		bucket.Dimension + ":" + routeFamily,
		bucket.Provider + ":" + bucket.OrgID + ":" + bucket.Host + ":" + bucket.CredentialFingerprint + ":" + bucket.Dimension,
		bucket.Provider + ":" + bucket.Host + ":" + bucket.Dimension,
		bucket.Provider + ":" + bucket.Dimension,
		bucket.Dimension,
		"*",
	}
}

// limitForBucket ports _limit_for_bucket verbatim.
func limitForBucket(bucket budgetEstimateBucket, routeFamily string, limits map[string]int, defaultLimit int) int {
	for _, key := range limitCandidateKeys(bucket, routeFamily) {
		if value, ok := limits[key]; ok {
			return value
		}
	}
	return defaultLimit
}

// budgetKeyFor ports _budget_key verbatim -- field order (provider, org_id,
// host, credential_fingerprint, dimension, route_family) matches
// providerfoundation.SyncBudgetKey.String() exactly (verified against
// Python source), reused directly rather than re-deriving a third copy of
// the same join.
func budgetKeyFor(bucket budgetEstimateBucket, routeFamily string) string {
	return providerfoundation.SyncBudgetKey{
		Provider:              bucket.Provider,
		OrgID:                 bucket.OrgID,
		Host:                  bucket.Host,
		CredentialFingerprint: bucket.CredentialFingerprint,
		Dimension:             bucket.Dimension,
		RouteFamily:           routeFamily,
	}.String()
}
