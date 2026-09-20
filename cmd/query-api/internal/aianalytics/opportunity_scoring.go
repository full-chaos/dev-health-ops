package aianalytics

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"math/big"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// clamp01 limits v to [0, 1].
func clamp01(v float64) float64 { return math.Max(0.0, math.Min(1.0, v)) }

// scoreRatio scores a ratio against its threshold: 0.5 at the threshold,
// rising above it, clamped to [0, 1]. scoreDelta is the same arithmetic for an
// absolute difference.
func scoreRatio(value, threshold float64) float64 {
	if threshold <= 0 {
		return 0.0
	}
	return clamp01(float64((value/threshold-1.0)/2.0) + 0.50)
}

func scoreDelta(value, threshold float64) float64 { return scoreRatio(value, threshold) }

// stableOpportunityID is the first 24 hex digits of the SHA-256 of
// "kind:entity:secondary", kind being the stored (lower-case) spelling.
func stableOpportunityID(kind, entityID, secondaryID string) string {
	sum := sha256.Sum256([]byte(kind + ":" + entityID + ":" + secondaryID))
	return hex.EncodeToString(sum[:])[:24]
}

// pct formats v as Python's "{v:.<digits>%}" does: v*100 in fixed notation.
func pct(v float64, digits int) string {
	s, err := pythonparity.FormatFixed(v*100, digits)
	if err != nil {
		return ""
	}
	return s + "%"
}

// fixed formats v as Python's "{v:.<digits>f}".
func fixed(v float64, digits int) string {
	s, err := pythonparity.FormatFixed(v, digits)
	if err != nil {
		return ""
	}
	return s
}

// ratioOrNil is numerator/denominator, nil when the denominator is not
// positive.
func ratioOrNil(numerator, denominator float64) *float64 {
	if denominator <= 0 {
		return nil
	}
	out := numerator / denominator
	return &out
}

// intRatioOrNil is numerator/denominator for counts, correctly rounded for any
// pair (integer true division does not round its operands first); nil when
// the denominator is not positive.
func intRatioOrNil(numerator, denominator int64) *float64 {
	if denominator <= 0 {
		return nil
	}
	out, _ := new(big.Rat).SetFrac(big.NewInt(numerator), big.NewInt(denominator)).Float64()
	return &out
}
