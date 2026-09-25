package licensing

import (
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// ProcessTier is the process-wide LicenseManager's tier in the Go api: the
// api refuses to start with both LICENSE_KEY and LICENSE_PUBLIC_KEY set
// (apiservice buildDeps), and without them Python's LicenseManager is the
// community tier.
const ProcessTier = "community"

// ProcessHasFeature is licensing/gating.py's has_feature(feature) in a
// process with no signed license: the community tier's features.
func ProcessHasFeature(feature string) bool { return tierHasFeature(ProcessTier, feature) }

// FeatureNotLicensedDetail is @require_feature's 402 detail:
// {"error": "feature_not_licensed", "feature", "required_tier",
// "current_tier"}, current_tier being the process tier. requiredTier nil is
// the decorator called without required_tier (null). The caller writes it as
// the 402's detail; this package stays free of the HTTP layer, which the
// worker binaries that import it do not link.
func FeatureNotLicensedDetail(feature string, requiredTier *string) *pyjson.Object {
	detail := pyjson.NewObject()
	detail.Set("error", "feature_not_licensed")
	detail.Set("feature", feature)
	if requiredTier != nil {
		detail.Set("required_tier", *requiredTier)
	} else {
		detail.Set("required_tier", nil)
	}
	detail.Set("current_tier", ProcessTier)
	return detail
}
