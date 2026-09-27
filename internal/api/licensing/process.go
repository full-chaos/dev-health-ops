package licensing

import (
	"sync/atomic"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// ProcessLicense is the verified payload of the process-wide license
// (licensing/gating.py's LicenseManager after initialize(): a signed
// LICENSE_KEY checked against LICENSE_PUBLIC_KEY). Only the fields a Go
// reader consults are kept: the tier, the payload's own feature map
// (has_feature reads payload.features, not the tier's defaults, once a
// license is held) and the grace flag.
//
// This is the self-hosted INSTALL license. It is not the org's entitlement
// (OrgHasFeature, org_licenses/organizations rows): R459, "Enterprise SSO !=
// Enterprise self hosted". A gate that accepts either consults both, each on
// its own terms; neither is derived from the other.
type ProcessLicense struct {
	Tier          string
	Features      map[string]bool
	InGracePeriod bool
}

// processLicense holds the process license the api binary verified at
// start-up (processlicense.Install); nil is Python's LicenseManager with no
// payload: the community tier. Python evaluates the license once, in the
// api lifespan, and never again for the life of the process; so does Go.
var processLicense atomic.Pointer[ProcessLicense]

// SetProcessLicense installs the verified process license; nil clears it
// (community). Only the api binary's start-up and tests call it.
func SetProcessLicense(license *ProcessLicense) { processLicense.Store(license) }

// CurrentProcessLicense is the installed process license, nil when none.
func CurrentProcessLicense() *ProcessLicense { return processLicense.Load() }

// ProcessTier is LicenseManager.tier: the payload's tier when a license is
// held, else community.
func ProcessTier() string {
	if license := processLicense.Load(); license != nil {
		return license.Tier
	}
	return "community"
}

// ProcessHasFeature is licensing/gating.py's has_feature(feature): the
// payload's features.get(feature, False) when a license is held, else the
// community tier's features.
func ProcessHasFeature(feature string) bool {
	if license := processLicense.Load(); license != nil {
		return license.Features[feature]
	}
	return tierHasFeature("community", feature)
}

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
	detail.Set("current_tier", ProcessTier())
	return detail
}

// B64Decode is base64.b64decode(text) with its defaults (validate=False);
// see pythonB64Decode.
func B64Decode(text string) ([]byte, error) { return pythonB64Decode(text) }
