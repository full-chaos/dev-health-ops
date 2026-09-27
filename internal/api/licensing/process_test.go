package licensing

import (
	"strings"
	"testing"
)

// has_feature reads the license payload's own feature map once a license is
// held -- not the tier's defaults -- and the 402 detail carries the
// process tier.
func TestProcessReadersFollowTheInstalledLicense(t *testing.T) {
	t.Cleanup(func() { SetProcessLicense(nil) })

	SetProcessLicense(nil)
	if ProcessTier() != "community" || ProcessHasFeature("sso_saml") {
		t.Fatalf("no license: tier %q, sso_saml %v", ProcessTier(), ProcessHasFeature("sso_saml"))
	}

	SetProcessLicense(&ProcessLicense{Tier: "team", Features: map[string]bool{"sso_saml": true}})
	if ProcessTier() != "team" || !ProcessHasFeature("sso_saml") {
		t.Fatalf("team license granting sso_saml: tier %q, sso_saml %v", ProcessTier(), ProcessHasFeature("sso_saml"))
	}

	SetProcessLicense(&ProcessLicense{Tier: "enterprise", Features: map[string]bool{}})
	if ProcessHasFeature("sso_saml") {
		t.Fatal("an enterprise license whose payload omits sso_saml granted it; has_feature reads payload.features")
	}
	detail, err := FeatureNotLicensedDetail("sso_saml", nil).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(detail), `"current_tier":"enterprise"`) {
		t.Fatalf("402 detail: %s", detail)
	}
}
