package saml

import (
	"context"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
)

// SSOFeature is the license feature every SAML route requires.
const SSOFeature = "sso_saml"

var ssoRequiredTier = "enterprise"

// LicensingEntitlements is the go-api SSO gate: the process license (installed
// at start-up with processlicense.Install), else the organization's license
// row or tier. Reads public.org_licenses (org_id, tier, features_override)
// and public.organizations (id, tier).
type LicensingEntitlements struct {
	DB licensing.Querier
}

// OrgHasSSO reports whether orgID may use SAML SSO.
func (e LicensingEntitlements) OrgHasSSO(ctx context.Context, orgID uuid.UUID) (bool, error) {
	if licensing.ProcessHasFeature(SSOFeature) {
		return true, nil
	}
	return licensing.OrgHasFeature(ctx, e.DB, orgID.String(), SSOFeature)
}

// notLicensedBody is the go-api 402 body.
func notLicensedBody() any {
	return map[string]any{"detail": licensing.FeatureNotLicensedDetail(SSOFeature, &ssoRequiredTier)}
}
