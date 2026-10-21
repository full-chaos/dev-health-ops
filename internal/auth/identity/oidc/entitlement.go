package oidc

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
)

const (
	ssoFeature      = "sso_saml"
	ssoRequiredTier = "enterprise"
)

// LicensingEntitlements is go-api's requireEntitlement (internal/api/sso/gate.go)
// on the same licensing code: the process license grants sso_saml, or the
// organization's own license row or tier does. Once the proxy forwards the
// OIDC routes this is the only entitlement gate. The runtime role reads
// public.org_licenses (org_id, tier, features_override) and
// public.organizations (id, tier).
type LicensingEntitlements struct{ Pool *pgxpool.Pool }

func (e LicensingEntitlements) SSOAllowed(ctx context.Context, orgID uuid.UUID) (bool, error) {
	if licensing.ProcessHasFeature(ssoFeature) {
		return true, nil
	}
	return licensing.OrgHasFeature(ctx, e.Pool, orgID.String(), ssoFeature)
}

func (LicensingEntitlements) DeniedDetail() any {
	tier := ssoRequiredTier
	return licensing.FeatureNotLicensedDetail(ssoFeature, &tier)
}
