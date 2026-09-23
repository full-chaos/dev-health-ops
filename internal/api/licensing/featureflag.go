package licensing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// TierOrder is licensing/types.py TIER_ORDER.
var TierOrder = []string{"community", "team", "enterprise"}

// TierRank is TIER_ORDER.index(tier), and false for a value outside
// LicenseTier.
func TierRank(tier string) (int, bool) {
	for index, value := range TierOrder {
		if value == tier {
			return index, true
		}
	}
	return 0, false
}

// Feature states are feature_flag_state's return values.
const (
	StateEnabled      = "enabled"
	StateDisabled     = "disabled"
	StateUnregistered = "unregistered"
)

// ResolveOrgTier is api/services/licensing.py resolve_org_tier for an org
// whose OrgLicense row the caller has not fetched: the org_licenses tier
// wins when a row exists; otherwise organizations.tier; otherwise
// "community". A value that is not a LicenseTier member (compared exactly,
// as LicenseTier(value) does) is "community", as Python's ValueError
// fallback is.
func ResolveOrgTier(ctx context.Context, q stateQueryer, orgID uuid.UUID) (string, error) {
	var licenseTier *string
	err := q.QueryRow(ctx, `SELECT tier FROM org_licenses WHERE org_id = $1 LIMIT 1`, orgID).Scan(&licenseTier)
	switch {
	case err == nil:
		return memberOrCommunity(licenseTier), nil
	case !errors.Is(err, pgx.ErrNoRows):
		return "", err
	}
	var orgTier *string
	err = q.QueryRow(ctx, `SELECT tier FROM organizations WHERE id = $1`, orgID).Scan(&orgTier)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return "community", nil
	case err != nil:
		return "", err
	}
	return memberOrCommunity(orgTier), nil
}

func memberOrCommunity(tier *string) string {
	if tier == nil {
		return "community"
	}
	if _, ok := TierRank(*tier); ok {
		return *tier
	}
	return "community"
}

// FeatureFlagState is api/services/licensing.py feature_flag_state for an
// org and feature: "disabled" when minTier is set and the org's resolved
// tier ranks below it, else "enabled" when the feature decision allows,
// "unregistered" when the feature key is not registered, and "disabled"
// otherwise. It also returns the resolved tier, which callers report in
// their 402 bodies. The feature_flags-table-absent branch of the Python
// function (a pre-migration database) cannot occur on a database the api
// role's posture check has already accepted, so it is not ported.
func FeatureFlagState(
	ctx context.Context, q stateQueryer, orgID uuid.UUID, featureKey, minTier string, now time.Time,
) (state, tier string, err error) {
	tier, err = ResolveOrgTier(ctx, q, orgID)
	if err != nil {
		return "", "", fmt.Errorf("resolve org tier: %w", err)
	}
	if minTier != "" {
		want, ok := TierRank(minTier)
		if !ok {
			return "", "", fmt.Errorf("unknown minimum tier %q", minTier)
		}
		if have, _ := TierRank(tier); have < want {
			return StateDisabled, tier, nil
		}
	}
	loaded, err := loadState(ctx, q, orgID.String(), featureKey, now.UTC())
	if err != nil {
		return "", "", fmt.Errorf("load feature state: %w", err)
	}
	decision := Decide(featureKey, loaded)
	switch {
	case decision.Allowed:
		return StateEnabled, tier, nil
	case decision.Reason == ReasonFeatureNotRegistered:
		return StateUnregistered, tier, nil
	default:
		return StateDisabled, tier, nil
	}
}
