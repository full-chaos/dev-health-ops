package licensing

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// OrgHasFeature is licensing/gating.py's _check_org_feature_async(feature,
// {"session", "org_id"}): the org's own license decides, in its order.
//
//  1. An org_licenses row wins: get_features_for_tier(LicenseTier(tier))
//     holding the feature allows, else a truthy features_override[feature]
//     allows, else deny. A tier outside LicenseTier denies (Python's
//     LicenseTier(tier) raises ValueError into the swallowing except).
//  2. With no org_licenses row, a truthy organizations.tier decides the same
//     way; a missing or empty tier denies.
//
// An org id that is not a UUID denies (uuid.UUID raises inside the try). A
// database error is returned with false: Python swallows it into False, and
// the caller denies and logs it loudly instead.
//
// Named limit: the process-wide LicenseManager (a signed LICENSE_KEY) that
// some callers consult after a denial is not part of this function; see the
// callers' own notes.
func OrgHasFeature(ctx context.Context, q Querier, orgID, feature string) (bool, error) {
	org, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		return false, nil
	}
	var tier *string
	var featuresOverride []byte
	err = q.QueryRow(ctx, `SELECT tier, features_override FROM org_licenses WHERE org_id = $1`, org).
		Scan(&tier, &featuresOverride)
	switch {
	case err == nil:
		if tier == nil {
			return false, nil
		}
		if _, ok := TierRank(*tier); !ok {
			return false, nil
		}
		if tierHasFeature(*tier, feature) {
			return true, nil
		}
		return featureOverrideTruthy(featuresOverride, feature), nil
	case !errors.Is(err, pgx.ErrNoRows):
		return false, err
	}
	var orgTier *string
	err = q.QueryRow(ctx, `SELECT tier FROM organizations WHERE id = $1`, org).Scan(&orgTier)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if orgTier == nil || *orgTier == "" {
		return false, nil
	}
	if _, ok := TierRank(*orgTier); !ok {
		return false, nil
	}
	return tierHasFeature(*orgTier, feature), nil
}

// tierHasFeature is get_features_for_tier(tier).get(feature, False).
func tierHasFeature(tier, feature string) bool {
	for _, entry := range FeaturesForTier(tier) {
		if entry.Key == feature {
			return entry.Enabled
		}
	}
	return false
}

// featureOverrideTruthy is `org_license.features_override and
// org_license.features_override.get(feature)`: anything that is not a JSON
// object has no .get and denies.
func featureOverrideTruthy(raw []byte, feature string) bool {
	if len(raw) == 0 {
		return false
	}
	value, err := pyjson.DecodeString(string(raw))
	if err != nil {
		return false
	}
	object, ok := value.(*pyjson.Object)
	if !ok || object.Len() == 0 {
		return false
	}
	item, present := object.Get(feature)
	return present && pyjson.Truthy(item)
}
