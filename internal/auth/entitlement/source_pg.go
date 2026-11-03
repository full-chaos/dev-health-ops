package entitlement

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// Querier is the pgx surface PGSource reads through.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// PGSource reads public.org_licenses (org_id, tier, features_override) and public.organizations (id, tier): the columns the
// auth runtime role may SELECT (authschema.PublicReadColumns). It gives licensing.OrgHasFeature's answer for every feature at once.
type PGSource struct{ Q Querier }

// Load returns the organization's tier and the features it holds. A missing row or an unknown tier holds nothing; a database error
// is returned, never read as "no entitlement".
func (s PGSource) Load(ctx context.Context, orgID uuid.UUID) (Content, error) {
	var tier *string
	var overrides []byte
	err := s.Q.QueryRow(ctx, `SELECT tier, features_override FROM org_licenses WHERE org_id = $1`, orgID).Scan(&tier, &overrides)
	switch {
	case err == nil:
		if tier == nil {
			return Content{Features: map[string]bool{}}, nil
		}
		c := tierContent(*tier)
		if validTier(*tier) {
			applyOverrides(c.Features, overrides)
		}
		return c, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return Content{}, err
	}
	var orgTier *string
	err = s.Q.QueryRow(ctx, `SELECT tier FROM organizations WHERE id = $1`, orgID).Scan(&orgTier)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && (orgTier == nil || *orgTier == "")) {
		return Content{Features: map[string]bool{}}, nil
	}
	if err != nil {
		return Content{}, err
	}
	return tierContent(*orgTier), nil
}

func validTier(tier string) bool {
	_, ok := licensing.TierRank(tier)
	return ok
}

func tierContent(tier string) Content {
	features := map[string]bool{}
	if !validTier(tier) {
		return Content{Features: features}
	}
	for _, f := range licensing.FeaturesForTier(tier) {
		if f.Enabled {
			features[f.Key] = true
		}
	}
	return Content{Tier: tier, Features: features}
}

func applyOverrides(features map[string]bool, raw []byte) {
	if len(raw) == 0 {
		return
	}
	value, err := pyjson.DecodeString(string(raw))
	if err != nil {
		return
	}
	object, ok := value.(*pyjson.Object)
	if !ok {
		return
	}
	for _, key := range object.Keys() {
		if item, present := object.Get(key); present && pyjson.Truthy(item) {
			features[key] = true
		}
	}
}
