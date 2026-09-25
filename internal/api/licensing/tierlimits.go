package licensing

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Tiers, lowest first (types.TIER_ORDER): LicenseTier's values.
const (
	TierCommunity  = "community"
	TierTeam       = "team"
	TierEnterprise = "enterprise"
)

// LimitValue is one TIER_LIMITS value: nil (unlimited), an int, or a float.
type LimitValue any

// TierLimit is one (key, value) of TIER_LIMITS, in the dict's order.
type TierLimit struct {
	Key   string
	Value LimitValue // nil, int64 or float64
}

// TierLimits is models.licensing.TIER_LIMITS (TIER_LIMITS_DEFAULTS).
var TierLimits = map[string][]TierLimit{
	TierCommunity: {
		{"max_users", int64(5)}, {"max_repos", int64(3)}, {"max_work_items", int64(1000)},
		{"retention_days", int64(30)}, {"backfill_days", int64(30)},
		{"api_rate_limit_per_min", int64(100)}, {"min_sync_interval_hours", int64(24)},
	},
	TierTeam: {
		{"max_users", int64(20)}, {"max_repos", int64(10)}, {"max_work_items", int64(10000)},
		{"retention_days", int64(90)}, {"backfill_days", int64(90)},
		{"api_rate_limit_per_min", int64(500)}, {"min_sync_interval_hours", int64(6)},
	},
	TierEnterprise: {
		{"max_users", nil}, {"max_repos", nil}, {"max_work_items", nil},
		{"retention_days", nil}, {"backfill_days", nil},
		{"api_rate_limit_per_min", nil}, {"min_sync_interval_hours", 0.25},
	},
}

// ErrTierLimitOverflow is TierLimit.typed_value's uncaught OverflowError:
// a tier_limits row whose text reads as an infinite float, where
// int(float(text)) raises. Python's get_limit reads every row of the tier,
// so one such row fails any limit lookup for that tier; the api answers its
// unhandled 500.
var ErrTierLimitOverflow = errors.New("tier_limits value is infinite: int(float(limit_value)) raises OverflowError")

// LicenseRow is the org_licenses fields get_limit reads (the first row for
// the org, as Query.first() without an ORDER BY returns it).
type LicenseRow struct {
	Tier *string
	// LimitsOverride is the stored JSON text; nil is SQL NULL.
	LimitsOverride *string
}

// TierLimitRow is one tier_limits row for the resolved tier.
type TierLimitRow struct {
	Key   string
	Value *string
}

// TierLimitInputs are the rows TierLimitService.get_limit reads.
type TierLimitInputs struct {
	License *LicenseRow
	// OrgTier is organizations.tier, read only when no license row exists
	// (nil: no organization row, or a NULL tier).
	OrgTier *string
	// TierRows loads the tier_limits rows of the resolved tier.
	TierRows func(tier string) ([]TierLimitRow, error)
}

// ResolveTier is resolve_org_tier: a license row's tier wins (a value
// LicenseTier refuses is COMMUNITY); else organizations.tier, likewise;
// else COMMUNITY.
func ResolveTier(license *LicenseRow, orgTier *string) string {
	if license != nil {
		if license.Tier != nil {
			if _, ok := tierIndex(*license.Tier); ok {
				return *license.Tier
			}
		}
		return TierCommunity
	}
	if orgTier != nil {
		if _, ok := tierIndex(*orgTier); ok {
			return *orgTier
		}
	}
	return TierCommunity
}

// coerceLimitMap is _coerce_limit_map: a JSON object's None, int (bool
// included: isinstance(True, int)) and float values, others dropped; any
// other JSON value is {}.
func coerceLimitMap(value pyjson.Value) map[string]pyjson.Value {
	out := map[string]pyjson.Value{}
	object, ok := value.(*pyjson.Object)
	if !ok {
		return out
	}
	for _, key := range object.Keys() {
		item, _ := object.Get(key)
		switch item.(type) {
		case nil, bool, pyjson.Int, pyjson.Float:
			out[key] = item
		}
	}
	return out
}

// typedValue is TierLimit.typed_value: nil text is None; float(text) that
// raises is None; a whole float is int(f) (exact, unbounded), else the
// float; NaN is None (int(nan) raises ValueError, caught); an infinite
// float raises OverflowError (not caught).
func typedValue(text *string) (pyjson.Value, error) {
	if text == nil {
		return nil, nil
	}
	f, ok := pythonparity.ParseFloat(*text)
	if !ok || math.IsNaN(f) {
		return nil, nil
	}
	if math.IsInf(f, 0) {
		return nil, ErrTierLimitOverflow
	}
	if f == math.Trunc(f) {
		whole, _ := new(big.Float).SetFloat64(f).Int(nil)
		return pyjson.Int{Int: whole}, nil
	}
	return pyjson.Float(f), nil
}

// GetLimitFrom is TierLimitService.get_limit over already-read rows: the
// license's limits_override entry for key when the override is truthy and
// holds it, else the tier_limits rows of the resolved tier over the
// TIER_LIMITS defaults (an absent key is None: unlimited).
func GetLimitFrom(inputs TierLimitInputs, key string) (pyjson.Value, error) {
	tier := ResolveTier(inputs.License, inputs.OrgTier)
	if inputs.License != nil && inputs.License.LimitsOverride != nil {
		override, err := pyjson.DecodeString(*inputs.License.LimitsOverride)
		if err != nil {
			return nil, fmt.Errorf("decode limits_override: %w", err)
		}
		if pyjson.Truthy(override) {
			if value, ok := coerceLimitMap(override)[key]; ok {
				return value, nil
			}
		}
	}
	limits := map[string]pyjson.Value{}
	for _, entry := range TierLimits[tier] {
		switch typed := entry.Value.(type) {
		case int64:
			limits[entry.Key] = pyjson.IntOf(typed)
		case float64:
			limits[entry.Key] = pyjson.Float(typed)
		default:
			limits[entry.Key] = nil
		}
	}
	rows, err := inputs.TierRows(tier)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		value, err := typedValue(row.Value)
		if err != nil {
			return nil, err
		}
		limits[row.Key] = value
	}
	return limits[key], nil
}

// CheckLimitFrom is TierLimitService.check_limit over already-read rows:
// allowed when the limit is None or current does not exceed it, else the
// refusal "Limit exceeded: key (current/limit)" with Python's str(limit).
func CheckLimitFrom(inputs TierLimitInputs, key string, current int64) (bool, string, error) {
	limit, err := GetLimitFrom(inputs, key)
	if err != nil || limit == nil {
		return true, "", err
	}
	if !exceeds(current, limit) {
		return true, "", nil
	}
	return false, fmt.Sprintf("Limit exceeded: %s (%d/%s)", key, current, pyjson.Repr(limit)), nil
}

// exceeds is Python's current > limit for an int current.
func exceeds(current int64, limit pyjson.Value) bool {
	return exceedsInt(big.NewInt(current), limit)
}

// exceedsInt is Python's current > limit for an int current of any size.
// The limit is one of get_limit's value types (None is handled by the
// callers; _coerce_limit_map and typed_value keep only numbers).
func exceedsInt(current *big.Int, limit pyjson.Value) bool {
	switch typed := limit.(type) {
	case bool:
		if typed {
			return current.Cmp(big.NewInt(1)) > 0
		}
		return current.Sign() > 0
	case pyjson.Int:
		return current.Cmp(typed.Int) > 0
	case pyjson.Float:
		// NaN compares false; otherwise Python compares an int and a float
		// exactly, as big.Float does (it holds ±Inf too).
		if math.IsNaN(float64(typed)) {
			return false
		}
		return new(big.Float).SetInt(current).Cmp(big.NewFloat(float64(typed))) > 0
	}
	return false
}

// CheckBackfillLimitFrom is TierLimitService.check_backfill_limit over
// already-read rows: allowed when backfill_days is None or requested does
// not exceed it, else "Backfill limit exceeded: requested N days, limit is
// L days" with Python's str() of each.
func CheckBackfillLimitFrom(inputs TierLimitInputs, requested *big.Int) (bool, string, error) {
	limit, err := GetLimitFrom(inputs, "backfill_days")
	if err != nil || limit == nil {
		return true, "", err
	}
	if !exceedsInt(requested, limit) {
		return true, "", nil
	}
	return false, fmt.Sprintf("Backfill limit exceeded: requested %s days, limit is %s days", requested.String(), pyjson.Str(limit)), nil
}

// Querier is the pgx surface the loaders use.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// LoadTierLimitInputs reads the rows get_limit reads, in its order: the
// org's first license row, organizations.tier only when there is none, and
// (lazily) the resolved tier's tier_limits rows.
func LoadTierLimitInputs(ctx context.Context, q Querier, orgID uuid.UUID) (TierLimitInputs, error) {
	inputs := TierLimitInputs{TierRows: func(tier string) ([]TierLimitRow, error) {
		rows, err := q.Query(ctx, `SELECT limit_key, limit_value FROM tier_limits WHERE tier = $1`, tier)
		if err != nil {
			return nil, fmt.Errorf("read tier_limits: %w", err)
		}
		defer rows.Close()
		var out []TierLimitRow
		for rows.Next() {
			var row TierLimitRow
			if err := rows.Scan(&row.Key, &row.Value); err != nil {
				return nil, fmt.Errorf("scan tier_limits: %w", err)
			}
			out = append(out, row)
		}
		return out, rows.Err()
	}}
	var license LicenseRow
	err := q.QueryRow(ctx, `SELECT tier, limits_override::text FROM org_licenses WHERE org_id = $1 LIMIT 1`, orgID).
		Scan(&license.Tier, &license.LimitsOverride)
	switch {
	case err == nil:
		inputs.License = &license
		return inputs, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return inputs, fmt.Errorf("read org license: %w", err)
	}
	err = q.QueryRow(ctx, `SELECT tier FROM organizations WHERE id = $1`, orgID).Scan(&inputs.OrgTier)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return inputs, fmt.Errorf("read organization tier: %w", err)
	}
	return inputs, nil
}

// CheckLimit is TierLimitService.check_limit(org, key, current) against
// the database.
func CheckLimit(ctx context.Context, q Querier, orgID uuid.UUID, key string, current int64) (bool, string, error) {
	inputs, err := LoadTierLimitInputs(ctx, q, orgID)
	if err != nil {
		return false, "", err
	}
	return CheckLimitFrom(inputs, key, current)
}
