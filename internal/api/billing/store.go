package billing

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// querier is a pool or a transaction.
type querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

// planRow is one billing_plans row. Metadata is the stored JSON document,
// decoded.
type planRow struct {
	ID                           uuid.UUID
	Key, Name, Tier              string
	Description, StripeProductID *string
	IsActive                     bool
	DisplayOrder                 int64
	Metadata                     pyjson.Value
	CreatedAt, UpdatedAt         time.Time
}

const planColumns = `id, key, name, description, tier, is_active, display_order, stripe_product_id, metadata::text, created_at, updated_at`

func scanPlan(row pgx.Row) (*planRow, error) {
	var plan planRow
	var metadata *string
	var active *bool
	err := row.Scan(&plan.ID, &plan.Key, &plan.Name, &plan.Description, &plan.Tier, &active,
		&plan.DisplayOrder, &plan.StripeProductID, &metadata, &plan.CreatedAt, &plan.UpdatedAt)
	if err != nil {
		return nil, err
	}
	plan.IsActive = active != nil && *active
	if metadata != nil {
		if plan.Metadata, err = pyjson.DecodeString(*metadata); err != nil {
			return nil, fmt.Errorf("billing_plans.metadata: %w", err)
		}
	}
	return &plan, nil
}

func loadPlan(ctx context.Context, q querier, id uuid.UUID) (*planRow, error) {
	plan, err := scanPlan(q.QueryRow(ctx, `SELECT `+planColumns+` FROM billing_plans WHERE id = $1`, id))
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return plan, err
}

// priceRow is one billing_prices row.
type priceRow struct {
	ID, PlanID           uuid.UUID
	Interval, Currency   string
	Amount               int64
	IsActive             bool
	StripePriceID        *string
	CreatedAt, UpdatedAt time.Time
}

const priceColumns = `id, plan_id, interval, amount, currency, is_active, stripe_price_id, created_at, updated_at`

func scanPrice(row pgx.Row) (priceRow, error) {
	var price priceRow
	var active *bool
	err := row.Scan(&price.ID, &price.PlanID, &price.Interval, &price.Amount, &price.Currency, &active,
		&price.StripePriceID, &price.CreatedAt, &price.UpdatedAt)
	price.IsActive = active != nil && *active
	return price, err
}

// loadPrices is _load_prices.
func loadPrices(ctx context.Context, q querier, planID uuid.UUID, includeInactive bool) ([]priceRow, error) {
	sql := `SELECT ` + priceColumns + ` FROM billing_prices WHERE plan_id = $1`
	if !includeInactive {
		sql += ` AND is_active IS true`
	}
	return collectPrices(ctx, q, sql+` ORDER BY amount ASC`, planID)
}

func collectPrices(ctx context.Context, q querier, sql string, args ...any) ([]priceRow, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []priceRow
	for rows.Next() {
		price, err := scanPrice(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, price)
	}
	return out, rows.Err()
}

// bundleRow is one feature_bundles row, as the plan response reads it.
type bundleRow struct {
	ID          uuid.UUID
	Key, Name   string
	Description *string
	Features    pyjson.Value
}

// loadBundles is _load_bundles.
func loadBundles(ctx context.Context, q querier, planID uuid.UUID) ([]bundleRow, error) {
	rows, err := q.Query(ctx, `SELECT fb.id, fb.key, fb.name, fb.description, fb.features::text
		FROM feature_bundles fb JOIN plan_feature_bundles pfb ON pfb.bundle_id = fb.id
		WHERE pfb.plan_id = $1 ORDER BY fb.key ASC`, planID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []bundleRow
	for rows.Next() {
		var bundle bundleRow
		var features *string
		if err := rows.Scan(&bundle.ID, &bundle.Key, &bundle.Name, &bundle.Description, &features); err != nil {
			return nil, err
		}
		if features != nil {
			if bundle.Features, err = pyjson.DecodeString(*features); err != nil {
				return nil, fmt.Errorf("feature_bundles.features: %w", err)
			}
		}
		out = append(out, bundle)
	}
	return out, rows.Err()
}

// normalizeTier is normalize_billing_tier with its "team" default.
func normalizeTier(value string) string { return normalizeBillingTier(value, "team") }

// normalizeBillingTier is normalize_billing_tier: the stripped, lowered
// tier when it is one of the three, else fallback.
func normalizeBillingTier(value, fallback string) string {
	switch lowered := pythonparity.Lower(pythonparity.Strip(value)); lowered {
	case "community", "team", "enterprise":
		return lowered
	}
	return fallback
}

// ensureDict is ensure_dict: a JSON object as is, anything else {}.
func ensureDict(value pyjson.Value) *pyjson.Object {
	if object, ok := value.(*pyjson.Object); ok {
		return object
	}
	return pyjson.NewObject()
}

// ensureStrList is ensure_str_list: the string items of a JSON array.
func ensureStrList(value pyjson.Value) []pyjson.Value {
	out := []pyjson.Value{}
	if list, ok := value.([]pyjson.Value); ok {
		for _, item := range list {
			if text, isString := item.(string); isString {
				out = append(out, text)
			}
		}
	}
	return out
}

func nullableString(value *string) pyjson.Value {
	if value == nil {
		return nil
	}
	return *value
}

func priceJSON(price priceRow) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", price.ID.String())
	out.Set("plan_id", price.PlanID.String())
	out.Set("interval", price.Interval)
	out.Set("amount", price.Amount)
	out.Set("currency", price.Currency)
	out.Set("is_active", price.IsActive)
	out.Set("stripe_price_id", nullableString(price.StripePriceID))
	return out
}

func bundleJSON(bundle bundleRow) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", bundle.ID.String())
	out.Set("key", bundle.Key)
	out.Set("name", bundle.Name)
	out.Set("description", nullableString(bundle.Description))
	out.Set("features", ensureStrList(bundle.Features))
	return out
}

// planResponse is _plan_to_response over plan's current state.
func planResponse(ctx context.Context, q querier, plan *planRow, includeInactivePrices bool) (*pyjson.Object, error) {
	prices, err := loadPrices(ctx, q, plan.ID, includeInactivePrices)
	if err != nil {
		return nil, err
	}
	bundles, err := loadBundles(ctx, q, plan.ID)
	if err != nil {
		return nil, err
	}
	out := pyjson.NewObject()
	out.Set("id", plan.ID.String())
	out.Set("key", plan.Key)
	out.Set("name", plan.Name)
	out.Set("description", nullableString(plan.Description))
	out.Set("tier", normalizeTier(plan.Tier))
	out.Set("is_active", plan.IsActive)
	out.Set("display_order", plan.DisplayOrder)
	out.Set("stripe_product_id", nullableString(plan.StripeProductID))
	out.Set("metadata", ensureDict(plan.Metadata))
	priceList := make([]pyjson.Value, len(prices))
	for index, price := range prices {
		priceList[index] = priceJSON(price)
	}
	out.Set("prices", priceList)
	bundleList := make([]pyjson.Value, len(bundles))
	for index, bundle := range bundles {
		bundleList[index] = bundleJSON(bundle)
	}
	out.Set("bundles", bundleList)
	return out, nil
}
