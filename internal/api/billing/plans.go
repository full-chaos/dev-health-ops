package billing

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

var superadminRequired = detail(http.StatusForbidden, "Superadmin access required")

func isSuperuser(user *policy.User) bool { return user != nil && user.IsSuperuser }

// parseID is _parse_uuid: Python's uuid.UUID(value), 400 "Invalid <name>".
func parseID(value, name string) (uuid.UUID, *reply) {
	id, err := pythonparity.ParseUUID(value)
	if err != nil {
		answer := detail(http.StatusBadRequest, "Invalid "+name)
		return uuid.Nil, &answer
	}
	return id, nil
}

// now is the route clock at Postgres' microsecond precision.
func (h handlers) nowUTC() time.Time { return h.now().UTC().Truncate(time.Microsecond) }

// listPlans is list_billing_plans.
func (h handlers) listPlans(w http.ResponseWriter, r *http.Request) {
	var errs pybody.Errors
	includeInactive, valid := errs.QueryBool("include_inactive", pybody.LastQueryValue(r.URL.Query(), "include_inactive"), false)
	if !valid {
		h.write(w, validation(errs))
		return
	}
	if includeInactive && !isSuperuser(policy.UserFrom(r.Context())) {
		h.write(w, superadminRequired)
		return
	}
	h.serve(w, r, "list plans", func(tx pgx.Tx) (reply, error) {
		sql := `SELECT ` + planColumns + ` FROM billing_plans`
		if !includeInactive {
			sql += ` WHERE is_active IS true`
		}
		rows, err := tx.Query(r.Context(), sql+` ORDER BY display_order ASC, name ASC`)
		if err != nil {
			return reply{}, err
		}
		var plans []*planRow
		for rows.Next() {
			plan, err := scanPlan(rows)
			if err != nil {
				rows.Close()
				return reply{}, err
			}
			plans = append(plans, plan)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return reply{}, err
		}
		out := make([]pyjson.Value, 0, len(plans))
		for _, plan := range plans {
			response, err := planResponse(r.Context(), tx, plan, includeInactive)
			if err != nil {
				return reply{}, err
			}
			out = append(out, response)
		}
		return ok(out), nil
	})
}

// getPlan is get_billing_plan.
func (h handlers) getPlan(w http.ResponseWriter, r *http.Request) {
	var errs pybody.Errors
	includeInactivePrices, valid := errs.QueryBool("include_inactive_prices",
		pybody.LastQueryValue(r.URL.Query(), "include_inactive_prices"), false)
	if !valid {
		h.write(w, validation(errs))
		return
	}
	user := policy.UserFrom(r.Context())
	h.serve(w, r, "get plan", func(tx pgx.Tx) (reply, error) {
		id, bad := parseID(r.PathValue("plan_id"), "plan_id")
		if bad != nil {
			return *bad, nil
		}
		plan, err := loadPlan(r.Context(), tx, id)
		if err != nil {
			return reply{}, err
		}
		if plan == nil || (!plan.IsActive && !isSuperuser(user)) {
			return detail(http.StatusNotFound, "Plan not found"), nil
		}
		if includeInactivePrices && !isSuperuser(user) {
			return superadminRequired, nil
		}
		response, err := planResponse(r.Context(), tx, plan, includeInactivePrices)
		if err != nil {
			return reply{}, err
		}
		return ok(response), nil
	})
}

// errOverflow is a Python int the column cannot hold: the flush fails and
// the route answers the unhandled 500.
var errOverflow = errors.New("billing: integer out of the column's range")

func int64Of(value *big.Int) (int64, error) {
	if !value.IsInt64() {
		return 0, errOverflow
	}
	return value.Int64(), nil
}

func dumps(value pyjson.Value) (string, error) { return pyjson.Dumps(value) }

// createPlan is create_billing_plan.
func (h handlers) createPlan(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	var errs pybody.Errors
	payload, valid := parseBody(&errs, body, parsePlanCreate)
	if !valid || len(errs) > 0 {
		h.write(w, validation(errs))
		return
	}
	if !isSuperuser(policy.UserFrom(r.Context())) {
		h.write(w, superadminRequired)
		return
	}
	h.serve(w, r, "create plan", func(tx pgx.Tx) (reply, error) {
		ctx := r.Context()
		now := h.nowUTC()
		order, err := int64Of(payload.DisplayOrder)
		if err != nil {
			return reply{}, err
		}
		metadata, err := dumps(payload.Metadata)
		if err != nil {
			return reply{}, err
		}
		id := uuid.New()
		if _, err := tx.Exec(ctx, `INSERT INTO billing_plans
			(id, key, name, description, tier, is_active, display_order, stripe_product_id, metadata, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::json, $10, $10)`,
			id, payload.Key, payload.Name, payload.Description, payload.Tier, payload.IsActive, order,
			payload.StripeProductID, metadata, now); err != nil {
			return reply{}, err
		}
		if err := h.replacePrices(ctx, tx, id, payload.Prices); err != nil {
			return reply{}, err
		}
		if answer, err := replaceBundles(ctx, tx, id, payload.BundleIDs); answer != nil || err != nil {
			return derefReply(answer), err
		}
		return h.planAnswer(ctx, tx, id)
	})
}

func derefReply(answer *reply) reply {
	if answer == nil {
		return reply{}
	}
	return *answer
}

// planAnswer re-reads the plan and renders it with every price, as the
// write routes do.
func (h handlers) planAnswer(ctx context.Context, q querier, id uuid.UUID) (reply, error) {
	plan, err := loadPlan(ctx, q, id)
	if err != nil {
		return reply{}, err
	}
	if plan == nil {
		return reply{}, errors.New("billing: plan vanished inside its own transaction")
	}
	response, err := planResponse(ctx, q, plan, true)
	if err != nil {
		return reply{}, err
	}
	return ok(response), nil
}

// replacePrices is _replace_prices: prices are matched to the plan's
// existing rows by (interval, currency) -- the LAST existing row per key
// wins, as the Python dict build does -- updated in place or inserted, and
// every existing row whose key is not in the request is deleted. The
// statements run in the order the Python flush emits them: updates, then
// inserts, then the delete.
func (h handlers) replacePrices(ctx context.Context, tx pgx.Tx, planID uuid.UUID, prices []priceInput) error {
	type key struct{ interval, currency string }
	rows, err := tx.Query(ctx, `SELECT id, interval, currency FROM billing_prices WHERE plan_id = $1`, planID)
	if err != nil {
		return err
	}
	existing := map[key]uuid.UUID{}
	var order []key
	for rows.Next() {
		var id uuid.UUID
		var k key
		if err := rows.Scan(&id, &k.interval, &k.currency); err != nil {
			rows.Close()
			return err
		}
		if _, seen := existing[k]; !seen {
			order = append(order, k)
		}
		existing[k] = id
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	now := h.nowUTC()
	incoming := map[key]bool{}
	type update struct {
		id     uuid.UUID
		price  priceInput
		amount int64
	}
	var updates []update
	var inserts []priceInput
	for _, price := range prices {
		k := key{price.Interval, price.Currency}
		incoming[k] = true
		amount, err := int64Of(price.Amount)
		if err != nil {
			return err
		}
		if id, found := existing[k]; found {
			updates = append(updates, update{id, price, amount})
			continue
		}
		inserts = append(inserts, price)
	}
	for _, u := range updates {
		if u.price.StripePriceID != nil && *u.price.StripePriceID != "" {
			_, err = tx.Exec(ctx, `UPDATE billing_prices SET amount = $2, is_active = $3, stripe_price_id = $4, updated_at = $5 WHERE id = $1`,
				u.id, u.amount, u.price.IsActive, *u.price.StripePriceID, now)
		} else {
			_, err = tx.Exec(ctx, `UPDATE billing_prices SET amount = $2, is_active = $3, updated_at = $4 WHERE id = $1`,
				u.id, u.amount, u.price.IsActive, now)
		}
		if err != nil {
			return err
		}
	}
	for _, price := range inserts {
		amount, _ := int64Of(price.Amount)
		if _, err := tx.Exec(ctx, `INSERT INTO billing_prices
			(id, plan_id, interval, amount, currency, is_active, stripe_price_id, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)`,
			uuid.New(), planID, price.Interval, amount, price.Currency, price.IsActive, price.StripePriceID, now); err != nil {
			return err
		}
	}
	var stale []uuid.UUID
	for _, k := range order {
		if !incoming[k] {
			stale = append(stale, existing[k])
		}
	}
	if len(stale) > 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM billing_prices WHERE id = ANY($1)`, stale); err != nil {
			return err
		}
	}
	return nil
}

// replaceBundles is _replace_bundles: the plan's links are cleared, every
// id is parsed (400 "Invalid bundle_id"), every bundle must exist (404),
// then the links are written in request order.
func replaceBundles(ctx context.Context, tx pgx.Tx, planID uuid.UUID, bundleIDs []string) (*reply, error) {
	if _, err := tx.Exec(ctx, `DELETE FROM plan_feature_bundles WHERE plan_id = $1`, planID); err != nil {
		return nil, err
	}
	parsed := make([]uuid.UUID, 0, len(bundleIDs))
	for _, raw := range bundleIDs {
		id, bad := parseID(raw, "bundle_id")
		if bad != nil {
			return bad, nil
		}
		parsed = append(parsed, id)
	}
	if len(parsed) == 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx, `SELECT id FROM feature_bundles WHERE id = ANY($1)`, parsed)
	if err != nil {
		return nil, err
	}
	found := map[uuid.UUID]bool{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		found[id] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, id := range parsed {
		if !found[id] {
			answer := detail(http.StatusNotFound, "One or more feature bundles were not found")
			return &answer, nil
		}
	}
	for _, id := range parsed {
		if _, err := tx.Exec(ctx, `INSERT INTO plan_feature_bundles (id, plan_id, bundle_id) VALUES ($1, $2, $3)`,
			uuid.New(), planID, id); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// updatePlan is update_billing_plan.
func (h handlers) updatePlan(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	var errs pybody.Errors
	payload, valid := parseBody(&errs, body, parsePlanUpdate)
	if !valid || len(errs) > 0 {
		h.write(w, validation(errs))
		return
	}
	if !isSuperuser(policy.UserFrom(r.Context())) {
		h.write(w, superadminRequired)
		return
	}
	h.serve(w, r, "update plan", func(tx pgx.Tx) (reply, error) {
		ctx := r.Context()
		id, bad := parseID(r.PathValue("plan_id"), "plan_id")
		if bad != nil {
			return *bad, nil
		}
		plan, err := loadPlan(ctx, tx, id)
		if err != nil {
			return reply{}, err
		}
		if plan == nil {
			return detail(http.StatusNotFound, "Plan not found"), nil
		}
		var sets []string
		var args []any
		set := func(column string, value any) {
			args = append(args, value)
			sets = append(sets, column+" = $"+itoa(len(args)+1))
		}
		if payload.Metadata.Set {
			metadata := pyjson.NewObject()
			if !payload.Metadata.Null && payload.Metadata.Value.Len() > 0 {
				metadata = payload.Metadata.Value
			}
			text, err := dumps(metadata)
			if err != nil {
				return reply{}, err
			}
			args = append(args, text)
			sets = append(sets, "metadata = $"+itoa(len(args)+1)+"::json")
		}
		if payload.Prices.Set && !payload.Prices.Null {
			if err := h.replacePrices(ctx, tx, id, payload.Prices.Value); err != nil {
				return reply{}, err
			}
		}
		if payload.BundleIDs.Set && !payload.BundleIDs.Null {
			if answer, err := replaceBundles(ctx, tx, id, payload.BundleIDs.Value); answer != nil || err != nil {
				return derefReply(answer), err
			}
		}
		for _, field := range []struct {
			column string
			value  pybody.Field[string]
		}{{"key", payload.Key}, {"name", payload.Name}, {"description", payload.Description}, {"tier", payload.Tier}} {
			if field.value.Set {
				set(field.column, nullableArg(field.value.Null, field.value.Value))
			}
		}
		if payload.IsActive.Set {
			set("is_active", nullableArg(payload.IsActive.Null, payload.IsActive.Value))
		}
		if payload.DisplayOrder.Set {
			if payload.DisplayOrder.Null {
				set("display_order", nil)
			} else {
				order, err := int64Of(payload.DisplayOrder.Value)
				if err != nil {
					return reply{}, err
				}
				set("display_order", order)
			}
		}
		if payload.StripeProductID.Set {
			set("stripe_product_id", nullableArg(payload.StripeProductID.Null, payload.StripeProductID.Value))
		}
		set("updated_at", h.nowUTC())
		sql := `UPDATE billing_plans SET ` + joinComma(sets) + ` WHERE id = $1`
		if _, err := tx.Exec(ctx, sql, append([]any{id}, args...)...); err != nil {
			return reply{}, err
		}
		return h.planAnswer(ctx, tx, id)
	})
}

func nullableArg[T any](null bool, value T) any {
	if null {
		return nil
	}
	return value
}

// deletePlan is delete_billing_plan: a soft delete.
func (h handlers) deletePlan(w http.ResponseWriter, r *http.Request) {
	if !isSuperuser(policy.UserFrom(r.Context())) {
		h.write(w, superadminRequired)
		return
	}
	h.serve(w, r, "delete plan", func(tx pgx.Tx) (reply, error) {
		id, bad := parseID(r.PathValue("plan_id"), "plan_id")
		if bad != nil {
			return *bad, nil
		}
		tag, err := tx.Exec(r.Context(), `UPDATE billing_plans SET is_active = false, updated_at = $2 WHERE id = $1`, id, h.nowUTC())
		if err != nil {
			return reply{}, err
		}
		if tag.RowsAffected() == 0 {
			return detail(http.StatusNotFound, "Plan not found"), nil
		}
		out := pyjson.NewObject()
		out.Set("deleted", true)
		return ok(out), nil
	})
}
