package billing

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stripe/stripe-go/v86"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

var (
	limitMin, limitMax, offsetMin = int64(1), int64(100), int64(0)
)

// pageQuery is the limit/offset/org_id query of the list routes, in
// FastAPI's parameter order.
type pageQuery struct {
	limit, offset int64
	orgID         *uuid.UUID
}

func readPage(r *http.Request, errs *pybody.Errors) pageQuery {
	query := r.URL.Query()
	var out pageQuery
	if limit, ok := errs.QueryInt("limit", pybody.LastQueryValue(query, "limit"), 20, &limitMin, &limitMax); ok {
		out.limit = limit.Int64()
	}
	if offset, ok := errs.QueryInt("offset", pybody.LastQueryValue(query, "offset"), 0, &offsetMin, nil); ok {
		if offset.IsInt64() {
			out.offset = offset.Int64()
		} else {
			out.offset = -1
		}
	}
	out.orgID, _ = errs.QueryUUID("org_id", pybody.LastQueryValue(query, "org_id"))
	return out
}

// resolveOrg is _resolve_org_id, with the named deviation: an org_id query
// value for another org is refused (403) unless the caller is a superuser
// or a member of that org. nil with no answer means "every org" (a
// superuser without org_id).
func (h handlers) resolveOrg(ctx context.Context, q querier, user *policy.User, param *uuid.UUID) (*uuid.UUID, *reply, error) {
	if param != nil {
		if !user.IsSuperuser {
			var member bool
			if err := q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM memberships WHERE user_id = $1 AND org_id = $2)`,
				user.ID, *param).Scan(&member); err != nil {
				return nil, nil, err
			}
			if !member {
				h.logger.WarnContext(ctx, "billing: org_id outside the caller's memberships refused",
					"user_id", user.ID.String(), "org_id", param.String())
				answer := detail(http.StatusForbidden, "Access forbidden")
				return nil, &answer, nil
			}
		}
		return param, nil, nil
	}
	if user.IsSuperuser {
		return nil, nil, nil
	}
	if user.OrgID != "" {
		id, err := pythonparity.ParseUUID(user.OrgID)
		if err != nil {
			answer := detail(http.StatusBadRequest, "Invalid organization")
			return nil, &answer, nil
		}
		return &id, nil, nil
	}
	answer := detail(http.StatusBadRequest, "Organization context required")
	return nil, &answer, nil
}

// subscriptionRow is one subscriptions row.
type subscriptionRow struct {
	ID, OrgID, PlanID, PriceID                               uuid.UUID
	StripeSubscriptionID, StripeCustomerID, Status           string
	PeriodStart, PeriodEnd, CanceledAt, TrialStart, TrialEnd *time.Time
	CancelAtPeriodEnd                                        *bool
}

const subscriptionColumns = `s.id, s.org_id, s.billing_plan_id, s.billing_price_id, s.stripe_subscription_id,
	s.stripe_customer_id, s.status, s.current_period_start, s.current_period_end, s.canceled_at,
	s.trial_start, s.trial_end, s.cancel_at_period_end`

func scanSubscription(row pgx.Row) (subscriptionRow, error) {
	var sub subscriptionRow
	err := row.Scan(&sub.ID, &sub.OrgID, &sub.PlanID, &sub.PriceID, &sub.StripeSubscriptionID, &sub.StripeCustomerID,
		&sub.Status, &sub.PeriodStart, &sub.PeriodEnd, &sub.CanceledAt, &sub.TrialStart, &sub.TrialEnd, &sub.CancelAtPeriodEnd)
	return sub, err
}

// orgFilter appends "AND s.org_id = $n" when org is set.
func orgFilter(sql string, args []any, org *uuid.UUID) (string, []any) {
	if org == nil {
		return sql, args
	}
	args = append(args, *org)
	return sql + ` AND s.org_id = $` + itoa(len(args)), args
}

// isoformat is _to_iso over a stored timestamp: datetime.isoformat() of
// the UTC value asyncpg returns, None when NULL.
func isoformat(value *time.Time) pyjson.Value {
	if value == nil {
		return nil
	}
	return pytime.ISOFormat(value.UTC())
}

func isoOrEmpty(value *time.Time) string {
	if value == nil {
		return ""
	}
	return pytime.ISOFormat(value.UTC())
}

// subscriptionView is SubscriptionView.
func (h handlers) subscriptionView(ctx context.Context, q querier, sub subscriptionRow) (*pyjson.Object, error) {
	plan, err := serializedRecord(ctx, q, `SELECT id, key, name, description, tier, is_active, display_order,
		stripe_product_id, metadata::text, created_at, updated_at FROM billing_plans WHERE id = $1`, sub.PlanID, planRecordKeys)
	if err != nil {
		return nil, err
	}
	price, err := serializedRecord(ctx, q, `SELECT id, plan_id, interval, amount, currency, is_active,
		stripe_price_id, created_at, updated_at FROM billing_prices WHERE id = $1`, sub.PriceID, priceRecordKeys)
	if err != nil {
		return nil, err
	}
	out := pyjson.NewObject()
	out.Set("id", sub.ID.String())
	out.Set("org_id", sub.OrgID.String())
	out.Set("stripe_subscription_id", sub.StripeSubscriptionID)
	out.Set("stripe_customer_id", sub.StripeCustomerID)
	out.Set("status", sub.Status)
	out.Set("current_period_start", isoOrEmpty(sub.PeriodStart))
	out.Set("current_period_end", isoOrEmpty(sub.PeriodEnd))
	out.Set("cancel_at_period_end", sub.CancelAtPeriodEnd != nil && *sub.CancelAtPeriodEnd)
	out.Set("canceled_at", isoformat(sub.CanceledAt))
	out.Set("trial_start", isoformat(sub.TrialStart))
	out.Set("trial_end", isoformat(sub.TrialEnd))
	out.Set("plan", plan)
	out.Set("price", price)
	return out, nil
}

// planRecordKeys and priceRecordKeys are the attribute names of the
// SQLAlchemy objects, in the order vars() lists a freshly loaded one.
var (
	planRecordKeys  = []string{"id", "key", "name", "description", "tier", "is_active", "display_order", "stripe_product_id", "metadata_", "created_at", "updated_at"}
	priceRecordKeys = []string{"id", "plan_id", "interval", "amount", "currency", "is_active", "stripe_price_id", "created_at", "updated_at"}
)

// serializedRecord is _serialize_record(db.get(Model, id)) or None: every
// attribute whose name contains "at" or "period" goes through _to_iso,
// which is isoformat() for a timestamp and str() for anything else -- so a
// dict attribute such as metadata_ becomes its Python repr text.
func serializedRecord(ctx context.Context, q querier, sql string, id uuid.UUID, keys []string) (pyjson.Value, error) {
	rows, err := q.Query(ctx, sql, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	values, err := rows.Values()
	if err != nil {
		return nil, err
	}
	out := pyjson.NewObject()
	for index, key := range keys {
		value, err := recordValue(values[index], key == "metadata_")
		if err != nil {
			return nil, err
		}
		if containsAny(key, "at", "period") {
			value, err = toISO(value, values[index])
			if err != nil {
				return nil, err
			}
		}
		out.Set(key, value)
	}
	return out, rows.Err()
}

// recordValue is a column value as the ORM attribute holds it, rendered
// the way pydantic serializes dict[str, Any].
func recordValue(value any, isJSON bool) (pyjson.Value, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case [16]byte:
		return uuid.UUID(v).String(), nil
	case string:
		if isJSON {
			return pyjson.DecodeString(v)
		}
		return v, nil
	case bool:
		return v, nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case time.Time:
		return pytime.ISOFormat(v.UTC()), nil
	}
	return nil, errUnexpected(value)
}

func toISO(rendered pyjson.Value, raw any) (pyjson.Value, error) {
	if raw == nil {
		return nil, nil
	}
	if _, isTime := raw.(time.Time); isTime {
		return rendered, nil
	}
	return pyjson.Repr(rendered), nil
}

func containsAny(text string, parts ...string) bool {
	for _, part := range parts {
		if len(part) <= len(text) {
			for index := 0; index+len(part) <= len(text); index++ {
				if text[index:index+len(part)] == part {
					return true
				}
			}
		}
	}
	return false
}

// listSubscriptions is list_subscriptions.
func (h handlers) listSubscriptions(w http.ResponseWriter, r *http.Request) {
	var errs pybody.Errors
	page := readPage(r, &errs)
	if len(errs) > 0 {
		h.write(w, validation(errs))
		return
	}
	h.serve(w, r, "list subscriptions", func(tx pgx.Tx) (reply, error) {
		ctx := r.Context()
		org, answer, err := h.resolveOrg(ctx, tx, policy.UserFrom(ctx), page.orgID)
		if answer != nil || err != nil {
			return derefReply(answer), err
		}
		if page.offset < 0 {
			return reply{}, errOverflow
		}
		where, args := orgFilter(`WHERE true`, nil, org)
		rows, err := tx.Query(ctx, `SELECT `+subscriptionColumns+` FROM subscriptions s `+where+
			` ORDER BY s.updated_at DESC LIMIT `+itoa(int(page.limit))+` OFFSET `+itoa(int(page.offset)), args...)
		if err != nil {
			return reply{}, err
		}
		var subs []subscriptionRow
		for rows.Next() {
			sub, err := scanSubscription(rows)
			if err != nil {
				rows.Close()
				return reply{}, err
			}
			subs = append(subs, sub)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return reply{}, err
		}
		var total int64
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM subscriptions s `+where, args...).Scan(&total); err != nil {
			return reply{}, err
		}
		items := make([]pyjson.Value, 0, len(subs))
		for _, sub := range subs {
			view, err := h.subscriptionView(ctx, tx, sub)
			if err != nil {
				return reply{}, err
			}
			items = append(items, view)
		}
		return ok(pageJSON(items, total, page)), nil
	})
}

func pageJSON(items []pyjson.Value, total int64, page pageQuery) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("items", items)
	out.Set("total", total)
	out.Set("limit", page.limit)
	out.Set("offset", page.offset)
	return out
}

// latestSubscription is SubscriptionService.get_for_org.
func latestSubscription(ctx context.Context, q querier, org *uuid.UUID) (*subscriptionRow, error) {
	where, args := orgFilter(`WHERE true`, nil, org)
	sub, err := scanSubscription(q.QueryRow(ctx, `SELECT `+subscriptionColumns+` FROM subscriptions s `+where+
		` ORDER BY s.updated_at DESC LIMIT 1`, args...))
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &sub, nil
}

// getSubscription is get_subscription.
func (h handlers) getSubscription(w http.ResponseWriter, r *http.Request) {
	var errs pybody.Errors
	orgParam, _ := errs.QueryUUID("org_id", pybody.LastQueryValue(r.URL.Query(), "org_id"))
	if len(errs) > 0 {
		h.write(w, validation(errs))
		return
	}
	h.serve(w, r, "get subscription", func(tx pgx.Tx) (reply, error) {
		ctx := r.Context()
		org, answer, err := h.resolveOrg(ctx, tx, policy.UserFrom(ctx), orgParam)
		if answer != nil || err != nil {
			return derefReply(answer), err
		}
		sub, err := latestSubscription(ctx, tx, org)
		if err != nil {
			return reply{}, err
		}
		if sub == nil {
			return detail(http.StatusNotFound, "No active subscription"), nil
		}
		view, err := h.subscriptionView(ctx, tx, *sub)
		if err != nil {
			return reply{}, err
		}
		return ok(view), nil
	})
}

// subscriptionHistory is get_subscription_history.
func (h handlers) subscriptionHistory(w http.ResponseWriter, r *http.Request) {
	var errs pybody.Errors
	page := readPage(r, &errs)
	if len(errs) > 0 {
		h.write(w, validation(errs))
		return
	}
	h.serve(w, r, "subscription history", func(tx pgx.Tx) (reply, error) {
		ctx := r.Context()
		org, answer, err := h.resolveOrg(ctx, tx, policy.UserFrom(ctx), page.orgID)
		if answer != nil || err != nil {
			return derefReply(answer), err
		}
		if page.offset < 0 {
			return reply{}, errOverflow
		}
		where, args := orgFilter(`WHERE true`, nil, org)
		from := ` FROM subscription_events e JOIN subscriptions s ON s.id = e.subscription_id `
		rows, err := tx.Query(ctx, `SELECT e.id, e.stripe_event_id, e.event_type, e.previous_status, e.new_status,
			e.processed_at, e.payload::text`+from+where+` ORDER BY e.processed_at DESC LIMIT `+itoa(int(page.limit))+
			` OFFSET `+itoa(int(page.offset)), args...)
		if err != nil {
			return reply{}, err
		}
		var items []pyjson.Value
		for rows.Next() {
			var id uuid.UUID
			var eventID, eventType, newStatus string
			var previous, payload *string
			var processed *time.Time
			if err := rows.Scan(&id, &eventID, &eventType, &previous, &newStatus, &processed, &payload); err != nil {
				rows.Close()
				return reply{}, err
			}
			item := pyjson.NewObject()
			item.Set("id", id.String())
			item.Set("stripe_event_id", eventID)
			item.Set("event_type", eventType)
			item.Set("previous_status", nullableString(previous))
			item.Set("new_status", newStatus)
			item.Set("processed_at", isoOrEmpty(processed))
			body, err := eventPayload(payload)
			if err != nil {
				rows.Close()
				return reply{}, err
			}
			item.Set("payload", body)
			items = append(items, item)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return reply{}, err
		}
		var total int64
		if err := tx.QueryRow(ctx, `SELECT count(*)`+from+where, args...).Scan(&total); err != nil {
			return reply{}, err
		}
		if items == nil {
			items = []pyjson.Value{}
		}
		return ok(pageJSON(items, total, page)), nil
	})
}

// eventPayload is `event.payload or {}` validated as dict[str, Any]: a
// falsy document is {}, an object is itself, anything else fails the
// response model (the unhandled 500).
func eventPayload(stored *string) (pyjson.Value, error) {
	if stored == nil {
		return pyjson.NewObject(), nil
	}
	value, err := pyjson.DecodeString(*stored)
	if err != nil {
		return nil, err
	}
	if object, ok := value.(*pyjson.Object); ok {
		return object, nil
	}
	if falsy(value) {
		return pyjson.NewObject(), nil
	}
	return nil, errUnexpected(value)
}

func falsy(value pyjson.Value) bool {
	switch v := value.(type) {
	case nil:
		return true
	case bool:
		return !v
	case string:
		return v == ""
	case pyjson.Int:
		return v.Sign() == 0
	case pyjson.Float:
		return v == 0
	case []pyjson.Value:
		return len(v) == 0
	}
	return false
}

// subscriptionTarget is the org resolution and subscription lookup the
// three mutation routes share: 400 "org_id required" for a superuser
// without org_id, 404 "No subscription found".
func (h handlers) subscriptionTarget(ctx context.Context, user *policy.User, orgParam *uuid.UUID) (*subscriptionRow, *reply, error) {
	if h.pool == nil {
		return nil, nil, errNoPool
	}
	org, answer, err := h.resolveOrg(ctx, h.pool, user, orgParam)
	if answer != nil || err != nil {
		return nil, answer, err
	}
	if org == nil {
		answer := detail(http.StatusBadRequest, "org_id required")
		return nil, &answer, nil
	}
	sub, err := latestSubscription(ctx, h.pool, org)
	if err != nil {
		return nil, nil, err
	}
	if sub == nil {
		answer := detail(http.StatusNotFound, "No subscription found")
		return nil, &answer, nil
	}
	return sub, nil, nil
}

var statusOK = func() *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("status", "ok")
	return out
}

// mutate runs a subscription mutation: org query and body errors in
// FastAPI's order, then the target, then the Stripe call. A Stripe failure
// is the unhandled 500.
func (h handlers) mutate(w http.ResponseWriter, r *http.Request, step string,
	parse func(*pybody.Errors, pybody.Body) (any, bool),
	call func(context.Context, *stripe.Client, *subscriptionRow, any) (*reply, error)) {
	var errs pybody.Errors
	orgParam, _ := errs.QueryUUID("org_id", pybody.LastQueryValue(r.URL.Query(), "org_id"))
	var payload any
	if parse != nil {
		body, _ := policy.BodyFrom(r.Context())
		payload, _ = parse(&errs, body)
	}
	if len(errs) > 0 {
		h.write(w, validation(errs))
		return
	}
	ctx := r.Context()
	sub, answer, err := h.subscriptionTarget(ctx, policy.UserFrom(ctx), orgParam)
	if err != nil {
		h.internal(w, r, step, err)
		return
	}
	if answer != nil {
		h.write(w, *answer)
		return
	}
	client, err := h.stripe.Client()
	if err != nil {
		h.internal(w, r, step, err)
		return
	}
	answer, err = call(ctx, client, sub, payload)
	if err != nil {
		h.internal(w, r, step, err)
		return
	}
	if answer != nil {
		h.write(w, *answer)
		return
	}
	h.write(w, ok(statusOK()))
}

// changePlan is change_plan: the subscription's FIRST item moves to the
// requested price, prorated.
func (h handlers) changePlan(w http.ResponseWriter, r *http.Request) {
	h.mutate(w, r, "change plan", func(errs *pybody.Errors, body pybody.Body) (any, bool) {
		return parseBody(errs, body, parseChangePlan)
	}, func(ctx context.Context, client *stripe.Client, sub *subscriptionRow, payload any) (*reply, error) {
		current, err := client.V1Subscriptions.Retrieve(ctx, sub.StripeSubscriptionID, nil)
		if err != nil {
			return nil, stripeFailure("subscription retrieve", err)
		}
		if current.Items == nil || len(current.Items.Data) == 0 || current.Items.Data[0] == nil || current.Items.Data[0].ID == "" {
			answer := detail(http.StatusBadRequest, "Stripe subscription has no items")
			return &answer, nil
		}
		_, err = client.V1Subscriptions.Update(ctx, sub.StripeSubscriptionID, &stripe.SubscriptionUpdateParams{
			Items:             []*stripe.SubscriptionUpdateItemParams{{ID: stripe.String(current.Items.Data[0].ID), Price: stripe.String(payload.(string))}},
			ProrationBehavior: stripe.String("create_prorations"),
		})
		return nil, stripeFailure("subscription update", err)
	})
}

// cancelSubscription is cancel_subscription.
func (h handlers) cancelSubscription(w http.ResponseWriter, r *http.Request) {
	h.mutate(w, r, "cancel subscription", func(errs *pybody.Errors, body pybody.Body) (any, bool) {
		return parseBody(errs, body, parseCancel)
	}, func(ctx context.Context, client *stripe.Client, sub *subscriptionRow, payload any) (*reply, error) {
		if payload.(bool) {
			_, err := client.V1Subscriptions.Cancel(ctx, sub.StripeSubscriptionID, nil)
			return nil, stripeFailure("subscription cancel", err)
		}
		_, err := client.V1Subscriptions.Update(ctx, sub.StripeSubscriptionID,
			&stripe.SubscriptionUpdateParams{CancelAtPeriodEnd: stripe.Bool(true)})
		return nil, stripeFailure("subscription update", err)
	})
}

// reactivateSubscription is reactivate_subscription.
func (h handlers) reactivateSubscription(w http.ResponseWriter, r *http.Request) {
	h.mutate(w, r, "reactivate subscription", nil,
		func(ctx context.Context, client *stripe.Client, sub *subscriptionRow, _ any) (*reply, error) {
			_, err := client.V1Subscriptions.Update(ctx, sub.StripeSubscriptionID,
				&stripe.SubscriptionUpdateParams{CancelAtPeriodEnd: stripe.Bool(false)})
			return nil, stripeFailure("subscription update", err)
		})
}
