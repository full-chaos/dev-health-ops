package billing

import (
	"context"
	"errors"
	"math"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The customer.subscription.* branches of the Stripe webhook. Two halves:
//
//   - processSubscriptionEvent is SubscriptionService.process_event, which
//     reads the deployed StripeObject correctly (it converts with to_dict):
//     parity.
//   - subscriptionUpdated, subscriptionDeleted and trialWillEnd are the
//     router's _handle_subscription_updated/_deleted/_trial_will_end. They
//     test isinstance(metadata, dict), which a StripeObject never is, so on
//     the deployed plane they skip every event (CHAOS-6525). These ports
//     implement what they were written to do, reading the event as a dict
//     whose fields are attributes: a named divergence, compared in the venue
//     against the Python handlers handed that view
//     (VENUE_STRIPE_SUBSCRIPTION_HANDLERS_AS_DICT).

// errSubscriptionMalformed is the ValueError class process_event raises
// (no event id, no data.object, an org_id that is not a UUID, no
// identifiers, no resolvable plan): the router logs a warning and writes
// nothing.
var errSubscriptionMalformed = errors.New("billing: malformed subscription event")

// errSubscriptionValue is a value Python would reach through str() of a
// StripeObject (an object or list where Stripe sends an id); that text
// carries a memory address no port can reproduce. Named limit: Go writes
// nothing for it, where Python writes that text.
var errSubscriptionValue = errors.New("billing: subscription value is an object or list")

// errSubscriptionConflict is process_event's IntegrityError on the event
// row: a rollback of the whole unit and a silent return.
var errSubscriptionConflict = errors.New("billing: subscription event already recorded")

// errNotIterable is Python's TypeError from iterating a value that is not
// iterable (a number, a bool, None, or a StripeObject).
var errNotIterable = errors.New("billing: value is not iterable")

// cancelledStatuses is _CANCELLED_STATUSES.
var cancelledStatuses = map[string]bool{"canceled": true, "incomplete_expired": true}

// stripeTruthy is bool() of a value read off a StripeObject: a JSON object
// is a StripeObject there, and a StripeObject is always true (it defines no
// __len__ or __bool__), even when empty.
func stripeTruthy(value pyjson.Value) bool {
	if _, isObject := value.(*pyjson.Object); isObject {
		return true
	}
	return pyjson.Truthy(value)
}

// stripeStr is str() of a value read off a StripeObject; an object or a
// list is errSubscriptionValue.
func stripeStr(value pyjson.Value) (string, error) {
	switch value.(type) {
	case *pyjson.Object, []pyjson.Value:
		return "", errSubscriptionValue
	}
	return pyjson.Str(value), nil
}

// asDict is SubscriptionService._as_dict over a decoded value: an object
// (a StripeObject's to_dict) as is, anything else {}.
func asDict(value pyjson.Value) *pyjson.Object { return ensureDict(value) }

// processSubscriptionEvent is router._process_subscription_event: one
// session running process_event, committed on success; every failure is
// logged and swallowed.
func (h handlers) processSubscriptionEvent(ctx context.Context, event pyjson.Value, eventType string, subscription pyjson.Value) {
	err := h.inTxFunc(ctx, func(tx pgx.Tx) error {
		return h.subscriptionEventTx(ctx, tx, event, eventType, subscription)
	})
	switch {
	case err == nil, errors.Is(err, errSubscriptionConflict):
	case errors.Is(err, errSubscriptionMalformed):
		h.logger.WarnContext(ctx, "Skipping malformed subscription event", "error", err.Error())
	default:
		h.logger.ErrorContext(ctx, "Failed to process subscription event", "error", err.Error())
	}
}

// subscriptionEventTx is SubscriptionService.process_event.
func (h handlers) subscriptionEventTx(ctx context.Context, tx pgx.Tx, event pyjson.Value, eventType string, subscription pyjson.Value) error {
	eventID, err := stripeStr(attr(event, "id", ""))
	if err != nil {
		return err
	}
	if eventID == "" {
		return errSubscriptionMalformed
	}
	var seen int
	switch err := tx.QueryRow(ctx, `SELECT 1 FROM subscription_events WHERE stripe_event_id = $1`, eventID).Scan(&seen); {
	case err == nil:
		return nil
	case !errors.Is(err, pgx.ErrNoRows):
		return err
	}
	if !strings.HasPrefix(eventType, "customer.subscription.") {
		return nil
	}
	if subscription == nil {
		return errSubscriptionMalformed
	}
	metadata := asDict(attr(subscription, "metadata", pyjson.NewObject()))
	orgValue, _ := metadata.Get("org_id")
	if !pyjson.Truthy(orgValue) {
		h.logger.WarnContext(ctx, "Subscription event has no org_id in metadata, skipping", "event_id", eventID)
		return nil
	}
	org, err := pythonparity.ParseUUID(pyjson.Str(orgValue))
	if err != nil {
		return errSubscriptionMalformed
	}
	var previousStatus *string
	if rawID := attr(subscription, "id", ""); stripeTruthy(rawID) {
		// _get_previous_status binds the raw value: asyncpg refuses any
		// value that is not a str for a text parameter.
		text, isText := rawID.(string)
		if !isText {
			return errSubscriptionValue
		}
		if err := tx.QueryRow(ctx, `SELECT status FROM subscriptions WHERE stripe_subscription_id = $1`, text).
			Scan(&previousStatus); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
	}
	row, err := h.upsertSubscription(ctx, tx, subscription, org, metadata)
	if err != nil {
		return err
	}
	payload, err := pyjson.Dumps(event)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO subscription_events (id, subscription_id, stripe_event_id, event_type, previous_status, new_status, payload)
		VALUES ($1, $2, $3, $4, $5, $6, $7::json)`, uuid.New(), row.id, eventID, eventType, previousStatus, row.status, payload)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "23") {
		return errSubscriptionConflict
	}
	return err
}

// upsertedSubscription is the part of a subscriptions row the upsert carries.
type upsertedSubscription struct {
	id, pricePlan, price   uuid.UUID
	status                 string
	periodStart, periodEnd time.Time
	customer               string
}

// upsertSubscription is SubscriptionService.upsert_from_stripe, including
// _sync_org_license in the same transaction.
func (h handlers) upsertSubscription(ctx context.Context, tx pgx.Tx, subscription pyjson.Value, org uuid.UUID, metadata *pyjson.Object) (upsertedSubscription, error) {
	subscriptionID, err := stripeStr(attr(subscription, "id", ""))
	if err != nil {
		return upsertedSubscription{}, err
	}
	customer, err := stripeStr(attr(subscription, "customer", ""))
	if err != nil {
		return upsertedSubscription{}, err
	}
	if subscriptionID == "" || customer == "" {
		return upsertedSubscription{}, errSubscriptionMalformed
	}
	var existing upsertedSubscription
	found := true
	switch err := tx.QueryRow(ctx, `SELECT id, billing_plan_id, billing_price_id, status, current_period_start, current_period_end
		FROM subscriptions WHERE stripe_subscription_id = $1`, subscriptionID).
		Scan(&existing.id, &existing.pricePlan, &existing.price, &existing.status, &existing.periodStart, &existing.periodEnd); {
	case errors.Is(err, pgx.ErrNoRows):
		found = false
	case err != nil:
		return upsertedSubscription{}, err
	}
	row := upsertedSubscription{customer: customer}
	priceID, err := subscriptionPriceID(subscription)
	if err != nil {
		return upsertedSubscription{}, err
	}
	resolved := false
	if priceID != "" {
		switch err := tx.QueryRow(ctx, `SELECT id, plan_id FROM billing_prices WHERE stripe_price_id = $1`, priceID).Scan(&row.price, &row.pricePlan); {
		case err == nil:
			resolved = true
		case !errors.Is(err, pgx.ErrNoRows):
			return upsertedSubscription{}, err
		}
	}
	if !resolved {
		if !found {
			return upsertedSubscription{}, errSubscriptionMalformed
		}
		row.price, row.pricePlan = existing.price, existing.pricePlan
	}
	now := h.nowUTC()
	// Status and the period bounds: on an update an absent key keeps the
	// stored value (getattr's default); a present one converts, None and
	// unreadable values becoming now.
	status, present := objectGet(subscription, "status")
	switch {
	case present:
		if row.status, err = stripeStr(status); err != nil {
			return upsertedSubscription{}, err
		}
	case found:
		row.status = existing.status
	default:
		row.status = "incomplete"
	}
	for _, bound := range []struct {
		key    string
		stored time.Time
		target *time.Time
	}{{"current_period_start", existing.periodStart, &row.periodStart}, {"current_period_end", existing.periodEnd, &row.periodEnd}} {
		value, present := objectGet(subscription, bound.key)
		if found && !present {
			*bound.target = bound.stored
			continue
		}
		at, err := toDatetime(value, false, now)
		if err != nil {
			return upsertedSubscription{}, err
		}
		*bound.target = *at
	}
	cancelAtPeriodEnd := stripeTruthy(attr(subscription, "cancel_at_period_end", false))
	var nullable [3]*time.Time
	for index, key := range []string{"canceled_at", "trial_start", "trial_end"} {
		if nullable[index], err = toDatetime(attr(subscription, key, nil), true, now); err != nil {
			return upsertedSubscription{}, err
		}
	}
	metadataText, err := pyjson.Dumps(metadata)
	if err != nil {
		return upsertedSubscription{}, err
	}
	if found {
		row.id = existing.id
		_, err = tx.Exec(ctx, `UPDATE subscriptions SET org_id = $2, billing_plan_id = $3, billing_price_id = $4,
			stripe_customer_id = $5, status = $6, current_period_start = $7, current_period_end = $8,
			cancel_at_period_end = $9, canceled_at = $10, trial_start = $11, trial_end = $12, metadata = $13::json,
			updated_at = $14 WHERE id = $1`,
			row.id, org, row.pricePlan, row.price, customer, row.status, row.periodStart, row.periodEnd,
			cancelAtPeriodEnd, nullable[0], nullable[1], nullable[2], metadataText, now)
	} else {
		row.id = uuid.New()
		_, err = tx.Exec(ctx, `INSERT INTO subscriptions (id, org_id, billing_plan_id, billing_price_id, stripe_subscription_id,
			stripe_customer_id, status, current_period_start, current_period_end, cancel_at_period_end, canceled_at,
			trial_start, trial_end, metadata, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::json, $15)`,
			row.id, org, row.pricePlan, row.price, subscriptionID, customer, row.status, row.periodStart, row.periodEnd,
			cancelAtPeriodEnd, nullable[0], nullable[1], nullable[2], metadataText, now)
	}
	if err != nil {
		return upsertedSubscription{}, err
	}
	return row, h.syncOrgLicense(ctx, tx, org, row, now)
}

// objectGet is a key of an object: present is false for a missing key or
// a value that is not an object (getattr's default applies).
func objectGet(value pyjson.Value, name string) (pyjson.Value, bool) {
	object, isObject := value.(*pyjson.Object)
	if !isObject {
		return nil, false
	}
	return object.Get(name)
}

// subscriptionPriceID is _extract_stripe_price_id: the first item whose
// price has a truthy id, as str(); "" when there is none.
func subscriptionPriceID(subscription pyjson.Value) (string, error) {
	data := attr(attr(subscription, "items", nil), "data", []pyjson.Value{})
	if !stripeTruthy(data) {
		return "", nil
	}
	items, err := iterate(data, true)
	if err != nil {
		return "", err
	}
	for _, item := range items {
		price := attr(item, "price", nil)
		if price == nil {
			continue
		}
		if id := attr(price, "id", nil); stripeTruthy(id) {
			return stripeStr(id)
		}
	}
	return "", nil
}

// iterate is a for loop over a decoded value: a list's items, a string's
// characters, a dict's keys. A StripeObject is not iterable (stripeObject),
// and neither are None, bools and numbers.
func iterate(value pyjson.Value, stripeObject bool) ([]pyjson.Value, error) {
	switch typed := value.(type) {
	case []pyjson.Value:
		return typed, nil
	case string:
		out := make([]pyjson.Value, 0, len(typed))
		for _, r := range typed {
			out = append(out, string(r))
		}
		return out, nil
	case *pyjson.Object:
		if stripeObject {
			return nil, errNotIterable
		}
		out := make([]pyjson.Value, 0, typed.Len())
		for _, key := range typed.Keys() {
			out = append(out, key)
		}
		return out, nil
	}
	return nil, errNotIterable
}

// toDatetime is SubscriptionService._to_dt: None is now (or None when
// nullable); a number is datetime.fromtimestamp(float(value), UTC); a
// string is fromisoformat after "Z" becomes "+00:00" (naive read as UTC),
// now or None when unreadable; anything else now or None. A timestamp
// Python refuses is an error.
func toDatetime(value pyjson.Value, nullable bool, now time.Time) (*time.Time, error) {
	fallback := func() (*time.Time, error) {
		if nullable {
			return nil, nil
		}
		return &now, nil
	}
	var seconds float64
	switch typed := value.(type) {
	case nil:
		return fallback()
	case bool:
		if typed {
			seconds = 1
		}
	case pyjson.Int:
		converted, _ := new(big.Float).SetInt(typed.Int).Float64()
		// float(int) past the largest double is OverflowError.
		if math.IsInf(converted, 0) {
			return nil, errTimestampOverflow
		}
		seconds = converted
	case pyjson.Float:
		seconds = float64(typed)
	case string:
		parsed, ok := pytime.FromISOFormat(strings.ReplaceAll(typed, "Z", "+00:00"))
		if !ok {
			return fallback()
		}
		at := parsed.Time.UTC()
		return &at, nil
	default:
		return fallback()
	}
	at, err := fromTimestamp(seconds)
	if err != nil {
		return nil, err
	}
	return &at, nil
}

// errTimestamp is fromtimestamp's refusal (OverflowError, ValueError or
// OSError).
var errTimestamp = errors.New("billing: timestamp out of range")

// errTimestampOverflow is the OverflowError part of it: a value past
// time_t, which trial_will_end's except clause does not catch.
var errTimestampOverflow = errors.New("billing: timestamp out of range for platform time_t")

// fromTimestamp is datetime.fromtimestamp(seconds, timezone.utc): the
// fraction rounded half-even to microseconds, the whole seconds within
// time_t (else errTimestampOverflow), the year within 1..9999 (else
// errTimestamp).
func fromTimestamp(seconds float64) (time.Time, error) {
	if math.IsNaN(seconds) {
		return time.Time{}, errTimestamp
	}
	if math.IsInf(seconds, 0) {
		return time.Time{}, errTimestampOverflow
	}
	whole, fraction := math.Modf(seconds)
	micro := math.RoundToEven(fraction * 1e6)
	if micro >= 1e6 {
		micro -= 1e6
		whole++
	} else if micro < 0 {
		micro += 1e6
		whole--
	}
	if whole < -9.223372036854775808e18 || whole >= 9.223372036854775808e18 {
		return time.Time{}, errTimestampOverflow
	}
	return unixUTC(int64(whole), int64(micro))
}

// unixUTC is the UTC datetime of whole seconds and microseconds, refused
// outside Python's years 1..9999.
func unixUTC(seconds, micro int64) (time.Time, error) {
	const minSeconds, maxSeconds = -62135596800, 253402300799 // 0001-01-01, 9999-12-31T23:59:59
	if seconds < minSeconds || seconds > maxSeconds {
		return time.Time{}, errTimestamp
	}
	return time.Unix(seconds, micro*1000).UTC(), nil
}

// syncOrgLicense is SubscriptionService._sync_org_license: the org's
// license follows the subscription's plan (its tier, and its bundles'
// features that the registry knows), or community with no features once
// the subscription is cancelled. A missing plan leaves the license as it
// is; an org or license managed manually is left as it is.
func (h handlers) syncOrgLicense(ctx context.Context, tx pgx.Tx, org uuid.UUID, row upsertedSubscription, now time.Time) error {
	status := row.status
	if status == "" {
		status = "active"
	}
	cancelled := cancelledStatuses[status]
	tier := "community"
	flags := pyjson.NewObject()
	var expires *time.Time
	if !cancelled {
		var planTier *string
		switch err := tx.QueryRow(ctx, `SELECT tier FROM billing_plans WHERE id = $1`, row.pricePlan).Scan(&planTier); {
		case errors.Is(err, pgx.ErrNoRows):
			h.logger.WarnContext(ctx, "BillingPlan not found for subscription; skipping org-license sync", "org_id", org.String())
			return nil
		case err != nil:
			return err
		}
		if planTier != nil {
			tier = normalizeBillingTier(*planTier, "community")
		}
		keys, err := planFeatureKeys(ctx, tx, row.pricePlan)
		if err != nil {
			return err
		}
		for _, key := range keys {
			flags.Set(key, true)
		}
		expires = &row.periodEnd
	}
	var licenseID uuid.UUID
	var licenseManagedBy *string
	licenseFound := true
	switch err := tx.QueryRow(ctx, `SELECT id, managed_by FROM org_licenses WHERE org_id = $1`, org).Scan(&licenseID, &licenseManagedBy); {
	case errors.Is(err, pgx.ErrNoRows):
		licenseFound = false
	case err != nil:
		return err
	}
	var orgManagedBy, orgTier *string
	orgFound := true
	switch err := tx.QueryRow(ctx, `SELECT managed_by, tier FROM organizations WHERE id = $1`, org).Scan(&orgManagedBy, &orgTier); {
	case errors.Is(err, pgx.ErrNoRows):
		orgFound = false
	case err != nil:
		return err
	}
	if (orgFound && orgManagedBy != nil && *orgManagedBy == "manual") ||
		(licenseFound && licenseManagedBy != nil && *licenseManagedBy == "manual") {
		h.logger.InfoContext(ctx, "Skipping Stripe org-license sync for manually-managed org", "org_id", org.String())
		return nil
	}
	flagsText, err := pyjson.Dumps(flags)
	if err != nil {
		return err
	}
	if !licenseFound {
		var customer *string
		if row.customer != "" {
			customer = &row.customer
		}
		_, err = tx.Exec(ctx, `INSERT INTO org_licenses (id, org_id, tier, license_key, licensed_users, licensed_repos,
			issued_at, expires_at, is_valid, validation_error, last_validated_at, license_type, customer_id, managed_by,
			features_override, limits_override, created_at, updated_at)
			VALUES ($1, $2, $3, NULL, NULL, NULL, NULL, $4, true, NULL, NULL, 'saas', $5, 'stripe', $6::json, '{}'::json, $7, $7)`,
			uuid.New(), org, tier, expires, customer, flagsText, now)
	} else {
		sets := `tier = $2, features_override = $3::json, expires_at = $4, is_valid = $5, updated_at = $6`
		args := []any{licenseID, tier, flagsText, expires, !cancelled, now}
		if row.customer != "" {
			sets += `, customer_id = $7`
			args = append(args, row.customer)
		}
		_, err = tx.Exec(ctx, `UPDATE org_licenses SET `+sets+` WHERE id = $1`, args...)
	}
	if err != nil {
		return err
	}
	if orgFound && (orgTier == nil || *orgTier != tier) {
		if _, err := tx.Exec(ctx, `UPDATE organizations SET tier = $2, updated_at = $3 WHERE id = $1`, org, tier, now); err != nil {
			return err
		}
	}
	h.logger.InfoContext(ctx, "OrgLicense synced", "org_id", org.String(), "tier", tier,
		"features", flags.Len(), "cancelled", cancelled)
	return nil
}

// planFeatureKeys is the bundle read of _sync_org_license: every bundle of
// the plan, its features (a dict's keys, a list's strings, else none) kept
// when the registry knows them, deduplicated and sorted.
func planFeatureKeys(ctx context.Context, tx pgx.Tx, plan uuid.UUID) ([]string, error) {
	rows, err := tx.Query(ctx, `SELECT fb.features::text FROM feature_bundles fb
		JOIN plan_feature_bundles pfb ON pfb.bundle_id = fb.id WHERE pfb.plan_id = $1`, plan)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	set := map[string]bool{}
	for rows.Next() {
		var text *string
		if err := rows.Scan(&text); err != nil {
			return nil, err
		}
		var features pyjson.Value
		if text != nil {
			if features, err = pyjson.DecodeString(*text); err != nil {
				return nil, err
			}
		}
		var candidates []string
		switch typed := features.(type) {
		case *pyjson.Object:
			candidates = typed.Keys()
		case []pyjson.Value:
			for _, item := range typed {
				if key, isText := item.(string); isText {
					candidates = append(candidates, key)
				}
			}
		}
		for _, key := range candidates {
			if licensing.IsStandardFeature(key) {
				set[key] = true
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}
