package billing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The router's subscription handlers, over the event read as a dict whose
// fields are attributes (the named divergence stated in subscription.go):
// a missing field is getattr's default, metadata is a dict, and items is
// the Stripe items list.

// errNotificationOrg is uuid.UUID(org_id) failing inside
// _enqueue_billing_notification.
var errNotificationOrg = errors.New("billing: notification org_id is not a UUID")

// errNoProducer is an api built without the job outbox producer.
var errNoProducer = errors.New("billing: job outbox producer is not configured")

// handlerOrgID is `metadata.get("org_id") if isinstance(metadata, dict)
// else None`.
func handlerOrgID(subscription pyjson.Value) pyjson.Value {
	metadata, isObject := attr(subscription, "metadata", pyjson.NewObject()).(*pyjson.Object)
	if !isObject {
		return nil
	}
	orgID, _ := metadata.Get("org_id")
	return orgID
}

// orgTier reads organizations.tier for an org_id value the way the
// handlers do inside their try: nil when the value is not a UUID string,
// the org is missing, its tier is empty, or the read fails.
func (h handlers) orgTier(ctx context.Context, orgID pyjson.Value) *string {
	text, isText := orgID.(string)
	if !isText || h.pool == nil {
		return nil
	}
	org, err := pythonparity.ParseUUID(text)
	if err != nil {
		return nil
	}
	var tier *string
	if err := h.pool.QueryRow(ctx, `SELECT tier FROM organizations WHERE id = $1`, org).Scan(&tier); err != nil {
		return nil
	}
	if tier == nil || *tier == "" {
		return nil
	}
	return tier
}

// subscriptionUpdated is _handle_subscription_updated: the tier from the
// items' prices, then for an org in the metadata a license signed for that
// tier and persisted; a changed tier queues subscription_changed. An items
// data value that cannot be iterated is the route's 500, as in Python.
func (h handlers) subscriptionUpdated(ctx context.Context, subscription pyjson.Value) error {
	customer := attr(subscription, "customer", nil)
	itemsData := attr(subscription, "items", nil)
	if !pyjson.Truthy(itemsData) {
		return nil
	}
	items, err := iterate(attr(itemsData, "data", []pyjson.Value{}), false)
	if err != nil {
		return err
	}
	var priceIDs []pyjson.Value
	for _, item := range items {
		if price := attr(item, "price", nil); pyjson.Truthy(price) {
			priceIDs = append(priceIDs, attr(price, "id", nil))
		}
	}
	tier, err := h.lineItemsTier(ctx, priceIDs)
	if err != nil {
		return err
	}
	orgID := handlerOrgID(subscription)
	if !pyjson.Truthy(orgID) {
		h.logger.InfoContext(ctx, "subscription.updated without org_id metadata", "customer", pyStr(customer))
		return nil
	}
	oldTier := h.orgTier(ctx, orgID)
	orgText, isText := orgID.(string)
	key := h.licenseKey.Reveal()
	if !isText || key == "" {
		h.logger.ErrorContext(ctx, "Failed to regenerate license", "org_id", pyStr(orgID))
		return nil
	}
	license, err := licensing.SignLicense(key, licensing.LicenseRequest{OrgID: orgText, Tier: tier,
		IssuedAt: h.now().Unix(), LicenseID: uuid.NewString()})
	if err != nil {
		h.logger.ErrorContext(ctx, "Failed to regenerate license", "org_id", orgText, "error", err.Error())
		return nil
	}
	h.logger.InfoContext(ctx, "License regenerated on subscription update", "org_id", orgText, "tier", tier)
	persisted := h.persistLicense(ctx, orgText, tier, license, customer)
	if persisted && oldTier != nil && *oldTier != tier {
		attributes := pyjson.NewObject()
		attributes.Set("old_tier", *oldTier)
		attributes.Set("new_tier", tier)
		if err := h.enqueueBillingNotification(ctx, "subscription_changed", orgID, attributes); err != nil {
			h.logger.WarnContext(ctx, "Failed to enqueue subscription changed email", "org_id", orgText, "error", err.Error())
		}
	}
	return nil
}

// subscriptionDeleted is _handle_subscription_deleted: for an org in the
// metadata, its current tier read, its license revoked, and
// subscription_cancelled queued with that tier.
func (h handlers) subscriptionDeleted(ctx context.Context, subscription pyjson.Value) {
	orgID := handlerOrgID(subscription)
	customer := attr(subscription, "customer", nil)
	if !pyjson.Truthy(orgID) {
		h.logger.InfoContext(ctx, "subscription.deleted without org_id metadata", "customer", pyStr(customer))
		return
	}
	h.logger.InfoContext(ctx, "Subscription deleted; org reverts to COMMUNITY", "org_id", pyStr(orgID), "customer", pyStr(customer))
	currentTier := "unknown"
	if tier := h.orgTier(ctx, orgID); tier != nil {
		currentTier = *tier
	}
	h.revokeLicense(ctx, orgID)
	attributes := pyjson.NewObject()
	attributes.Set("tier", currentTier)
	if err := h.enqueueBillingNotification(ctx, "subscription_cancelled", orgID, attributes); err != nil {
		h.logger.WarnContext(ctx, "Failed to enqueue subscription cancelled email", "org_id", pyStr(orgID), "error", err.Error())
	}
}

// revokeLicense is _revoke_license: an org managed manually is left as it
// is; otherwise the org's tier and its license become community and the
// license invalid (a row already there changes nothing). Failures log.
func (h handlers) revokeLicense(ctx context.Context, orgID pyjson.Value) {
	text, isText := orgID.(string)
	if !isText {
		h.logger.ErrorContext(ctx, "Failed to revoke license", "org_id", pyStr(orgID))
		return
	}
	org, err := pythonparity.ParseUUID(text)
	if err != nil {
		return
	}
	err = h.inTxFunc(ctx, func(tx pgx.Tx) error {
		var orgManagedBy, licenseManagedBy *string
		orgFound, licenseFound := true, true
		switch err := tx.QueryRow(ctx, `SELECT managed_by FROM organizations WHERE id = $1`, org).Scan(&orgManagedBy); {
		case errors.Is(err, pgx.ErrNoRows):
			orgFound = false
		case err != nil:
			return err
		}
		switch err := tx.QueryRow(ctx, `SELECT managed_by FROM org_licenses WHERE org_id = $1`, org).Scan(&licenseManagedBy); {
		case errors.Is(err, pgx.ErrNoRows):
			licenseFound = false
		case err != nil:
			return err
		}
		if (orgFound && orgManagedBy != nil && *orgManagedBy == "manual") ||
			(licenseFound && licenseManagedBy != nil && *licenseManagedBy == "manual") {
			h.logger.InfoContext(ctx, "Stripe subscription.deleted ignored for manually-managed org; tier preserved.", "org_id", text)
			return nil
		}
		now := h.nowUTC()
		if _, err := tx.Exec(ctx, `UPDATE organizations SET tier = 'community', updated_at = $2
			WHERE id = $1 AND tier IS DISTINCT FROM 'community'`, org, now); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE org_licenses SET is_valid = false, tier = 'community', updated_at = $2
			WHERE org_id = $1 AND (is_valid OR tier IS DISTINCT FROM 'community')`, org, now)
		return err
	})
	if err != nil {
		h.logger.ErrorContext(ctx, "Failed to revoke license", "org_id", text, "error", err.Error())
	}
}

// trialWillEnd is _handle_trial_will_end: for an org in the metadata and a
// readable trial_end, trial_expiring queued with the days left (rounded
// up) and the trial's end date. A trial_end past time_t is the route's
// 500 (Python's OverflowError is outside its except).
func (h handlers) trialWillEnd(ctx context.Context, subscription pyjson.Value) error {
	orgID := handlerOrgID(subscription)
	customer := attr(subscription, "customer", nil)
	if !pyjson.Truthy(orgID) {
		h.logger.InfoContext(ctx, "subscription.trial_will_end without org_id metadata", "customer", pyStr(customer))
		return nil
	}
	raw := attr(subscription, "trial_end", nil)
	if raw == nil {
		h.logger.InfoContext(ctx, "subscription.trial_will_end missing trial_end", "org_id", pyStr(orgID))
		return nil
	}
	trialEnd, err := trialEndTime(raw)
	switch {
	case errors.Is(err, errTimestampOverflow):
		return err
	case err != nil:
		h.logger.WarnContext(ctx, "subscription.trial_will_end invalid trial_end", "trial_end", pyStr(raw), "org_id", pyStr(orgID))
		return nil
	}
	now := h.now().UTC().Truncate(time.Microsecond)
	// int((trial_end - now).total_seconds()): the microsecond difference
	// divided exactly, then truncated toward zero.
	difference := (trialEnd.Unix()-now.Unix())*1_000_000 - int64(now.Nanosecond()/1000)
	quotient, _ := new(big.Rat).SetFrac64(difference, 1_000_000).Float64()
	remaining := max(int64(0), int64(math.Trunc(quotient)))
	days := (remaining + 86399) / 86400
	attributes := pyjson.NewObject()
	attributes.Set("days_remaining", pyjson.IntOf(days))
	attributes.Set("trial_end_date", trialEnd.Format(time.DateOnly))
	if err := h.enqueueBillingNotification(ctx, "trial_expiring", orgID, attributes); err != nil {
		h.logger.WarnContext(ctx, "Failed to enqueue trial expiring email", "org_id", pyStr(orgID), "error", err.Error())
		return nil
	}
	h.logger.InfoContext(ctx, "Trial ending soon", "org_id", pyStr(orgID), "customer", pyStr(customer), "days_remaining", days)
	return nil
}

// trialEndTime is datetime.fromtimestamp(int(raw), tz=timezone.utc): int()
// of a bool, an int, a float (truncated; NaN is ValueError, an infinity
// OverflowError) or a numeric string (Python's int() grammar); anything
// else is TypeError.
func trialEndTime(raw pyjson.Value) (time.Time, error) {
	var whole *big.Int
	switch typed := raw.(type) {
	case bool:
		whole = big.NewInt(0)
		if typed {
			whole = big.NewInt(1)
		}
	case pyjson.Int:
		whole = typed.Int
	case pyjson.Float:
		value := float64(typed)
		if math.IsNaN(value) {
			return time.Time{}, errTimestamp
		}
		if math.IsInf(value, 0) {
			return time.Time{}, errTimestampOverflow
		}
		whole, _ = new(big.Float).SetFloat64(math.Trunc(value)).Int(nil)
	case string:
		parsed, err := pythonparity.ParseInt(typed)
		if err != nil {
			return time.Time{}, errTimestamp
		}
		whole = parsed
	default:
		return time.Time{}, errTimestamp
	}
	if !whole.IsInt64() {
		return time.Time{}, errTimestampOverflow
	}
	return unixUTC(whole.Int64(), 0)
}

// enqueueBillingNotification is _enqueue_billing_notification without a
// provider event id: the intent row keyed by
// billing:<type>:<org>:<sha256 of the canonical attributes>, reused when
// the key exists, then its operational.billing_notification handoff
// published to the job outbox.
func (h handlers) enqueueBillingNotification(ctx context.Context, notificationType string, orgID pyjson.Value, attributes *pyjson.Object) error {
	text, isText := orgID.(string)
	if !isText {
		return errNotificationOrg
	}
	org, err := pythonparity.ParseUUID(text)
	if err != nil {
		return errNotificationOrg
	}
	canonical, err := pyjson.MarshalCanonical(attributes)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(canonical)
	key := "billing:" + notificationType + ":" + org.String() + ":" + hex.EncodeToString(digest[:])
	stored, err := pyjson.Dumps(attributes)
	if err != nil {
		return err
	}
	var notificationID uuid.UUID
	err = h.inTxFunc(ctx, func(tx pgx.Tx) error {
		switch err := tx.QueryRow(ctx, `INSERT INTO billing_notifications (id, org_id, notification_type, idempotency_key, attributes, created_at)
			VALUES ($1, $2, $3, $4, $5::json, $6) ON CONFLICT (idempotency_key) DO NOTHING RETURNING id`,
			uuid.New(), org, notificationType, key, stored, h.nowUTC()).Scan(&notificationID); {
		case errors.Is(err, pgx.ErrNoRows):
			return tx.QueryRow(ctx, `SELECT id FROM billing_notifications WHERE idempotency_key = $1`, key).Scan(&notificationID)
		default:
			return err
		}
	})
	if err != nil {
		return err
	}
	if h.producer == nil {
		return errNoProducer
	}
	id, orgText := notificationID.String(), org.String()
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &orgText,
		CorrelationID:   "billing-notification:" + id,
		IdempotencyKey:  key,
		Domain:          jobcontract.DomainLink{Type: "billing_notification", ID: id},
		Payload:         jobcontract.BillingNotificationPayload{NotificationID: id},
	}
	if err := h.producer.PublishStandalone(ctx, jobcontract.KindBillingNotification, envelope); !joboutbox.IsPublished(err) {
		return err
	}
	return nil
}
