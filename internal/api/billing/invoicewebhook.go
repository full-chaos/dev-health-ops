package billing

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The invoice.* branch of the Stripe webhook. The Python handler cannot run
// (CHAOS-6526: its duplicate-event check raises, its dedupe row would break
// subscription_events' NOT NULL columns, it reads the org only from the
// invoice's own metadata, which Stripe leaves empty on subscription
// invoices, and it parses the Stripe subscription id as a UUID). This port
// processes the event as Stripe sends it today, a named divergence:
//
//   - The invoice row is upserted on its unique Stripe id and its line items
//     are replaced, in one transaction, so a redelivered event writes the
//     same state again instead of a second one. Notification intents are
//     keyed by the Stripe event id, so a redelivery reuses its intent.
//   - Status moves forward only (draft, open, then paid, void or
//     uncollectible; paid and void are final, uncollectible may still be
//     paid): an event that would move it back changes nothing.
//   - The org is the first of: the invoice's metadata.org_id, the
//     subscription's metadata.org_id (parent.subscription_details), the local
//     subscription row for the Stripe subscription id, the license whose
//     customer_id is the Stripe customer. None found: logged, nothing
//     written, 200.

// invoiceStatusRank orders invoice statuses: a later rank is never replaced
// by an earlier one.
func invoiceStatusRank(status string) int {
	switch status {
	case "paid", "void", "uncollectible":
		return 2
	case "open", "payment_failed":
		return 1
	}
	return 0
}

// invoiceStatusFollows reports whether status may replace stored: never a
// lower rank; among the terminal ones only uncollectible may still become
// paid (Stripe lets an uncollectible invoice be paid later), and paid and
// void are final.
func invoiceStatusFollows(stored, status string) bool {
	switch {
	case invoiceStatusRank(status) < invoiceStatusRank(stored):
		return false
	case invoiceStatusRank(stored) < 2 || stored == status:
		return true
	}
	return stored == "uncollectible" && status == "paid"
}

// invoiceLineEvents are the event types that carry the invoice's current
// lines.
var invoiceLineEvents = map[string]bool{
	"invoice.created": true, "invoice.updated": true, "invoice.finalized": true,
	"invoice.paid": true, "invoice.payment_failed": true,
}

// invoiceString is a string-valued field of an object ("" when absent or not
// a string).
func invoiceString(value pyjson.Value, name string) string {
	text, _ := attr(value, name, nil).(string)
	return text
}

// stripeRef is an expandable Stripe reference: the string itself, or an
// expanded object's id.
func stripeRef(value pyjson.Value) string {
	if text, isText := value.(string); isText {
		return text
	}
	return invoiceString(value, "id")
}

// intField is an integer field (fallback when absent, null or not an
// integer that fits int32, the column type).
func intField(value pyjson.Value, name string, fallback int64) int64 {
	number, isInt := attr(value, name, nil).(pyjson.Int)
	if !isInt || !number.IsInt64() || number.Int64() > 1<<31-1 || number.Int64() < -1<<31 {
		return fallback
	}
	return number.Int64()
}

// unixField is a Unix-seconds field as a UTC time (nil when absent or not a
// valid timestamp).
func unixField(value pyjson.Value, name string) *time.Time {
	number, isInt := attr(value, name, nil).(pyjson.Int)
	if !isInt || !number.IsInt64() {
		return nil
	}
	at, err := unixUTC(number.Int64(), 0)
	if err != nil {
		return nil
	}
	return &at
}

// invoiceEvent is the invoice branch. An error is infrastructure (the
// database): the route answers 500 so Stripe retries. Everything the
// payload decides is logged and answered 200.
func (h handlers) invoiceEvent(ctx context.Context, eventType string, eventID pyjson.Value, invoice pyjson.Value) error {
	stripeInvoiceID := invoiceString(invoice, "id")
	if stripeInvoiceID == "" {
		h.logger.WarnContext(ctx, "Skipping invoice webhook event without an invoice id", "type", eventType)
		return nil
	}
	subscriptionDetails := attr(attr(invoice, "parent", nil), "subscription_details", nil)
	stripeSubscriptionID := stripeRef(attr(subscriptionDetails, "subscription", nil))
	if stripeSubscriptionID == "" {
		stripeSubscriptionID = stripeRef(attr(invoice, "subscription", nil))
	}
	customer := stripeRef(attr(invoice, "customer", nil))
	metadata := ensureDict(attr(invoice, "metadata", nil))
	now := h.nowUTC()
	var org uuid.UUID
	resolved, applied := false, false
	err := h.inTxFunc(ctx, func(tx pgx.Tx) error {
		var localSubscription *uuid.UUID
		var subscriptionOrg *uuid.UUID
		if stripeSubscriptionID != "" {
			var id, subOrg uuid.UUID
			switch err := tx.QueryRow(ctx, `SELECT id, org_id FROM subscriptions WHERE stripe_subscription_id = $1`,
				stripeSubscriptionID).Scan(&id, &subOrg); {
			case err == nil:
				localSubscription, subscriptionOrg = &id, &subOrg
			case !errors.Is(err, pgx.ErrNoRows):
				return err
			}
		}
		candidates := []pyjson.Value{}
		if value, present := metadata.Get("org_id"); present {
			candidates = append(candidates, value)
		}
		if value, present := ensureDict(attr(subscriptionDetails, "metadata", nil)).Get("org_id"); present {
			candidates = append(candidates, value)
		}
		for _, candidate := range candidates {
			text, isText := candidate.(string)
			if !isText {
				continue
			}
			parsed, err := pythonparity.ParseUUID(text)
			if err != nil {
				continue
			}
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM organizations WHERE id = $1)`, parsed).Scan(&exists); err != nil {
				return err
			}
			if exists {
				org, resolved = parsed, true
				break
			}
		}
		if !resolved && subscriptionOrg != nil {
			org, resolved = *subscriptionOrg, true
		}
		if !resolved && customer != "" {
			switch err := tx.QueryRow(ctx, `SELECT org_id FROM org_licenses WHERE customer_id = $1 ORDER BY org_id LIMIT 1`,
				customer).Scan(&org); {
			case err == nil:
				resolved = true
			case !errors.Is(err, pgx.ErrNoRows):
				return err
			}
		}
		if !resolved {
			return nil
		}
		var err error
		applied, err = h.upsertInvoice(ctx, tx, eventType, invoice, stripeInvoiceID, org, localSubscription, customer, metadata, now)
		return err
	})
	if err != nil {
		return err
	}
	if !resolved {
		h.logger.WarnContext(ctx, "Skipping invoice webhook event: no org for the invoice", "type", eventType,
			"invoice", stripeInvoiceID, "customer", customer)
		return nil
	}
	if !applied {
		// An event older than the stored status queues nothing either.
		return nil
	}
	h.logger.InfoContext(ctx, "Invoice webhook event applied", "type", eventType, "invoice", stripeInvoiceID, "org_id", org.String())
	return h.invoiceNotification(ctx, eventType, eventID, invoice, org)
}

// upsertInvoice writes the invoice row (status forward only) and, for the
// events that carry them, its line items; applied is false for an event
// older than the stored status, which writes nothing.
func (h handlers) upsertInvoice(ctx context.Context, tx pgx.Tx, eventType string, invoice pyjson.Value, stripeInvoiceID string,
	org uuid.UUID, subscription *uuid.UUID, customer string, metadata *pyjson.Object, now time.Time) (applied bool, err error) {
	var invoiceID uuid.UUID
	var storedStatus string
	var storedPaidAt, storedVoidedAt *time.Time
	found := true
	switch err := tx.QueryRow(ctx, `SELECT id, status, paid_at, voided_at FROM invoices WHERE stripe_invoice_id = $1 FOR UPDATE`,
		stripeInvoiceID).Scan(&invoiceID, &storedStatus, &storedPaidAt, &storedVoidedAt); {
	case errors.Is(err, pgx.ErrNoRows):
		found = false
	case err != nil:
		return false, err
	}
	status := invoiceString(invoice, "status")
	if status == "" {
		status = "draft"
	}
	switch eventType {
	case "invoice.paid":
		status = "paid"
	case "invoice.voided":
		status = "void"
	case "invoice.marked_uncollectible":
		status = "uncollectible"
	case "invoice.payment_failed":
		if invoiceStatusRank(status) < 2 {
			status = "payment_failed"
		}
	}
	if found && !invoiceStatusFollows(storedStatus, status) {
		h.logger.InfoContext(ctx, "Invoice webhook event older than the stored status; not applied",
			"type", eventType, "invoice", stripeInvoiceID, "stored", storedStatus, "event_status", status)
		return false, nil
	}
	transitions := attr(invoice, "status_transitions", nil)
	paidAt, voidedAt := unixField(transitions, "paid_at"), unixField(transitions, "voided_at")
	amountRemaining := intField(invoice, "amount_remaining", 0)
	switch status {
	case "paid":
		amountRemaining = 0
		if paidAt == nil {
			paidAt = storedPaidAt
		}
		if paidAt == nil {
			paidAt = &now
		}
	case "void":
		if voidedAt == nil {
			voidedAt = storedVoidedAt
		}
		if voidedAt == nil {
			voidedAt = &now
		}
	}
	currency := pythonparity.Lower(invoiceString(invoice, "currency"))
	if currency == "" {
		currency = "usd"
	}
	metadataText, err := pyjson.Dumps(metadata)
	if err != nil {
		return false, err
	}
	var hosted, pdf, paymentIntent *string
	if text := invoiceString(invoice, "hosted_invoice_url"); text != "" {
		hosted = &text
	}
	if text := invoiceString(invoice, "invoice_pdf"); text != "" {
		pdf = &text
	}
	if text := stripeRef(attr(invoice, "payment_intent", nil)); text != "" {
		paymentIntent = &text
	}
	if !found {
		invoiceID = uuid.New()
	}
	if _, err := tx.Exec(ctx, `INSERT INTO invoices (id, org_id, subscription_id, stripe_invoice_id, stripe_customer_id, status,
		amount_due, amount_paid, amount_remaining, currency, period_start, period_end, hosted_invoice_url, pdf_url,
		payment_intent_id, finalized_at, paid_at, voided_at, attempt_count, metadata, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20::json, $21, $21)
		ON CONFLICT (stripe_invoice_id) DO UPDATE SET org_id = EXCLUDED.org_id, subscription_id = EXCLUDED.subscription_id,
		stripe_customer_id = EXCLUDED.stripe_customer_id, status = EXCLUDED.status, amount_due = EXCLUDED.amount_due,
		amount_paid = EXCLUDED.amount_paid, amount_remaining = EXCLUDED.amount_remaining, currency = EXCLUDED.currency,
		period_start = EXCLUDED.period_start, period_end = EXCLUDED.period_end,
		hosted_invoice_url = EXCLUDED.hosted_invoice_url, pdf_url = EXCLUDED.pdf_url,
		payment_intent_id = COALESCE(EXCLUDED.payment_intent_id, invoices.payment_intent_id),
		finalized_at = EXCLUDED.finalized_at, paid_at = EXCLUDED.paid_at, voided_at = EXCLUDED.voided_at,
		attempt_count = EXCLUDED.attempt_count, metadata = EXCLUDED.metadata, updated_at = EXCLUDED.updated_at`,
		invoiceID, org, subscription, stripeInvoiceID, customer, status,
		intField(invoice, "amount_due", 0), intField(invoice, "amount_paid", 0), amountRemaining, currency,
		unixField(invoice, "period_start"), unixField(invoice, "period_end"), hosted, pdf, paymentIntent,
		unixField(transitions, "finalized_at"), paidAt, voidedAt, intField(invoice, "attempt_count", 0), metadataText, now); err != nil {
		return false, err
	}
	if !invoiceLineEvents[eventType] {
		return true, nil
	}
	lines, isList := attr(attr(invoice, "lines", nil), "data", nil).([]pyjson.Value)
	if !isList {
		return true, nil
	}
	if _, err := tx.Exec(ctx, `DELETE FROM invoice_line_items WHERE invoice_id = $1`, invoiceID); err != nil {
		return false, err
	}
	for _, line := range lines {
		var lineID, description, priceID *string
		if text := invoiceString(line, "id"); text != "" {
			lineID = &text
		}
		if text := invoiceString(line, "description"); text != "" {
			description = &text
		}
		price := stripeRef(attr(attr(attr(line, "pricing", nil), "price_details", nil), "price", nil))
		if price == "" {
			price = stripeRef(attr(line, "price", nil))
		}
		if price != "" {
			priceID = &price
		}
		period := attr(line, "period", nil)
		if _, err := tx.Exec(ctx, `INSERT INTO invoice_line_items (id, invoice_id, stripe_line_item_id, description, amount,
			quantity, period_start, period_end, stripe_price_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			uuid.New(), invoiceID, lineID, description, intField(line, "amount", 0), intField(line, "quantity", 1),
			unixField(period, "start"), unixField(period, "end"), priceID); err != nil {
			return false, err
		}
	}
	return true, nil
}

// invoiceNotification queues invoice_receipt for invoice.paid and
// payment_failed for invoice.payment_failed, keyed by the event id. A
// failure is a warning; the invoice state is already committed.
func (h handlers) invoiceNotification(ctx context.Context, eventType string, eventID pyjson.Value, invoice pyjson.Value, org uuid.UUID) error {
	attributes := pyjson.NewObject()
	var notificationType string
	currency := invoiceString(invoice, "currency")
	if currency == "" {
		currency = "usd"
	}
	switch eventType {
	case "invoice.paid":
		notificationType = "invoice_receipt"
		attributes.Set("amount_cents", pyjson.IntOf(intField(invoice, "amount_due", 0)))
		attributes.Set("currency", currency)
		attributes.Set("invoice_url", invoiceString(invoice, "hosted_invoice_url"))
	case "invoice.payment_failed":
		notificationType = "payment_failed"
		attempts := intField(invoice, "attempt_count", 1)
		if attempts == 0 {
			attempts = 1
		}
		attributes.Set("amount_cents", pyjson.IntOf(intField(invoice, "amount_due", 0)))
		attributes.Set("currency", currency)
		attributes.Set("attempt_count", pyjson.IntOf(attempts))
	default:
		return nil
	}
	eventText, _ := eventID.(string)
	if err := h.enqueueBillingNotificationFor(ctx, notificationType, org.String(), eventText, attributes); err != nil {
		h.logger.WarnContext(ctx, "Failed to enqueue invoice email", "type", notificationType, "org_id", org.String(), "error", err.Error())
	}
	return nil
}
