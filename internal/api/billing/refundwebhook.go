package billing

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// refundEvent is RefundService.process_webhook for charge.refunded (every
// refund the charge lists) and charge.refund.updated (the refund itself),
// and, as a Go-only widening, for Stripe's own refund.created, refund.updated
// and refund.failed (the refund itself): the events Stripe sends for every
// refund, where charge.refund.updated is only for selected payment methods.
// The refunds land in one transaction; a redelivery upserts the same rows.
//
// Two named deviations from Python, both from the write-first refund route
// (CHAOS-6632): a refund whose row was reserved before Stripe answered is
// found by its `refund_id` metadata and completed, never inserted a second
// time; and a terminal status is never moved back to pending by an event
// that arrives late.
func (h handlers) refundEvent(ctx context.Context, eventType string, eventID pyjson.Value, dataObject pyjson.Value) error {
	if dataObject == nil {
		h.logger.WarnContext(ctx, "Refund webhook event has no data object; not recorded", "event_id", pyStr(eventID), "event_type", eventType)
		countRefundDecision(ctx, skipNoDataObject)
		return nil
	}
	var refunds []pyjson.Value
	chargeID := ""
	if eventType == "charge.refunded" {
		chargeID = pyTextOrEmpty(attr(dataObject, "id", nil))
		list, isList := attr(attr(dataObject, "refunds", nil), "data", nil).([]pyjson.Value)
		if !isList {
			// Current Stripe API versions leave the refund list off the
			// charge: its refunds arrive as refund events. The event is
			// counted with the unapplied ones so this stays visible.
			h.logger.InfoContext(ctx, "charge.refunded carries no refund list; its refunds are recorded from the refund events",
				"event_id", pyStr(eventID), "charge", chargeID)
			unhandledEvents.Add(ctx, 1, metric.WithAttributes(attribute.String("event_type", eventType)))
			countRefundDecision(ctx, skipChargeWithoutRefund)
			return nil
		}
		refunds = list
	} else {
		refunds = []pyjson.Value{dataObject}
	}
	return h.inTxFunc(ctx, func(tx pgx.Tx) error {
		for _, refund := range refunds {
			if err := h.upsertStripeRefund(ctx, tx, refund, chargeID); err != nil {
				return err
			}
		}
		return nil
	})
}

// pyTextOrEmpty is the text of a string value; "" for anything else.
func pyTextOrEmpty(value pyjson.Value) string {
	text, _ := value.(string)
	return text
}

// terminalRefundStatus is a status Stripe does not leave.
func terminalRefundStatus(status string) bool {
	return status == "succeeded" || status == "failed" || status == "canceled"
}

// storedRefund is the columns of a refunds row the upsert reads.
type storedRefund struct {
	id                                           uuid.UUID
	org                                          uuid.UUID
	invoice                                      *uuid.UUID
	charge, intent, reason, description, failure *string
	currency, status                             string
	amount                                       int32
	stripeRefundID                               *string
}

const storedRefundColumns = `id, org_id, invoice_id, stripe_charge_id, stripe_payment_intent_id, reason, description,
	failure_reason, currency, status, amount, stripe_refund_id`

func scanStoredRefund(row pgx.Row) (storedRefund, error) {
	var stored storedRefund
	err := row.Scan(&stored.id, &stored.org, &stored.invoice, &stored.charge, &stored.intent, &stored.reason,
		&stored.description, &stored.failure, &stored.currency, &stored.status, &stored.amount, &stored.stripeRefundID)
	return stored, err
}

// refundText is a string field that keeps the stored value when the event
// lacks the key and is cleared by a null, as Python's `_obj_get(refund, key,
// stored)` does. A value that is neither text nor null is refused.
func refundText(event pyjson.Value, name string, stored *string) (*string, bool) {
	object, isObject := event.(*pyjson.Object)
	if !isObject {
		return stored, true
	}
	value, present := object.Get(name)
	switch {
	case !present:
		return stored, true
	case value == nil:
		return nil, true
	}
	text, isText := value.(string)
	if !isText {
		return nil, false
	}
	return &text, true
}

// refundRef is an expandable Stripe reference in a refund (charge,
// payment_intent): the string, or an expanded object's id.
func refundRef(event pyjson.Value, name string, stored *string) *string {
	object, isObject := event.(*pyjson.Object)
	if !isObject {
		return stored
	}
	value, present := object.Get(name)
	switch {
	case !present:
		return stored
	case value == nil:
		return nil
	}
	if id := stripeRef(value); id != "" {
		return &id
	}
	return stored
}

func (h handlers) upsertStripeRefund(ctx context.Context, tx pgx.Tx, event pyjson.Value, chargeID string) error {
	stripeRefundID := pyTextOrEmpty(attr(event, "id", nil))
	if stripeRefundID == "" {
		h.logger.WarnContext(ctx, "Refund webhook event names no refund id; not recorded")
		countRefundDecision(ctx, skipNoRefundID)
		return nil
	}
	var amount *int64
	if number, isInt := attr(event, "amount", nil).(pyjson.Int); isInt {
		if !fitsInt4(number) {
			h.logger.ErrorContext(ctx, "Refusing refund webhook event: the amount exceeds the stored integer range",
				"stripe_refund_id", stripeRefundID)
			countRefundDecision(ctx, skipAmount)
			return nil
		}
		value := number.Int64()
		amount = &value
	} else if raw := attr(event, "amount", nil); raw != nil {
		h.logger.ErrorContext(ctx, "Refusing refund webhook event: the amount is not an integer", "stripe_refund_id", stripeRefundID)
		countRefundDecision(ctx, skipAmount)
		return nil
	}
	metadata := ensureDict(attr(event, "metadata", nil))
	var metaOrg, metaInvoice, metaRow *uuid.UUID
	for name, target := range map[string]**uuid.UUID{"org_id": &metaOrg, "invoice_id": &metaInvoice, "refund_id": &metaRow} {
		if text, isText := attr(metadata, name, nil).(string); isText && text != "" {
			if id, err := pythonparity.ParseUUID(text); err == nil {
				*target = &id
			}
		}
	}

	intent := ""
	if ref := refundRef(event, "payment_intent", nil); ref != nil {
		intent = *ref
	}

	// The row: by its Stripe id.
	stored, found := storedRefund{}, false
	switch row, err := scanStoredRefund(tx.QueryRow(ctx, `SELECT `+storedRefundColumns+` FROM refunds WHERE stripe_refund_id = $1 FOR UPDATE`, stripeRefundID)); {
	case err == nil:
		stored, found = row, true
	case !errors.Is(err, pgx.ErrNoRows):
		return err
	}

	// Who owns a refund not stored yet is decideRefundOwner's one rule.
	var org, intentInvoice *uuid.UUID
	if !found {
		invoices, err := h.invoicesOfPayment(ctx, tx, intent)
		if err != nil {
			return err
		}
		var waiting *storedRefund
		if metaRow != nil {
			switch row, err := scanStoredRefund(tx.QueryRow(ctx, `SELECT `+storedRefundColumns+` FROM refunds
				WHERE id = $1 AND stripe_refund_id IS NULL FOR UPDATE`, *metaRow)); {
			case err == nil:
				waiting = &row
			case !errors.Is(err, pgx.ErrNoRows):
				return err
			}
		}
		decision := decideRefundOwner(refundOwnerInput{Invoices: invoices, MetaOrg: metaOrg, Waiting: waiting})
		if decision.Skip != "" {
			h.logSkippedRefund(ctx, decision.Skip, stripeRefundID, intent, len(invoices), metadata, metaRow)
			countRefundDecision(ctx, decision.Skip)
			return nil
		}
		if decision.Note != "" {
			countRefundDecision(ctx, decision.Note)
		}
		if decision.Note == noteOverriddenByPayment {
			h.logger.WarnContext(ctx, "Refund webhook event's org metadata names another org than its payment's invoice; the payment's org is used",
				"stripe_refund_id", stripeRefundID, "payment_intent", intent, "org_id_received", metaOrg.String(), "org_id", decision.Org.String())
		}
		org, intentInvoice = decision.Org, decision.Invoice
		if decision.Adopt {
			stored, found = *waiting, true
		}
	}

	stripeCharge := chargeID
	if stripeCharge == "" {
		if ref := refundRef(event, "charge", nil); ref != nil {
			stripeCharge = *ref
		}
	}
	currency := strings.ToLower(pyTextOrEmpty(attr(event, "currency", nil)))
	status := pyTextOrEmpty(attr(event, "status", nil))
	metadataText, err := pyjson.Dumps(metadata)
	if err != nil {
		return err
	}
	now := h.nowUTC()

	if !found {
		if exists, err := rowExists(ctx, tx, `SELECT EXISTS (SELECT 1 FROM organizations WHERE id = $1)`, *org); err != nil {
			return err
		} else if !exists {
			h.logger.ErrorContext(ctx, "Refusing refund webhook event: the refund names an organization that does not exist",
				"stripe_refund_id", stripeRefundID, "org_id", org.String())
			countRefundDecision(ctx, skipOrgMissing)
			return nil
		}
		invoice := intentInvoice
		if invoice == nil && metaInvoice != nil {
			if owned, err := invoiceOfOrg(ctx, tx, *metaInvoice, *org); err != nil {
				return err
			} else if owned {
				invoice = metaInvoice
			} else {
				h.logger.WarnContext(ctx, "Refund webhook event names an invoice that is not this org's; stored without it",
					"stripe_refund_id", stripeRefundID, "invoice_id", metaInvoice.String(), "org_id", org.String())
				countRefundDecision(ctx, noteInvoiceNotOrgs)
			}
		}
		if currency == "" {
			currency = "usd"
		}
		if status == "" {
			status = "pending"
		}
		insertAmount := int64(0)
		if amount != nil {
			insertAmount = *amount
		}
		reason, _ := refundText(event, "reason", nil)
		description, _ := refundText(event, "description", nil)
		failure, _ := refundText(event, "failure_reason", nil)
		var intent *string
		if ref := refundRef(event, "payment_intent", nil); ref != nil {
			intent = ref
		}
		// A concurrent delivery of the same refund may insert the row first;
		// the insert then does nothing and the row is updated below.
		tag, err := tx.Exec(ctx, `INSERT INTO refunds (id, org_id, invoice_id, subscription_id, stripe_refund_id, stripe_charge_id,
			stripe_payment_intent_id, amount, currency, status, reason, description, failure_reason, initiated_by, metadata, created_at, updated_at)
			VALUES (gen_random_uuid(), $1, $2, NULL, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULL, $12::json, $13, $13)
			ON CONFLICT (stripe_refund_id) DO NOTHING`,
			*org, invoice, stripeRefundID, &stripeCharge, intent, insertAmount, currency, status, reason, description, failure, metadataText, now)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 1 {
			return nil
		}
		row, err := scanStoredRefund(tx.QueryRow(ctx, `SELECT `+storedRefundColumns+` FROM refunds WHERE stripe_refund_id = $1 FOR UPDATE`, stripeRefundID))
		if err != nil {
			return err
		}
		stored = row
	}

	// The update: Python's assignments, on the stored row.
	newCharge := stored.charge
	if stripeCharge != "" {
		newCharge = &stripeCharge
	}
	newIntent := refundRef(event, "payment_intent", stored.intent)
	newAmount := stored.amount
	if amount != nil {
		newAmount = int32(*amount)
	}
	newCurrency := stored.currency
	if currency != "" {
		newCurrency = currency
	}
	if newCurrency == "" {
		newCurrency = "usd"
	}
	// A settled refund is never moved back by an event that says it is not
	// settled (pending, requires_action): Stripe orders no events.
	stale := terminalRefundStatus(stored.status) && status != "" && !terminalRefundStatus(status)
	newStatus, newFailure := stored.status, stored.failure
	if status != "" && !stale {
		newStatus = status
	}
	if !stale {
		// A null (or absent) failure reason is a sparser snapshot, not a
		// retraction: a reason once recorded is kept.
		if failure, valid := refundText(event, "failure_reason", stored.failure); valid && (failure != nil || stored.failure == nil) {
			newFailure = failure
		}
	}
	newReason, valid := refundText(event, "reason", stored.reason)
	if !valid {
		newReason = stored.reason
	}
	newDescription := stored.description
	if text, isText := attr(event, "description", nil).(string); isText && stored.description == nil {
		newDescription = &text
	}
	newInvoice := stored.invoice
	if newInvoice == nil && metaInvoice != nil {
		if owned, err := invoiceOfOrg(ctx, tx, *metaInvoice, stored.org); err != nil {
			return err
		} else if owned {
			newInvoice = metaInvoice
		}
	}
	_, err = tx.Exec(ctx, `UPDATE refunds SET stripe_refund_id = $2, stripe_charge_id = $3, stripe_payment_intent_id = $4, amount = $5,
		currency = $6, status = $7, reason = $8, description = $9, failure_reason = $10, metadata = $11::json, invoice_id = $12,
		updated_at = $13 WHERE id = $1`,
		stored.id, stripeRefundID, newCharge, newIntent, newAmount, newCurrency, newStatus, newReason, newDescription, newFailure,
		metadataText, newInvoice, now)
	return err
}

// rowExists runs an EXISTS query.
func rowExists(ctx context.Context, tx pgx.Tx, sql string, id uuid.UUID) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, sql, id).Scan(&exists)
	return exists, err
}

// paymentInvoice is an invoice found by its payment intent.
type paymentInvoice struct{ id, org uuid.UUID }

// invoicesOfPayment is the invoices paid by a payment intent (at most two
// are read: one is a match, two are ambiguous).
func (h handlers) invoicesOfPayment(ctx context.Context, tx pgx.Tx, intent string) ([]paymentInvoice, error) {
	if intent == "" {
		return nil, nil
	}
	rows, err := tx.Query(ctx, `SELECT id, org_id FROM invoices WHERE payment_intent_id = $1 ORDER BY created_at LIMIT 2`, intent)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []paymentInvoice
	for rows.Next() {
		var invoice paymentInvoice
		if err := rows.Scan(&invoice.id, &invoice.org); err != nil {
			return nil, err
		}
		out = append(out, invoice)
	}
	return out, rows.Err()
}

// invoiceOfOrg reports whether an invoice exists and belongs to the org.
func invoiceOfOrg(ctx context.Context, tx pgx.Tx, invoice, org uuid.UUID) (bool, error) {
	var owned bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM invoices WHERE id = $1 AND org_id = $2)`, invoice, org).Scan(&owned)
	return owned, err
}

// logSkippedRefund is the log line of a skip decideRefundOwner made.
func (h handlers) logSkippedRefund(ctx context.Context, reason, stripeRefundID, intent string, invoicesFound int, metadata *pyjson.Object, waiting *uuid.UUID) {
	fields := []any{"reason", reason, "stripe_refund_id", stripeRefundID, "payment_intent", intent, "invoices_found", invoicesFound,
		"org_id_received", pyStr(attr(metadata, "org_id", nil))}
	if waiting != nil {
		fields = append(fields, "refund_id", waiting.String())
	}
	switch reason {
	case skipWaitingRowMisfit:
		h.logger.ErrorContext(ctx, "Refund webhook event names a waiting refund that does not fit its payment or org; not recorded", fields...)
	case skipSeveralInvoices:
		h.logger.WarnContext(ctx, "Refund webhook event's payment is held by several invoices; not recorded", fields...)
	default:
		h.logger.WarnContext(ctx, "Refund webhook event names no organization and no single invoice; not recorded", fields...)
	}
}
