package billing

import (
	"context"
	"errors"
	"maps"
	"math/big"
	"net/http"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stripe/stripe-go/v86"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

var refundReason = pybody.Literal("duplicate", "fraudulent", "requested_by_customer")

// refundRequest is CreateRefundRequest.
type refundRequest struct {
	invoiceID           string
	amount              *big.Int
	reason, description *string
}

// parseCreateRefund is CreateRefundRequest.
func parseCreateRefund(m pybody.Model) (refundRequest, bool) {
	invoiceID := pybody.Get(m, "invoice_id", pybody.Required, pybody.Str)
	amount := pybody.Get(m, "amount", pybody.Nullable, pybody.IntGE(1))
	reason := pybody.Get(m, "reason", pybody.Nullable, refundReason)
	description := pybody.Get(m, "description", pybody.Nullable, pybody.Str)
	request := refundRequest{invoiceID: invoiceID.Value}
	if amount.Set && !amount.Null {
		request.amount = amount.Value
	}
	if reason.Set && !reason.Null {
		request.reason = &reason.Value
	}
	if description.Set && !description.Null {
		request.description = &description.Value
	}
	return request, invoiceID.OK && amount.OK && reason.OK && description.OK
}

var superuserRequired = detail(http.StatusForbidden, "Superuser access required")

// createRefund is create_refund, made to work (CHAOS-6478, a named
// divergence: the Python route selects invoice columns that do not exist
// and answers 500 for every refund). A paid invoice is refunded through its
// Stripe payment: the payment intent stored on the invoice, else the one
// Stripe lists for the invoice (/v1/invoice_payments; the current API no
// longer puts payment_intent on the invoice), which is then stored back; a
// payment made by a bare charge is refunded by the charge. The refundable
// balance is the amount paid less the pending and succeeded refunds.
func (h handlers) createRefund(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	var errs pybody.Errors
	request, valid := parseBody(&errs, body, parseCreateRefund)
	if !valid || len(errs) > 0 {
		h.write(w, validation(errs))
		return
	}
	user := policy.UserFrom(r.Context())
	if !user.IsSuperuser {
		h.write(w, superuserRequired)
		return
	}
	invoiceID, err := pythonparity.ParseUUID(request.invoiceID)
	if err != nil {
		h.write(w, detail(http.StatusBadRequest, "Invalid identifier"))
		return
	}
	actor, err := pythonparity.ParseUUID(user.UserID)
	if err != nil {
		h.write(w, detail(http.StatusBadRequest, "Invalid identifier"))
		return
	}
	h.serve(w, r, "create refund", func(tx pgx.Tx) (reply, error) {
		ctx := r.Context()
		var org uuid.UUID
		var subscription *uuid.UUID
		var status, currency, stripeInvoiceID string
		var amountPaid int64
		var paymentIntent *string
		// Locked, so two refunds of one invoice cannot both pass the
		// balance check.
		switch err := tx.QueryRow(ctx, `SELECT org_id, subscription_id, status, amount_paid, currency, payment_intent_id, stripe_invoice_id
			FROM invoices WHERE id = $1 FOR UPDATE`, invoiceID).
			Scan(&org, &subscription, &status, &amountPaid, &currency, &paymentIntent, &stripeInvoiceID); {
		case errors.Is(err, pgx.ErrNoRows):
			return detail(http.StatusNotFound, "Invoice not found"), nil
		case err != nil:
			return reply{}, err
		}
		if pythonparity.Lower(status) != "paid" {
			return detail(http.StatusBadRequest, "Invoice is not paid"), nil
		}
		var refunded int64
		if err := tx.QueryRow(ctx, `SELECT coalesce(sum(amount), 0) FROM refunds WHERE invoice_id = $1 AND status IN ('pending', 'succeeded')`,
			invoiceID).Scan(&refunded); err != nil {
			return reply{}, err
		}
		refundable := amountPaid - refunded
		if refundable <= 0 {
			return detail(http.StatusBadRequest, "Invoice has already been fully refunded"), nil
		}
		amount := refundable
		if request.amount != nil {
			if !request.amount.IsInt64() || request.amount.Int64() > refundable {
				return detail(http.StatusBadRequest, "Refund amount exceeds refundable balance"), nil
			}
			amount = request.amount.Int64()
		}
		client, err := h.stripe.Client()
		if err != nil {
			return detail(http.StatusInternalServerError, err.Error()), nil
		}
		var charge string
		if paymentIntent == nil || *paymentIntent == "" {
			intent, chargeID, err := invoicePayment(ctx, client, stripeInvoiceID)
			if err != nil {
				h.logger.ErrorContext(ctx, "billing: stripe invoice payments read failed", "error", pyStripeError(err))
				return detail(http.StatusBadGateway, "Failed to read the invoice payment: "+pyStripeError(err)), nil
			}
			switch {
			case intent != "":
				paymentIntent = &intent
				if _, err := tx.Exec(ctx, `UPDATE invoices SET payment_intent_id = $2 WHERE id = $1`, invoiceID, intent); err != nil {
					return reply{}, err
				}
			case chargeID != "":
				charge = chargeID
			default:
				return detail(http.StatusBadRequest, "Invoice has no Stripe payment to refund"), nil
			}
		}
		params := &stripe.RefundCreateParams{Amount: stripe.Int64(amount),
			Metadata: map[string]string{"org_id": org.String(), "invoice_id": invoiceID.String()}}
		if paymentIntent != nil && *paymentIntent != "" {
			params.PaymentIntent = paymentIntent
		} else {
			params.Charge = stripe.String(charge)
		}
		if request.reason != nil {
			params.Reason = request.reason
		}
		refund, err := client.V1Refunds.Create(ctx, params)
		if err != nil {
			h.logger.ErrorContext(ctx, "billing: stripe refund failed", "error", pyStripeError(err))
			return detail(http.StatusBadGateway, "Failed to create refund: "+pyStripeError(err)), nil
		}
		chargeID := charge
		if refund.Charge != nil && refund.Charge.ID != "" {
			chargeID = refund.Charge.ID
		}
		var refundIntent *string
		if refund.PaymentIntent != nil && refund.PaymentIntent.ID != "" {
			refundIntent = &refund.PaymentIntent.ID
		} else if paymentIntent != nil && *paymentIntent != "" {
			refundIntent = paymentIntent
		}
		refundStatus := string(refund.Status)
		if refundStatus == "" {
			refundStatus = "pending"
		}
		var failure *string
		if refund.FailureReason != "" {
			text := string(refund.FailureReason)
			failure = &text
		}
		metadata := pyjson.NewObject()
		for _, key := range slices.Sorted(maps.Keys(refund.Metadata)) {
			metadata.Set(key, refund.Metadata[key])
		}
		metadataText, err := pyjson.Dumps(metadata)
		if err != nil {
			return reply{}, err
		}
		id, now := uuid.New(), h.nowUTC()
		if _, err := tx.Exec(ctx, `INSERT INTO refunds (id, org_id, invoice_id, subscription_id, stripe_refund_id, stripe_charge_id,
			stripe_payment_intent_id, amount, currency, status, reason, description, failure_reason, initiated_by, metadata,
			created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::json, $16, $16)`,
			id, org, invoiceID, subscription, refund.ID, chargeID, refundIntent, amount, pythonparity.Lower(currency), refundStatus,
			request.reason, request.description, failure, actor, metadataText, now); err != nil {
			return reply{}, err
		}
		out, err := scanRefundJSON(tx.QueryRow(ctx, `SELECT `+refundColumns+` FROM refunds WHERE id = $1`, id))
		if err != nil {
			return reply{}, err
		}
		return ok(out), nil
	})
}

// invoicePayment is the invoice's paid Stripe payment: its payment intent,
// or its charge when the payment is a bare charge ("" for both when the
// invoice has no paid payment).
func invoicePayment(ctx context.Context, client *stripe.Client, stripeInvoiceID string) (intent, charge string, err error) {
	params := &stripe.InvoicePaymentListParams{Invoice: stripe.String(stripeInvoiceID), Status: stripe.String("paid")}
	for payment, err := range client.V1InvoicePayments.List(ctx, params).All(ctx) {
		if err != nil {
			return "", "", err
		}
		if payment.Payment == nil {
			continue
		}
		if payment.Payment.PaymentIntent != nil && payment.Payment.PaymentIntent.ID != "" {
			return payment.Payment.PaymentIntent.ID, "", nil
		}
		if payment.Payment.Charge != nil && payment.Payment.Charge.ID != "" {
			return "", payment.Payment.Charge.ID, nil
		}
	}
	return "", "", nil
}

const refundColumns = `id, org_id, invoice_id, subscription_id, stripe_refund_id, stripe_charge_id,
	stripe_payment_intent_id, amount, currency, status, reason, description, failure_reason, initiated_by,
	metadata::text, created_at, updated_at`

func scanRefundJSON(row pgx.Row) (*pyjson.Object, error) {
	var id, org uuid.UUID
	var invoiceID, subscriptionID, initiatedBy *uuid.UUID
	var refundID, chargeID, currency, status string
	var intentID, reason, description, failure, metadata *string
	var amount int32
	var created, updated *time.Time
	if err := row.Scan(&id, &org, &invoiceID, &subscriptionID, &refundID, &chargeID, &intentID, &amount, &currency,
		&status, &reason, &description, &failure, &initiatedBy, &metadata, &created, &updated); err != nil {
		return nil, err
	}
	var stored pyjson.Value
	if metadata != nil {
		decoded, err := pyjson.DecodeString(*metadata)
		if err != nil {
			return nil, err
		}
		stored = decoded
	}
	out := pyjson.NewObject()
	out.Set("id", id.String())
	out.Set("org_id", org.String())
	out.Set("invoice_id", uuidOrNull(invoiceID))
	out.Set("subscription_id", uuidOrNull(subscriptionID))
	out.Set("stripe_refund_id", refundID)
	out.Set("stripe_charge_id", chargeID)
	out.Set("stripe_payment_intent_id", nullableString(intentID))
	out.Set("amount", int64(amount))
	out.Set("currency", currency)
	out.Set("status", status)
	out.Set("reason", nullableString(reason))
	out.Set("description", nullableString(description))
	out.Set("failure_reason", nullableString(failure))
	out.Set("initiated_by", uuidOrNull(initiatedBy))
	out.Set("metadata", ensureDict(stored))
	out.Set("created_at", pydanticTime(created))
	out.Set("updated_at", pydanticTime(updated))
	return out, nil
}

var one, hundred = big.NewInt(1), big.NewInt(100)

// listRefunds is list_refunds: limit and offset are bare ints, clamped
// (not refused) to 1..100 and >= 0; org_id is honoured as given (the route
// is superuser-only).
func (h handlers) listRefunds(w http.ResponseWriter, r *http.Request) {
	var errs pybody.Errors
	query := r.URL.Query()
	limit, _ := errs.QueryInt("limit", pybody.LastQueryValue(query, "limit"), 20, nil, nil)
	offset, _ := errs.QueryInt("offset", pybody.LastQueryValue(query, "offset"), 0, nil, nil)
	orgID, _ := errs.QueryUUID("org_id", pybody.LastQueryValue(query, "org_id"))
	if len(errs) > 0 {
		h.write(w, validation(errs))
		return
	}
	if !policy.UserFrom(r.Context()).IsSuperuser {
		h.write(w, superuserRequired)
		return
	}
	page := pageQuery{limit: 20, offset: -1}
	switch {
	case limit.Cmp(one) < 0:
		page.limit = 1
	case limit.Cmp(hundred) > 0:
		page.limit = 100
	default:
		page.limit = limit.Int64()
	}
	if offset.Sign() < 0 {
		page.offset = 0
	} else if offset.IsInt64() {
		page.offset = offset.Int64()
	}
	h.serve(w, r, "list refunds", func(tx pgx.Tx) (reply, error) {
		ctx := r.Context()
		if page.offset < 0 {
			return reply{}, errOverflow
		}
		const where = `WHERE ($1::uuid IS NULL OR s.org_id = $1)`
		rows, err := tx.Query(ctx, `SELECT `+refundColumns+` FROM refunds s `+where+` ORDER BY s.created_at DESC LIMIT $2 OFFSET $3`,
			orgID, page.limit, page.offset)
		if err != nil {
			return reply{}, err
		}
		items := []pyjson.Value{}
		for rows.Next() {
			item, err := scanRefundJSON(rows)
			if err != nil {
				rows.Close()
				return reply{}, err
			}
			items = append(items, item)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return reply{}, err
		}
		var total int64
		if err := tx.QueryRow(ctx, `SELECT count(s.id) FROM refunds s `+where, orgID).Scan(&total); err != nil {
			return reply{}, err
		}
		return ok(pageJSON(items, total, page)), nil
	})
}

// getRefund is get_refund.
func (h handlers) getRefund(w http.ResponseWriter, r *http.Request) {
	var errs pybody.Errors
	orgID, _ := errs.QueryUUID("org_id", pybody.LastQueryValue(r.URL.Query(), "org_id"))
	if len(errs) > 0 {
		h.write(w, validation(errs))
		return
	}
	if !policy.UserFrom(r.Context()).IsSuperuser {
		h.write(w, superuserRequired)
		return
	}
	id, err := pythonparity.ParseUUID(r.PathValue("refund_id"))
	if err != nil {
		h.write(w, detail(http.StatusBadRequest, "Invalid identifier"))
		return
	}
	h.serve(w, r, "get refund", func(tx pgx.Tx) (reply, error) {
		return refundReply(r.Context(), tx, id, orgID)
	})
}

func refundReply(ctx context.Context, q querier, id uuid.UUID, org *uuid.UUID) (reply, error) {
	sql, args := orgFilter(`WHERE s.id = $1`, []any{id}, org)
	refund, err := scanRefundJSON(q.QueryRow(ctx, `SELECT `+refundColumns+` FROM refunds s `+sql, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return detail(http.StatusNotFound, "Refund not found"), nil
	}
	if err != nil {
		return reply{}, err
	}
	return ok(refund), nil
}
