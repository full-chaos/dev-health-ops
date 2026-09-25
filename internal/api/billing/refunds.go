package billing

import (
	"context"
	"errors"
	"maps"
	"math/big"
	"net/http"
	"slices"
	"strings"
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
	ctx := r.Context()
	clientKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	// 1. Reserve: the refund row is written, pending, before Stripe is
	// called, so a refund Stripe makes always has a local row.
	var pending pendingRefund
	answer, err := h.inTx(ctx, func(tx pgx.Tx) (reply, error) {
		var early *reply
		pending, early, err = h.reserveRefund(ctx, tx, request, invoiceID, actor, clientKey)
		if early != nil {
			return *early, err
		}
		return reply{}, err
	})
	switch {
	case err != nil:
		h.internal(w, r, "create refund", err)
		return
	case pending.id == uuid.Nil:
		h.write(w, answer)
		return
	}
	// 2. Stripe, with the row's own idempotency key: a retry of this row
	// never makes a second refund.
	client, err := h.stripe.Client()
	if err != nil {
		h.write(w, detail(http.StatusInternalServerError, err.Error()))
		return
	}
	metadata := map[string]string{"org_id": pending.org.String(), "invoice_id": invoiceID.String(), "refund_id": pending.id.String()}
	if pending.requested != "" {
		metadata["requested_amount"] = pending.requested
	}
	params := &stripe.RefundCreateParams{Amount: stripe.Int64(pending.amount), Metadata: metadata}
	params.SetIdempotencyKey("refund:" + pending.id.String())
	if pending.intent != "" {
		params.PaymentIntent = stripe.String(pending.intent)
	} else {
		params.Charge = stripe.String(pending.charge)
	}
	if pending.reason != nil {
		params.Reason = pending.reason
	}
	var refund *stripe.Refund
	if pending.resumed {
		// Stripe may already have made it, and may have pruned the key. A
		// failed read leaves the row pending: its outcome is still unknown.
		made, err := madeRefund(ctx, client, pending)
		if err != nil {
			h.logger.ErrorContext(ctx, "billing: stripe refund lookup failed", "refund_id", pending.id.String(), "error", pyStripeError(err))
			h.write(w, detail(http.StatusBadGateway, "Failed to read the refund from Stripe: "+pyStripeError(err)))
			return
		}
		refund = made
	}
	var stripeErr error
	if refund == nil {
		refund, stripeErr = client.V1Refunds.Create(ctx, params)
	}
	// 3. Complete the row: Stripe's refund, or its refusal. An outcome
	// Stripe did not state (a network or server error) leaves the row
	// pending, its amount held, for the retry to complete.
	if stripeErr != nil {
		h.logger.ErrorContext(ctx, "billing: stripe refund failed", "refund_id", pending.id.String(), "error", pyStripeError(stripeErr))
		if refusedByStripe(stripeErr) {
			// Committed on its own: inTx rolls back a 4xx/5xx reply.
			if err := h.inTxFunc(ctx, func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `UPDATE refunds SET status = 'failed', failure_reason = $2, updated_at = $3 WHERE id = $1`,
					pending.id, pyStripeError(stripeErr), h.nowUTC())
				return err
			}); err != nil {
				h.internal(w, r, "create refund", err)
				return
			}
		}
		h.write(w, detail(http.StatusBadGateway, "Failed to create refund: "+pyStripeError(stripeErr)))
		return
	}
	answer, err = h.inTx(ctx, func(tx pgx.Tx) (reply, error) {
		return h.completeRefund(ctx, tx, pending, refund)
	})
	if err != nil {
		h.internal(w, r, "create refund", err)
		return
	}
	h.write(w, answer)
}

// sameText reports whether two optional texts are equal (absent equals
// absent).
func sameText(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// pendingRefund is a reserved refund row and what its Stripe call needs.
type pendingRefund struct {
	id, org        uuid.UUID
	amount         int64
	intent, charge string
	reason         *string
	// requested is the request's amount as sent ("balance" when left
	// out), kept in the row and the Stripe metadata.
	requested string
	// resumed marks a row reserved by an earlier request, whose refund
	// Stripe may already have made.
	resumed bool
}

// refundRow is a stored refund as the create route compares and resumes it.
type refundRow struct {
	id                                                            uuid.UUID
	invoice                                                       *uuid.UUID
	amount                                                        int64
	status                                                        string
	refundID, charge, intent, reason, description, requested, key *string
}

// loadRefundRow reads the one refund the condition names (found is false
// when there is none).
func loadRefundRow(ctx context.Context, tx pgx.Tx, condition string, args ...any) (refundRow, bool, error) {
	var row refundRow
	err := tx.QueryRow(ctx, `SELECT id, invoice_id, amount, status, stripe_refund_id, stripe_charge_id, stripe_payment_intent_id, reason,
		description, metadata::jsonb ->> 'requested_amount', idempotency_key FROM refunds WHERE `+condition+` ORDER BY created_at LIMIT 1`, args...).
		Scan(&row.id, &row.invoice, &row.amount, &row.status, &row.refundID, &row.charge, &row.intent, &row.reason, &row.description,
			&row.requested, &row.key)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return refundRow{}, false, nil
	case err != nil:
		return refundRow{}, false, err
	}
	return row, true, nil
}

// unresolved reports a pending row Stripe has not answered for.
func (row refundRow) unresolved() bool { return row.status == "pending" && row.refundID == nil }

// sameRequest reports whether request is the one that made row: the same
// invoice, amount as sent (an amount left out is not an amount given),
// reason and description. A row from before the requested amount was kept
// matches only a request that gives the row's amount: a request that
// leaves it out cannot be told from a first request that gave one.
func (row refundRow) sameRequest(invoiceID uuid.UUID, request refundRequest) bool {
	if row.invoice == nil || *row.invoice != invoiceID || !sameText(request.reason, row.reason) || !sameText(request.description, row.description) {
		return false
	}
	if row.requested != nil {
		return *row.requested == requestedAmount(request)
	}
	return request.amount != nil && request.amount.IsInt64() && request.amount.Int64() == row.amount
}

// resume is the pending refund for a row reserved by an earlier request.
func (row refundRow) resume(org uuid.UUID) pendingRefund {
	resumed := pendingRefund{id: row.id, org: org, amount: row.amount, reason: row.reason, resumed: true}
	if row.intent != nil {
		resumed.intent = *row.intent
	}
	if row.charge != nil {
		resumed.charge = *row.charge
	}
	if row.requested != nil {
		resumed.requested = *row.requested
	}
	return resumed
}

// requestedAmount is the request's amount as sent: its value, or "balance"
// when it leaves the amount out.
func requestedAmount(request refundRequest) string {
	if request.amount == nil {
		return "balance"
	}
	return request.amount.String()
}

// madeRefund is the refund Stripe already made for a resumed row, found by
// the row id in its metadata (nil when there is none): after Stripe prunes
// an idempotency key, the key alone would make a second refund.
func madeRefund(ctx context.Context, client *stripe.Client, pending pendingRefund) (*stripe.Refund, error) {
	params := &stripe.RefundListParams{}
	if pending.intent != "" {
		params.PaymentIntent = stripe.String(pending.intent)
	} else {
		params.Charge = stripe.String(pending.charge)
	}
	for refund, err := range client.V1Refunds.List(ctx, params).All(ctx) {
		if err != nil {
			return nil, err
		}
		if refund.Metadata["refund_id"] == pending.id.String() {
			return refund, nil
		}
	}
	return nil, nil
}

// refusedByStripe reports a refusal Stripe stated: a card or
// invalid-request error on 400, 402 or 404. Anything else (408, 409, 429,
// 5xx, a network error) may follow a refund Stripe made, so the outcome is
// unknown.
func refusedByStripe(err error) bool {
	var apiError *stripe.Error
	if !errors.As(err, &apiError) {
		return false
	}
	switch apiError.Type {
	case stripe.ErrorTypeCard, stripe.ErrorTypeInvalidRequest:
	default:
		return false
	}
	switch apiError.HTTPStatusCode {
	case http.StatusBadRequest, http.StatusPaymentRequired, http.StatusNotFound:
		return true
	}
	return false
}

// reserveRefund locks the invoice, answers a repeated Idempotency-Key with
// its refund (or resumes it while still pending), checks the balance,
// resolves the Stripe payment and writes the pending row. A non-nil early
// reply ends the request.
func (h handlers) reserveRefund(ctx context.Context, tx pgx.Tx, request refundRequest, invoiceID, actor uuid.UUID,
	clientKey string) (pendingRefund, *reply, error) {
	early := func(answer reply) (pendingRefund, *reply, error) { return pendingRefund{}, &answer, nil }
	var org uuid.UUID
	var subscription *uuid.UUID
	var status, currency, stripeInvoiceID string
	var amountPaid int64
	var paymentIntent *string
	// Locked, so two refunds of one invoice cannot both pass the balance
	// check, and a repeated key finds the first request's row.
	switch err := tx.QueryRow(ctx, `SELECT org_id, subscription_id, status, amount_paid, currency, payment_intent_id, stripe_invoice_id
		FROM invoices WHERE id = $1 FOR UPDATE`, invoiceID).
		Scan(&org, &subscription, &status, &amountPaid, &currency, &paymentIntent, &stripeInvoiceID); {
	case errors.Is(err, pgx.ErrNoRows):
		return early(detail(http.StatusNotFound, "Invoice not found"))
	case err != nil:
		return pendingRefund{}, nil, err
	}
	if clientKey != "" {
		keyed, found, err := loadRefundRow(ctx, tx, `org_id = $1 AND idempotency_key = $2`, org, clientKey)
		switch {
		case err != nil:
			return pendingRefund{}, nil, err
		case found && !keyed.sameRequest(invoiceID, request):
			return early(detail(http.StatusConflict, "Idempotency-Key was used for a different refund request"))
		case found && !keyed.unresolved():
			out, err := scanRefundJSON(tx.QueryRow(ctx, `SELECT `+refundColumns+` FROM refunds WHERE id = $1`, keyed.id))
			if err != nil {
				return pendingRefund{}, nil, err
			}
			return early(ok(out))
		case found:
			// Still pending: the first request's Stripe outcome is not
			// recorded. Resume it.
			return keyed.resume(org), nil, nil
		}
	}
	if pythonparity.Lower(status) != "paid" {
		return early(detail(http.StatusBadRequest, "Invoice is not paid"))
	}
	// A refund whose Stripe outcome is unknown blocks any other: Stripe may
	// have made it. Only its own Idempotency-Key resumes it, or, for one
	// made without a key, the same request again without a key.
	pendingRow, unresolved, err := loadRefundRow(ctx, tx, `invoice_id = $1 AND status = 'pending' AND stripe_refund_id IS NULL`, invoiceID)
	if err != nil {
		return pendingRefund{}, nil, err
	}
	if unresolved {
		if clientKey == "" && pendingRow.key == nil && pendingRow.sameRequest(invoiceID, request) {
			return pendingRow.resume(org), nil, nil
		}
		return early(detail(http.StatusConflict, "A refund for this invoice is still pending; retry it with its Idempotency-Key"))
	}
	var refunded int64
	if err := tx.QueryRow(ctx, `SELECT coalesce(sum(amount), 0) FROM refunds WHERE invoice_id = $1 AND status IN ('pending', 'succeeded')`,
		invoiceID).Scan(&refunded); err != nil {
		return pendingRefund{}, nil, err
	}
	refundable := amountPaid - refunded
	if refundable <= 0 {
		return early(detail(http.StatusBadRequest, "Invoice has already been fully refunded"))
	}
	amount := refundable
	if request.amount != nil {
		if !request.amount.IsInt64() || request.amount.Int64() > refundable {
			return early(detail(http.StatusBadRequest, "Refund amount exceeds refundable balance"))
		}
		amount = request.amount.Int64()
	}
	client, err := h.stripe.Client()
	if err != nil {
		return early(detail(http.StatusInternalServerError, err.Error()))
	}
	var charge string
	if paymentIntent == nil || *paymentIntent == "" {
		intent, chargeID, err := invoicePayment(ctx, client, stripeInvoiceID)
		if err != nil {
			h.logger.ErrorContext(ctx, "billing: stripe invoice payments read failed", "error", pyStripeError(err))
			return early(detail(http.StatusBadGateway, "Failed to read the invoice payment: "+pyStripeError(err)))
		}
		switch {
		case intent != "":
			paymentIntent = &intent
			if _, err := tx.Exec(ctx, `UPDATE invoices SET payment_intent_id = $2 WHERE id = $1`, invoiceID, intent); err != nil {
				return pendingRefund{}, nil, err
			}
		case chargeID != "":
			charge = chargeID
		default:
			return early(detail(http.StatusBadRequest, "Invoice has no Stripe payment to refund"))
		}
	}
	reserved := pendingRefund{id: uuid.New(), org: org, amount: amount, charge: charge, reason: request.reason, requested: requestedAmount(request)}
	var intentColumn *string
	if paymentIntent != nil && *paymentIntent != "" {
		reserved.intent = *paymentIntent
		intentColumn = paymentIntent
	}
	var chargeColumn, keyColumn *string
	if charge != "" {
		chargeColumn = &charge
	}
	if clientKey != "" {
		keyColumn = &clientKey
	}
	now := h.nowUTC()
	// No Stripe ids yet: they are written when Stripe answers.
	if _, err := tx.Exec(ctx, `INSERT INTO refunds (id, org_id, invoice_id, subscription_id, stripe_refund_id, stripe_charge_id,
		stripe_payment_intent_id, amount, currency, status, reason, description, failure_reason, initiated_by, metadata,
		idempotency_key, created_at, updated_at)
		VALUES ($1, $2, $3, $4, NULL, $5, $6, $7, $8, 'pending', $9, $10, NULL, $11, json_build_object('requested_amount', $14::text), $12, $13, $13)`,
		reserved.id, org, invoiceID, subscription, chargeColumn, intentColumn, amount,
		pythonparity.Lower(currency), request.reason, request.description, actor, keyColumn, now, requestedAmount(request)); err != nil {
		return pendingRefund{}, nil, err
	}
	return reserved, nil, nil
}

// completeRefund writes Stripe's refund onto its reserved row and answers
// the row.
func (h handlers) completeRefund(ctx context.Context, tx pgx.Tx, pending pendingRefund, refund *stripe.Refund) (reply, error) {
	var chargeID *string
	if pending.charge != "" {
		chargeID = &pending.charge
	}
	if refund.Charge != nil && refund.Charge.ID != "" {
		chargeID = &refund.Charge.ID
	}
	var refundIntent *string
	if refund.PaymentIntent != nil && refund.PaymentIntent.ID != "" {
		refundIntent = &refund.PaymentIntent.ID
	} else if pending.intent != "" {
		refundIntent = &pending.intent
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
	// A completion never reopens a settled row nor erases a recorded reason
	// (an event may have settled it while the route waited for Stripe): when
	// it is protecting one, it says so.
	var priorStatus string
	var priorFailure *string
	if err := tx.QueryRow(ctx, `SELECT status, failure_reason FROM refunds WHERE id = $1 FOR UPDATE`, pending.id).Scan(&priorStatus, &priorFailure); err != nil {
		return reply{}, err
	}
	if terminalRefundStatus(priorStatus) && !terminalRefundStatus(refundStatus) {
		h.noteRefundRule(ctx, noteCompletionKeptState, "refund_id", pending.id.String(), "stored_status", priorStatus, "stripe_status", refundStatus)
	} else if priorFailure != nil && failure == nil {
		h.noteRefundRule(ctx, noteCompletionKeptFail, "refund_id", pending.id.String(), "failure_reason", *priorFailure)
	}
	if _, err := tx.Exec(ctx, `UPDATE refunds SET stripe_refund_id = $2, stripe_charge_id = $3, stripe_payment_intent_id = $4,
		status = CASE WHEN status IN ('succeeded', 'failed', 'canceled') AND $5 NOT IN ('succeeded', 'failed', 'canceled') THEN status ELSE $5 END,
		failure_reason = CASE WHEN status IN ('succeeded', 'failed', 'canceled') AND $5 NOT IN ('succeeded', 'failed', 'canceled') THEN failure_reason ELSE COALESCE($6, failure_reason) END,
		metadata = $7::json, updated_at = $8 WHERE id = $1`,
		pending.id, refund.ID, chargeID, refundIntent, refundStatus, failure, metadataText, h.nowUTC()); err != nil {
		return reply{}, err
	}
	out, err := scanRefundJSON(tx.QueryRow(ctx, `SELECT `+refundColumns+` FROM refunds WHERE id = $1`, pending.id))
	if err != nil {
		return reply{}, err
	}
	return ok(out), nil
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
	var currency, status string
	var refundID, chargeID, intentID, reason, description, failure, metadata *string
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
	out.Set("stripe_refund_id", nullableString(refundID))
	out.Set("stripe_charge_id", nullableString(chargeID))
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
