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

var refundReason = pybody.Literal("duplicate", "fraudulent", "requested_by_customer")

// parseCreateRefund is CreateRefundRequest. Its fields are validated for
// the 422 only: the route never reaches a use of them (see createRefund).
func parseCreateRefund(m pybody.Model) (string, bool) {
	invoiceID := pybody.Get(m, "invoice_id", pybody.Required, pybody.Str)
	amount := pybody.Get(m, "amount", pybody.Nullable, pybody.IntGE(1))
	reason := pybody.Get(m, "reason", pybody.Nullable, refundReason)
	description := pybody.Get(m, "description", pybody.Nullable, pybody.Str)
	return invoiceID.Value, invoiceID.OK && amount.OK && reason.OK && description.OK
}

// errRefundInvoiceColumns is the failure of every Python create_refund
// that passes validation: RefundService._get_invoice selects
// invoices.stripe_charge_id and invoices.stripe_payment_intent_id, which
// the invoices table has never had, so Postgres refuses the statement and
// the route answers the unhandled 500. Named limit, ported as is: this
// port refunds nothing, as the Python route refunds nothing.
var errRefundInvoiceColumns = errors.New("billing: create refund reads invoice columns that do not exist (the Python route fails the same way)")

var superuserRequired = detail(http.StatusForbidden, "Superuser access required")

// createRefund is create_refund.
func (h handlers) createRefund(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	var errs pybody.Errors
	invoiceID, valid := parseBody(&errs, body, parseCreateRefund)
	if !valid || len(errs) > 0 {
		h.write(w, validation(errs))
		return
	}
	if !policy.UserFrom(r.Context()).IsSuperuser {
		h.write(w, superuserRequired)
		return
	}
	if _, err := pythonparity.ParseUUID(invoiceID); err != nil {
		h.write(w, detail(http.StatusBadRequest, "Invalid identifier"))
		return
	}
	h.internal(w, r, "create refund", errRefundInvoiceColumns)
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
		where, args := orgFilter(`WHERE true`, nil, orgID)
		rows, err := tx.Query(ctx, `SELECT `+refundColumns+` FROM refunds s `+where+
			` ORDER BY s.created_at DESC LIMIT `+itoa(int(page.limit))+` OFFSET `+itoa(int(page.offset)), args...)
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
		if err := tx.QueryRow(ctx, `SELECT count(s.id) FROM refunds s `+where, args...).Scan(&total); err != nil {
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
