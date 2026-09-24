package billing

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// pydanticTime is a datetime field of a response model: pydantic's JSON
// form, None when NULL.
func pydanticTime(value *time.Time) pyjson.Value {
	if value == nil {
		return nil
	}
	return pytime.Pydantic(pytime.UTC(*value))
}

// invoiceOrg is the invoice routes' org scope: `org_id` counts only for a
// superuser (`_resolve_org_id(user, org_id if user.is_superuser else
// None)`), so no caller reaches another org's invoices.
func invoiceOrg(ctx context.Context, h handlers, q querier, user *policy.User, param *uuid.UUID) (*uuid.UUID, *reply, error) {
	if !user.IsSuperuser {
		param = nil
	}
	return h.resolveOrg(ctx, q, user, param)
}

const invoiceColumns = `id, org_id, subscription_id, stripe_invoice_id, stripe_customer_id, status, amount_due,
	amount_paid, amount_remaining, currency, period_start, period_end, hosted_invoice_url, pdf_url,
	payment_intent_id, finalized_at, paid_at, voided_at, attempt_count, metadata::text, created_at, updated_at`

type invoiceRow struct {
	ID, OrgID                                             uuid.UUID
	SubscriptionID                                        *uuid.UUID
	StripeInvoiceID, StripeCustomerID, Status, Currency   string
	AmountDue, AmountPaid, AmountRemaining, AttemptCount  int64
	PeriodStart, PeriodEnd, FinalizedAt, PaidAt, VoidedAt *time.Time
	CreatedAt, UpdatedAt                                  *time.Time
	HostedInvoiceURL, PDFURL, PaymentIntentID             *string
	Metadata                                              *string
}

func scanInvoice(row pgx.Row) (invoiceRow, error) {
	var inv invoiceRow
	var due, paid, remaining, attempts int32
	err := row.Scan(&inv.ID, &inv.OrgID, &inv.SubscriptionID, &inv.StripeInvoiceID, &inv.StripeCustomerID, &inv.Status,
		&due, &paid, &remaining, &inv.Currency, &inv.PeriodStart, &inv.PeriodEnd, &inv.HostedInvoiceURL, &inv.PDFURL,
		&inv.PaymentIntentID, &inv.FinalizedAt, &inv.PaidAt, &inv.VoidedAt, &attempts, &inv.Metadata, &inv.CreatedAt, &inv.UpdatedAt)
	inv.AmountDue, inv.AmountPaid, inv.AmountRemaining, inv.AttemptCount = int64(due), int64(paid), int64(remaining), int64(attempts)
	return inv, err
}

func uuidOrNull(value *uuid.UUID) pyjson.Value {
	if value == nil {
		return nil
	}
	return value.String()
}

// invoiceJSON is _to_invoice_response.
func invoiceJSON(ctx context.Context, q querier, inv invoiceRow, withLines bool) (*pyjson.Object, error) {
	metadata, err := eventPayload(inv.Metadata)
	if err != nil {
		return nil, err
	}
	out := pyjson.NewObject()
	out.Set("id", inv.ID.String())
	out.Set("org_id", inv.OrgID.String())
	out.Set("subscription_id", uuidOrNull(inv.SubscriptionID))
	out.Set("stripe_invoice_id", inv.StripeInvoiceID)
	out.Set("stripe_customer_id", inv.StripeCustomerID)
	out.Set("status", inv.Status)
	out.Set("amount_due", inv.AmountDue)
	out.Set("amount_paid", inv.AmountPaid)
	out.Set("amount_remaining", inv.AmountRemaining)
	out.Set("currency", inv.Currency)
	out.Set("period_start", pydanticTime(inv.PeriodStart))
	out.Set("period_end", pydanticTime(inv.PeriodEnd))
	out.Set("hosted_invoice_url", nullableString(inv.HostedInvoiceURL))
	out.Set("pdf_url", nullableString(inv.PDFURL))
	out.Set("payment_intent_id", nullableString(inv.PaymentIntentID))
	out.Set("finalized_at", pydanticTime(inv.FinalizedAt))
	out.Set("paid_at", pydanticTime(inv.PaidAt))
	out.Set("voided_at", pydanticTime(inv.VoidedAt))
	out.Set("attempt_count", inv.AttemptCount)
	out.Set("metadata", metadata)
	out.Set("created_at", pydanticTime(inv.CreatedAt))
	out.Set("updated_at", pydanticTime(inv.UpdatedAt))
	lines := []pyjson.Value{}
	if withLines {
		rows, err := q.Query(ctx, `SELECT id, stripe_line_item_id, description, amount, quantity, period_start,
			period_end, stripe_price_id FROM invoice_line_items WHERE invoice_id = $1`, inv.ID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id uuid.UUID
			var lineID, description, priceID *string
			var amount, quantity int32
			var start, end *time.Time
			if err := rows.Scan(&id, &lineID, &description, &amount, &quantity, &start, &end, &priceID); err != nil {
				rows.Close()
				return nil, err
			}
			line := pyjson.NewObject()
			line.Set("id", id.String())
			line.Set("stripe_line_item_id", nullableString(lineID))
			line.Set("description", nullableString(description))
			line.Set("amount", int64(amount))
			line.Set("quantity", int64(quantity))
			line.Set("period_start", pydanticTime(start))
			line.Set("period_end", pydanticTime(end))
			line.Set("stripe_price_id", nullableString(priceID))
			lines = append(lines, line)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	out.Set("line_items", lines)
	return out, nil
}

// listInvoices is list_invoices.
func (h handlers) listInvoices(w http.ResponseWriter, r *http.Request) {
	var errs pybody.Errors
	page := readPage(r, &errs)
	status := pybody.LastQueryValue(r.URL.Query(), "status")
	if len(errs) > 0 {
		h.write(w, validation(errs))
		return
	}
	h.serve(w, r, "list invoices", func(tx pgx.Tx) (reply, error) {
		ctx := r.Context()
		org, answer, err := invoiceOrg(ctx, h, tx, policy.UserFrom(ctx), page.orgID)
		if answer != nil || err != nil {
			return derefReply(answer), err
		}
		if page.offset < 0 {
			return reply{}, errOverflow
		}
		filters := []any{org, nonEmpty(status)}
		const where = `WHERE ($1::uuid IS NULL OR org_id = $1) AND ($2::text IS NULL OR status = $2)`
		var total int64
		if err := tx.QueryRow(ctx, `SELECT count(id) FROM invoices `+where, filters...).Scan(&total); err != nil {
			return reply{}, err
		}
		rows, err := tx.Query(ctx, `SELECT `+invoiceColumns+` FROM invoices `+where+` ORDER BY created_at DESC LIMIT $3 OFFSET $4`,
			append(filters, page.limit, page.offset)...)
		if err != nil {
			return reply{}, err
		}
		var invoices []invoiceRow
		for rows.Next() {
			inv, err := scanInvoice(rows)
			if err != nil {
				rows.Close()
				return reply{}, err
			}
			invoices = append(invoices, inv)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return reply{}, err
		}
		items := make([]pyjson.Value, 0, len(invoices))
		for _, inv := range invoices {
			item, err := invoiceJSON(ctx, tx, inv, false)
			if err != nil {
				return reply{}, err
			}
			items = append(items, item)
		}
		return ok(pageJSON(items, total, page)), nil
	})
}

// loadInvoice is InvoiceService.get_invoice.
func loadInvoice(ctx context.Context, q querier, id uuid.UUID, org *uuid.UUID) (*invoiceRow, error) {
	sql, args := `SELECT `+invoiceColumns+` FROM invoices WHERE id = $1`, []any{id}
	if org != nil {
		sql += ` AND org_id = $2`
		args = append(args, *org)
	}
	inv, err := scanInvoice(q.QueryRow(ctx, sql, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &inv, nil
}

// invoiceTarget is the id parse, org scope and lookup the two single-
// invoice routes share.
func (h handlers) invoiceTarget(ctx context.Context, q querier, r *http.Request, orgParam *uuid.UUID) (*invoiceRow, *uuid.UUID, *reply, error) {
	id, err := pythonparity.ParseUUID(r.PathValue("invoice_id"))
	if err != nil {
		answer := detail(http.StatusBadRequest, "Invalid invoice id")
		return nil, nil, &answer, nil
	}
	org, answer, err := invoiceOrg(ctx, h, q, policy.UserFrom(ctx), orgParam)
	if answer != nil || err != nil {
		return nil, nil, answer, err
	}
	inv, err := loadInvoice(ctx, q, id, org)
	if err != nil {
		return nil, nil, nil, err
	}
	if inv == nil {
		answer := detail(http.StatusNotFound, "Invoice not found")
		return nil, nil, &answer, nil
	}
	return inv, org, nil, nil
}

// getInvoice is get_invoice.
func (h handlers) getInvoice(w http.ResponseWriter, r *http.Request) {
	var errs pybody.Errors
	orgParam, _ := errs.QueryUUID("org_id", pybody.LastQueryValue(r.URL.Query(), "org_id"))
	if len(errs) > 0 {
		h.write(w, validation(errs))
		return
	}
	h.serve(w, r, "get invoice", func(tx pgx.Tx) (reply, error) {
		inv, _, answer, err := h.invoiceTarget(r.Context(), tx, r, orgParam)
		if answer != nil || err != nil {
			return derefReply(answer), err
		}
		body, err := invoiceJSON(r.Context(), tx, *inv, true)
		if err != nil {
			return reply{}, err
		}
		return ok(body), nil
	})
}

// voidInvoice is void_invoice: only an open invoice; Stripe voids it
// first, then the local row is marked void with nothing remaining.
func (h handlers) voidInvoice(w http.ResponseWriter, r *http.Request) {
	var errs pybody.Errors
	orgParam, _ := errs.QueryUUID("org_id", pybody.LastQueryValue(r.URL.Query(), "org_id"))
	if len(errs) > 0 {
		h.write(w, validation(errs))
		return
	}
	h.serve(w, r, "void invoice", func(tx pgx.Tx) (reply, error) {
		ctx := r.Context()
		inv, org, answer, err := h.invoiceTarget(ctx, tx, r, orgParam)
		if answer != nil || err != nil {
			return derefReply(answer), err
		}
		if inv.Status != "open" {
			return detail(http.StatusBadRequest, "Only open invoices can be voided (current status: "+inv.Status+")"), nil
		}
		client, err := h.stripe.Client()
		if err != nil {
			return detail(http.StatusInternalServerError, err.Error()), nil
		}
		if _, err := client.V1Invoices.VoidInvoice(ctx, inv.StripeInvoiceID, nil); err != nil {
			h.logger.ErrorContext(ctx, "billing: stripe invoice void failed", "error", pyStripeError(err))
			return detail(http.StatusBadGateway, "Failed to void invoice: "+pyStripeError(err)), nil
		}
		now := h.nowUTC()
		if _, err := tx.Exec(ctx, `UPDATE invoices SET status = 'void', voided_at = $2, updated_at = $2,
			amount_remaining = 0 WHERE stripe_invoice_id = $1`, inv.StripeInvoiceID, now); err != nil {
			return reply{}, err
		}
		refreshed, err := loadInvoice(ctx, tx, inv.ID, org)
		if err != nil {
			return reply{}, err
		}
		if refreshed == nil {
			return detail(http.StatusNotFound, "Invoice not found"), nil
		}
		body, err := invoiceJSON(ctx, tx, *refreshed, true)
		if err != nil {
			return reply{}, err
		}
		return ok(body), nil
	})
}
