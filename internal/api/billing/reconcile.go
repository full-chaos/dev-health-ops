package billing

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
)

// reconcileReport is ReconciliationReport. mismatches and missing_local
// are always empty here: see reconcileTable.
type reconcileReport struct {
	subscriptions, invoices, refunds int64
	missingStripe                    []string
}

// reconcileTable is _fetch_local_rows then _compare against the Stripe
// side. Named limit, ported as is: the Python _fetch_stripe_rows calls
// StripeClient.<resource>.list(limit=100), which raises TypeError (the v1
// service takes params=, not keywords); the error is swallowed and the
// Stripe side is always empty, so no Stripe call is made, every local row
// with a Stripe id is "missing in Stripe", and nothing is ever a status
// mismatch or missing locally. A local read failure is swallowed as no
// rows, as the Python does.
func (h handlers) reconcileTable(ctx context.Context, tx pgx.Tx, table, stripeColumn string, org *uuid.UUID) (int64, []string) {
	sql, args := `SELECT `+stripeColumn+` FROM `+table, []any{}
	if org != nil {
		sql += ` WHERE org_id = $1`
		args = append(args, *org)
	}
	var count int64
	missing := []string{}
	seen := map[string]bool{}
	err := savepoint(ctx, tx, func(sp pgx.Tx) error {
		rows, err := sp.Query(ctx, sql, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var stripeID *string
			if err := rows.Scan(&stripeID); err != nil {
				return err
			}
			count++
			if stripeID != nil && !seen[*stripeID] {
				seen[*stripeID] = true
				missing = append(missing, *stripeID)
			}
		}
		return rows.Err()
	})
	if err != nil {
		h.logger.ErrorContext(ctx, "billing: failed loading local billing rows", "table", table, "error", err.Error())
		return 0, []string{}
	}
	return count, missing
}

func stringList(values []string) []pyjson.Value {
	out := make([]pyjson.Value, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}

// logReport is _log_report (only for an org-scoped run).
func (h handlers) logReport(ctx context.Context, tx pgx.Tx, org uuid.UUID, report reconcileReport) {
	status, action := "matched", "reconciliation.completed"
	if len(report.missingStripe) > 0 {
		status, action = "mismatch", "reconciliation.mismatch_found"
	}
	local := pyjson.NewObject()
	local.Set("subscriptions_checked", report.subscriptions)
	local.Set("invoices_checked", report.invoices)
	local.Set("refunds_checked", report.refunds)
	stripe := pyjson.NewObject()
	stripe.Set("missing_local", []pyjson.Value{})
	stripe.Set("missing_stripe", stringList(report.missingStripe))
	stripe.Set("mismatch_count", int64(0))
	h.writeAudit(ctx, tx, auditRecord{
		OrgID: org, ResourceID: uuid.New(), Action: action, ResourceType: "reconciliation",
		Description: "Completed billing reconciliation run", Status: status, LocalState: local, StripeState: stripe,
	})
}

// reconcile is trigger_reconciliation (reconcile_all).
func (h handlers) reconcile(w http.ResponseWriter, r *http.Request) {
	var errs pybody.Errors
	org, _ := errs.QueryUUID("org_id", pybody.LastQueryValue(r.URL.Query(), "org_id"))
	if len(errs) > 0 {
		h.write(w, validation(errs))
		return
	}
	if !policy.UserFrom(r.Context()).IsSuperuser {
		h.write(w, superadminRequired)
		return
	}
	if _, err := h.stripe.Client(); err != nil {
		h.internal(w, r, "reconcile", err)
		return
	}
	h.serve(w, r, "reconcile", func(tx pgx.Tx) (reply, error) {
		ctx := r.Context()
		started := h.nowUTC()
		if org != nil {
			h.writeAudit(ctx, tx, auditRecord{
				OrgID: *org, ResourceID: uuid.New(), Action: "reconciliation.started", ResourceType: "reconciliation",
				Description: "Started billing reconciliation", Status: "unresolved",
			})
		}
		var subs, invoices reconcileReport
		subs.subscriptions, subs.missingStripe = h.reconcileTable(ctx, tx, "subscriptions", "stripe_subscription_id", org)
		if org != nil {
			h.logReport(ctx, tx, *org, subs)
		}
		invoices.invoices, invoices.missingStripe = h.reconcileTable(ctx, tx, "invoices", "stripe_invoice_id", org)
		if org != nil {
			h.logReport(ctx, tx, *org, invoices)
		}
		refundCount, refundMissing := h.reconcileTable(ctx, tx, "refunds", "stripe_refund_id", org)
		combined := reconcileReport{
			subscriptions: subs.subscriptions, invoices: invoices.invoices, refunds: refundCount,
			missingStripe: append(append(append([]string{}, subs.missingStripe...), invoices.missingStripe...), refundMissing...),
		}
		completed := h.nowUTC()
		if org != nil {
			h.logReport(ctx, tx, *org, combined)
		}
		out := pyjson.NewObject()
		out.Set("started_at", pytime.ISOFormat(started))
		out.Set("completed_at", pytime.ISOFormat(completed))
		out.Set("subscriptions_checked", combined.subscriptions)
		out.Set("invoices_checked", combined.invoices)
		out.Set("refunds_checked", combined.refunds)
		out.Set("mismatches", []pyjson.Value{})
		out.Set("missing_local", []pyjson.Value{})
		out.Set("missing_stripe", stringList(combined.missingStripe))
		return ok(out), nil
	})
}
