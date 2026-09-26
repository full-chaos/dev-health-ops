package billing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stripe/stripe-go/v86"

	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
)

// The reconciliation is ReconciliationService.reconcile_all as the Python
// code reads, with one named divergence: the Python _fetch_stripe_rows
// calls StripeClient.<resource>.list(limit=100), which stripe-python
// refuses with a TypeError (its services take params=), so the Python
// Stripe side is always empty. Here each resource is listed 100 a page
// with every page followed, as the Python auto_paging_iter would. The
// venue compares this against the Python plane with only that call
// patched (VENUE_STRIPE_LIST_KWARGS).

// localRow is one _fetch_local_rows row.
type localRow struct {
	id       uuid.UUID
	stripeID *string
	status   *string
}

// stripeRow is one _fetch_stripe_rows row: status is the object's status,
// None when absent or null.
type stripeRow struct {
	id     string
	status pyjson.Value
}

// mismatch is ReconciliationMismatch (field is always "status").
type mismatch struct {
	resourceType string
	resourceID   uuid.UUID
	stripeID     string
	localValue   pyjson.Value
	stripeValue  pyjson.Value
}

// reconcileReport is ReconciliationReport.
type reconcileReport struct {
	subscriptions, invoices, refunds int64
	mismatches                       []mismatch
	missingLocal, missingStripe      []string
}

// fetchLocalRows is _fetch_local_rows: the same statement, no order (the
// report lists follow the database's row order on both planes). A read
// failure is logged and read as no rows.
func (h handlers) fetchLocalRows(ctx context.Context, tx pgx.Tx, table, stripeColumn string, org *uuid.UUID, since *time.Time) []localRow {
	sql, args := `SELECT id, `+stripeColumn+` as stripe_id, status, updated_at, org_id FROM `+table, []any{}
	var where []string
	if org != nil {
		args = append(args, *org)
		where = append(where, fmt.Sprintf("org_id = $%d", len(args)))
	}
	if since != nil {
		args = append(args, *since)
		where = append(where, fmt.Sprintf("updated_at >= $%d", len(args)))
	}
	if len(where) > 0 {
		sql += ` WHERE ` + strings.Join(where, ` AND `)
	}
	var out []localRow
	err := savepoint(ctx, tx, func(sp pgx.Tx) error {
		rows, err := sp.Query(ctx, sql, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row localRow
			var ignoredUpdated, ignoredOrg any
			if err := rows.Scan(&row.id, &row.stripeID, &row.status, &ignoredUpdated, &ignoredOrg); err != nil {
				return err
			}
			out = append(out, row)
		}
		return rows.Err()
	})
	if err != nil {
		h.logger.ErrorContext(ctx, "billing: failed loading local billing rows", "table", table, "error", err.Error())
		return nil
	}
	return out
}

// fetchStripeRows is _fetch_stripe_rows with the list call made as
// intended: every object of the resource, 100 a page. Any failure, part
// way included, is logged and read as no rows.
func (h handlers) fetchStripeRows(ctx context.Context, client *stripe.Client, resource string) []stripeRow {
	var out []stripeRow
	add := func(response *stripe.APIResponse) error {
		object, err := rawObject(response)
		if err != nil {
			return err
		}
		id, _ := object.Get("id")
		if !pyjson.Truthy(id) {
			return nil
		}
		status, _ := object.Get("status")
		out = append(out, stripeRow{id: pyStr(id), status: status})
		return nil
	}
	params := stripe.ListParams{Limit: stripe.Int64(100)}
	var err error
	switch resource {
	case "subscriptions":
		for item, listErr := range client.V1Subscriptions.List(ctx, &stripe.SubscriptionListParams{ListParams: params}).All(ctx) {
			if err = listErr; err == nil {
				err = add(item.LastResponse)
			}
			if err != nil {
				break
			}
		}
	case "invoices":
		for item, listErr := range client.V1Invoices.List(ctx, &stripe.InvoiceListParams{ListParams: params}).All(ctx) {
			if err = listErr; err == nil {
				err = add(item.LastResponse)
			}
			if err != nil {
				break
			}
		}
	case "refunds":
		for item, listErr := range client.V1Refunds.List(ctx, &stripe.RefundListParams{ListParams: params}).All(ctx) {
			if err = listErr; err == nil {
				err = add(item.LastResponse)
			}
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		h.logger.ErrorContext(ctx, "billing: failed loading stripe billing rows", "resource", resource, "error", pyStripeError(err))
		return nil
	}
	return out
}

// orderedKeys is a Python dict built by assignment: a repeated key keeps
// its first position and takes the last value.
type orderedKeys[T any] struct {
	keys   []string
	values map[string]T
}

func (o *orderedKeys[T]) set(key string, value T) {
	if o.values == nil {
		o.values = map[string]T{}
	}
	if _, seen := o.values[key]; !seen {
		o.keys = append(o.keys, key)
	}
	o.values[key] = value
}

func optionalText(value *string) pyjson.Value {
	if value == nil {
		return nil
	}
	return *value
}

// compareRows is _compare.
func compareRows(resourceType string, local []localRow, remote []stripeRow) reconcileReport {
	var byStripe orderedKeys[localRow]
	for _, row := range local {
		if row.stripeID != nil {
			byStripe.set(*row.stripeID, row)
		}
	}
	var byID orderedKeys[stripeRow]
	for _, row := range remote {
		byID.set(row.id, row)
	}
	report := reconcileReport{missingLocal: []string{}, missingStripe: []string{}}
	for _, stripeID := range byStripe.keys {
		row := byStripe.values[stripeID]
		remoteRow, found := byID.values[stripeID]
		if !found {
			report.missingStripe = append(report.missingStripe, stripeID)
			continue
		}
		localValue := optionalText(row.status)
		if !sameStatus(localValue, remoteRow.status) {
			report.mismatches = append(report.mismatches, mismatch{resourceType: resourceType, resourceID: row.id,
				stripeID: stripeID, localValue: localValue, stripeValue: remoteRow.status})
		}
	}
	for _, id := range byID.keys {
		if _, found := byStripe.values[id]; !found {
			report.missingLocal = append(report.missingLocal, id)
		}
	}
	count := int64(len(local))
	switch resourceType {
	case "subscription":
		report.subscriptions = count
	case "invoice":
		report.invoices = count
	case "refund":
		report.refunds = count
	}
	return report
}

// sameStatus is Python == between the stored status (a str or None) and
// the Stripe one (any JSON value).
func sameStatus(local, remote pyjson.Value) bool {
	switch l := local.(type) {
	case nil:
		return remote == nil
	case string:
		r, isString := remote.(string)
		return isString && r == l
	}
	return false
}

// pyStr is str() of a status value inside the mismatch description.
func pyStr(value pyjson.Value) string {
	switch v := value.(type) {
	case nil:
		return "None"
	case string:
		return v
	}
	return pyjson.Repr(value)
}

func stringList(values []string) []pyjson.Value {
	out := make([]pyjson.Value, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}

func statusState(value pyjson.Value) *pyjson.Object {
	state := pyjson.NewObject()
	state.Set("field", "status")
	state.Set("value", value)
	return state
}

// logReport is _log_report (only for an org-scoped run).
func (h handlers) logReport(ctx context.Context, tx pgx.Tx, org uuid.UUID, report reconcileReport) {
	for _, m := range report.mismatches {
		h.writeAudit(ctx, tx, auditRecord{
			OrgID: org, ResourceID: m.resourceID, Action: "reconciliation.mismatch_found", ResourceType: m.resourceType,
			Description: fmt.Sprintf("Mismatch on status: local=%s, stripe=%s", pyStr(m.localValue), pyStr(m.stripeValue)),
			Status:      "mismatch", LocalState: statusState(m.localValue), StripeState: statusState(m.stripeValue),
		})
	}
	status, action := "matched", "reconciliation.completed"
	if len(report.mismatches) > 0 || len(report.missingLocal) > 0 || len(report.missingStripe) > 0 {
		status, action = "mismatch", "reconciliation.mismatch_found"
	}
	local := pyjson.NewObject()
	local.Set("subscriptions_checked", report.subscriptions)
	local.Set("invoices_checked", report.invoices)
	local.Set("refunds_checked", report.refunds)
	stripeState := pyjson.NewObject()
	stripeState.Set("missing_local", stringList(report.missingLocal))
	stripeState.Set("missing_stripe", stringList(report.missingStripe))
	stripeState.Set("mismatch_count", int64(len(report.mismatches)))
	h.writeAudit(ctx, tx, auditRecord{
		OrgID: org, ResourceID: uuid.New(), Action: action, ResourceType: "reconciliation",
		Description: "Completed billing reconciliation run", Status: status, LocalState: local, StripeState: stripeState,
	})
}

func mismatchJSON(m mismatch) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("resource_type", m.resourceType)
	out.Set("resource_id", m.resourceID.String())
	out.Set("stripe_id", m.stripeID)
	out.Set("field", "status")
	out.Set("local_value", m.localValue)
	out.Set("stripe_value", m.stripeValue)
	out.Set("severity", "critical")
	return out
}

func concat[T any](parts ...[]T) []T {
	out := []T{}
	for _, part := range parts {
		out = append(out, part...)
	}
	return out
}

// reconcileResult is what reconcile_all returns: the combined report and the
// two instants it stamps.
type reconcileResult struct {
	report             reconcileReport
	started, completed time.Time
}

// reconcileAll is ReconciliationService.reconcile_all: the subscription, invoice
// and refund comparisons of one organisation (every organisation when org is nil),
// each with its audit rows when an organisation is named.
func (h handlers) reconcileAll(ctx context.Context, tx pgx.Tx, client *stripe.Client, org *uuid.UUID) reconcileResult {
	started := h.nowUTC()
	if org != nil {
		h.writeAudit(ctx, tx, auditRecord{
			OrgID: *org, ResourceID: uuid.New(), Action: "reconciliation.started", ResourceType: "reconciliation",
			Description: "Started billing reconciliation", Status: "unresolved",
		})
	}
	step := func(resourceType, table, stripeColumn string) reconcileReport {
		local := h.fetchLocalRows(ctx, tx, table, stripeColumn, org, nil)
		return compareRows(resourceType, local, h.fetchStripeRows(ctx, client, table))
	}
	subs := step("subscription", "subscriptions", "stripe_subscription_id")
	if org != nil {
		h.logReport(ctx, tx, *org, subs)
	}
	invoices := step("invoice", "invoices", "stripe_invoice_id")
	if org != nil {
		h.logReport(ctx, tx, *org, invoices)
	}
	refunds := step("refund", "refunds", "stripe_refund_id")
	combined := reconcileReport{
		subscriptions: subs.subscriptions, invoices: invoices.invoices, refunds: refunds.refunds,
		mismatches:    concat(subs.mismatches, invoices.mismatches, refunds.mismatches),
		missingLocal:  concat(subs.missingLocal, invoices.missingLocal, refunds.missingLocal),
		missingStripe: concat(subs.missingStripe, invoices.missingStripe, refunds.missingStripe),
	}
	completed := h.nowUTC()
	if org != nil {
		h.logReport(ctx, tx, *org, combined)
	}
	return reconcileResult{report: combined, started: started, completed: completed}
}

// reconcileInvoices is ReconciliationService.reconcile_invoices with a since: the
// invoices updated on or after it are compared with Stripe's (whose listing takes
// no since), and the run is logged for an organisation. Only the count of local
// invoices it checked is used by its caller.
func (h handlers) reconcileInvoices(ctx context.Context, tx pgx.Tx, client *stripe.Client, org *uuid.UUID, since *time.Time) reconcileReport {
	local := h.fetchLocalRows(ctx, tx, "invoices", "stripe_invoice_id", org, since)
	report := compareRows("invoice", local, h.fetchStripeRows(ctx, client, "invoices"))
	if org != nil {
		h.logReport(ctx, tx, *org, report)
	}
	return report
}

// reportJSON is ReconciliationReport.to_dict.
func reportJSON(result reconcileResult) *pyjson.Object {
	mismatches := make([]pyjson.Value, 0, len(result.report.mismatches))
	for _, m := range result.report.mismatches {
		mismatches = append(mismatches, mismatchJSON(m))
	}
	out := pyjson.NewObject()
	out.Set("started_at", pytime.ISOFormat(result.started))
	out.Set("completed_at", pytime.ISOFormat(result.completed))
	out.Set("subscriptions_checked", result.report.subscriptions)
	out.Set("invoices_checked", result.report.invoices)
	out.Set("refunds_checked", result.report.refunds)
	out.Set("mismatches", mismatches)
	out.Set("missing_local", stringList(result.report.missingLocal))
	out.Set("missing_stripe", stringList(result.report.missingStripe))
	return out
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
	client, err := h.stripe.Client()
	if err != nil {
		h.internal(w, r, "reconcile", err)
		return
	}
	h.serve(w, r, "reconcile", func(tx pgx.Tx) (reply, error) {
		return ok(reportJSON(h.reconcileAll(r.Context(), tx, client, org))), nil
	})
}

// Reconcile runs the billing reconciliation outside HTTP, for the operator verb
// (`billing reconcile`); the API's reconcile route runs the same code.
type Reconcile struct {
	Pool   *pgxpool.Pool
	Stripe *stripeclient.Provider
	Logger *slog.Logger
	// Now is injectable for tests; nil means time.Now.
	Now func() time.Time
}

func (s Reconcile) handlers() handlers {
	h := handlers{pool: s.Pool, stripe: s.Stripe, logger: s.Logger, now: s.Now}
	if h.logger == nil {
		h.logger = slog.New(slog.DiscardHandler)
	}
	if h.now == nil {
		h.now = time.Now
	}
	return h
}

// Run is the body of `billing reconcile`: reconcile_all for org (every
// organisation when nil) and, when since is set, reconcile_invoices from that
// instant, whose count of checked invoices replaces the combined report's. The
// writes (the audit rows of a named organisation) are committed when the run
// ends. The result is the report as ReconciliationReport.to_dict lays it out.
func (s Reconcile) Run(ctx context.Context, org *uuid.UUID, since *time.Time) (*pyjson.Object, error) {
	if s.Pool == nil {
		return nil, errors.New("billing: no database pool")
	}
	client, err := s.Stripe.Client()
	if err != nil {
		return nil, err
	}
	h := s.handlers()
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result := h.reconcileAll(ctx, tx, client, org)
	if since != nil {
		result.report.invoices = h.reconcileInvoices(ctx, tx, client, org, since).invoices
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return reportJSON(result), nil
}
