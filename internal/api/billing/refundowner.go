package billing

import (
	"context"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// A refund event that is not recorded, or is recorded on a rule Python does
// not have, says so in the log and on refundEventDecisions, by one of these
// reasons. Nothing that applies less than the event asked for is silent.
const (
	// Skips: the event answers 200 and writes nothing.
	skipNoDataObject        = "no_data_object"
	skipNoRefundID          = "no_refund_id"
	skipAmount              = "amount_not_storable"
	skipSeveralInvoices     = "payment_held_by_several_invoices"
	skipNoOwner             = "no_owner"
	skipWaitingRowMisfit    = "waiting_row_does_not_fit"
	skipOrgMissing          = "org_does_not_exist"
	skipChargeWithoutRefund = "charge_refunded_without_refund_list"
	skipEmptyRefundList     = "charge_refunded_empty_refund_list"
	// Applied on a Go-only rule (named divergences from Python).
	noteOwnerFromPayment    = "org_from_payment_invoice_without_metadata"
	noteOverriddenByPayment = "org_metadata_overridden_by_payment"
	noteInvoiceNotOrgs      = "metadata_invoice_not_the_orgs"
	noteWaitingRowCompleted = "waiting_row_completed"
	noteSettledKept         = "settled_status_kept"
	noteFailureKept         = "failure_reason_kept"
	noteViaExtension        = "applied_via_refund_event_extension"
	noteCompletionKeptState = "route_completion_kept_settled_status"
	noteCompletionKeptFail  = "route_completion_kept_failure_reason"
)

// refundEventDecisions counts the refund events that were skipped or applied
// on a rule Python does not have, by reason.
var refundEventDecisions = func() metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/api/billing").Int64Counter(
		"dev_health_api_stripe_refund_event_decisions_total",
		metric.WithDescription("Stripe refund events skipped, or applied on a Go-only rule, by reason"))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("dev_health_api_stripe_refund_event_decisions_total")
	}
	return counter
}()

// countRefundDecision records one decision on the counter.
func countRefundDecision(ctx context.Context, reason string) {
	refundEventDecisions.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// noteRefundRule records a Go-only rule that was applied: an Info line naming
// the rule and the refund, and one count, so nothing that leaves Python's
// behaviour is silent.
func (h handlers) noteRefundRule(ctx context.Context, reason string, fields ...any) {
	h.logger.InfoContext(ctx, "Refund applied on a Go-only rule", append([]any{"rule", reason}, fields...)...)
	countRefundDecision(ctx, reason)
}

// refundOwnerInput is what decides who owns a refund not stored yet.
type refundOwnerInput struct {
	// Invoices are the invoices paid by the event's payment intent (at most
	// two are read: one is a match, two are several).
	Invoices []paymentInvoice
	// MetaOrg is the org the event's metadata names (nil: none or not a uuid).
	MetaOrg *uuid.UUID
	// MetaInvoice is the invoice the event's metadata names (nil: none or not
	// a uuid). It never chooses an owner; it only has to agree with the
	// invoice a waiting row is linked to.
	MetaInvoice *uuid.UUID
	// Waiting is the write-first row the event's `refund_id` metadata names
	// and that still has no Stripe id (nil: none).
	Waiting *storedRefund
}

// refundOwnerDecision is the one decision.
type refundOwnerDecision struct {
	// Skip is the reason nothing is recorded ("" = record).
	Skip string
	// Org owns the refund; nil only when a waiting row is completed and no
	// org is named anywhere (the row keeps its own).
	Org *uuid.UUID
	// Invoice is the payment's invoice (exactly one holds the payment).
	Invoice *uuid.UUID
	// Adopt completes Waiting instead of inserting a row.
	Adopt bool
	// Note names a Go-only rule the decision applied ("" = Python's).
	Note string
}

// decideRefundOwner is the whole ownership rule of a refund event, in one
// place: the payment decides, the metadata only when the payment names none.
//
//	invoices holding the payment: none    -> the metadata's org (none named: no owner)
//	                              one     -> that invoice's org and the invoice, whatever the metadata says
//	                              several -> skip (metadata never picks an owner for an ambiguous payment)
//
// A waiting row is completed only when it fits that owner: the same org, and
// the same invoice as the payment's (else the metadata's) when both name one. A row that names the refund and does
// not fit stops the event, since a second row would collide with the row's
// own completion. The `refund_id` metadata never overrides the payment's org.
func decideRefundOwner(in refundOwnerInput) refundOwnerDecision {
	var out refundOwnerDecision
	switch {
	case len(in.Invoices) > 1:
		return refundOwnerDecision{Skip: skipSeveralInvoices}
	case len(in.Invoices) == 1:
		out.Org, out.Invoice = &in.Invoices[0].org, &in.Invoices[0].id
		if in.MetaOrg != nil && *in.MetaOrg != *out.Org {
			out.Note = noteOverriddenByPayment
		} else if in.MetaOrg == nil {
			out.Note = noteOwnerFromPayment
		}
	default:
		out.Org = in.MetaOrg
	}
	if in.Waiting == nil {
		if out.Org == nil {
			return refundOwnerDecision{Skip: skipNoOwner}
		}
		return out
	}
	// The invoice a waiting row must agree with: the payment's, else the one
	// the metadata names.
	expected := out.Invoice
	if expected == nil {
		expected = in.MetaInvoice
	}
	fits := (out.Org == nil || in.Waiting.org == *out.Org) &&
		(expected == nil || in.Waiting.invoice == nil || *in.Waiting.invoice == *expected)
	if !fits {
		return refundOwnerDecision{Skip: skipWaitingRowMisfit}
	}
	out.Adopt = true
	return out
}
