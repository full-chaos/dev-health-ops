//go:build integration

package billingvenue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// webhookRefundOrgs are the refund event cases' own orgs, and
// webhookRefundInvoice is the paid invoice of the first.
var webhookRefundOrgs = []string{
	"aaaaaaaa-0000-4000-8000-000000000001", "aaaaaaaa-0000-4000-8000-000000000002",
}

const webhookRefundInvoice = "aaaaaaaa-0000-4000-8000-0000000000f1"

// refundSeed adds the orgs, one invoice and one stored refund the refund
// events act on. The stored refund (re_seeded) belongs to the second org
// and carries a description, a charge and a payment intent of its own.
func refundSeed(t *testing.T, ctx context.Context, admin *pgxpool.Pool) {
	t.Helper()
	for index, id := range webhookRefundOrgs {
		if _, err := admin.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier) VALUES ($1, $2, $2, 'community')`,
			id, fmt.Sprintf("webhook-refund-%d", index)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := admin.Exec(ctx, `INSERT INTO invoices (id, org_id, stripe_invoice_id, stripe_customer_id, status, amount_due, amount_paid, currency,
		metadata, created_at, updated_at) VALUES ($1, $2, 'in_refund_evt', 'cus_refund', 'paid', 5000, 5000, 'usd', '{}', now(), now())`,
		webhookRefundInvoice, webhookRefundOrgs[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `INSERT INTO refunds (id, org_id, invoice_id, stripe_refund_id, stripe_charge_id, stripe_payment_intent_id,
		amount, currency, status, reason, description, metadata, created_at, updated_at) VALUES
		(gen_random_uuid(), $1, NULL, 're_seeded', 'ch_seeded', 'pi_seeded', 500, 'usd', 'pending', 'duplicate', 'stored description', '{"kept": "no"}',
		 now(), now())`, webhookRefundOrgs[1]); err != nil {
		t.Fatal(err)
	}
}

// refundRequests are the refund event cases: charge.refund.updated for a
// refund the platform has not seen (created), seen (updated), and seen
// under another org's name; every field Python assigns, present, absent and
// null; the events that must write nothing; and charge.refunded with and
// without the refund list.
func refundRequests(t *testing.T, event webhookEventFunc) {
	t.Helper()
	org, invoice := webhookRefundOrgs, webhookRefundInvoice
	type fields map[string]any
	updated := func(name, refundID string, set fields) {
		event(name, "charge.refund.updated.json", "charge.refund.updated", func(_, object map[string]any) {
			object["id"] = refundID
			for key, value := range set {
				if value == "<absent>" {
					delete(object, key)
					continue
				}
				object[key] = value
			}
		})
	}
	meta := func(orgID, invoiceID any) fields {
		return fields{"metadata": map[string]any{"org_id": orgID, "invoice_id": invoiceID}}
	}

	// Created, then the same refund again with a new status (an update),
	// then a status Stripe does not leave (failed) with its reason, then
	// the reason absent (kept) and null (cleared).
	updated("refund: created (org 1, invoice)", "re_evt_1", fields{
		"amount": 700, "currency": "USD", "status": "pending", "reason": "requested_by_customer",
		"description": "first description", "failure_reason": nil, "charge": "ch_evt_1", "payment_intent": "pi_evt_1",
		"metadata": map[string]any{"org_id": org[0], "invoice_id": invoice, "note": "é ✓"},
	})
	updated("refund: the same refund succeeded", "re_evt_1", fields{
		"amount": 700, "status": "succeeded", "description": "second description",
		"metadata": map[string]any{"org_id": org[0], "invoice_id": invoice, "note": "changed"},
	})
	updated("refund: failed with a reason", "re_evt_2", with(meta(org[0], invoice), fields{
		"amount": 300, "status": "failed", "failure_reason": "lost_or_stolen_card", "reason": "fraudulent",
	}))
	updated("refund: failure reason absent keeps it, reason null clears it", "re_evt_2", with(meta(org[0], invoice), fields{
		"amount": 300, "status": "canceled", "failure_reason": "<absent>", "reason": nil, "payment_intent": nil,
	}))
	updated("refund: charge, payment intent, amount and currency absent keep the stored ones", "re_evt_2", with(meta(org[0], invoice), fields{
		"amount": "<absent>", "currency": "<absent>", "charge": "<absent>", "payment_intent": "<absent>", "status": "<absent>",
	}))

	// A stored refund: no org in the event, so its own org counts; the
	// stored description stays; the expanded charge object gives its id.
	updated("refund: stored refund, no org in the event", "re_seeded", fields{
		"metadata": map[string]any{}, "amount": 600, "status": "succeeded", "description": "event description",
		"charge": map[string]any{"id": "ch_expanded", "object": "charge"}, "payment_intent": "pi_new", "reason": "<absent>",
	})
	// An org in the event that is not the stored refund's: the row is kept
	// and only its fields change.
	updated("refund: stored refund, the event names another org", "re_seeded", with(meta(org[0], nil), fields{"status": "failed", "failure_reason": "expired_or_canceled_card"}))

	// Events that write nothing.
	updated("refund: unknown refund, no org", "re_evt_none", fields{"metadata": map[string]any{}})
	updated("refund: unknown refund, org not a uuid", "re_evt_bad_org", meta("org-abc", invoice))
	updated("refund: unknown refund, metadata null", "re_evt_null_meta", fields{"metadata": nil})
	updated("refund: no id", "", fields{"metadata": map[string]any{"org_id": org[0]}})

	// An invoice id that is not a uuid is ignored; an unknown-status refund
	// is stored as Stripe says it.
	updated("refund: invoice id not a uuid", "re_evt_3", with(meta(org[0], "not-a-uuid"), fields{"amount": 100, "status": "requires_action"}))
	updated("refund: nothing but id and org", "re_evt_4", fields{
		"metadata": map[string]any{"org_id": org[1]}, "amount": "<absent>", "currency": "<absent>", "status": "<absent>",
		"reason": "<absent>", "description": "<absent>", "charge": "<absent>", "payment_intent": "<absent>", "failure_reason": "<absent>",
	})

	// charge.refunded: the charge's refund list (one new refund, one
	// already stored), and the charge without one.
	event("charge.refunded: two listed refunds", "charge.refunded.json", "charge.refunded", func(_, object map[string]any) {
		object["id"] = "ch_listed"
		object["refunds"] = map[string]any{"object": "list", "data": []any{
			map[string]any{"id": "re_evt_list_1", "object": "refund", "amount": 250, "currency": "usd", "status": "succeeded",
				"reason": nil, "charge": "ch_listed", "payment_intent": "pi_listed", "metadata": map[string]any{"org_id": org[1], "invoice_id": invoice}},
			map[string]any{"id": "re_seeded", "object": "refund", "amount": 500, "currency": "usd", "status": "succeeded", "metadata": map[string]any{}},
		}}
	})
	event("charge.refunded: no refund list", "charge.refunded.json", "charge.refunded", func(_, object map[string]any) {
		delete(object, "refunds")
	})
	event("charge.refunded: refunds not a list", "charge.refunded.json", "charge.refunded", func(_, object map[string]any) {
		object["refunds"] = map[string]any{"data": "x"}
	})
}

func with(base map[string]any, more map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range base {
		out[key] = value
	}
	for key, value := range more {
		out[key] = value
	}
	return out
}

// refundTables are the refund rows the events write, with ids and clock
// times blanked (the id is generated per plane; Python does not move
// updated_at and Go does, so only its order against created_at is kept).
// The metadata column is left out: Python's update stores {} for every
// event (its ensure_dict refuses the SDK's StripeObject, so the metadata
// is dropped) and Go stores the event's metadata. refundMetadata compares
// that named divergence separately.
func refundTables(t *testing.T, ctx context.Context, start time.Time) map[string]func(string) string {
	ts := func(column string) string { return webhookTimestamp(column, start) }
	return map[string]func(string) string{
		"refunds": func(uri string) string {
			return tableRows(t, ctx, uri, `SELECT coalesce(stripe_refund_id, '<null>'), org_id::text, coalesce(invoice_id::text, '<null>'),
				coalesce(subscription_id::text, '<null>'), coalesce(stripe_charge_id, '<null>'), coalesce(stripe_payment_intent_id, '<null>'),
				amount, currency, status, coalesce(reason, '<null>'), coalesce(description, '<null>'), coalesce(failure_reason, '<null>'),
				coalesce(idempotency_key, '<null>'), initiated_by IS NULL, `+ts("created_at")+`, updated_at >= created_at
				FROM refunds ORDER BY stripe_refund_id`)
		},
	}
}

// measureRefunds fails the run when the Go plane wrote none of what the
// refund cases exist to compare.
func measureRefunds(t *testing.T, ctx context.Context, uri string) {
	t.Helper()
	checks := map[string]string{
		"refunds created by events":     `SELECT count(*) FROM refunds WHERE stripe_refund_id LIKE 're_evt_%'`,
		"a refund with a failure":       `SELECT count(*) FROM refunds WHERE failure_reason IS NOT NULL`,
		"a refund from a charge's list": `SELECT count(*) FROM refunds WHERE stripe_refund_id = 're_evt_list_1'`,
		"a stored refund updated":       `SELECT count(*) FROM refunds WHERE stripe_refund_id = 're_seeded' AND status <> 'pending'`,
	}
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for name, query := range checks {
		var count int
		if err := pool.QueryRow(ctx, query).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			t.Errorf("the Go plane wrote no %s: the refund cases measured nothing there", name)
		}
	}
}

// refundMetadata holds the named divergence in the metadata column: no
// row an event wrote carries metadata on the Python plane, and the Go
// plane keeps the event's own. When Python stores it too, the first check
// fails and the divergence is gone.
func refundMetadata(t *testing.T, ctx context.Context, pythonURI, goURI string) string {
	t.Helper()
	const kept = `SELECT count(*) FILTER (WHERE metadata::text <> '{}')::text || ' of ' || count(*)::text FROM refunds WHERE stripe_refund_id LIKE 're_evt_%'`
	python, goPlane := tableRows(t, ctx, pythonURI, kept), tableRows(t, ctx, goURI, kept)
	if python != "0 of 5" {
		t.Errorf("python kept metadata on event-written refunds (%s): the divergence is gone, compare the column", python)
	}
	if goPlane != "5 of 5" {
		t.Errorf("go kept metadata on %s event-written refunds, want 5 of 5 (every event carried some)", goPlane)
	}
	changed := tableRows(t, ctx, goURI, `SELECT metadata->>'note' FROM refunds WHERE stripe_refund_id = 're_evt_1'`)
	if changed != "changed" {
		t.Errorf("go stored metadata note %q for re_evt_1, want the second event's %q", changed, "changed")
	}
	return fmt.Sprintf("refund metadata: python %s, go %s (named divergence)\n", python, goPlane)
}
