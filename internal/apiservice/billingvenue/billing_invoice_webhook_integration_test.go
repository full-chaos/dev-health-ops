//go:build integration

package billingvenue

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// invoiceWebhookSubscription is the Stripe subscription id of the real
// test-mode invoice fixture; the test seeds a local subscription with it.
const invoiceWebhookSubscription = "sub_1UJ8n0EIXptJX86ewrDPuKoH"

// TestInvoiceWebhookAppliesTestModeEvents is the invoice branch of the
// Stripe webhook against real test-mode invoice event bodies (CHAOS-6526):
// the Go plane records each invoice once, moves its status forward only,
// resolves the org the way the event allows, and queues one notification
// per paid or failed event. The Python plane's invoice handler cannot run
// (its duplicate-event check raises): the test records its answers and that
// it wrote no invoice, the named divergence, so the proof is Go-only.
func TestInvoiceWebhookAppliesTestModeEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fake := newFakeStripe()
	goStripe := httptest.NewServer(fake.plane("go"))
	t.Cleanup(goStripe.Close)
	env := webhookEnv()
	pythonEnv := []string{"VENUE_STRIPE_EVENT_AS_DICT=invoice.created,invoice.finalized,invoice.paid,invoice.payment_failed,invoice.voided,invoice.updated,invoice.marked_uncollectible",
		"VENUE_PY_TRACEBACKS=1"}
	for key, value := range env {
		pythonEnv = append(pythonEnv, key+"="+value)
	}
	var seed billingFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(), PythonEnv: pythonEnv,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seed = billingSeed(t, ctx, admin)
			if _, err := admin.Exec(ctx, `INSERT INTO subscriptions (id, org_id, billing_plan_id, billing_price_id, stripe_subscription_id,
				stripe_customer_id, status, current_period_start, current_period_end, created_at, updated_at)
				VALUES ('11111111-0000-4000-8000-0000000000aa', $1, $2, $3, $4, 'cus_VJmL8AsRlG1mDG', 'active', now(), now(), now(), now())`,
				seed.orgB, seed.planTeam, seed.priceTeamM, invoiceWebhookSubscription); err != nil {
				t.Fatal(err)
			}
			return seed.tokenSpecs()
		},
	})
	loaded, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(key string) (string, bool) {
		value, ok := env[key]
		return value, ok
	}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		APIAddress: "127.0.0.1:0", RiverDatabaseSchema: "river", APIDatabaseRole: venue.Roles["api"],
		APIDatabaseURI: secrets.NewValue(venue.GoAPIDatabaseURI(t)),
		APIJWTSecret:   secrets.NewValue(venueKey), APIJWTIssuer: "dev-health-ops", APIJWTAudience: "dev-health-api",
		CORSAllowedOrigins: []string{"http://localhost:3000"},
		StripeSecretKey:    loaded.StripeSecretKey, APIBilling: loaded.APIBilling,
		StripeWebhookSecret: loaded.StripeWebhookSecret, LicensePrivateKey: loaded.LicensePrivateKey,
	}
	base := startBillingVenueAPI(t, ctx, cfg, venue, goStripe.URL)

	stamp := time.Now().Unix() + 200
	orgA, orgB, orgD := seed.orgA.String(), seed.orgB.String(), seed.orgD.String()
	var requests []venueoracle.Request
	send := func(name, eventID, eventType string, edit func(object map[string]any)) {
		value := loadWebhookFixture(t, "invoice.paid.json")
		value["type"], value["id"] = eventType, eventID
		object := value["data"].(map[string]any)["object"].(map[string]any)
		edit(object)
		body, _ := json.Marshal(value)
		requests = append(requests, venueoracle.Request{Name: name, Method: "POST", Path: webhookPath,
			Headers: map[string]string{"Stripe-Signature": webhookSignature(webhookVenueSecret, stamp, body), "Content-Type": "application/json"},
			Body:    venueoracle.B64(string(body))})
	}
	invoice := func(id, status string, paidAt any, parentSub any, metadata any, customer string) func(map[string]any) {
		return func(object map[string]any) {
			object["id"], object["status"], object["customer"] = id, status, customer
			object["metadata"] = metadata
			object["status_transitions"].(map[string]any)["paid_at"] = paidAt
			object["parent"] = parentSub
		}
	}
	subParent := func(metadata map[string]any) map[string]any {
		return map[string]any{"type": "subscription_details", "quote_details": nil,
			"subscription_details": map[string]any{"subscription": invoiceWebhookSubscription, "metadata": metadata}}
	}
	unknownParent := map[string]any{"type": "subscription_details", "quote_details": nil,
		"subscription_details": map[string]any{"subscription": "sub_unknown", "metadata": map[string]any{}}}
	customer := "cus_VJmL8AsRlG1mDG"

	// A subscription invoice whose org comes from the local subscription:
	// created, finalized, paid, the paid event redelivered, then an older
	// update that must not move it back.
	send("sub: created", "evt_in_sub_created", "invoice.created", invoice("in_sub", "draft", nil, subParent(map[string]any{}), map[string]any{}, customer))
	send("sub: finalized", "evt_in_sub_finalized", "invoice.finalized", invoice("in_sub", "open", nil, subParent(map[string]any{}), map[string]any{}, customer))
	send("sub: paid", "evt_in_sub_paid", "invoice.paid", invoice("in_sub", "paid", 1790241482, subParent(map[string]any{}), map[string]any{}, customer))
	send("sub: paid redelivered", "evt_in_sub_paid", "invoice.paid", invoice("in_sub", "paid", 1790241482, subParent(map[string]any{}), map[string]any{}, customer))
	send("sub: older update after paid", "evt_in_sub_updated", "invoice.updated", invoice("in_sub", "open", nil, subParent(map[string]any{}), map[string]any{}, customer))
	// The org from each other source.
	send("metadata org: payment failed", "evt_in_meta_failed", "invoice.payment_failed", invoice("in_meta", "open", nil, unknownParent, map[string]any{"org_id": orgD}, "cus_other"))
	send("subscription metadata org: voided", "evt_in_submeta_void", "invoice.voided", invoice("in_submeta", "void", nil,
		map[string]any{"type": "subscription_details", "subscription_details": map[string]any{"subscription": "sub_unknown", "metadata": map[string]any{"org_id": orgA}}},
		map[string]any{}, "cus_other"))
	send("customer license org: finalized", "evt_in_cust_final", "invoice.finalized", invoice("in_cust", "open", nil, unknownParent, map[string]any{}, "cus_A"))
	send("unknown metadata org falls through to the customer", "evt_in_fall", "invoice.finalized", invoice("in_fall", "open", nil, unknownParent,
		map[string]any{"org_id": "99999999-0000-4000-8000-000000000001"}, "cus_A"))
	// Uncollectible, then paid later (allowed); void is final.
	send("uncollectible", "evt_in_unc", "invoice.marked_uncollectible", invoice("in_unc", "uncollectible", nil, unknownParent, map[string]any{"org_id": orgB}, "cus_other"))
	send("uncollectible then paid", "evt_in_unc_paid", "invoice.paid", invoice("in_unc", "paid", 1790241600, unknownParent, map[string]any{"org_id": orgB}, "cus_other"))
	send("paid after void", "evt_in_submeta_paid", "invoice.paid", invoice("in_submeta", "paid", 1790241700,
		map[string]any{"type": "subscription_details", "subscription_details": map[string]any{"subscription": "sub_unknown", "metadata": map[string]any{"org_id": orgA}}},
		map[string]any{}, "cus_other"))
	// No org anywhere: nothing written.
	send("no org", "evt_in_none", "invoice.paid", invoice("in_none", "paid", 1790241482, unknownParent, map[string]any{}, "cus_other"))
	send("org not a uuid", "evt_in_bad", "invoice.finalized", invoice("in_bad", "open", nil, unknownParent, map[string]any{"org_id": "org-abc"}, "cus_other"))

	receipt := ""
	for _, request := range requests {
		response := venueoracle.Do(t, base, request)
		receipt += fmt.Sprintf("%-55s go=%d\n", request.Name, response.Status)
		if response.Status != 200 || strings.TrimSpace(response.Body) != `{"status":"ok"}` {
			t.Errorf("%s: go answered %d %s", request.Name, response.Status, response.Body)
		}
	}
	goURI := venue.AdminURI(t, venue.GoDB)
	row := func(query string, args ...any) string {
		pool, err := pgxpool.New(ctx, goURI)
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		rows, err := pool.Query(ctx, query, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				t.Fatal(err)
			}
			fields := make([]string, len(values))
			for index, value := range values {
				fields[index] = fmt.Sprint(value)
			}
			out = append(out, strings.Join(fields, " "))
		}
		return strings.Join(out, " | ")
	}
	expect := func(label, got, want string) {
		receipt += fmt.Sprintf("%-55s %s\n", label, venueoracle.Mark(got == want))
		if got != want {
			t.Errorf("%s:\n got  %s\n want %s", label, got, want)
		}
	}
	paidAt := time.Unix(1790241482, 0).UTC()
	expect("in_sub: one row, paid, org from the local subscription", row(`SELECT org_id::text, subscription_id::text, status, amount_due,
		amount_remaining, currency, paid_at = $2, finalized_at IS NOT NULL FROM invoices WHERE stripe_invoice_id = $1`, "in_sub", paidAt),
		fmt.Sprintf("%s 11111111-0000-4000-8000-0000000000aa paid 1000 0 usd true true", orgB))
	expect("in_sub: line items replaced, price from pricing.price_details", row(`SELECT count(*), min(stripe_price_id), min(amount)
		FROM invoice_line_items l JOIN invoices i ON i.id = l.invoice_id WHERE i.stripe_invoice_id = 'in_sub'`),
		"1 price_1UJ8muEIXptJX86eoKRExtq0 1000")
	expect("in_meta: payment_failed, org from metadata", row(`SELECT org_id::text, status FROM invoices WHERE stripe_invoice_id = 'in_meta'`),
		orgD+" payment_failed")
	expect("in_submeta: void, org from subscription metadata, paid after void ignored", row(`SELECT org_id::text, status, voided_at IS NOT NULL,
		paid_at IS NULL FROM invoices WHERE stripe_invoice_id = 'in_submeta'`), orgA+" void true true")
	expect("in_cust: org from the license customer", row(`SELECT org_id::text, status FROM invoices WHERE stripe_invoice_id = 'in_cust'`), orgA+" open")
	expect("in_fall: unknown metadata org falls through", row(`SELECT org_id::text FROM invoices WHERE stripe_invoice_id = 'in_fall'`), orgA)
	expect("in_unc: uncollectible then paid", row(`SELECT status, amount_remaining FROM invoices WHERE stripe_invoice_id = 'in_unc'`), "paid 0")
	expect("no org, org not a uuid: nothing written", row(`SELECT count(*) FROM invoices WHERE stripe_invoice_id IN ('in_none', 'in_bad')`), "0")
	expect("notifications: one receipt per paid event, one failure", row(`SELECT notification_type, idempotency_key FROM billing_notifications
		WHERE notification_type IN ('invoice_receipt', 'payment_failed') ORDER BY idempotency_key`),
		strings.Join([]string{
			"invoice_receipt billing:invoice_receipt:" + orgB + ":evt_in_sub_paid",
			"invoice_receipt billing:invoice_receipt:" + orgB + ":evt_in_unc_paid",
			"payment_failed billing:payment_failed:" + orgD + ":evt_in_meta_failed",
		}, " | "))
	expect("outbox: one handoff per notification", row(`SELECT count(*) FROM worker_job_outbox o JOIN billing_notifications n
		ON n.idempotency_key = o.dedupe_key WHERE n.notification_type IN ('invoice_receipt', 'payment_failed')`), "3")
	expect("receipt attributes", row(`SELECT attributes::text FROM billing_notifications WHERE idempotency_key = $1`,
		"billing:invoice_receipt:"+orgB+":evt_in_sub_paid"), `{"amount_cents": 1000, "currency": "usd", "invoice_url": "https://invoice.stripe.test/venue"}`)

	// The Python plane: the named divergence (its handler 500s and writes no
	// invoice).
	python := venue.ServePython(t, requests)
	statuses := map[int]int{}
	for _, response := range python {
		statuses[response.Status]++
	}
	pythonRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), `SELECT count(*) FROM invoices WHERE stripe_invoice_id LIKE 'in\_%'`)
	receipt += fmt.Sprintf("python plane (named divergence): answers %v, invoice rows %s\n", statuses, pythonRows)
	if pythonRows != "0" {
		t.Errorf("the Python plane wrote invoices (%s): the named divergence no longer holds; compare the planes instead", pythonRows)
	}
	t.Log("\n" + receipt)
	venueoracle.WriteGoOnlyProof(t, "Go applies test-mode invoice events (rows, status order, org resolution, notifications); the Python handler 500s and writes nothing (CHAOS-6526)")
}
