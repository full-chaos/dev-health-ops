//go:build integration

package billingvenue

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestRefundCreate covers the refund route against the fake Stripe: the
// payment intent stored on the invoice, else the one Stripe's invoice
// payments name (stored back), else the bare charge; the refundable balance;
// and Stripe's refusal. The Python route raises on an invoice column the
// model lacks, so the proof is Go-only.
func TestRefundCreate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fake := newFakeStripe()
	goStripe := httptest.NewServer(fake.plane("go"))
	t.Cleanup(goStripe.Close)
	var pythonEnv []string
	for key, value := range billingEnv {
		pythonEnv = append(pythonEnv, key+"="+value)
	}
	var seed billingFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(), PythonEnv: pythonEnv,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seed = billingSeed(t, ctx, admin)
			specs := seed.tokenSpecs()
			for name, spec := range ledgerSeed(t, ctx, admin, seed) {
				specs[name] = spec
			}
			return specs
		},
	})
	goURI := venue.AdminURI(t, venue.GoDB)
	admin, err := pgxpool.New(ctx, goURI)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	// Paid invoices with no stored payment intent: Stripe's invoice
	// payments name an intent, a bare charge, an intent Stripe refuses to
	// refund, or nothing.
	const invIntent, invCharge, invBad, invNone = "77777777-0000-4000-8000-000000000001", "77777777-0000-4000-8000-000000000002",
		"77777777-0000-4000-8000-000000000003", "77777777-0000-4000-8000-000000000004"
	if _, err := admin.Exec(ctx, `INSERT INTO invoices (id, org_id, stripe_invoice_id, stripe_customer_id, status, amount_due, amount_paid,
		currency, metadata, created_at, updated_at) VALUES
		('`+invIntent+`', $1, 'in_pi', 'cus_A', 'paid', 1000, 1000, 'usd', '{}', now(), now()),
		('`+invCharge+`', $1, 'in_charge', 'cus_A', 'paid', 500, 500, 'USD', '{}', now(), now()),
		('`+invBad+`', $1, 'in_pi_bad', 'cus_A', 'paid', 300, 300, 'usd', '{}', now(), now()),
		('`+invNone+`', $1, 'in_none', 'cus_A', 'paid', 200, 200, 'usd', '{}', now(), now())`, seed.orgA); err != nil {
		t.Fatal(err)
	}
	// An empty stored intent counts as none; a pending refund holds its
	// amount, a failed one does not.
	if _, err := admin.Exec(ctx, `UPDATE invoices SET payment_intent_id = '' WHERE id = '`+invCharge+`'`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `INSERT INTO refunds (id, org_id, invoice_id, stripe_refund_id, stripe_charge_id, amount, currency, status,
		metadata, created_at, updated_at) VALUES
		(gen_random_uuid(), $1, '`+invIntent+`', 're_held', 'ch_x', 100, 'usd', 'pending', '{}', '2026-01-01', '2026-01-01'),
		(gen_random_uuid(), $1, '`+invIntent+`', 're_failed', 'ch_x', 50, 'usd', 'failed', '{}', '2026-01-01', '2026-01-01')`, seed.orgA); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(key string) (string, bool) {
		value, ok := billingEnv[key]
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
	}
	base := startBillingVenueAPI(t, ctx, cfg, venue, goStripe.URL)

	receipt := ""
	refund := func(name, principal, body string, wantStatus int, want map[string]any) {
		response := venueoracle.Do(t, base, venueoracle.Request{Name: name, Method: "POST", Path: "/api/v1/billing/refunds",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens[principal], "Content-Type": "application/json"},
			Body:    venueoracle.B64(body)})
		var got map[string]any
		_ = json.Unmarshal([]byte(response.Body), &got)
		pass := response.Status == wantStatus
		for key, value := range want {
			if prefix, ok := value.(string); ok && strings.HasSuffix(prefix, "*") {
				text, _ := got[key].(string)
				pass = pass && strings.HasPrefix(text, strings.TrimSuffix(prefix, "*"))
			} else if got[key] != value {
				pass = false
			}
		}
		receipt += fmt.Sprintf("%-55s go=%d %s\n", name, response.Status, venueoracle.Mark(pass))
		if !pass {
			t.Errorf("%s: go answered %d %s; want %d with %v", name, response.Status, response.Body, wantStatus, want)
		}
	}
	// The seeded paid invoice: 7000 paid, pi_A2 stored, 1000 refunded.
	refund("stored intent, part", "super", `{"invoice_id":"`+invPaidA+`","amount":2000,"reason":"duplicate","description":"Twice"}`, 200,
		map[string]any{"amount": float64(2000), "stripe_payment_intent_id": "pi_A2", "stripe_charge_id": "ch_of_pi_A2",
			"status": "succeeded", "reason": "duplicate", "description": "Twice", "currency": "eur"})
	refund("over the balance", "super", `{"invoice_id":"`+invPaidA+`","amount":4001}`, 400,
		map[string]any{"detail": "Refund amount exceeds refundable balance"})
	refund("the rest of the balance", "super", `{"invoice_id":"`+invPaidA+`"}`, 200, map[string]any{"amount": float64(4000)})
	refund("fully refunded", "super", `{"invoice_id":"`+invPaidA+`","amount":1}`, 400,
		map[string]any{"detail": "Invoice has already been fully refunded"})
	refund("missing invoice", "super", `{"invoice_id":"`+seed.orgD.String()+`"}`, 404, map[string]any{"detail": "Invoice not found"})
	refund("open invoice", "super", `{"invoice_id":"`+invOpenA+`"}`, 400, map[string]any{"detail": "Invoice is not paid"})
	refund("member", "memberA", `{"invoice_id":"`+invPaidA+`"}`, 403, nil)
	refund("intent from invoice payments", "super", `{"invoice_id":"`+invIntent+`","amount":300}`, 200,
		map[string]any{"stripe_payment_intent_id": "pi_listed", "amount": float64(300)})
	refund("a JSON true amount is 1, as the request model coerces it", "super", `{"invoice_id":"`+invIntent+`","amount":true}`, 200,
		map[string]any{"stripe_payment_intent_id": "pi_listed", "amount": float64(1), "status": "pending", "failure_reason": "lost_or_stolen_card"})
	// 1000 paid, 100 pending, 301 refunded: 599 left.
	refund("over the balance a pending refund holds", "super", `{"invoice_id":"`+invIntent+`","amount":600}`, 400,
		map[string]any{"detail": "Refund amount exceeds refundable balance"})
	refund("beyond a 64-bit amount", "super", `{"invoice_id":"`+invIntent+`","amount":18446744073709551617}`, 400,
		map[string]any{"detail": "Refund amount exceeds refundable balance"})
	refund("exactly the balance, a failed refund not held", "super", `{"invoice_id":"`+invIntent+`","amount":599}`, 200,
		map[string]any{"amount": float64(599), "status": "succeeded"})
	refund("bare charge from invoice payments", "super", `{"invoice_id":"`+invCharge+`"}`, 200,
		map[string]any{"stripe_charge_id": "ch_bare", "stripe_payment_intent_id": nil, "amount": float64(500), "currency": "usd"})
	refund("no Stripe payment", "super", `{"invoice_id":"`+invNone+`"}`, 400,
		map[string]any{"detail": "Invoice has no Stripe payment to refund"})
	refund("Stripe refuses", "super", `{"invoice_id":"`+invBad+`"}`, 502, map[string]any{"detail": "Failed to create refund: *"})

	expect := func(label, query, want string) {
		got := venueoracle.TableRows(t, ctx, goURI, query)
		receipt += fmt.Sprintf("%-55s %s\n", label, venueoracle.Mark(got == want))
		if got != want {
			t.Errorf("%s:\n got  %s\n want %s", label, got, want)
		}
	}
	expect("refund rows written, by the superuser",
		`SELECT string_agg(i.stripe_invoice_id || '=' || r.amount::text || ':' || r.status || ':' || (r.initiated_by = '`+seed.super.String()+`')::text,
			',' ORDER BY i.stripe_invoice_id, r.amount) FROM refunds r JOIN invoices i ON i.id = r.invoice_id
			WHERE r.created_at > now() - interval '1 hour'`,
		"in_A2=2000:succeeded:true,in_A2=4000:succeeded:true,in_charge=500:succeeded:true,in_pi=1:pending:true,in_pi=300:succeeded:true,in_pi=599:succeeded:true,in_pi_bad=300:failed:true")
	expect("the listed intent is stored on the invoice",
		`SELECT string_agg(stripe_invoice_id || '=' || coalesce(payment_intent_id, 'null'), ',' ORDER BY stripe_invoice_id) FROM invoices
			WHERE stripe_invoice_id IN ('in_pi', 'in_charge', 'in_none')`, "in_charge=,in_none=null,in_pi=pi_listed")
	expect("the refund metadata names the org and invoice",
		`SELECT DISTINCT (metadata::jsonb - 'refund_id')::text FROM refunds WHERE stripe_payment_intent_id = 'pi_listed'`,
		`{"org_id": "`+seed.orgA.String()+`", "invoice_id": "`+invIntent+`"}`)
	// What reached Stripe: the listed payments are the paid ones, and each
	// refund names its payment, amount, reason and metadata.
	var calls []string
	for _, call := range fake.calls["go"] {
		if strings.HasPrefix(call, "POST /v1/refunds") || strings.HasPrefix(call, "GET /v1/invoice_payments") {
			// Each refund names its local row (a per-run id), checked
			// here and then left out of the comparison.
			call = strings.SplitN(call, " | version=", 2)[0]
			if strings.HasPrefix(call, "POST /v1/refunds") {
				if !refundRowID.MatchString(call) {
					t.Errorf("refund create without its row id: %s", call)
				}
				call = refundRowID.ReplaceAllString(call, "")
			}
			calls = append(calls, call)
		}
	}
	gotCalls := strings.Join(calls, "\n")
	receipt += fmt.Sprintf("%-55s %s\n", "stripe calls", venueoracle.Mark(gotCalls == wantRefundCalls(seed.orgA.String())))
	if gotCalls != wantRefundCalls(seed.orgA.String()) {
		t.Errorf("stripe calls:\n%s", gotCalls)
	}
	t.Log("\n" + receipt)
	venueoracle.WriteGoOnlyProof(t, "Go creates Stripe refunds for paid invoices from the stored or listed payment; the Python route raises on a missing invoice column")
}

// refundRowID is the refund-create form's local row id.
var refundRowID = regexp.MustCompile(`&metadata\[refund_id\]=[0-9a-f-]{36}`)

func wantRefundCalls(org string) string {
	refund := func(invoice, amount, payment, reason string) string {
		form := "amount=" + amount
		if strings.HasPrefix(payment, "ch_") {
			form += "&charge=" + payment
		}
		form += "&metadata[invoice_id]=" + invoice + "&metadata[org_id]=" + org
		if strings.HasPrefix(payment, "pi_") {
			form += "&payment_intent=" + payment
		}
		if reason != "" {
			form += "&reason=" + reason
		}
		return "POST /v1/refunds | query= | form=" + form
	}
	payments := func(invoice string) string {
		return "GET /v1/invoice_payments | query=invoice=" + invoice + "&status=paid | form="
	}
	return strings.Join([]string{
		refund(invPaidA, "2000", "pi_A2", "duplicate"),
		refund(invPaidA, "4000", "pi_A2", ""),
		payments("in_pi"),
		refund("77777777-0000-4000-8000-000000000001", "300", "pi_listed", ""),
		refund("77777777-0000-4000-8000-000000000001", "1", "pi_listed", ""),
		refund("77777777-0000-4000-8000-000000000001", "599", "pi_listed", ""),
		payments("in_charge"),
		refund("77777777-0000-4000-8000-000000000002", "500", "ch_bare", ""),
		payments("in_none"),
		payments("in_pi_bad"),
		refund("77777777-0000-4000-8000-000000000003", "300", "pi_fail", ""),
	}, "\n")
}
