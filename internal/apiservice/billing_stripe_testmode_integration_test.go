//go:build integration

package apiservice

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stripe/stripe-go/v86"

	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestStripeTestModeBillingDifferential drives both planes against REAL
// Stripe test mode: it runs only with DEV_HEALTH_STRIPE_TEST_MODE=1 and a
// STRIPE_SECRET_KEY from the runner's environment, and refuses any key that
// is not a test-mode key. The key is never printed.
//
// Each plane gets its own Stripe customer (Stripe's default test card,
// pm_card_visa) and subscription, so one plane's writes never change what
// the other reads. Pull runs on both planes before either plane creates a
// product. Afterwards both subscriptions are read back from Stripe and
// compared field by field; the products and prices the run created are
// archived and the subscriptions canceled at cleanup.
func TestStripeTestModeBillingDifferential(t *testing.T) {
	if os.Getenv("DEV_HEALTH_STRIPE_TEST_MODE") != "1" {
		t.Skip("real Stripe test mode runs only with DEV_HEALTH_STRIPE_TEST_MODE=1 (local receipt)")
	}
	key := os.Getenv("STRIPE_SECRET_KEY")
	if !strings.HasPrefix(key, "sk_test_") {
		t.Fatal("STRIPE_SECRET_KEY is not a Stripe test-mode key; refusing to run (value not printed)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	client, _ := stripeclient.New(stripeclient.Options{Key: key}).Client()
	run := "venue-" + uuid.NewString()[:8]

	// Stripe fixtures: one product with two monthly prices, and per plane a
	// customer paying with the default test card on a subscription to the
	// first price.
	product, err := client.V1Products.Create(ctx, &stripe.ProductCreateParams{Name: stripe.String("Venue " + run),
		Metadata: map[string]string{"plan_key": run, "tier": "team"}})
	if err != nil {
		t.Fatalf("create product: %v", err)
	}
	var created []string
	created = append(created, product.ID)
	newPrice := func(amount int64) *stripe.Price {
		price, err := client.V1Prices.Create(ctx, &stripe.PriceCreateParams{Product: stripe.String(product.ID), UnitAmount: stripe.Int64(amount),
			Currency: stripe.String("usd"), Recurring: &stripe.PriceCreateRecurringParams{Interval: stripe.String("month")}})
		if err != nil {
			t.Fatalf("create price: %v", err)
		}
		return price
	}
	priceA, priceB := newPrice(1000), newPrice(2000)
	type planeStripe struct{ customer, subscription, openInvoice, paidInvoice string }
	planes := map[string]planeStripe{}
	// Registered before the fixtures, so a fixture failure part way still
	// cleans up what was created.
	t.Cleanup(func() {
		cleanup := context.Background()
		for _, plane := range planes {
			_, _ = client.V1Subscriptions.Cancel(cleanup, plane.subscription, nil)
			_, _ = client.V1Customers.Delete(cleanup, plane.customer, nil)
		}
		for _, id := range created {
			_, _ = client.V1Products.Update(cleanup, id, &stripe.ProductUpdateParams{Active: stripe.Bool(false)})
		}
	})
	for _, plane := range []string{"py", "go"} {
		customer, err := client.V1Customers.Create(ctx, &stripe.CustomerCreateParams{Name: stripe.String(run + " " + plane), Email: stripe.String(run + "-" + plane + "@example.com"),
			PaymentMethod:   stripe.String("pm_card_visa"),
			InvoiceSettings: &stripe.CustomerCreateInvoiceSettingsParams{DefaultPaymentMethod: stripe.String("pm_card_visa")}})
		if err != nil {
			t.Fatalf("create customer: %v", err)
		}
		planes[plane] = planeStripe{customer: customer.ID}
		sub, err := client.V1Subscriptions.Create(ctx, &stripe.SubscriptionCreateParams{Customer: stripe.String(customer.ID),
			Items: []*stripe.SubscriptionCreateItemParams{{Price: stripe.String(priceA.ID)}}})
		if err != nil {
			t.Fatalf("create subscription: %v", err)
		}
		// An open invoice to void, beside the subscription's first
		// invoice, which the default test card has paid.
		if _, err := client.V1InvoiceItems.Create(ctx, &stripe.InvoiceItemCreateParams{Customer: stripe.String(customer.ID),
			Amount: stripe.Int64(500), Currency: stripe.String("usd")}); err != nil {
			t.Fatalf("create invoice item: %v", err)
		}
		draft, err := client.V1Invoices.Create(ctx, &stripe.InvoiceCreateParams{Customer: stripe.String(customer.ID),
			CollectionMethod: stripe.String("send_invoice"), DaysUntilDue: stripe.Int64(30),
			PendingInvoiceItemsBehavior: stripe.String("include")})
		if err != nil {
			t.Fatalf("create invoice: %v", err)
		}
		open, err := client.V1Invoices.FinalizeInvoice(ctx, draft.ID, nil)
		if err != nil {
			t.Fatalf("finalize invoice: %v", err)
		}
		planes[plane] = planeStripe{customer.ID, sub.ID, open.ID, sub.LatestInvoice.ID}
	}

	// The venue: one org whose subscription and license point at the
	// Python plane's Stripe objects; the Go copy is then pointed at Go's.
	org, owner, superuser, plan, price, barePlan := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	openInvoice, paidInvoice := uuid.New(), uuid.New()
	env := map[string]string{"STRIPE_SECRET_KEY": key, "STRIPE_PRICE_ID_TEAM": priceA.ID, "APP_BASE_URL": "https://app.venue.test"}
	// The Python reconciliation's Stripe list call is patched to the call
	// it was written to make (a named divergence: unpatched it raises).
	pythonEnv := []string{"VENUE_STRIPE_LIST_KWARGS=1"}
	for name, value := range env {
		pythonEnv = append(pythonEnv, name+"="+value)
	}
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(), PythonEnv: pythonEnv,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			for _, statement := range []struct {
				sql  string
				args []any
			}{
				{`INSERT INTO organizations (id, slug, name, tier) VALUES ($1, 'stripe-venue', 'Stripe Venue', 'team')`, []any{org}},
				{`INSERT INTO users (id, email, is_superuser, is_active, token_version) VALUES ($1,'sv-owner@x',false,true,0), ($2,'sv-su@x',true,true,0)`, []any{owner, superuser}},
				{`INSERT INTO memberships (id, user_id, org_id, role) VALUES (gen_random_uuid(), $1, $2, 'owner')`, []any{owner, org}},
				{`INSERT INTO org_licenses (id, org_id, tier, licensed_users, licensed_repos, is_valid, customer_id, created_at, updated_at)
					VALUES (gen_random_uuid(), $1, 'team', 5, 5, true, $2, now(), now())`, []any{org, planes["py"].customer}},
				{`INSERT INTO billing_plans (id, key, name, description, tier, metadata, created_at, updated_at) VALUES
					($1, 'sv-local', 'SV Local', 'Local plan', 'team', '{}', now(), now()),
					($2, 'sv-bare', 'SV Bare', NULL, 'team', '{}', now(), now())`, []any{plan, barePlan}},
				{`INSERT INTO billing_prices (id, plan_id, interval, amount, currency, created_at, updated_at) VALUES ($1, $2, 'monthly', 1500, 'usd', now(), now())`, []any{price, plan}},
				{`INSERT INTO subscriptions (id, org_id, billing_plan_id, billing_price_id, stripe_subscription_id, stripe_customer_id, status,
					current_period_start, current_period_end) VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, 'active', now(), now() + interval '30 days')`,
					[]any{org, plan, price, planes["py"].subscription, planes["py"].customer}},
				// The paid invoice is stored as open, so the void reaches
				// Stripe and Stripe refuses it.
				{`INSERT INTO invoices (id, org_id, stripe_invoice_id, stripe_customer_id, status, amount_due, currency, metadata, created_at, updated_at)
					VALUES ($1, $3, $4, $6, 'open', 500, 'usd', '{}', now(), now()),
						($2, $3, $5, $6, 'open', 1000, 'usd', '{}', now() - interval '1 minute', now() - interval '1 minute')`,
					[]any{openInvoice, paidInvoice, org, planes["py"].openInvoice, planes["py"].paidInvoice, planes["py"].customer}},
			} {
				if _, err := admin.Exec(ctx, statement.sql, statement.args...); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}
			return map[string]map[string]any{
				"owner": {"user_id": owner.String(), "email": "sv-owner@example.com", "org_id": org.String(), "role": "owner"},
				"super": {"user_id": superuser.String(), "email": "sv-su@example.com", "org_id": org.String(), "role": "member", "is_superuser": true},
			}
		},
	})
	goAdmin, err := pgxpool.New(ctx, venue.AdminURI(t, venue.GoDB))
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{`UPDATE subscriptions SET stripe_subscription_id = $1, stripe_customer_id = $2 WHERE org_id = $3`,
		`UPDATE org_licenses SET customer_id = $2 WHERE org_id = $3 AND $1 = $1`} {
		if _, err := goAdmin.Exec(ctx, statement, planes["go"].subscription, planes["go"].customer, org); err != nil {
			t.Fatal(err)
		}
	}
	for id, stripeID := range map[uuid.UUID]string{openInvoice: planes["go"].openInvoice, paidInvoice: planes["go"].paidInvoice} {
		if _, err := goAdmin.Exec(ctx, `UPDATE invoices SET stripe_invoice_id = $1, stripe_customer_id = $2 WHERE id = $3`,
			stripeID, planes["go"].customer, id); err != nil {
			t.Fatal(err)
		}
	}
	goAdmin.Close()
	loaded, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(name string) (string, bool) {
		value, ok := env[name]
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
	base := startVenueAPI(t, ctx, cfg, venue)

	request := func(name, method, path, body, token string) venueoracle.Request {
		r := venueoracle.Request{Name: name, Method: method, Path: path, Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens[token]}}
		if body != "" {
			r.Body = venueoracle.B64(body)
			r.Headers["Content-Type"] = "application/json"
		}
		return r
	}
	perPlane := regexp.MustCompile(strings.Join([]string{
		regexp.QuoteMeta(planes["py"].subscription), regexp.QuoteMeta(planes["go"].subscription),
		regexp.QuoteMeta(planes["py"].customer), regexp.QuoteMeta(planes["go"].customer),
		`cs_test_[A-Za-z0-9]+`, `https://checkout\.stripe\.com/[^"]+`, `https://billing\.stripe\.com/[^"]+`,
		`prod_[A-Za-z0-9]+`, `price_[A-Za-z0-9]+`, `in_[A-Za-z0-9]+`, `req_[A-Za-z0-9]+`,
	}, "|"))
	// The reconciliation lists the whole shared test account, and the
	// planes run minutes apart (the Go plane's subscription is still active
	// while Python lists), so only that list of Stripe objects no local row
	// holds is blanked; the mismatches, the counts and missing_stripe are
	// compared.
	accountWide := regexp.MustCompile(`"missing_local":\[[^\]]*\]`)
	normalize := func(request venueoracle.Request, body string) string {
		if request.Name == "reconcile org" {
			body = accountWide.ReplaceAllString(body, `"missing_local":"<account-wide>"`)
		}
		body = perPlane.ReplaceAllString(body, "<stripe>")
		return billingNormalizer(map[string]bool{org.String(): true, plan.String(): true, price.String(): true, barePlan.String(): true,
			openInvoice.String(): true, paidInvoice.String(): true},
			time.Now().Add(-time.Hour))(body)
	}
	var receipt strings.Builder
	phase := func(requests []venueoracle.Request) {
		python := venue.ServePython(t, requests)
		receipt.WriteString(venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{Normalize: normalize}))
	}
	// Pull first, on both planes, before either creates a product.
	phase([]venueoracle.Request{request("pull (test account)", "POST", "/api/v1/billing/plans/pull-stripe", "", "super")})
	phase([]venueoracle.Request{
		request("sync local plan", "POST", "/api/v1/billing/plans/"+plan.String()+"/sync-stripe", "", "super"),
		// Stripe refuses the empty description a plan without one sends.
		request("sync plan without description", "POST", "/api/v1/billing/plans/"+barePlan.String()+"/sync-stripe", "", "super"),
		request("checkout (trial)", "POST", "/api/v1/billing/checkout",
			`{"tier":"team","success_url":"https://app.venue.test/ok","cancel_url":"https://app.venue.test/no"}`, "owner"),
		// Relative URLs pass the local check; Stripe refuses them.
		request("checkout relative urls", "POST", "/api/v1/billing/checkout", `{"tier":"team","success_url":"/ok","cancel_url":"/no"}`, "owner"),
		request("portal", "POST", "/api/v1/billing/portal?return_url=https://app.venue.test/back", "", "owner"),
		request("subscription", "GET", "/api/v1/billing/subscriptions", "", "owner"),
		request("change plan", "POST", "/api/v1/billing/subscriptions/change-plan", `{"price_id":"`+priceB.ID+`"}`, "owner"),
		request("cancel at period end", "POST", "/api/v1/billing/subscriptions/cancel", `{}`, "owner"),
		request("reactivate", "POST", "/api/v1/billing/subscriptions/reactivate", "", "owner"),
		request("cancel immediately", "POST", "/api/v1/billing/subscriptions/cancel", `{"immediately":true}`, "owner"),
		request("change plan after cancel", "POST", "/api/v1/billing/subscriptions/change-plan", `{"price_id":"`+priceB.ID+`"}`, "owner"),
		request("void open invoice", "POST", "/api/v1/billing/invoices/"+openInvoice.String()+"/void", "", "owner"),
		request("void paid invoice (Stripe refuses)", "POST", "/api/v1/billing/invoices/"+paidInvoice.String()+"/void", "", "owner"),
		request("invoices after void", "GET", "/api/v1/billing/invoices", "", "owner"),
		request("refund (the Python 500)", "POST", "/api/v1/billing/refunds", `{"invoice_id":"`+paidInvoice.String()+`"}`, "super"),
		request("reconcile org", "POST", "/api/v1/billing/reconcile?org_id="+org.String(), "", "super"),
	})

	// Created products (sync) are archived at cleanup.
	for _, uri := range []string{venue.AdminURI(t, venue.SourceDB), venue.AdminURI(t, venue.GoDB)} {
		rows := venueoracle.TableRows(t, ctx, uri, `SELECT stripe_product_id FROM billing_plans WHERE key IN ('sv-local', 'sv-bare') AND stripe_product_id IS NOT NULL`)
		for _, id := range strings.Split(rows, " | ") {
			if id = strings.TrimSpace(id); strings.HasPrefix(id, "prod_") {
				created = append(created, id)
			}
		}
	}

	// Stripe state each plane left behind, field by field.
	state := func(id string) string {
		sub, err := client.V1Subscriptions.Retrieve(ctx, id, nil)
		if err != nil {
			t.Fatalf("retrieve subscription: %v", err)
		}
		items := []string{}
		for _, item := range sub.Items.Data {
			items = append(items, item.Price.ID)
		}
		return fmt.Sprintf("status=%s cancel_at_period_end=%t canceled=%t items=%v", sub.Status, sub.CancelAtPeriodEnd, sub.CanceledAt != 0, items)
	}
	invoiceState := func(p planeStripe) string {
		var parts []string
		for _, id := range []string{p.openInvoice, p.paidInvoice} {
			invoice, err := client.V1Invoices.Retrieve(ctx, id, nil)
			if err != nil {
				t.Fatalf("retrieve invoice: %v", err)
			}
			parts = append(parts, fmt.Sprintf("%s/%d", invoice.Status, invoice.AmountRemaining))
		}
		return strings.Join(parts, " ")
	}
	pyInvoices, goInvoices := invoiceState(planes["py"]), invoiceState(planes["go"])
	fmt.Fprintf(&receipt, "stripe invoice state after the run: python{%s} go{%s} %s\n", pyInvoices, goInvoices, venueoracle.Mark(pyInvoices == goInvoices))
	if pyInvoices != goInvoices || !strings.HasPrefix(goInvoices, "void/") {
		t.Errorf("stripe invoice state: python %s go %s (want the open invoice void on both)", pyInvoices, goInvoices)
	}
	pyState, goState := state(planes["py"].subscription), state(planes["go"].subscription)
	fmt.Fprintf(&receipt, "stripe subscription state after the run: python{%s} go{%s} %s\n", pyState, goState, venueoracle.Mark(pyState == goState))
	if pyState != goState {
		t.Errorf("stripe subscription state differs:\n python %s\n go     %s", pyState, goState)
	}
	for _, table := range []string{
		`SELECT key, name, tier, is_active, display_order, stripe_product_id IS NOT NULL, metadata::text FROM billing_plans ORDER BY key`,
		`SELECT b.key, p.interval, p.amount, p.currency, p.is_active, p.stripe_price_id IS NOT NULL FROM billing_prices p JOIN billing_plans b ON b.id = p.plan_id ORDER BY b.key, p.interval, p.amount`,
		`SELECT action, resource_type, description, local_state::text FROM billing_audit_log ORDER BY created_at, action`,
		`SELECT status, amount_remaining, voided_at IS NULL FROM invoices ORDER BY amount_due`,
	} {
		pyRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), table)
		goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), table)
		fmt.Fprintf(&receipt, "rows %q: %s\n", table[:40], venueoracle.Mark(pyRows == goRows))
		if pyRows != goRows {
			t.Errorf("rows differ for %s:\n python %s\n go     %s", table, pyRows, goRows)
		}
	}
	if path := os.Getenv("DEV_HEALTH_VENUE_RECEIPT"); path != "" {
		_ = os.WriteFile(path, []byte(receipt.String()), 0o600)
	}
	t.Log("\n" + receipt.String())
}
