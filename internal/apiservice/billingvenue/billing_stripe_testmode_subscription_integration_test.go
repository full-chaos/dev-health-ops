//go:build integration

package billingvenue

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
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

// TestStripeTestModeSubscriptionLifecycle runs a real subscription through
// Stripe TEST mode the way a checkout leaves it (no org in the
// subscription's metadata; the org's license holds the Stripe customer):
// subscribe on the team price, upgrade to the enterprise price, cancel. The
// real customer.subscription.* and invoice.* events Stripe emitted are
// fetched from /v1/events and delivered to the Go route twice. The org must
// be resolved, the subscription recorded and cancelled, the license upgraded
// then revoked, the invoices recorded, and the redelivery must change
// nothing. It runs only with DEV_HEALTH_STRIPE_TEST_MODE=1 and a test-mode
// STRIPE_SECRET_KEY (never printed); what it creates in Stripe is cancelled,
// deleted and archived at cleanup.
//
//venueoracle:local-only needs a Stripe test-mode key hosted CI does not hold
func TestStripeTestModeSubscriptionLifecycle(t *testing.T) {
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
	run := "lifecycle-" + uuid.NewString()[:8]
	started := time.Now().Add(-time.Minute).Unix()

	product, err := client.V1Products.Create(ctx, &stripe.ProductCreateParams{Name: stripe.String("Venue " + run)})
	if err != nil {
		t.Fatalf("create product: %v", err)
	}
	var customerID, subscriptionID string
	t.Cleanup(func() {
		cleanup := context.Background()
		if subscriptionID != "" {
			_, _ = client.V1Subscriptions.Cancel(cleanup, subscriptionID, nil)
		}
		if customerID != "" {
			_, _ = client.V1Customers.Delete(cleanup, customerID, nil)
		}
		_, _ = client.V1Products.Update(cleanup, product.ID, &stripe.ProductUpdateParams{Active: stripe.Bool(false)})
	})
	newPrice := func(amount int64) string {
		price, err := client.V1Prices.Create(ctx, &stripe.PriceCreateParams{Product: stripe.String(product.ID), UnitAmount: stripe.Int64(amount),
			Currency: stripe.String("usd"), Recurring: &stripe.PriceCreateRecurringParams{Interval: stripe.String("month")}})
		if err != nil {
			t.Fatalf("create price: %v", err)
		}
		return price.ID
	}
	teamPrice, enterprisePrice := newPrice(1000), newPrice(5000)
	customer, err := client.V1Customers.Create(ctx, &stripe.CustomerCreateParams{Name: stripe.String(run), Email: stripe.String(run + "@example.com"),
		PaymentMethod:   stripe.String("pm_card_visa"),
		InvoiceSettings: &stripe.CustomerCreateInvoiceSettingsParams{DefaultPaymentMethod: stripe.String("pm_card_visa")}})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	customerID = customer.ID
	subscription, err := client.V1Subscriptions.Create(ctx, &stripe.SubscriptionCreateParams{Customer: stripe.String(customer.ID),
		Items: []*stripe.SubscriptionCreateItemParams{{Price: stripe.String(teamPrice)}}})
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	subscriptionID = subscription.ID
	if _, err := client.V1Subscriptions.Update(ctx, subscription.ID, &stripe.SubscriptionUpdateParams{
		Items:             []*stripe.SubscriptionUpdateItemParams{{ID: stripe.String(subscription.Items.Data[0].ID), Price: stripe.String(enterprisePrice)}},
		ProrationBehavior: stripe.String("none")}); err != nil {
		t.Fatalf("upgrade subscription: %v", err)
	}
	if _, err := client.V1Subscriptions.Cancel(ctx, subscription.ID, nil); err != nil {
		t.Fatalf("cancel subscription: %v", err)
	}

	// The events Stripe emitted for this subscription (it lists them within
	// seconds; wait for the cancellation).
	fetch := func() []json.RawMessage {
		var out []json.RawMessage
		query := url.Values{"limit": {"100"}, "created[gte]": {fmt.Sprint(started)}}
		for _, eventType := range []string{"customer.subscription.*", "invoice.*"} {
			query.Set("type", eventType)
			request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.stripe.com/v1/events?"+query.Encode(), nil)
			request.SetBasicAuth(key, "")
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatalf("list events: %v", err)
			}
			body, _ := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("list events: HTTP %d", response.StatusCode)
			}
			var page struct {
				Data []json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal(body, &page); err != nil {
				t.Fatal(err)
			}
			for _, raw := range page.Data {
				if strings.Contains(string(raw), subscription.ID) || strings.Contains(string(raw), customer.ID) {
					out = append(out, raw)
				}
			}
		}
		return out
	}
	var events []json.RawMessage
	for attempt := 0; attempt < 20; attempt++ {
		events = fetch()
		if strings.Contains(fmt.Sprint(eventTypes(events)), "customer.subscription.deleted") {
			break
		}
		time.Sleep(3 * time.Second)
	}
	types := eventTypes(events)
	for _, want := range []string{"customer.subscription.created", "customer.subscription.updated", "customer.subscription.deleted", "invoice.paid"} {
		if !strings.Contains(fmt.Sprint(types), want) {
			t.Fatalf("Stripe emitted no %s for the run (got %v)", want, types)
		}
	}

	env := webhookEnv()
	env["STRIPE_PRICE_ID_TEAM"], env["STRIPE_PRICE_ID_ENTERPRISE"] = teamPrice, enterprisePrice
	var pythonEnv []string
	for name, value := range env {
		pythonEnv = append(pythonEnv, name+"="+value)
	}
	var seed billingFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(), PythonEnv: pythonEnv,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seed = billingSeed(t, ctx, admin)
			// What the checkout webhook leaves: org D's license holds the
			// Stripe customer. The prices map to the seeded plans.
			for _, statement := range []struct {
				sql  string
				args []any
			}{
				{`INSERT INTO org_licenses (id, org_id, tier, is_valid, customer_id, managed_by, created_at, updated_at)
					VALUES (gen_random_uuid(), $1, 'team', true, $2, 'stripe', now(), now())`, []any{seed.orgD, customer.ID}},
				{`INSERT INTO billing_prices (id, plan_id, interval, amount, currency, is_active, stripe_price_id, created_at, updated_at) VALUES
					(gen_random_uuid(), $1, 'monthly', 1000, 'usd', true, $3, now(), now()),
					(gen_random_uuid(), $2, 'monthly', 5000, 'usd', true, $4, now(), now())`,
					[]any{seed.planTeam, seed.planEnterprise, teamPrice, enterprisePrice}},
			} {
				if _, err := admin.Exec(ctx, statement.sql, statement.args...); err != nil {
					t.Fatal(err)
				}
			}
			return seed.tokenSpecs()
		},
	})
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
		StripeWebhookSecret: loaded.StripeWebhookSecret, LicensePrivateKey: loaded.LicensePrivateKey,
	}
	base := startBillingVenueAPI(t, ctx, cfg, venue, "")

	stamp := time.Now().Unix() + 200
	deliver := func(round string) {
		for index := len(events) - 1; index >= 0; index-- {
			body := []byte(events[index])
			response := venueoracle.Do(t, base, venueoracle.Request{Name: round, Method: "POST", Path: webhookPath,
				Headers: map[string]string{"Stripe-Signature": webhookSignature(webhookVenueSecret, stamp, body), "Content-Type": "application/json"},
				Body:    venueoracle.B64(string(body))})
			if response.Status != http.StatusOK {
				t.Errorf("%s %s: go answered %d %s", round, types[index], response.Status, response.Body)
			}
		}
	}
	goURI := venue.AdminURI(t, venue.GoDB)
	org := seed.orgD.String()
	state := func() string {
		return venueoracle.TableRows(t, ctx, goURI, `SELECT
			(SELECT string_agg(org_id::text || ' ' || status, ',') FROM subscriptions WHERE stripe_subscription_id = '`+subscription.ID+`') || ' / ' ||
			(SELECT count(*) FROM subscription_events e JOIN subscriptions s ON s.id = e.subscription_id WHERE s.stripe_subscription_id = '`+subscription.ID+`')::text || ' events / ' ||
			(SELECT tier || ' ' || is_valid::text FROM org_licenses WHERE org_id = '`+org+`') || ' / ' ||
			(SELECT count(*) FROM invoices WHERE org_id = '`+org+`')::text || ' invoices / ' ||
			(SELECT coalesce(string_agg(notification_type, ',' ORDER BY notification_type), '') FROM billing_notifications WHERE org_id = '`+org+`')`)
	}
	deliver("first")
	first := state()
	deliver("redelivered")
	second := state()
	if !strings.HasPrefix(first, org+" canceled / ") {
		t.Errorf("the subscription is not recorded as org D's and cancelled: %s", first)
	}
	if !strings.Contains(first, " / community false / ") {
		t.Errorf("the cancellation did not revoke org D's license: %s", first)
	}
	if strings.Contains(first, " / 0 invoices / ") {
		t.Errorf("no invoice recorded for org D: %s", first)
	}
	if !strings.Contains(first, "subscription_cancelled") || !strings.Contains(first, "invoice_receipt") {
		t.Errorf("the cancellation or the receipt was not queued: %s", first)
	}
	if strings.Count(first, "subscription_changed") != 1 {
		t.Errorf("the upgrade (team to enterprise) was not announced exactly once: %s", first)
	}
	changed := venueoracle.TableRows(t, ctx, goURI, `SELECT attributes::text FROM billing_notifications
		WHERE org_id = '`+org+`' AND notification_type = 'subscription_changed'`)
	if changed != `{"old_tier": "team", "new_tier": "enterprise"}` {
		t.Errorf("the upgrade announcement names %s, want team to enterprise", changed)
	}
	if second != first {
		t.Errorf("the redelivery changed state:\n first  %s\n second %s", first, second)
	}
	t.Logf("%d real events (%v); after delivery: %s; redelivery unchanged: %t", len(events), types, first, second == first)
	venueoracle.WriteGoOnlyProof(t, "Go applies a real Stripe test-mode subscription lifecycle without org metadata: org resolved, license upgraded then revoked, invoices recorded, redelivery unchanged")
}

// eventTypes lists the events' types, in order.
func eventTypes(events []json.RawMessage) []string {
	types := make([]string, len(events))
	for index, raw := range events {
		var event struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(raw, &event)
		types[index] = event.Type
	}
	return types
}
