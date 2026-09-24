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

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestStripeTestModeInvoiceEvents replays real invoice events from the
// Stripe TEST-mode account into the Go route: every event is fetched from
// Stripe's /v1/events, signed with the venue secret and delivered twice.
// Each invoice must be recorded once, the redelivery must change nothing,
// and each paid or failed event must queue exactly one notification. It
// runs only with DEV_HEALTH_STRIPE_TEST_MODE=1 and a test-mode
// STRIPE_SECRET_KEY (never printed); the invoices' subscriptions are mapped
// to a venue org.
//
//venueoracle:local-only needs a Stripe test-mode key hosted CI does not hold
func TestStripeTestModeInvoiceEvents(t *testing.T) {
	if os.Getenv("DEV_HEALTH_STRIPE_TEST_MODE") != "1" {
		t.Skip("real Stripe test mode runs only with DEV_HEALTH_STRIPE_TEST_MODE=1 (local receipt)")
	}
	key := os.Getenv("STRIPE_SECRET_KEY")
	if !strings.HasPrefix(key, "sk_test_") {
		t.Fatal("STRIPE_SECRET_KEY is not a Stripe test-mode key; refusing to run (value not printed)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Real invoice events from the test account, newest first.
	var events []json.RawMessage
	for _, eventType := range []string{"invoice.created", "invoice.finalized", "invoice.paid", "invoice.payment_failed", "invoice.voided", "invoice.updated"} {
		query := url.Values{"type": {eventType}, "limit": {"10"}}
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.stripe.com/v1/events?"+query.Encode(), nil)
		request.SetBasicAuth(key, "")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatalf("list %s events: %v", eventType, err)
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("list %s events: HTTP %d", eventType, response.StatusCode)
		}
		var page struct {
			Data []json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			t.Fatal(err)
		}
		events = append(events, page.Data...)
	}
	type invoiceEvent struct {
		ID, Type string
		Data     struct {
			Object struct {
				ID, Customer, Status string
				Parent               *struct {
					SubscriptionDetails *struct {
						Subscription string `json:"subscription"`
					} `json:"subscription_details"`
				} `json:"parent"`
			} `json:"object"`
		} `json:"data"`
	}
	// invoices are the ones the venue org resolves for (their subscription
	// is seeded); unmapped are the rest, which must leave no row.
	subscriptions, invoices, unmapped := map[string]bool{}, map[string]bool{}, map[string]bool{}
	notifying := map[string]bool{}
	var parsed []invoiceEvent
	for _, raw := range events {
		var event invoiceEvent
		if err := json.Unmarshal(raw, &event); err != nil {
			t.Fatal(err)
		}
		parsed = append(parsed, event)
		if parent := event.Data.Object.Parent; parent != nil && parent.SubscriptionDetails != nil && parent.SubscriptionDetails.Subscription != "" {
			subscriptions[parent.SubscriptionDetails.Subscription] = true
			invoices[event.Data.Object.ID] = true
			if event.Type == "invoice.paid" || event.Type == "invoice.payment_failed" {
				notifying[event.ID] = true
			}
		} else {
			unmapped[event.Data.Object.ID] = true
		}
	}
	for invoice := range invoices {
		delete(unmapped, invoice)
	}
	if len(events) < 3 || len(subscriptions) == 0 {
		t.Fatalf("the test account holds %d invoice events over %d subscriptions: too few to measure", len(events), len(subscriptions))
	}

	env := webhookEnv()
	var pythonEnv []string
	for name, value := range env {
		pythonEnv = append(pythonEnv, name+"="+value)
	}
	var seed billingFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(), PythonEnv: pythonEnv,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seed = billingSeed(t, ctx, admin)
			for subscription := range subscriptions {
				if _, err := admin.Exec(ctx, `INSERT INTO subscriptions (id, org_id, billing_plan_id, billing_price_id, stripe_subscription_id,
					stripe_customer_id, status, current_period_start, current_period_end, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'cus_testmode', 'active', now(), now(), now(), now())`,
					uuid.New(), seed.orgB, seed.planTeam, seed.priceTeamM, subscription); err != nil {
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
		// Oldest first, as Stripe delivers them.
		for index := len(events) - 1; index >= 0; index-- {
			body := []byte(events[index])
			response := venueoracle.Do(t, base, venueoracle.Request{Name: round + " " + parsed[index].ID, Method: "POST", Path: webhookPath,
				Headers: map[string]string{"Stripe-Signature": webhookSignature(webhookVenueSecret, stamp, body), "Content-Type": "application/json"},
				Body:    venueoracle.B64(string(body))})
			if response.Status != http.StatusOK {
				t.Errorf("%s %s (%s): go answered %d %s", round, parsed[index].ID, parsed[index].Type, response.Status, response.Body)
			}
		}
	}
	goURI := venue.AdminURI(t, venue.GoDB)
	state := func() string {
		return venueoracle.TableRows(t, ctx, goURI, `SELECT (SELECT count(*) FROM invoices)::text || ' invoices, ' ||
			(SELECT count(*) FROM invoice_line_items)::text || ' lines, ' ||
			(SELECT count(*) FROM billing_notifications WHERE notification_type IN ('invoice_receipt', 'payment_failed'))::text || ' notifications, ' ||
			(SELECT string_agg(stripe_invoice_id || '=' || status, ',' ORDER BY stripe_invoice_id) FROM invoices)`)
	}
	deliver("first")
	first := state()
	deliver("redelivered")
	second := state()
	wantPrefix := fmt.Sprintf("%d invoices, ", len(invoices))
	if !strings.HasPrefix(first, wantPrefix) {
		t.Errorf("after the first delivery: %s, want %d invoices (one per Stripe invoice with a mapped subscription)", first, len(invoices))
	}
	for invoice := range unmapped {
		if strings.Contains(first, invoice+"=") {
			t.Errorf("invoice %s has no resolvable org but was written", invoice)
		}
	}
	if !strings.Contains(first, fmt.Sprintf(" %d notifications, ", len(notifying))) {
		t.Errorf("after the first delivery: %s, want %d notifications (one per paid or failed event)", first, len(notifying))
	}
	if second != first {
		t.Errorf("the redelivery changed state:\n first  %s\n second %s", first, second)
	}
	t.Logf("%d real test-mode invoice events (%d invoices with a mapped subscription, %d without an org, %d subscriptions, %d paid/failed); after delivery: %s; redelivery unchanged: %t",
		len(events), len(invoices), len(unmapped), len(subscriptions), len(notifying), first, second == first)
	venueoracle.WriteGoOnlyProof(t, "Go applies real Stripe test-mode invoice events once; a redelivery changes nothing")
}
