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

// TestSubscriptionSkipLinesNameTheEvent sends subscription events that the
// route skips or fails on, through the real Go api, and reads its log: every
// skip and failure line carries the Stripe event, its type, the Stripe
// subscription and customer, the org exactly as the metadata gave it, and the
// price id, so the decision can be rebuilt from the log alone. Go-only: the
// Python api logs none of these fields.
func TestSubscriptionSkipLinesNameTheEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fake := newFakeStripe()
	goStripe := httptest.NewServer(fake.plane("go"))
	t.Cleanup(goStripe.Close)
	env := webhookEnv()
	var pythonEnv []string
	for key, value := range env {
		pythonEnv = append(pythonEnv, key+"="+value)
	}
	var seed billingFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(), PythonEnv: pythonEnv,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seed = billingSeed(t, ctx, admin)
			subscriptionSeed(t, ctx, admin, seed)
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
	receipt := ""
	// send delivers a customer.subscription.* event built from the captured
	// updated event, then returns the log lines it produced that carry the
	// given message.
	send := func(name, eventID, eventType string, created int64, metadata map[string]any, price string, message string) string {
		t.Helper()
		venueLogs.Reset()
		value := loadWebhookFixture(t, "customer.subscription.updated.json")
		value["type"], value["id"], value["created"] = eventType, eventID, created
		object := value["data"].(map[string]any)["object"].(map[string]any)
		object["id"], object["customer"], object["metadata"] = "sub_log_"+name, "cus_log_"+name, metadata
		setPrice(object, price)
		body, _ := json.Marshal(value)
		response := venueoracle.Do(t, base, venueoracle.Request{Name: name, Method: "POST", Path: webhookPath,
			Headers: map[string]string{"Stripe-Signature": webhookSignature(webhookVenueSecret, stamp, body), "Content-Type": "application/json"},
			Body:    venueoracle.B64(string(body))})
		if response.Status != 200 {
			t.Errorf("%s answered %d %s", name, response.Status, response.Body)
		}
		for _, line := range venueLogs.Lines() {
			if strings.Contains(line, message) {
				return line
			}
		}
		t.Errorf("%s: no log line %q in:\n%s", name, message, strings.Join(venueLogs.Lines(), "\n"))
		return ""
	}
	expect := func(label, line string, want ...string) {
		t.Helper()
		missing := []string{}
		for _, field := range want {
			if !strings.Contains(line, field) {
				missing = append(missing, field)
			}
		}
		receipt += fmt.Sprintf("%-64s %s\n", label, venueoracle.Mark(len(missing) == 0))
		if len(missing) > 0 {
			t.Errorf("%s: the line lacks %v:\n%s", label, missing, line)
		}
	}
	const known = "price_ent_cfg"
	const unknownPrice = "price_not_in_any_table"
	orgOK, orgUnknown := webhookSubOrgs[0], "dddddddd-0000-4000-8000-00000000f00d"

	// No org in the metadata, and none owns the subscription or customer.
	line := send("noorg", "evt_log_noorg", "customer.subscription.updated", 1790000100, map[string]any{}, known, "none owns its subscription")
	expect("no org: the skip names the event, subscription, customer, org and price", line,
		"event_id=evt_log_noorg", "event_type=customer.subscription.updated", "stripe_subscription_id=sub_log_noorg",
		"customer=cus_log_noorg", "org_id_received=None", "price_id="+known)
	// The hook's own line for the same event.
	line = send("noorg", "evt_log_noorg2", "customer.subscription.updated", 1790000101, map[string]any{}, known, "subscription.updated without org_id metadata")
	expect("no org, updated: the hook line names the event, subscription and price", line,
		"event_id=evt_log_noorg2", "event_type=customer.subscription.updated", "stripe_subscription_id=sub_log_noorg", "price_id="+known)
	line = send("noorg", "evt_log_noorg3", "customer.subscription.deleted", 1790000102, map[string]any{}, known, "subscription.deleted without org_id metadata")
	expect("no org, deleted: the hook line names the event, subscription and price", line,
		"event_id=evt_log_noorg3", "event_type=customer.subscription.deleted", "stripe_subscription_id=sub_log_noorg", "org_id_received=None", "price_id="+known)
	line = send("noorg", "evt_log_noorg4", "customer.subscription.trial_will_end", 1790000103, map[string]any{}, known, "subscription.trial_will_end without org_id metadata")
	expect("no org, trial_will_end: the hook line names the event, subscription and price", line,
		"event_id=evt_log_noorg4", "event_type=customer.subscription.trial_will_end", "stripe_subscription_id=sub_log_noorg", "price_id="+known)

	// An org id that is not a uuid.
	line = send("badorg", "evt_log_badorg", "customer.subscription.created", 1790000110, map[string]any{"org_id": "org-abc"}, known, "Skipping malformed subscription event")
	expect("org not a uuid: the malformed skip names the event, org as received and price", line,
		"event_id=evt_log_badorg", "org_id_received=org-abc", "price_id="+known, "stripe_subscription_id=sub_log_badorg")

	// A price no table holds, for a subscription not stored yet.
	line = send("noprice", "evt_log_noprice", "customer.subscription.created", 1790000120, map[string]any{"org_id": orgOK}, unknownPrice, "Skipping malformed subscription event")
	expect("unknown price: the malformed skip names the price and the org", line,
		"event_id=evt_log_noprice", "org_id_received="+orgOK, "price_id="+unknownPrice)

	// An org that does not exist: the insert fails its foreign key.
	line = send("wrongorg", "evt_log_wrongorg", "customer.subscription.created", 1790000130, map[string]any{"org_id": orgUnknown}, known, "Failed to process subscription event")
	expect("org that does not exist: the failure names the org as received", line,
		"event_id=evt_log_wrongorg", "org_id_received="+orgUnknown, "price_id="+known, "stripe_subscription_id=sub_log_wrongorg")

	// An event older than the newest recorded for its subscription.
	send("lifecycle", "evt_log_new", "customer.subscription.created", 1790000200, map[string]any{"org_id": orgOK}, known, "OrgLicense synced")
	line = send("lifecycle", "evt_log_old", "customer.subscription.updated", 1790000150, map[string]any{"org_id": orgOK}, known, "older than the newest recorded")
	expect("older event: the line names the event, subscription, org and price", line,
		"event_id=evt_log_old", "stripe_subscription_id=sub_log_lifecycle", "org_id_received="+orgOK, "price_id="+known)

	// A price the configured map does not know, on an update that resolves an org.
	send("tier", "evt_log_tier", "customer.subscription.updated", 1790000300, map[string]any{"org_id": webhookSubOrgs[1]}, unknownPrice, "No recognized price ID")
	line = ""
	for _, l := range venueLogs.Lines() {
		if strings.Contains(l, "No recognized price ID") {
			line = l
		}
	}
	expect("unknown price on update: the tier warning names the price", line, "price_ids=", unknownPrice)

	t.Log("\n" + receipt)
}
