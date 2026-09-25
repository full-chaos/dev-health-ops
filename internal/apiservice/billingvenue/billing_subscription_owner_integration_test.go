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

// TestSubscriptionEventsResolveTheOwningOrg covers subscription events whose
// metadata names no org, as every subscription created by a checkout before
// the checkout set subscription_data.metadata carries (CHAOS-6525): the org
// is the one owning the stored subscription, else the one whose license
// holds the Stripe customer. The Python plane skips such events (it reads
// the org from the metadata only), so the proof is Go-only.
func TestSubscriptionEventsResolveTheOwningOrg(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fake := newFakeStripe()
	goStripe := httptest.NewServer(fake.plane("go"))
	t.Cleanup(goStripe.Close)
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
			// The configured enterprise price, mapped to the enterprise plan.
			if _, err := admin.Exec(ctx, `INSERT INTO billing_prices (id, plan_id, interval, amount, currency, is_active, stripe_price_id,
				created_at, updated_at) VALUES (gen_random_uuid(), $1, 'monthly', 5100, 'usd', true, 'price_ent_cfg', now(), now())`,
				seed.planEnterprise); err != nil {
				t.Fatal(err)
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
	base := startBillingVenueAPI(t, ctx, cfg, venue, goStripe.URL)

	stamp := time.Now().Unix() + 200
	orgA := seed.orgA.String()
	receipt := ""
	sendAt := func(name, eventID, eventType string, created int64, edit func(object map[string]any)) {
		value := loadWebhookFixture(t, subscriptionFixture)
		value["type"], value["id"] = eventType, eventID
		if created != 0 {
			value["created"] = created
		}
		object := value["data"].(map[string]any)["object"].(map[string]any)
		object["metadata"] = map[string]any{}
		edit(object)
		body, _ := json.Marshal(value)
		response := venueoracle.Do(t, base, venueoracle.Request{Name: name, Method: "POST", Path: webhookPath,
			Headers: map[string]string{"Stripe-Signature": webhookSignature(webhookVenueSecret, stamp, body), "Content-Type": "application/json"},
			Body:    venueoracle.B64(string(body))})
		receipt += fmt.Sprintf("%-60s go=%d\n", name, response.Status)
		if response.Status != 200 {
			t.Errorf("%s: go answered %d %s", name, response.Status, response.Body)
		}
	}
	send := func(name, eventID, eventType string, edit func(object map[string]any)) {
		sendAt(name, eventID, eventType, 0, edit)
	}
	trialEnd := time.Now().Add(3*24*time.Hour + 12*time.Hour).Unix()
	// A new subscription for the customer of org A's license (cus_A).
	send("created: no metadata, license customer", "evt_own_created", "customer.subscription.created", func(object map[string]any) {
		object["id"], object["customer"], object["status"] = "sub_own_new", "cus_A", "active"
		setPrice(object, "price_seed_y")
	})
	// The seeded sub_A (org A), another customer: the stored subscription decides.
	send("updated: no metadata, stored subscription", "evt_own_updated", "customer.subscription.updated", func(object map[string]any) {
		object["id"], object["customer"], object["status"] = "sub_A", "cus_other", "active"
		setPrice(object, "price_seed_y")
	})
	// An upgrade of the stored subscription to the configured enterprise
	// price: the plan sync moves org A to enterprise, and the upgrade is
	// announced once, from the tier before the sync.
	send("updated: upgrade to enterprise", "evt_own_upgrade", "customer.subscription.updated", func(object map[string]any) {
		object["id"], object["customer"], object["status"] = "sub_A", "cus_other", "active"
		setPrice(object, "price_ent_cfg")
	})
	send("trial_will_end: no metadata, license customer", "evt_own_trial", "customer.subscription.trial_will_end", func(object map[string]any) {
		object["id"], object["customer"], object["trial_end"] = "sub_own_new", "cus_A", trialEnd
	})
	send("deleted: no metadata, stored subscription", "evt_own_deleted", "customer.subscription.deleted", func(object map[string]any) {
		object["id"], object["customer"], object["status"] = "sub_own_new", "cus_other", "canceled"
		setPrice(object, "price_seed_y")
	})
	// The cancellation redelivered, then an update older than it: neither
	// changes the subscription, the license or the notifications.
	send("deleted: redelivered", "evt_own_deleted", "customer.subscription.deleted", func(object map[string]any) {
		object["id"], object["customer"], object["status"] = "sub_own_new", "cus_other", "canceled"
		setPrice(object, "price_seed_y")
	})
	sendAt("updated: older than the cancellation", "evt_own_late", "customer.subscription.updated", 1000, func(object map[string]any) {
		object["id"], object["customer"], object["status"] = "sub_own_new", "cus_A", "active"
		setPrice(object, "price_ent_y")
	})
	send("created: no metadata, nobody owns it", "evt_own_none", "customer.subscription.created", func(object map[string]any) {
		object["id"], object["customer"], object["status"] = "sub_own_none", "cus_nobody", "active"
		setPrice(object, "price_seed_y")
	})

	goURI := venue.AdminURI(t, venue.GoDB)
	expect := func(label, query, want string) {
		got := venueoracle.TableRows(t, ctx, goURI, query)
		receipt += fmt.Sprintf("%-60s %s\n", label, venueoracle.Mark(got == want))
		if got != want {
			t.Errorf("%s:\n got  %s\n want %s", label, got, want)
		}
	}
	expect("the new subscription is recorded for org A, then cancelled",
		`SELECT org_id::text || ' ' || status FROM subscriptions WHERE stripe_subscription_id = 'sub_own_new'`, orgA+" canceled")
	expect("the stored subscription's event is recorded",
		`SELECT count(*)::text FROM subscription_events WHERE stripe_event_id = 'evt_own_updated'`, "1")
	expect("org A's license is revoked by the cancellation",
		`SELECT tier || ' ' || is_valid::text FROM org_licenses WHERE org_id = '`+orgA+`'`, "community false")
	expect("the cancellation and the trial end are queued for org A",
		`SELECT string_agg(notification_type, ',' ORDER BY notification_type) FROM billing_notifications
			WHERE org_id = '`+orgA+`' AND notification_type IN ('subscription_cancelled', 'trial_expiring')`, "subscription_cancelled,trial_expiring")
	expect("the late update is recorded, not applied",
		`SELECT count(*)::text || ' ' || min(new_status) FROM subscription_events WHERE stripe_event_id = 'evt_own_late'`, "1 canceled")
	expect("one cancellation notification despite the redelivery",
		`SELECT count(*)::text FROM billing_notifications WHERE org_id = '`+orgA+`' AND notification_type = 'subscription_cancelled'`, "1")
	expect("the upgrade announced once, team to enterprise; nothing for the late update",
		`SELECT count(*)::text || ' ' || min(attributes::text) FROM billing_notifications WHERE org_id = '`+orgA+`' AND notification_type = 'subscription_changed'`,
		`1 {"old_tier": "team", "new_tier": "enterprise"}`)
	expect("a subscription nobody owns is not recorded",
		`SELECT count(*)::text FROM subscriptions WHERE stripe_subscription_id = 'sub_own_none'`, "0")
	// Org A subscribes again; then the old cancellation is redelivered. It
	// was applied once already, so it must not revoke the new license.
	send("created: a new subscription after the cancellation", "evt_own_next", "customer.subscription.created", func(object map[string]any) {
		object["id"], object["customer"], object["status"] = "sub_own_next", "cus_A", "active"
		setPrice(object, "price_seed_y")
	})
	// The checkout that created it issues the license (checkout.session.completed,
	// outside this test): set here as that route leaves it.
	if got := venueoracle.TableRows(t, ctx, goURI, `UPDATE org_licenses SET tier = 'team', is_valid = true WHERE org_id = '`+orgA+`' RETURNING org_id::text`); got != orgA {
		t.Fatalf("license reissue: %s", got)
	}
	before := venueoracle.TableRows(t, ctx, goURI, `SELECT tier || ' ' || is_valid::text FROM org_licenses WHERE org_id = '`+orgA+`'`)
	send("deleted: redelivered after the new subscription", "evt_own_deleted", "customer.subscription.deleted", func(object map[string]any) {
		object["id"], object["customer"], object["status"] = "sub_own_new", "cus_other", "canceled"
		setPrice(object, "price_seed_y")
	})
	if !strings.HasSuffix(before, " true") {
		t.Errorf("the new subscription did not validate org A's license: %s", before)
	}
	expect("the redelivered cancellation leaves the new license as it was",
		`SELECT tier || ' ' || is_valid::text FROM org_licenses WHERE org_id = '`+orgA+`'`, before)
	t.Log("\n" + receipt)
	venueoracle.WriteGoOnlyProof(t, "Go resolves the org of subscription events without org metadata from the stored subscription or the license customer; the Python plane skips them (CHAOS-6525)")
}
