//go:build integration

package billingvenue

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestRefundEventSettledGrid executes the status-order rule over its whole
// input grid, through the real Go api and the real Python api: a refund
// already stored {pending, succeeded, failed with a reason} x the event's
// status {pending, requires_action, succeeded, failed, canceled} x the event's
// failure reason {null, a reason} x event type {charge.refund.updated,
// refund.created, refund.updated, refund.failed, charge.refunded with its
// list} = 150 events per plane. Each outcome (status and failure reason of
// the row) is read back and held to two hand-written tables: Go never
// replaces a settled status with a status that says it is not settled, and
// never erases a recorded failure reason; Python assigns what the event
// says (and drops Stripe's own refund events). The cells where the two
// differ are the named divergences, counted.
func TestRefundEventSettledGrid(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	fake := newFakeStripe()
	goStripe := httptest.NewServer(fake.plane("go"))
	t.Cleanup(goStripe.Close)
	env := webhookEnv()
	var pythonEnv []string
	for key, value := range env {
		pythonEnv = append(pythonEnv, key+"="+value)
	}

	type cell struct {
		index                 int
		event, stored, status string
		failure               string // "null" or a reason
		refund, rowID         string
	}
	var cells []cell
	events := []string{"charge.refund.updated", "refund.created", "refund.updated", "refund.failed", "charge.refunded"}
	for _, event := range events {
		for _, stored := range []string{"pending", "succeeded", "failed"} {
			for _, status := range []string{"pending", "requires_action", "succeeded", "failed", "canceled"} {
				for _, failure := range []string{"null", "expired_or_canceled_card"} {
					index := len(cells)
					cells = append(cells, cell{index: index, event: event, stored: stored, status: status, failure: failure,
						refund: fmt.Sprintf("re_set_%03d", index), rowID: fmt.Sprintf("cccccccc-0000-4000-8000-%012d", 5000+index)})
				}
			}
		}
	}
	const storedReason = "lost_or_stolen_card"
	var seed billingFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(), PythonEnv: pythonEnv,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seed = billingSeed(t, ctx, admin)
			for _, c := range cells {
				var reason any
				if c.stored == "failed" {
					reason = storedReason
				}
				if _, err := admin.Exec(ctx, `INSERT INTO refunds (id, org_id, invoice_id, stripe_refund_id, stripe_charge_id, amount, currency, status, failure_reason, metadata, created_at, updated_at)
					VALUES ($1, $2, NULL, $3, 'ch_set', 100, 'usd', $4, $5, '{}', now(), now())`, c.rowID, seed.orgA, c.refund, c.stored, reason); err != nil {
					t.Fatal(err)
				}
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

	stamp := time.Now().Unix() + 300
	requests := make([]venueoracle.Request, len(cells))
	for index, c := range cells {
		fixture := c.event + ".json"
		switch c.event {
		case "refund.failed":
			fixture = "charge.refund.updated.json"
		}
		value := loadWebhookFixture(t, fixture)
		value["type"], value["id"] = c.event, fmt.Sprintf("evt_set_%03d", c.index)
		object := value["data"].(map[string]any)["object"].(map[string]any)
		var failure any
		if c.failure != "null" {
			failure = c.failure
		}
		refund := map[string]any{"id": c.refund, "object": "refund", "amount": 100, "currency": "usd", "status": c.status,
			"charge": "ch_set", "payment_intent": nil, "reason": nil, "failure_reason": failure, "metadata": map[string]any{"org_id": seed.orgA.String()}}
		if c.event == "charge.refunded" {
			object["id"] = "ch_set"
			object["refunds"] = map[string]any{"object": "list", "data": []any{refund}}
		} else {
			for key, item := range refund {
				object[key] = item
			}
		}
		body, _ := json.Marshal(value)
		requests[index] = venueoracle.Request{Name: c.refund, Method: "POST", Path: webhookPath,
			Headers: map[string]string{"Stripe-Signature": webhookSignature(webhookVenueSecret, stamp, body), "Content-Type": "application/json"},
			Body:    venueoracle.B64(string(body))}
	}
	python := venue.ServePython(t, requests)

	goURI, pyURI := venue.AdminURI(t, venue.GoDB), venue.AdminURI(t, venue.SourceDB)
	outcome := func(uri string, c cell) string {
		pool, err := pgxpool.New(ctx, uri)
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		var status, failure string
		var rows int
		if err := pool.QueryRow(ctx, `SELECT count(*), coalesce(min(status), '-'), coalesce(min(coalesce(failure_reason, '<null>')), '-') FROM refunds WHERE stripe_refund_id = $1`, c.refund).Scan(&rows, &status, &failure); err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("%d row(s) %s/%s", rows, status, failure)
	}
	// Go: a settled status is never replaced by one that says it is not
	// settled, and a recorded reason is never erased by an event without one.
	goWant := func(c cell) string {
		storedFailure := "<null>"
		if c.stored == "failed" {
			storedFailure = storedReason
		}
		status, failure := c.status, storedFailure
		if c.stored != "pending" && (c.status == "pending" || c.status == "requires_action") {
			status = c.stored // settled: never reopened
		} else if c.failure != "null" {
			failure = c.failure
		}
		return fmt.Sprintf("1 row(s) %s/%s", status, failure)
	}
	// Python: the event's status and failure reason are assigned as sent, and
	// Stripe's own refund events are dropped.
	pythonWant := func(c cell) string {
		if c.event == "refund.created" || c.event == "refund.updated" || c.event == "refund.failed" {
			storedFailure := "<null>"
			if c.stored == "failed" {
				storedFailure = storedReason
			}
			return fmt.Sprintf("1 row(s) %s/%s", c.stored, storedFailure)
		}
		failure := "<null>"
		if c.failure != "null" {
			failure = c.failure
		}
		return fmt.Sprintf("1 row(s) %s/%s", c.status, failure)
	}

	divergent := 0
	for _, c := range cells {
		response := venueoracle.Do(t, base, requests[c.index])
		name := fmt.Sprintf("stored=%s event=%s status=%s failure=%s", c.stored, c.event, c.status, c.failure)
		if response.Status != 200 {
			t.Errorf("%s: go answered %d", name, response.Status)
		}
		if have, want := outcome(goURI, c), goWant(c); have != want {
			t.Errorf("%s: go holds %q, want %q", name, have, want)
		}
		if have, want := outcome(pyURI, c), pythonWant(c); have != want {
			t.Errorf("%s: python holds %q, want %q (status %d)", name, have, want, python[c.index].Status)
		}
		if goWant(c) != pythonWant(c) {
			divergent++
		}
	}
	if len(cells) != 150 {
		t.Errorf("the grid holds %d cells, want 150", len(cells))
	}
	t.Logf("cells %d; go and python differ in %d (named: Stripe's own refund events applied, a settled status never reopened, a failure reason never erased)", len(cells), divergent)
	venueoracle.WriteProof(t)
}
