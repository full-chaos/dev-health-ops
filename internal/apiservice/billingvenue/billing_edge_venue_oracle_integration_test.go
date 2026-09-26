//go:build integration

package billingvenue

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/billing"
	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestVenueOracleBillingEdge holds the Go billing-edge listener to the real
// Python billing edge app (dev_health_ops.api.billing_edge:app, served by the
// venue through VENUE_PY_APP): GET and HEAD /health under every combination
// of the three secrets (set, unset) and postgres (reachable, not), and, with
// everything configured, the webhook (a signed event, a bad and a missing
// signature) and the 404 grid over paths x the catch-all's methods.
func TestVenueOracleBillingEdge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := webhookEnv()
	pythonEnv := []string{"VENUE_PY_APP=dev_health_ops.api.billing_edge:app"}
	for key, value := range env {
		pythonEnv = append(pythonEnv, key+"="+value)
	}
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(), PythonEnv: pythonEnv,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			return billingSeed(t, ctx, admin).tokenSpecs()
		},
	})
	upPool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(upPool.Close)
	const unreachable = "postgresql://nobody:nothing@127.0.0.1:1/none"
	downPool, err := pgxpool.New(ctx, unreachable)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(downPool.Close)

	// The Go edge, configured like a scenario: the three secrets and the pool.
	edge := func(stripeKey, webhookSecret, licenseKey string, pool *pgxpool.Pool) string {
		cfg := config.Config{APIBillingEdgeAddress: "127.0.0.1:0"}
		routes := billing.EdgeRoutes(billing.Deps{
			Pool: pool, Stripe: stripeclient.New(stripeclient.Options{Key: stripeKey}), Logger: quietLogger(),
			WebhookSecret: secrets.NewValue(webhookSecret), LicensePrivateKey: secrets.NewValue(licenseKey), StripeKey: secrets.NewValue(stripeKey),
		})
		server, err := apiservice.NewEdgeServer(cfg, quietLogger(), routes)
		if err != nil {
			t.Fatal(err)
		}
		ts := httptest.NewServer(server.Handler())
		t.Cleanup(ts.Close)
		return ts.URL
	}

	receipt := ""
	// The health grid: 3 secrets x postgres = 16 scenarios, GET and HEAD.
	scenarios := 0
	for mask := 0; mask < 8; mask++ {
		for _, postgresUp := range []bool{true, false} {
			stripeKey, webhookSecret, licenseKey := env["STRIPE_SECRET_KEY"], env["STRIPE_WEBHOOK_SECRET"], env["LICENSE_PRIVATE_KEY"]
			extra := []string{}
			set := func(bit int, name string, value *string) {
				if mask&(1<<bit) == 0 {
					*value = ""
					extra = append(extra, name+"=")
				}
			}
			set(0, "STRIPE_SECRET_KEY", &stripeKey)
			set(1, "STRIPE_WEBHOOK_SECRET", &webhookSecret)
			set(2, "LICENSE_PRIVATE_KEY", &licenseKey)
			pool := upPool
			if !postgresUp {
				pool = downPool
				extra = append(extra, "POSTGRES_URI="+unreachable)
			}
			name := fmt.Sprintf("secrets=%03b postgres=%v", mask, postgresUp)
			requests := []venueoracle.Request{
				{Name: name + " GET /health", Method: "GET", Path: "/health", Headers: map[string]string{}},
				{Name: name + " HEAD /health", Method: "HEAD", Path: "/health", Headers: map[string]string{}},
			}
			python := venue.ServePythonWithEnv(t, extra, requests)
			receipt += venueoracle.Diff(t, edge(stripeKey, webhookSecret, licenseKey, pool), requests, python, venueoracle.DiffOptions{})
			scenarios++
		}
	}

	// Fully configured: the webhook and the 404 grid.
	base := edge(env["STRIPE_SECRET_KEY"], env["STRIPE_WEBHOOK_SECRET"], env["LICENSE_PRIVATE_KEY"], upPool)
	stamp := time.Now().Unix() + 300
	eventBody := []byte(`{"id": "evt_edge_probe", "object": "event", "type": "customer.created", "data": {"object": {}}}`)
	signed := map[string]string{"Stripe-Signature": webhookSignature(webhookVenueSecret, stamp, eventBody), "Content-Type": "application/json"}
	var requests []venueoracle.Request
	requests = append(requests,
		venueoracle.Request{Name: "webhook: signed, an unhandled type", Method: "POST", Path: webhookPath, Headers: signed, Body: venueoracle.B64(string(eventBody))},
		venueoracle.Request{Name: "webhook: bad signature", Method: "POST", Path: webhookPath,
			Headers: map[string]string{"Stripe-Signature": webhookSignature("whsec_other", stamp, eventBody), "Content-Type": "application/json"}, Body: venueoracle.B64(string(eventBody))},
		venueoracle.Request{Name: "webhook: no signature header", Method: "POST", Path: webhookPath, Headers: map[string]string{"Content-Type": "application/json"}, Body: venueoracle.B64(string(eventBody))},
		venueoracle.Request{Name: "webhook: not json", Method: "POST", Path: webhookPath, Headers: map[string]string{"Content-Type": "application/json", "Stripe-Signature": webhookSignature(webhookVenueSecret, stamp, []byte("not json"))}, Body: venueoracle.B64("not json")},
	)
	paths := []string{"/health", "/health/", "/healthz", "/HEALTH", "/", "/api/v1/billing/webhooks/stripe", "/api/v1/billing/webhooks/stripe/",
		"/api/v1/billing/plans", "/api/v1/billing/checkout", "/x/y", "/health/x", "/docs", "/openapi.json", "/metrics"}
	for _, path := range paths {
		// The catch-all's seven methods answer its 404; any other method keeps
		// Starlette's partial-match 405 (the first route whose path matches, else
		// the catch-all's own list).
		for _, method := range []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD", "TRACE", "PROPFIND", "PURGE"} {
			if (path == "/health" && (method == "GET" || method == "HEAD")) || (path == "/api/v1/billing/webhooks/stripe" && method == "POST") {
				continue // the health grid and the webhook cases above
			}
			body := (*string)(nil)
			if method == "POST" || method == "PUT" || method == "PATCH" || method == "PROPFIND" {
				body = venueoracle.B64("{}")
			}
			requests = append(requests, venueoracle.Request{Name: method + " " + path, Method: method, Path: path, Headers: map[string]string{"Content-Type": "application/json"}, Body: body})
		}
	}
	python := venue.ServePython(t, requests)
	receipt += venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{})
	t.Logf("health scenarios %d (x GET and HEAD), grid requests %d, SAME %d\n%s", scenarios, len(requests), strings.Count(receipt, "SAME"), receipt)
}
