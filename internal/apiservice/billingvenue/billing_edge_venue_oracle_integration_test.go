//go:build integration

package billingvenue

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
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
//
// The Python plane's answers are a frozen golden executed on pythonBuild (the
// last build that carries billing_edge.py), so this oracle survives that
// file's deletion. The webhook signatures are stamped from the recorded clock,
// and the Go edge runs on it, so a frozen run sends the bytes the recording
// sent and both planes judge the signature at the same instant.
func TestVenueOracleBillingEdge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	golden := venueoracle.OpenGolden(t, goldenSpec(t.Name(), goldenDigest(t.Name())))
	start := time.Now().UTC()
	env := webhookEnv()
	// Only a recording serves Python, and only from the pinned checkout (pythonBuild)
	// that still carries this module; the module is deleted on main.
	pythonEnv := []string{"VENUE_PY_APP=dev_health_ops.api.billing_edge:app"}
	for key, value := range env {
		pythonEnv = append(pythonEnv, key+"="+value)
	}
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: golden.PythonRoot(t, venueRoot()), JWTKey: venueKey, Logger: quietLogger(), PythonEnv: pythonEnv,
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

	// The Go edge's clock: the recorded instant, read back once the health grid
	// has run (the recording holds it), so the webhook signatures below are
	// judged at the time they were stamped for on both planes.
	goNow := start
	// The Go edge, configured like a scenario: the three secrets and the pool.
	edge := func(stripeKey, webhookSecret, licenseKey string, pool *pgxpool.Pool) string {
		cfg := config.Config{APIBillingEdgeAddress: "127.0.0.1:0"}
		routes := billing.EdgeRoutes(billing.Deps{
			Now:  func() time.Time { return goNow },
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
			python := golden.PythonWithEnv(t, venue, extra, requests)
			receipt += venueoracle.Diff(t, edge(stripeKey, webhookSecret, licenseKey, pool), requests, python, venueoracle.DiffOptions{Golden: golden})
			scenarios++
		}
	}

	// Fully configured: the webhook and the 404 grid. The stamp is an hour
	// ahead of the recorded instant: a signature timestamp is refused only when
	// it is too old, on both planes, and the recording runs for minutes.
	goNow = recordedAt(t, golden, start)
	base := edge(env["STRIPE_SECRET_KEY"], env["STRIPE_WEBHOOK_SECRET"], env["LICENSE_PRIVATE_KEY"], upPool)
	stamp := goNow.Unix() + 3600
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
	python := golden.Python(t, venue, requests)
	receipt += venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{Golden: golden})

	// NAMED LIMITS (CHAOS-6520 r1, ruled): the Go edge keeps the main
	// listener's transport bounds, which are stricter than the in-process
	// Python edge the venue serves (no ingress and no uvicorn layer in front of
	// TestClient). Pinned here as the two planes' own answers, so the
	// difference cannot change or widen unseen: a webhook body above the 50 MiB
	// ingress bound is Python's signature check (400) and Go's 413; a head above
	// the request-head bound is Python's catch-all 404 and Go's 431.
	limits := []venueoracle.Request{
		{Name: "named limit: webhook body above the bound", Method: "POST", Path: webhookPath, Headers: map[string]string{"Content-Type": "application/json"},
			Body: venueoracle.B64(strings.Repeat("a", 50<<20+1))},
		{Name: "named limit: request head above the bound", Method: "GET", Path: "/unknown", Headers: map[string]string{"X-Big": strings.Repeat("a", 2<<20)}},
	}
	// The head bound lives on the listener, not the handler: serve the edge
	// through its own Start, as dho api does.
	edgeServer, err := apiservice.NewEdgeServer(config.Config{APIBillingEdgeAddress: "127.0.0.1:0"}, quietLogger(), billing.EdgeRoutes(billing.Deps{
		Pool: upPool, Stripe: stripeclient.New(stripeclient.Options{Key: env["STRIPE_SECRET_KEY"]}), Logger: quietLogger(),
		WebhookSecret: secrets.NewValue(env["STRIPE_WEBHOOK_SECRET"]), LicensePrivateKey: secrets.NewValue(env["LICENSE_PRIVATE_KEY"]), StripeKey: secrets.NewValue(env["STRIPE_SECRET_KEY"]),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := edgeServer.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = edgeServer.Shutdown(context.Background()) })
	limitBase := "http://" + edgeServer.Address()
	limitPython := golden.Python(t, venue, limits)
	golden.Consumed(t, limitPython...) // compared below against the two planes' own named answers
	wantPython := []struct {
		status int
		body   string
	}{{400, `{"detail":"Invalid Stripe signature"}`}, {404, `{"detail":"Not Found"}`}}
	wantGo := []struct {
		status int
		body   string
	}{{413, `{"detail":"Request Entity Too Large"}`}, {431, ""}}
	for index, request := range limits {
		if limitPython[index].Status != wantPython[index].status || limitPython[index].Body != wantPython[index].body {
			t.Errorf("%s: python %d %q, want %d %q", request.Name, limitPython[index].Status, limitPython[index].Body, wantPython[index].status, wantPython[index].body)
		}
		var body io.Reader
		if request.Body != nil {
			raw, err := base64.StdEncoding.DecodeString(*request.Body)
			if err != nil {
				t.Fatal(err)
			}
			body = bytes.NewReader(raw)
		}
		goRequest, err := http.NewRequest(request.Method, limitBase+request.Path, body)
		if err != nil {
			t.Fatal(err)
		}
		for key, value := range request.Headers {
			goRequest.Header.Set(key, value)
		}
		response, err := http.DefaultClient.Do(goRequest)
		if err != nil {
			t.Fatalf("%s: go: %v", request.Name, err)
		}
		text, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != wantGo[index].status || (wantGo[index].body != "" && string(text) != wantGo[index].body) {
			t.Errorf("%s: go %d %q, want %d %q", request.Name, response.StatusCode, text, wantGo[index].status, wantGo[index].body)
		}
	}
	golden.Finish(t)
	t.Logf("health scenarios %d (x GET and HEAD), grid requests %d, SAME %d\n%s", scenarios, len(requests), strings.Count(receipt, "SAME"), receipt)
}
