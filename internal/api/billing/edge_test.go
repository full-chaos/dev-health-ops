package billing

import (
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// TestEdgeHealthStatusGrid holds the billing-edge /health decision to the
// Python rule over every input: "ok" only when postgres and the three secrets
// are all ok, whatever stripe_client says (CHAOS-6520).
func TestEdgeHealthStatusGrid(t *testing.T) {
	values := []string{healthOK, healthNotConfigured, healthDown}
	checked := 0
	for _, postgres := range values {
		for _, key := range values {
			for _, webhook := range values {
				for _, license := range values {
					for _, client := range values {
						in := edgeHealthInput{Postgres: postgres, StripeSecretKey: key, StripeWebhookSecret: webhook, LicensePrivateKey: license, StripeClient: client}
						want := "down"
						if postgres == healthOK && key == healthOK && webhook == healthOK && license == healthOK {
							want = "ok"
						}
						if got := edgeHealthStatus(in); got != want {
							t.Fatalf("%+v: status %q, want %q", in, got, want)
						}
						checked++
					}
				}
			}
		}
	}
	if checked != 243 {
		t.Fatalf("grid covered %d inputs, want 243", checked)
	}
}

// TestEdgeHealthBodyShape: the body is {"status", "services"} with the services
// in the order Python builds them (the three secrets, stripe_client, postgres).
func TestEdgeHealthBodyShape(t *testing.T) {
	body, err := pyjson.Dumps(edgeHealthBody(edgeHealthInput{
		Postgres: healthDown, StripeSecretKey: healthOK, StripeWebhookSecret: healthNotConfigured, LicensePrivateKey: healthOK, StripeClient: healthOK,
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"status": "down", "services": {"stripe_secret_key": "ok", "stripe_webhook_secret": "not_configured", "license_private_key": "ok", "stripe_client": "ok", "postgres": "down"}}`
	if body != want {
		t.Fatalf("body\n got %s\nwant %s", body, want)
	}
}

// TestEdgeRoutesAreOnlyTheEdgeSet: the listener serves the webhook and
// GET|HEAD /health and nothing else; the billing host must reach no other
// route (the reason the edge is a second listener).
func TestEdgeRoutesAreOnlyTheEdgeSet(t *testing.T) {
	got := map[string]bool{}
	for _, route := range EdgeRoutes(Deps{}) {
		got[route.Method+" "+route.Pattern] = true
	}
	want := map[string]bool{
		http.MethodPost + " " + prefix + "/webhooks/stripe": true,
		http.MethodGet + " /health":                         true,
		http.MethodHead + " /health":                        true,
	}
	if len(got) != len(want) {
		t.Fatalf("edge routes %v, want %v", got, want)
	}
	for key := range want {
		if !got[key] {
			t.Fatalf("edge routes %v lack %s", got, key)
		}
	}
}
