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

// TestEdgeHealthBodyShape: the body is {"status"} only, for every input; no
// per-service or secret state reaches a caller.
func TestEdgeHealthBodyShape(t *testing.T) {
	body, err := pyjson.Dumps(edgeHealthBody(edgeHealthInput{
		Postgres: healthDown, StripeSecretKey: healthOK, StripeWebhookSecret: healthNotConfigured, LicensePrivateKey: healthOK, StripeClient: healthOK,
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"status": "down"}`
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

// TestEdgeHealthBodyNamesNoServiceForAnyInput: whatever the inputs, the body
// holds the status key only.
func TestEdgeHealthBodyNamesNoServiceForAnyInput(t *testing.T) {
	values := []string{healthOK, healthNotConfigured, healthDown}
	for _, a := range values {
		for _, b := range values {
			body, err := pyjson.Dumps(edgeHealthBody(edgeHealthInput{Postgres: a, StripeSecretKey: b, StripeWebhookSecret: a, LicensePrivateKey: b, StripeClient: a}))
			if err != nil {
				t.Fatal(err)
			}
			if body != `{"status": "ok"}` && body != `{"status": "down"}` {
				t.Fatalf("body %s", body)
			}
		}
	}
}
