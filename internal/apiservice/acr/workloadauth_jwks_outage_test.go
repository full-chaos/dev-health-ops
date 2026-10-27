package acr

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func wlJWKSIssuer(t *testing.T, mode, kid string, pub ed25519.PublicKey) *httptest.Server {
	t.Helper()
	gate := make(chan struct{})
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch mode {
		case "status503":
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		case "connectionClosed":
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		case "hang":
			select {
			case <-gate:
			case <-r.Context().Done():
			}
			return
		}
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			doc, _ := json.Marshal(map[string]any{"issuer": srv.URL, "jwks_uri": srv.URL + "/.well-known/jwks.json", "id_token_signing_alg_values_supported": []string{"EdDSA"}})
			_, _ = w.Write(doc)
		case "/.well-known/jwks.json":
			doc, _ := json.Marshal(map[string]any{"keys": []map[string]string{{"kty": "OKP", "crv": "Ed25519", "alg": "EdDSA", "use": "sig", "kid": kid, "x": base64.RawURLEncoding.EncodeToString(pub)}}})
			_, _ = w.Write(doc)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(gate) })
	return srv
}

func wlJWKSAuth(t *testing.T, mode, publishedKid string, signer wlSigner) (*WorkloadAuth, string) {
	t.Helper()
	srv := wlJWKSIssuer(t, mode, publishedKid, signer.pub)
	client, err := authverify.NewJWKSClient(authverify.JWKSClientConfig{Issuer: srv.URL, HTTPClient: srv.Client(), Timeout: 300 * time.Millisecond, Now: func() time.Time { return wlNow }})
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := authverify.NewWorkloadVerifier(authverify.WorkloadVerifierConfig{Issuer: srv.URL, Audience: "dev-health-api", Keys: client})
	if err != nil {
		t.Fatal(err)
	}
	claims := wlClaims()
	claims["iss"] = srv.URL
	return &WorkloadAuth{Verifier: verifier, Subject: wlSubject, Now: func() time.Time { return wlNow }}, signer.sign(wlHeader(), claims)
}

func TestWorkloadJWKSOutageAnswers503OnEveryRoute(t *testing.T) {
	signer := newWLSigner(t)
	for _, mode := range []string{"status503", "connectionClosed", "hang"} {
		for _, route := range wlRoutesUnderTest {
			t.Run(mode+"/"+route.key, func(t *testing.T) {
				auth, token := wlJWKSAuth(t, mode, wlKid, signer)
				h := newWLHarness(t, auth)
				before := wlCounts(t, "devhealth_acr_workload_refused_total")
				if _, declared := before[route.key+"/"+ReasonUnavailable]; !declared {
					t.Fatalf("series %s/%s was not declared before the first request", route.key, ReasonUnavailable)
				}
				internalBefore, llmBefore := internalRequestCounts(t), llmConfigRequestCounts(t)
				rec := do(h.handler, route.path, "Bearer "+token)
				if rec.Code != http.StatusServiceUnavailable {
					t.Fatalf("status = %d %s, want 503", rec.Code, rec.Body.String())
				}
				if strings.TrimSpace(rec.Body.String()) != `{"detail":"Service unavailable"}` {
					t.Fatalf("body = %s", rec.Body.String())
				}
				if got := wlDelta(before, wlCounts(t, "devhealth_acr_workload_refused_total")); !reflect.DeepEqual(got, map[string]int64{route.key + "/" + ReasonUnavailable: 1}) {
					t.Fatalf("refused counter delta = %v, want %s/%s once", got, route.key, ReasonUnavailable)
				}
				if got := wlDelta(internalBefore, internalRequestCounts(t)); len(got) != 0 {
					t.Fatalf("an outage added internal request outcome series: %v", got)
				}
				if got := wlDelta(llmBefore, llmConfigRequestCounts(t)); len(got) != 0 {
					t.Fatalf("an outage added llm-config request outcome series: %v", got)
				}
				if h.creds.findCalls != 0 || len(h.creds.used) != 0 || h.llm.gotToken != "" {
					t.Fatalf("an unavailable verdict reached a store: finds %d used %v", h.creds.findCalls, h.creds.used)
				}
			})
		}
	}
}

func TestWorkloadUnknownKidWithReachableJWKSStays401(t *testing.T) {
	signer := newWLSigner(t)
	for _, route := range wlRoutesUnderTest {
		t.Run(route.key, func(t *testing.T) {
			auth, token := wlJWKSAuth(t, "ok", "kid-other", signer)
			h := newWLHarness(t, auth)
			before := wlCounts(t, "devhealth_acr_workload_refused_total")
			rec := do(h.handler, route.path, "Bearer "+token)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d %s, want 401", rec.Code, rec.Body.String())
			}
			if got := wlDelta(before, wlCounts(t, "devhealth_acr_workload_refused_total")); !reflect.DeepEqual(got, map[string]int64{route.key + "/" + ReasonUnknownKid: 1}) {
				t.Fatalf("refused counter delta = %v", got)
			}
		})
	}
}

func TestWorkloadUnavailableSeriesIsDeclaredAtZeroOnEveryRoute(t *testing.T) {
	if !slices.Contains(workloadReasons, ReasonUnavailable) {
		t.Fatalf("workloadReasons %v lacks %s", workloadReasons, ReasonUnavailable)
	}
	acrMetricReader()
	Routes(Deps{})
	seen := wlCounts(t, "devhealth_acr_workload_refused_total")
	for _, route := range wlRoutesUnderTest {
		if _, ok := seen[route.key+"/"+ReasonUnavailable]; !ok {
			t.Errorf("no zero series for %s/%s", route.key, ReasonUnavailable)
		}
	}
}

func llmConfigRequestCounts(t *testing.T) map[string]int64 {
	return counterByAttribute(t, "devhealth_acr_llm_config_requests_total", func(p metricdata.DataPoint[int64]) (string, bool) {
		outcome, _ := p.Attributes.Value("outcome")
		return outcome.AsString(), true
	})
}
