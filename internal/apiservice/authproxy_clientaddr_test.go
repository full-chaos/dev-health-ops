package apiservice

import (
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
)

// proxyProofEntry is a test signing key (32 bytes, base64url without padding).
var proxyProofEntry = "k1:" + base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("q", 32)))

// With AUTH_CLIENT_ADDR_SIGNING_KEY set, every forwarded route carries one proof of the X-Forwarded-For value go-api
// writes, and auth-service's verifier (mode signed) names the client go-api trusts; a client's own proof header never
// reaches auth-service.
func TestAuthProxyProvesTheClientAddressItForwards(t *testing.T) {
	keys, err := clientaddr.ParseProofKeys(clientaddr.EnvVerifyKey, proxyProofEntry, clientaddr.MaxVerifyKeys)
	if err != nil {
		t.Fatal(err)
	}
	policy := clientaddr.Policy{Mode: clientaddr.ModeSigned, Verifier: clientaddr.NewVerifier(keys)}
	var mu sync.Mutex
	outcomes := map[string]clientaddr.Outcome{}
	bound := map[string]string{}
	stub := newStubUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		_, outcome := policy.Verifier.Check(r)
		mu.Lock()
		outcomes[r.Method+" "+r.URL.Path] = outcome
		bound[r.Method+" "+r.URL.Path] = policy.Resolve(r).String()
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{}`)
	})
	env := allOn(stub.URL)
	env[clientaddr.EnvSigningKey] = proxyProofEntry
	handler := proxyFixture(t, mustProxyConfig(t, env))
	for _, call := range proxyCalls {
		request := call.request(t)
		request.Header.Set("X-Request-ID", "client-correlation-1")
		request.Header.Set(clientaddr.ProofHeader, "v1.k1.1800000000.client-written")
		request.Header.Set("X-Forwarded-For", "203.0.113.99")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusCreated {
			t.Errorf("%s %s = %d, want the upstream 201", call.method, call.target, recorder.Code)
			continue
		}
		seen := stub.last(t)
		if got := seen.header.Values(clientaddr.ProofHeader); len(got) != 1 || strings.Contains(got[0], "client-written") {
			t.Errorf("%s %s: proof lines %q, want go-api's one", call.method, call.target, got)
		}
		key := seen.method + " " + seen.path
		mu.Lock()
		outcome, addr := outcomes[key], bound[key]
		mu.Unlock()
		if outcome != clientaddr.OutcomeValid || addr != "192.0.2.10" {
			t.Errorf("%s %s: proof %s, bound %s, want valid and 192.0.2.10", call.method, call.target, outcome, addr)
		}
	}
}

// Without the signing key go-api sends no proof, and a client's proof header is dropped as before.
func TestAuthProxyWithoutASigningKeySendsNoProof(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{}`)
	})
	handler := proxyFixture(t, mustProxyConfig(t, allOn(stub.URL)))
	for _, call := range proxyCalls {
		request := call.request(t)
		request.Header.Set(clientaddr.ProofHeader, "v1.k1.1800000000.client-written")
		handler.ServeHTTP(httptest.NewRecorder(), request)
		if got := stub.last(t).header.Values(clientaddr.ProofHeader); len(got) != 0 {
			t.Errorf("%s %s: proof reached upstream: %q", call.method, call.target, got)
		}
	}
}

func TestParseAuthProxyConfigRefusesABadSigningKey(t *testing.T) {
	short := base64.RawURLEncoding.EncodeToString([]byte("short-key"))
	for _, value := range []string{"k1:" + short, proxyProofEntry + "," + proxyProofEntry, "no-colon"} {
		env := allOn("http://auth-service:8095")
		env[clientaddr.EnvSigningKey] = value
		_, err := ParseAuthProxyConfig(lookupFrom(env))
		if err == nil {
			t.Errorf("signing key %d bytes long accepted", len(value))
			continue
		}
		if strings.Contains(err.Error(), short) || strings.Contains(err.Error(), proxyProofEntry[3:]) {
			t.Errorf("the error echoes the key: %v", err)
		}
	}
}
