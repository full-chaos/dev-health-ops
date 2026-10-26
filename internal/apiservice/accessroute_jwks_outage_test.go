package apiservice

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
)

func goAPIJWKSIssuer(t *testing.T, mode, kid string, pub ed25519.PublicKey) *httptest.Server {
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

type goAPIJWKSRig struct {
	*routedFixture
	adapter routedAccessVerify
}

func goAPIJWKSFixture(t *testing.T, mode, publishedKid string) (*goAPIJWKSRig, string) {
	t.Helper()
	minter := newEdDSAMinter(t)
	srv := goAPIJWKSIssuer(t, mode, publishedKid, minter.keys["probe-kid"])
	client, err := authverify.NewJWKSClient(authverify.JWKSClientConfig{Issuer: srv.URL, HTTPClient: srv.Client(), Timeout: 300 * time.Millisecond, Now: func() time.Time { return probeNow }})
	if err != nil {
		t.Fatal(err)
	}
	f := newRoutedFixture(t, newProbeStore(), userRows(t), srv.URL, client, nil, probeNow)
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: srv.URL, Audience: accessTokenAudienceAPI, Keys: client})
	if err != nil {
		t.Fatal(err)
	}
	validator, err := accessverify.New(accessverify.Config{
		Rows: userRows(t), EdDSA: verifier, Meter: sdkmetric.NewMeterProvider().Meter("test"), Logger: quietLogger(), Now: func() time.Time { return probeNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	return &goAPIJWKSRig{routedFixture: f, adapter: routedAccessVerify{validator}}, minter.mint(probeUser, map[string]any{"iss": srv.URL})
}

func TestGoAPIUserTokenJWKSOutageAnswers503(t *testing.T) {
	for _, mode := range []string{"status503", "connectionClosed", "hang"} {
		t.Run(mode, func(t *testing.T) {
			f, token := goAPIJWKSFixture(t, mode, "probe-kid")
			_, err := f.adapter.Validate(context.Background(), goAPIAccessService, "GET "+probeAuthenticated, token)
			if !errors.Is(err, policy.ErrUnavailable) {
				t.Fatalf("err=%v, want policy.ErrUnavailable", err)
			}
			recorder := httptest.NewRecorder()
			policy.WriteAuthFailure(recorder, "Invalid or expired token", err)
			if recorder.Code != http.StatusServiceUnavailable || strings.TrimSpace(recorder.Body.String()) != `{"detail":{"message":"Database temporarily unavailable"}}` {
				t.Fatalf("guard answer %d %s, want the existing 503", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestGoAPIUserTokenUnknownKidWithReachableJWKSStays401(t *testing.T) {
	f, token := goAPIJWKSFixture(t, "ok", "kid-other")
	_, err := f.adapter.Validate(context.Background(), goAPIAccessService, "GET "+probeAuthenticated, token)
	if err == nil || errors.Is(err, policy.ErrUnavailable) {
		t.Fatalf("err=%v, want a refusal", err)
	}
	got := probeDo(f.handler, probeRequest{"unknown kid", http.MethodGet, probeAuthenticated, bearer(token)})
	if got.Status != http.StatusUnauthorized || got.Body != genericRefusal {
		t.Fatalf("answer %d %q, want 401 %s", got.Status, got.Body, genericRefusal)
	}
}
