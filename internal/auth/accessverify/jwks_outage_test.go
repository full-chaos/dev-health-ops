package accessverify

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-go/authverify"
)

func jwksIssuer(t *testing.T, mode, kid string, pub ed25519.PublicKey) *httptest.Server {
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
			doc, _ := json.Marshal(map[string]any{"keys": []map[string]string{{"kty": "OKP", "crv": "Ed25519", "alg": "EdDSA", "use": "sig", "kid": kid, "x": b64(pub)}}})
			_, _ = w.Write(doc)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(gate) })
	return srv
}

var jwksOutageModes = []string{"status503", "connectionClosed", "hang"}

func jwksOutageFixture(t *testing.T, mode, publishedKid string) (*fixture, *httptest.Server) {
	t.Helper()
	f := newFixture(t, false, false)
	srv := jwksIssuer(t, mode, publishedKid, f.pub)
	client, err := authverify.NewJWKSClient(authverify.JWKSClientConfig{Issuer: srv.URL, HTTPClient: srv.Client(), Timeout: 300 * time.Millisecond, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: srv.URL, Audience: "dev-health-api", Keys: client})
	if err != nil {
		t.Fatal(err)
	}
	f.reader = sdkmetric.NewManualReader()
	f.v, err = New(Config{
		Rows:   []Row{{Service: svc, Profile: userRoute, AcceptsUser: true}},
		EdDSA:  verifier,
		Meter:  sdkmetric.NewMeterProvider(sdkmetric.WithReader(f.reader)).Meter("test"),
		Logger: slog.New(slog.DiscardHandler),
		Now:    func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return f, srv
}

func jwksOutageToken(f *fixture, issuer string) string {
	claims := goodClaims()
	claims["iss"] = issuer
	return f.eddsa(goodHeader(), claims)
}

func TestValidate_JWKSOutageIsUnavailableNotKidUnknown(t *testing.T) {
	for _, mode := range jwksOutageModes {
		t.Run(mode, func(t *testing.T) {
			f, srv := jwksOutageFixture(t, mode, "kid-1")
			_, err := f.v.Validate(context.Background(), svc, userRoute, jwksOutageToken(f, srv.URL))
			if !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrRefused) {
				t.Fatalf("err=%v, want ErrUnavailable and not a refusal", err)
			}
			c := f.counters()
			if c[refKey(userRoute, ReasonUnavailable)] != 1 || c[refKey(userRoute, ReasonKidUnknown)] != 0 {
				t.Fatalf("counters %v", c)
			}
		})
	}
}

func TestValidate_UnknownKidWithReachableJWKSStaysKidUnknown(t *testing.T) {
	f, srv := jwksOutageFixture(t, "ok", "kid-other")
	_, err := f.v.Validate(context.Background(), svc, userRoute, jwksOutageToken(f, srv.URL))
	if !errors.Is(err, ErrRefused) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("err=%v, want a refusal", err)
	}
	c := f.counters()
	if c[refKey(userRoute, ReasonKidUnknown)] != 1 || c[refKey(userRoute, ReasonUnavailable)] != 0 {
		t.Fatalf("counters %v", c)
	}
}
