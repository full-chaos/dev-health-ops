package apiservice

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

func configuredAccessVerify(t *testing.T) (built bool, err error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	components, err := configureWith(ctx, config.Config{APIAddress: "127.0.0.1:0"}, health.NewRegistry(time.Second), quietLogger(),
		func(deps *Deps) { built = deps.AccessVerify != nil })
	for index := len(components) - 1; index >= 0; index-- {
		_ = components[index].Shutdown(context.Background())
	}
	return built, err
}

func TestConfigure_ConstructsAccessVerifyWhenIssuerIsSet(t *testing.T) {
	t.Setenv(envAccessTokenIssuer, "https://auth.example.test")
	built, err := configuredAccessVerify(t)
	if err != nil || !built {
		t.Fatalf("built=%v err=%v", built, err)
	}
}

func TestConfigure_NoAccessVerifyWithoutIssuer(t *testing.T) {
	t.Setenv(envAccessTokenIssuer, "")
	built, err := configuredAccessVerify(t)
	if err != nil || built {
		t.Fatalf("built=%v err=%v", built, err)
	}
}

func TestConfigure_RefusesInvalidAccessTokenIssuer(t *testing.T) {
	t.Setenv(envAccessTokenIssuer, "http://auth.example.test")
	built, err := configuredAccessVerify(t)
	if err == nil || built {
		t.Fatalf("built=%v err=%v", built, err)
	}
}

// configureForAccessVerify runs the prod construction path with a database
// configured (the pool is lazy) and the issuer set or unset.
func configureForAccessVerify(t *testing.T, issuerSet bool) Deps {
	t.Helper()
	issuer := ""
	if issuerSet {
		issuer = "https://auth.example.test"
	}
	t.Setenv(envAccessTokenIssuer, issuer)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var deps Deps
	components, err := configureWith(ctx, apiTestConfig(), health.NewRegistry(time.Second), quietLogger(), func(built *Deps) { deps = *built })
	for index := len(components) - 1; index >= 0; index-- {
		_ = components[index].Shutdown(context.Background())
	}
	if err != nil {
		t.Fatal(err)
	}
	return deps
}

// wiringIssuer is an https issuer whose JWKS kid set a test changes.
type wiringIssuer struct {
	srv *httptest.Server

	mu   sync.Mutex
	keys map[string]ed25519.PublicKey
	kids []string
	gets int
}

func newWiringIssuer(t *testing.T, kids ...string) *wiringIssuer {
	t.Helper()
	w := &wiringIssuer{keys: map[string]ed25519.PublicKey{}}
	for _, kid := range kids {
		pub, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		w.keys[kid] = pub
	}
	w.kids = kids
	w.srv = httptest.NewTLSServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.mu.Lock()
		w.gets++
		keys := make([]map[string]string, 0, len(w.kids))
		for _, kid := range w.kids {
			keys = append(keys, map[string]string{"kty": "OKP", "crv": "Ed25519", "alg": "EdDSA", "use": "sig", "kid": kid, "x": base64.RawURLEncoding.EncodeToString(w.keys[kid])})
		}
		w.mu.Unlock()
		var doc []byte
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			doc, _ = json.Marshal(map[string]any{"issuer": w.srv.URL, "jwks_uri": w.srv.URL + "/.well-known/jwks.json", "id_token_signing_alg_values_supported": []string{"EdDSA"}})
		case "/.well-known/jwks.json":
			doc, _ = json.Marshal(map[string]any{"keys": keys})
		default:
			http.NotFound(rw, r)
			return
		}
		_, _ = rw.Write(doc)
	}))
	t.Cleanup(w.srv.Close)
	return w
}

func (w *wiringIssuer) publish(kids ...string) {
	w.mu.Lock()
	w.kids = kids
	w.mu.Unlock()
}

func (w *wiringIssuer) requests() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.gets
}

// go-api's access-token key source is built from accessverify.JWKSConfig and
// counted under its service: a kid the JWKS drops is refused 5 s after the last
// fetch, and the build makes no request (product rule 3: it never holds a
// start).
func TestBuildAccessVerify_UsesTheBoundedJWKS(t *testing.T) {
	issuer := newWiringIssuer(t, "kid-k", "kid-j")
	var mu sync.Mutex
	at := time.Unix(1_800_000_000, 0)
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return at
	}
	reader := sdkmetric.NewManualReader()
	jwksTestHook = func(cfg *authverify.JWKSClientConfig, meter *metric.Meter) {
		cfg.HTTPClient = issuer.srv.Client()
		cfg.Now = clock
		*meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	}
	t.Cleanup(func() { jwksTestHook = nil })
	lookup := func(name string) (string, bool) {
		if name == envAccessTokenIssuer {
			return issuer.srv.URL, true
		}
		return "", false
	}
	validator, keys, err := buildAccessVerify(lookup, nil, quietLogger())
	if err != nil || validator == nil || keys == nil {
		t.Fatalf("validator %v keys %v err %v", validator != nil, keys != nil, err)
	}
	if n := issuer.requests(); n != 0 {
		t.Fatalf("the build made %d requests, want 0", n)
	}
	resolver, ok := keys.(authverify.KeyResolver)
	if !ok {
		t.Fatalf("key source %T is not a KeyResolver", keys)
	}
	if key, err := resolver.ResolveKey("kid-k"); key == nil || err != nil {
		t.Fatalf("first fetch: key %v err %v", key != nil, err)
	}
	if n := jwksFetchCount(t, reader, goAPIAccessService, "jwks", "ok"); n != 1 {
		t.Fatalf("go-api JWKS fetch ok count %d, want 1", n)
	}
	issuer.publish("kid-j")
	mu.Lock()
	at = at.Add(5 * time.Second)
	mu.Unlock()
	if key, err := resolver.ResolveKey("kid-k"); key != nil || err != nil {
		t.Fatalf("5 s after the fetch, the JWKS without the kid: key %v err %v, want an unknown kid", key != nil, err)
	}
}

// jwksFetchCount is the JWKS fetch counter's value for service, document and
// result on reader.
func jwksFetchCount(t *testing.T, reader *sdkmetric.ManualReader, service, document, result string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != accessverify.JWKSFetchMetric || !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				s, _ := dp.Attributes.Value("service")
				d, _ := dp.Attributes.Value("document")
				r, _ := dp.Attributes.Value("result")
				if s.AsString() == service && d.AsString() == document && r.AsString() == result {
					return dp.Value
				}
			}
		}
	}
	return 0
}
