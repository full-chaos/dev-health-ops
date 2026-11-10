package accessverify

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-go/authverify"
)

// The key-compromise objective: a verifier refuses a dropped kid at most 30 s
// after the decision, and auth-service's JWKS drops it at most 20 s after it.
const (
	compromiseObjective = 30 * time.Second
	authServiceDropMax  = 20 * time.Second
)

type cfgClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *cfgClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *cfgClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// cfgIssuer is an https issuer whose JWKS kid set, failure mode and response
// headers a test changes between fetches. It counts GETs per path.
type cfgIssuer struct {
	srv  *httptest.Server
	gate chan struct{}

	mu        sync.Mutex
	all       map[string]ed25519.PublicKey
	published []string
	mode      string
	jwksFails bool
	header    map[string]string
	gets      map[string]int
}

func newCfgIssuer(t *testing.T) *cfgIssuer {
	t.Helper()
	i := &cfgIssuer{gate: make(chan struct{}), all: map[string]ed25519.PublicKey{}, header: map[string]string{}, gets: map[string]int{}}
	i.srv = httptest.NewTLSServer(http.HandlerFunc(i.serve))
	t.Cleanup(i.srv.Close)
	t.Cleanup(func() { close(i.gate) })
	return i
}

func (i *cfgIssuer) serve(w http.ResponseWriter, r *http.Request) {
	i.mu.Lock()
	i.gets[r.URL.Path]++
	mode := i.mode
	if i.jwksFails && r.URL.Path == "/.well-known/jwks.json" {
		mode = "status503"
	}
	header := map[string]string{}
	for name, value := range i.header {
		header[name] = value
	}
	keys := make([]map[string]string, 0, len(i.published))
	for _, kid := range i.published {
		keys = append(keys, map[string]string{"kty": "OKP", "crv": "Ed25519", "alg": "EdDSA", "use": "sig", "kid": kid, "x": b64(i.all[kid])})
	}
	i.mu.Unlock()
	switch mode {
	case "status503":
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	case "connectionClosed":
		if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
			_ = conn.Close()
		}
		return
	case "hang":
		select {
		case <-i.gate:
		case <-r.Context().Done():
		}
		return
	}
	for name, value := range header {
		w.Header().Set(name, value)
	}
	switch r.URL.Path {
	case "/.well-known/openid-configuration":
		doc, _ := json.Marshal(map[string]any{"issuer": i.srv.URL, "jwks_uri": i.srv.URL + "/.well-known/jwks.json", "id_token_signing_alg_values_supported": []string{"EdDSA"}})
		_, _ = w.Write(doc)
	case "/.well-known/jwks.json":
		doc, _ := json.Marshal(map[string]any{"keys": keys})
		_, _ = w.Write(doc)
	default:
		http.NotFound(w, r)
	}
}

// publish sets the kids the JWKS lists; a kid gets a key the first time.
func (i *cfgIssuer) publish(t *testing.T, kids ...string) {
	t.Helper()
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, kid := range kids {
		if _, ok := i.all[kid]; !ok {
			pub, _, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			i.all[kid] = pub
		}
	}
	i.published = append([]string(nil), kids...)
	sort.Strings(i.published)
}

func (i *cfgIssuer) setMode(mode string) {
	i.mu.Lock()
	i.mode = mode
	i.mu.Unlock()
}

func (i *cfgIssuer) failJWKS(fail bool) {
	i.mu.Lock()
	i.jwksFails = fail
	i.mu.Unlock()
}

func (i *cfgIssuer) setHeader(name, value string) {
	i.mu.Lock()
	i.header[name] = value
	i.mu.Unlock()
}

func (i *cfgIssuer) count(path string) int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.gets[path]
}

func (i *cfgIssuer) total() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	n := 0
	for _, c := range i.gets {
		n += c
	}
	return n
}

// client builds the JWKS client from JWKSConfig, with the test's TLS client
// and clock. The hang mode lowers Timeout only so the test does not wait the
// real fetch timeout; no assert reads wall time.
func (i *cfgIssuer) client(t *testing.T, clock *cfgClock, mode string) *authverify.JWKSClient {
	t.Helper()
	cfg := JWKSConfig(i.srv.URL)
	cfg.HTTPClient = i.srv.Client()
	cfg.Now = clock.Now
	if mode == "hang" {
		cfg.Timeout = 200 * time.Millisecond
	}
	client, err := authverify.NewJWKSClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func admitted(c *authverify.JWKSClient, kid string) bool {
	key, err := c.ResolveKey(kid)
	return err == nil && key != nil
}

func wantAdmitted(t *testing.T, step string, c *authverify.JWKSClient, kid string) {
	t.Helper()
	if !admitted(c, kid) {
		t.Fatalf("%s: kid %s is not admitted", step, kid)
	}
}

func wantNotAdmitted(t *testing.T, step string, c *authverify.JWKSClient, kid string) {
	t.Helper()
	if key, err := c.ResolveKey(kid); key != nil {
		t.Fatalf("%s: kid %s is admitted (err %v)", step, kid, err)
	}
}

// wantUnknownKid: a reachable JWKS that does not list kid, so the verifier
// refuses it as unknown_kid (401), not as unavailable.
func wantUnknownKid(t *testing.T, step string, c *authverify.JWKSClient, kid string) {
	t.Helper()
	if key, err := c.ResolveKey(kid); key != nil || err != nil {
		t.Fatalf("%s: kid %s: key %v err %v, want an unknown kid", step, kid, key != nil, err)
	}
}

var cfgOutageModes = []string{"status503", "connectionClosed", "hang"}

func TestJWKSConfig_ExactValuesAndTheBound(t *testing.T) {
	const issuer = "https://auth.example.test"
	cfg := JWKSConfig(issuer)
	if cfg.Issuer != issuer || cfg.CacheTTL != 5*time.Second || cfg.Timeout != 2*time.Second ||
		cfg.RefreshFloor != time.Second || cfg.OutageGrace != 10*time.Minute {
		t.Fatalf("JWKSConfig = issuer %q ttl %v timeout %v floor %v grace %v, want %q 5s 2s 1s 10m0s",
			cfg.Issuer, cfg.CacheTTL, cfg.Timeout, cfg.RefreshFloor, cfg.OutageGrace, issuer)
	}
	if cfg.HTTPClient != nil || cfg.Now != nil || cfg.MaxBodyBytes != 0 || cfg.MaxKeys != 0 {
		t.Fatal("JWKSConfig sets an HTTP client, a clock or a body/key limit")
	}
	if authServiceDropMax+cfg.CacheTTL+cfg.Timeout > compromiseObjective {
		t.Fatalf("drop %v + ttl %v + timeout %v is past the %v objective", authServiceDropMax, cfg.CacheTTL, cfg.Timeout, compromiseObjective)
	}
	if authServiceDropMax+cfg.RefreshFloor+cfg.Timeout > compromiseObjective {
		t.Fatalf("drop %v + floor %v + timeout %v is past the %v objective", authServiceDropMax, cfg.RefreshFloor, cfg.Timeout, compromiseObjective)
	}
}

// Product rule 3: the values never refuse a start, and building the client
// does no I/O, so an unreachable issuer does not hold a start either.
func TestJWKSConfig_NeverBlocksAStart(t *testing.T) {
	if _, err := authverify.NewJWKSClient(JWKSConfig("https://auth.example.test")); err != nil {
		t.Fatalf("an unreachable issuer: %v", err)
	}
	issuer := newCfgIssuer(t)
	issuer.publish(t, "kid-k")
	_ = issuer.client(t, &cfgClock{t: now}, "")
	if n := issuer.total(); n != 0 {
		t.Fatalf("building the client made %d requests, want 0", n)
	}
}

func TestJWKSConfig_DroppedKidRefusedWithinTheBound(t *testing.T) {
	issuer := newCfgIssuer(t)
	issuer.publish(t, "kid-k", "kid-j")
	clock := &cfgClock{t: now}
	client := issuer.client(t, clock, "")
	wantAdmitted(t, "first fetch", client, "kid-k")
	issuer.publish(t, "kid-j")
	clock.advance(5 * time.Second)
	wantUnknownKid(t, "5 s after the fetch, the JWKS without the kid", client, "kid-k")
	wantAdmitted(t, "the kid still listed", client, "kid-j")
	if n := issuer.count("/.well-known/jwks.json"); n != 2 {
		t.Fatalf("jwks fetches %d, want 2", n)
	}
}

// A refresh that meets auth-service's 503 (the moment it drops a latched
// key) is retried after the 1 s floor, and that fetch drops the kid.
func TestJWKSConfig_RefreshAfterA503AtTheDropRefusesWithinTheFloor(t *testing.T) {
	issuer := newCfgIssuer(t)
	issuer.publish(t, "kid-k", "kid-j")
	clock := &cfgClock{t: now}
	client := issuer.client(t, clock, "")
	wantAdmitted(t, "first fetch", client, "kid-k")
	clock.advance(5 * time.Second)
	issuer.setMode("status503")
	wantAdmitted(t, "the 503 at the drop serves the cached set inside the grace", client, "kid-k")
	issuer.setMode("")
	issuer.publish(t, "kid-j")
	clock.advance(time.Second)
	wantUnknownKid(t, "1 s later, the JWKS without the kid", client, "kid-k")
}

func TestJWKSConfig_OutageKeepsCachedKidsInsideTheGrace(t *testing.T) {
	for _, mode := range cfgOutageModes {
		t.Run(mode, func(t *testing.T) {
			issuer := newCfgIssuer(t)
			issuer.publish(t, "kid-k")
			clock := &cfgClock{t: now}
			client := issuer.client(t, clock, mode)
			wantAdmitted(t, "first fetch", client, "kid-k")
			issuer.setMode(mode)
			clock.advance(5 * time.Second)
			wantAdmitted(t, "past the TTL, the refresh fails", client, "kid-k")
			clock.advance(10*time.Minute - time.Second)
			wantAdmitted(t, "1 s before the grace ends", client, "kid-k")
			clock.advance(time.Second)
			key, err := client.ResolveKey("kid-k")
			if key != nil || !errors.Is(err, authverify.ErrKeySourceUnavailable) {
				t.Fatalf("at the end of the grace: key %v err %v, want unavailable", key != nil, err)
			}
		})
	}
}

// A kid that a successful fetch no longer lists stays refused when the next
// fetch fails: the grace serves only the last successful set.
func TestJWKSConfig_DroppedKidStaysRefusedAfterAFailedFetch(t *testing.T) {
	for _, mode := range cfgOutageModes {
		t.Run(mode, func(t *testing.T) {
			issuer := newCfgIssuer(t)
			issuer.publish(t, "kid-k", "kid-j")
			clock := &cfgClock{t: now}
			client := issuer.client(t, clock, mode)
			wantAdmitted(t, "fetch 1 lists the kid", client, "kid-k")
			wantAdmitted(t, "fetch 1 lists the other kid", client, "kid-j")
			issuer.publish(t, "kid-j")
			clock.advance(5 * time.Second)
			wantUnknownKid(t, "fetch 2 without the kid", client, "kid-k")
			wantAdmitted(t, "fetch 2 keeps the other kid", client, "kid-j")
			issuer.setMode(mode)
			clock.advance(5 * time.Second)
			wantAdmitted(t, "fetch 3 fails, the grace serves the last set", client, "kid-j")
			wantNotAdmitted(t, "fetch 3 fails", client, "kid-k")
			clock.advance(10*time.Minute - time.Second)
			wantAdmitted(t, "1 s before the grace ends", client, "kid-j")
			wantNotAdmitted(t, "1 s before the grace ends", client, "kid-k")
		})
	}
}

// auth-service sends public, max-age=300 on both documents; the client keeps
// its own 5 s TTL.
func TestJWKSConfig_IgnoresMaxAge(t *testing.T) {
	issuer := newCfgIssuer(t)
	issuer.setHeader("Cache-Control", "public, max-age=300")
	issuer.setHeader("Age", "0")
	issuer.publish(t, "kid-k", "kid-j")
	clock := &cfgClock{t: now}
	client := issuer.client(t, clock, "")
	wantAdmitted(t, "first fetch", client, "kid-k")
	issuer.publish(t, "kid-j")
	clock.advance(5 * time.Second)
	wantUnknownKid(t, "5 s later, max-age 300 notwithstanding", client, "kid-k")
	for _, path := range []string{"/.well-known/openid-configuration", "/.well-known/jwks.json"} {
		if n := issuer.count(path); n != 2 {
			t.Fatalf("%s fetched %d times, want 2", path, n)
		}
	}
}

// jwksReadings are the fetch counter points (key "document/result") and the
// grace-state gauge of one service on reader; grace is -1 with no point.
func jwksReadings(t *testing.T, reader *sdkmetric.ManualReader, service string) (map[string]int64, int64) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	fetches, grace := map[string]int64{}, int64(-1)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				if m.Name != JWKSFetchMetric {
					continue
				}
				for _, dp := range data.DataPoints {
					s, _ := dp.Attributes.Value("service")
					d, _ := dp.Attributes.Value("document")
					r, _ := dp.Attributes.Value("result")
					if s.AsString() == service {
						fetches[d.AsString()+"/"+r.AsString()] = dp.Value
					}
				}
			case metricdata.Gauge[int64]:
				if m.Name != JWKSGraceMetric {
					continue
				}
				for _, dp := range data.DataPoints {
					if s, _ := dp.Attributes.Value("service"); s.AsString() == service {
						grace = dp.Value
					}
				}
			}
		}
	}
	return fetches, grace
}

func wantReadings(t *testing.T, step string, reader *sdkmetric.ManualReader, service string, fetches map[string]int64, grace int64) {
	t.Helper()
	got, gotGrace := jwksReadings(t, reader, service)
	for key, want := range fetches {
		if got[key] != want {
			t.Fatalf("%s: fetch %s = %d, want %d (all %v)", step, key, got[key], want, got)
		}
	}
	if gotGrace != grace {
		t.Fatalf("%s: grace state %d, want %d", step, gotGrace, grace)
	}
}

// A failed fetch moves the fail counter and the grace state; a good fetch
// moves ok and sets the state back to fresh.
func TestNewJWKSKeys_CountsFetchesAndReportsTheGraceState(t *testing.T) {
	issuer := newCfgIssuer(t)
	issuer.publish(t, "kid-k")
	clock := &cfgClock{t: now}
	reader := sdkmetric.NewManualReader()
	cfg := JWKSConfig(issuer.srv.URL)
	cfg.HTTPClient = issuer.srv.Client()
	cfg.Now = clock.Now
	client, err := NewJWKSKeys(cfg, "svc-a", sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	wantReadings(t, "before any fetch", reader, "svc-a", map[string]int64{"discovery/ok": 0, "discovery/fail": 0}, -1)
	wantAdmitted(t, "first fetch", client, "kid-k")
	wantReadings(t, "after a good fetch", reader, "svc-a",
		map[string]int64{"discovery/ok": 1, "jwks/ok": 1, "discovery/fail": 0, "jwks/fail": 0}, JWKSStateFresh)
	issuer.setMode("status503")
	clock.advance(5 * time.Second)
	wantAdmitted(t, "past the TTL the refresh fails", client, "kid-k")
	wantReadings(t, "after a failed fetch", reader, "svc-a",
		map[string]int64{"discovery/ok": 1, "jwks/ok": 1, "discovery/fail": 1}, JWKSStateGrace)
	clock.advance(10 * time.Minute)
	wantNotAdmitted(t, "past the grace", client, "kid-k")
	wantReadings(t, "past the grace", reader, "svc-a", map[string]int64{"discovery/fail": 2}, JWKSStateExpired)
	issuer.setMode("")
	clock.advance(time.Second)
	wantAdmitted(t, "the issuer is back", client, "kid-k")
	wantReadings(t, "after the recovery", reader, "svc-a",
		map[string]int64{"discovery/ok": 2, "jwks/ok": 2, "discovery/fail": 2, "jwks/fail": 0}, JWKSStateFresh)
}

// A JWKS GET that fails after a good discovery GET counts under jwks/fail.
func TestNewJWKSKeys_CountsAFailedJWKSGet(t *testing.T) {
	issuer := newCfgIssuer(t)
	issuer.publish(t, "kid-k")
	reader := sdkmetric.NewManualReader()
	cfg := JWKSConfig(issuer.srv.URL)
	cfg.HTTPClient = issuer.srv.Client()
	cfg.Now = (&cfgClock{t: now}).Now
	client, err := NewJWKSKeys(cfg, "svc-b", sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	issuer.failJWKS(true)
	wantNotAdmitted(t, "the JWKS GET fails", client, "kid-k")
	wantReadings(t, "the JWKS GET fails", reader, "svc-b", map[string]int64{"discovery/ok": 1, "jwks/fail": 1, "jwks/ok": 0}, JWKSStateExpired)
}
