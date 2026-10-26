package authruntime

import (
	"context"
	"crypto/ed25519"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

type fixedKeys struct{ set token.KeySet }

func (f fixedKeys) KeySet(context.Context) (token.KeySet, error) { return f.set, nil }

// TestJWKSAnswersWhileDependencyRoutesStall drives the production server
// options with routes that hold their in-flight slot until the request
// deadline (a stalled dependency) and asserts the key route still answers 200
// and fast.
func TestJWKSAnswersWhileDependencyRoutesStall(t *testing.T) {
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(map[string]string{
		authconfig.EnvDatabaseURI:            "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:         "/nonexistent",
		authconfig.EnvSigningKeyID:           "stall-test",
		authconfig.EnvRateLimitPerSecond:     "10000",
		authconfig.EnvRateLimitBurst:         "10000",
		authconfig.EnvRequestTimeout:         "2s",
		authconfig.EnvAPIAddress:             "127.0.0.1:0",
		authconfig.EnvOperatorAddress:        "127.0.0.1:1",
		authconfig.EnvHealthCheckTimeout:     "1s",
		authconfig.EnvShutdownTimeout:        "5s",
		authconfig.EnvDatabaseConnectTimeout: "1s",
	})})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	public, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	set, err := token.NewKeySet([]token.Key{{KeyID: "k1", State: token.State("active"), Algorithm: token.Algorithm, PublicKey: public}})
	if err != nil {
		t.Fatal(err)
	}
	stall := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	routes := []httpapi.Route{{Method: http.MethodGet, Pattern: "/v1/stall", Handler: stall}}
	routes = append(routes, authapi.WellKnownRoutes(authapi.WellKnownDeps{Keys: fixedKeys{set}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})...)
	server, err := httpapi.NewServer(apiServerOptions(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), routes))
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 512, MaxConnsPerHost: 0}}

	var stallers sync.WaitGroup
	for range 3 * maxInFlightRequests {
		stallers.Add(1)
		go func() {
			defer stallers.Done()
			resp, err := client.Get(ts.URL + "/v1/stall")
			if err == nil {
				_ = resp.Body.Close()
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)

	const probes = 256
	statuses := make([]int, probes)
	took := make([]time.Duration, probes)
	var wg sync.WaitGroup
	for i := range probes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			resp, err := client.Get(ts.URL + "/.well-known/jwks.json")
			took[i] = time.Since(start)
			if err == nil {
				statuses[i] = resp.StatusCode
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	stallers.Wait()

	counts := map[int]int{}
	for _, s := range statuses {
		counts[s]++
	}
	sort.Slice(took, func(a, b int) bool { return took[a] < took[b] })
	p99 := took[probes*99/100]
	t.Logf("JWKS statuses=%v p50=%s p99=%s", counts, took[probes/2], p99)
	if counts[http.StatusOK] != probes {
		t.Errorf("JWKS answered %v while dependency routes stalled; want %d x 200", counts, probes)
	}
	if p99 > 500*time.Millisecond {
		t.Errorf("JWKS p99 = %s while dependency routes stalled; want <= 500ms", p99)
	}
}
