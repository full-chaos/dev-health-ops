package authruntime

import (
	"context"
	"crypto/ed25519"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
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
// options with routes that hold their in-flight slot until the test releases
// them (a stalled dependency) and asserts the key route still answers 200 while
// every one of them is held: a key route that waited for a slot could answer
// only after the release, which comes after the probes.
func TestJWKSAnswersWhileDependencyRoutesStall(t *testing.T) {
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(map[string]string{
		authconfig.EnvDatabaseURI:            "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:         "/nonexistent",
		authconfig.EnvSigningKeyID:           "stall-test",
		authconfig.EnvRateLimitPerSecond:     "10000",
		authconfig.EnvRateLimitBurst:         "10000",
		authconfig.EnvRequestTimeout:         "30s",
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
	release := make(chan struct{})
	var held atomic.Int32
	stall := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		held.Add(1)
		<-release
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
	// The waits below are guards for the failure path only; a passing run never reaches them. Without the release every stalled
	// handler stays in its slot, so a key route that is limited by those slots never answers.
	for held.Load() < maxInFlightRequests {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-t.Context().Done():
			t.Fatal("the stalling routes did not fill their in-flight slots")
		}
	}

	const probes = 256
	statuses := make([]int, probes)
	var wg sync.WaitGroup
	for i := range probes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get(ts.URL + "/.well-known/jwks.json")
			if err == nil {
				statuses[i] = resp.StatusCode
				_ = resp.Body.Close()
			}
		}()
	}
	probesDone := make(chan struct{})
	go func() { wg.Wait(); close(probesDone) }()
	select {
	case <-probesDone:
	case <-time.After(30 * time.Second):
		close(release)
		t.Fatal("the JWKS probes are still waiting while the stalled routes hold their slots: the key route is not isolated from them")
	}
	close(release)
	stallers.Wait()

	counts := map[int]int{}
	for _, s := range statuses {
		counts[s]++
	}
	t.Logf("JWKS statuses=%v", counts)
	if counts[http.StatusOK] != probes {
		t.Errorf("JWKS answered %v while dependency routes stalled; want %d x 200", counts, probes)
	}
}
