//go:build integration

package externalingestvenue

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// roundRobinBackends serves each request from the next of several Go api
// servers: one caller talking to a fleet of replicas behind a load balancer.
func roundRobinBackends(t *testing.T, backends ...string) string {
	t.Helper()
	proxies := make([]*httputil.ReverseProxy, len(backends))
	for i, backend := range backends {
		target, err := url.Parse(backend)
		if err != nil {
			t.Fatal(err)
		}
		proxies[i] = httputil.NewSingleHostReverseProxy(target)
	}
	var next atomic.Uint64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxies[(next.Add(1)-1)%uint64(len(proxies))].ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server.URL
}

// TestExternalIngestAuthLimiterVenueOracle is CHAOS-6507's proof: auth.py
// drives slowapi's underlying `limits` limiter directly, so the ingest-auth
// throttles are a FIXED window per client address in the storage every api
// replica shares -- 100 attempts/minute (hit() on every attempt, first), and 30
// failures/minute (test() before the request, hit() on each failure). The
// real Python api and a fleet of TWO real Go api servers behind a round-robin
// balancer must answer the same burst the same way: the 31st failure from one
// address is the "failed attempts" 429 whichever replica takes it (per-process
// buckets would admit 30 failures per replica), the 101st attempt is the
// "attempts" 429, and another address has its own budget.
//
// Every request carries its own X-Forwarded-For (each plane trusts its own
// peer: the in-process Python client is "testclient", the Go replicas' peer is
// 127.0.0.1). No request carries a bearer, so each is an auth failure that
// never reaches the database.
func TestExternalIngestAuthLimiterVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:      root,
		JWTKey:    "venue-oracle-jwt-signing-key-32-bytes-min",
		PythonEnv: []string{"TRUSTED_PROXIES=testclient"},
	})
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	replica := func() string {
		pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		client, err := valkey.Open(ctx, valkey.DefaultConfig(venue.ValkeyURI))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(client.Close)
		server, err := apiservice.NewServer(config.Config{APIAddress: "127.0.0.1:0"}, logger, apiservice.Routes(apiservice.Deps{Pool: pool, Valkey: client}, logger))
		if err != nil {
			t.Fatal(err)
		}
		if err := server.Start(ctx); err != nil {
			t.Fatalf("start server: %v (api role gaps: %s)", err, venue.DiagnoseAPIRole(t, ctx))
		}
		t.Cleanup(func() { _ = server.Shutdown(ctx) })
		return "http://" + server.Address()
	}
	balanced := roundRobinBackends(t, replica(), replica())

	burst := func(name string, count int, address string) []venueoracle.Request {
		out := make([]venueoracle.Request, count)
		for index := range out {
			out[index] = venueoracle.Request{
				Name: fmt.Sprintf("%s #%03d", name, index+1), Method: "GET", Path: "/api/v1/external-ingest/batches",
				Headers: map[string]string{"X-Forwarded-For": address},
			}
		}
		return out
	}
	var requests []venueoracle.Request
	// 105 attempts with no credentials from one address: 30 answered 401, then
	// the failed-attempts 429 up to the hundredth attempt, then the attempts 429.
	requests = append(requests, burst("no credentials, one address", 105, "10.201.0.1")...)
	// Another address has its own budget on both throttles.
	requests = append(requests, burst("no credentials, other address", 3, "10.201.0.2")...)

	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, balanced, requests, python, venueoracle.DiffOptions{})
	t.Logf("%d requests compared\n%s", len(requests), receipt)

	// A burst of simultaneous credential-less requests from one address: auth.py
	// runs test() then hit() back to back with no await for such a request, so
	// its failure counter is atomic per request and exactly 30 of them are
	// answered 401 whatever the arrival order; the two Go replicas, sharing one
	// store, must admit exactly as many. The comparison is the histogram of
	// (status, body) -- the order of simultaneous requests is not defined.
	const simultaneous = 90
	pythonBurst := venue.ServePython(t, burst("simultaneous", simultaneous, "10.201.0.9"))
	want := map[string]int{}
	for _, response := range pythonBurst {
		want[fmt.Sprintf("%d %s", response.Status, response.Body)]++
	}
	got := map[string]int{}
	var mu sync.Mutex
	var group sync.WaitGroup
	start := make(chan struct{})
	for range simultaneous {
		group.Add(1)
		go func() {
			defer group.Done()
			request, err := http.NewRequest(http.MethodGet, balanced+"/api/v1/external-ingest/batches", nil)
			if err != nil {
				return
			}
			request.Header.Set("X-Forwarded-For", "10.201.0.9")
			<-start
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				mu.Lock()
				got["transport error "+err.Error()]++
				mu.Unlock()
				return
			}
			body, _ := io.ReadAll(response.Body)
			_ = response.Body.Close()
			mu.Lock()
			got[fmt.Sprintf("%d %s", response.StatusCode, strings.TrimSpace(string(body)))]++
			mu.Unlock()
		}()
	}
	close(start)
	group.Wait()
	for key, count := range want {
		if got[key] != count {
			t.Errorf("simultaneous burst: python answered %q x%d, go x%d\n  python histogram %v\n  go histogram     %v", key, count, got[key], want, got)
		}
	}
	for key := range got {
		if _, ok := want[key]; !ok {
			t.Errorf("simultaneous burst: go answered %q x%d, python never", key, got[key])
		}
	}
}
