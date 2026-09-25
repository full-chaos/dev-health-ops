//go:build integration

package externalingestvenue

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestPagerDutyWebhookLimiterVenueOracle is CHAOS-6508's proof: the PagerDuty
// webhook's 60/minute limit is slowapi's, a fixed window per (client IP, exact
// request path) in the storage every api replica shares, and the real Python
// api and the real Go api must answer the same bursts the same way. The Go
// bucket used to be one in-process counter per IP across every binding, so
// the sixty-first delivery to ONE binding was refused by Python and, with the
// deliveries spread over bindings, refused by Go where Python answered.
//
// Every request carries its own X-Forwarded-For (each plane trusts its own
// peer: the in-process Python client is "testclient", the Go server's is
// 127.0.0.1), so the caller's address is the header's. No delivery carries a
// subscription header: each answers the same non-429 from the endpoint after
// its limit check, on both planes.
func TestPagerDutyWebhookLimiterVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:      root,
		JWTKey:    "venue-oracle-jwt-signing-key-32-bytes-min",
		PythonEnv: []string{"TRUSTED_PROXIES=testclient"},
	})
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1")
	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	goValkey, err := valkey.Open(ctx, valkey.DefaultConfig(venue.ValkeyURI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(goValkey.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := apiservice.NewServer(config.Config{APIAddress: "127.0.0.1:0"}, logger, apiservice.Routes(apiservice.Deps{Pool: pool, Valkey: goValkey}, logger))
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(ctx); err != nil {
		t.Fatalf("start server: %v (api role gaps: %s)", err, venue.DiagnoseAPIRole(t, ctx))
	}
	t.Cleanup(func() { _ = server.Shutdown(ctx) })
	goBase := "http://" + server.Address()

	burst := func(name string, count int, path func(int) string, forwarded func(int) string) []venueoracle.Request {
		out := make([]venueoracle.Request, count)
		for index := range out {
			out[index] = venueoracle.Request{
				Name: fmt.Sprintf("%s #%03d", name, index+1), Method: "POST", Path: path(index),
				Headers: map[string]string{"X-Forwarded-For": forwarded(index)},
			}
		}
		return out
	}
	binding := func(id string) string { return "/api/v1/webhooks/pagerduty/" + id }
	fixed := func(address string) func(int) string { return func(int) string { return address } }
	distinct := func(index int) string { return fmt.Sprintf("10.9.%d.%d", index/256%256, index%256) }
	bindingA, bindingB, bindingC := uuid.NewString(), uuid.NewString(), uuid.NewString()

	var requests []venueoracle.Request
	// 65 deliveries to ONE binding from one address: 60 answered, then 429.
	requests = append(requests, burst("one binding, one address", 65, func(int) string { return binding(bindingA) }, fixed("10.200.0.1"))...)
	// Another binding from the same address has its own budget.
	requests = append(requests, burst("other binding, same address", 3, func(int) string { return binding(bindingB) }, fixed("10.200.0.1"))...)
	// The first binding from another address has its own budget too.
	requests = append(requests, burst("first binding, other address", 3, func(int) string { return binding(bindingA) }, fixed("10.200.0.2"))...)
	// 65 addresses, one delivery each to one binding: none is limited.
	requests = append(requests, burst("one binding, distinct addresses", 65, func(int) string { return binding(bindingC) }, distinct)...)
	// 65 DISTINCT bindings from one address: each has its own budget, none is limited.
	requests = append(requests, burst("distinct bindings, one address", 65, func(int) string { return binding(uuid.NewString()) }, fixed("10.200.0.3"))...)

	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{})
	t.Logf("%d requests compared\n%s", len(requests), receipt)
}
