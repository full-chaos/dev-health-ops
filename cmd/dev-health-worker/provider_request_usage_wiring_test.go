package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

func TestProviderSyncExecutorsGetTheProcessRequestUsageWriter(t *testing.T) {
	t.Parallel()
	runtimeConfig, err := githubWorkItemsRuntimeConfigFrom(validGitHubWorkItemsRuntimeConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	claim := providerSyncOwnershipMetricsClaim(t, "github")
	session := &providersync.LeaseSession{
		Repository: ownershipMetricsWiringLeaseRepository{}, Claim: claim,
		LeaseDuration: time.Minute, Deadline: time.Now().Add(time.Hour),
	}
	handler, _ := buildProviderSyncHandlerWithGitHubWorkItemsRuntimeConfig(
		nil, nil, &ownershipMetricsWiringConn{}, nil, nil, nil, nil, slog.Default(), runtimeConfig,
	)
	writer := providersync.NewRequestUsageWriter(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	withRequestUsage(handler, writer)
	executor, err := handler.BuildExecutor(session)
	if err != nil || executor.RequestUsage != writer {
		t.Fatalf("err=%v RequestUsage=%p want %p", err, executor.RequestUsage, writer)
	}
}

// The writer's pool is its own single connection: a slow usage insert can
// hold at most that one, never one the unit's effect writes use.
func TestRequestUsageClickHousePoolIsItsOwnSingleConnection(t *testing.T) {
	t.Parallel()
	shared := clickhousestore.DefaultConfig("clickhouse://ABC-123@localhost:9000/default")
	usage := requestUsageClickHouseConfig("clickhouse://ABC-123@localhost:9000/default")
	if usage.MaxOpenConns != 1 || usage.MaxIdleConns != 1 || shared.MaxOpenConns <= 1 {
		t.Fatalf("usage pool=%d/%d shared pool=%d", usage.MaxOpenConns, usage.MaxIdleConns, shared.MaxOpenConns)
	}
	if err := usage.Validate(); err != nil {
		t.Fatalf("usage pool config invalid: %v", err)
	}
}

func TestPagerDutyOAuthDoerCountsRefreshesAsExecutionSpend(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/redirect" {
			http.Redirect(writer, request, "/token", http.StatusFound)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	doer := pagerDutyOAuthDoer()
	ledger := providersync.NewRequestLedger()
	ctx := providersync.WithRequestLedger(context.Background(), ledger)
	for _, path := range []string{"/token", "/redirect"} {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := doer.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if path == "/redirect" && response.StatusCode != http.StatusFound {
			t.Fatalf("redirect was followed: status %d", response.StatusCode)
		}
	}
	if got := ledger.Totals(); got != (providersync.RequestUsageTotals{Requests: 2, Responses: 2}) {
		t.Fatalf("totals=%+v", got)
	}
}
