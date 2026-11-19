package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

type clientCancelLogRecord struct {
	level slog.Level
	msg   string
	attrs map[string]string
}

type clientCancelLogHandler struct {
	mu      *sync.Mutex
	records *[]clientCancelLogRecord
}

func (clientCancelLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h clientCancelLogHandler) Handle(_ context.Context, record slog.Record) error {
	attrs := map[string]string{}
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.String()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.records = append(*h.records, clientCancelLogRecord{level: record.Level, msg: record.Message, attrs: attrs})
	return nil
}

func (h clientCancelLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h clientCancelLogHandler) WithGroup(string) slog.Handler      { return h }

// captureClientCancelLogs routes slog and the log package (slog.SetDefault
// redirects it) into one list for the test.
func captureClientCancelLogs(t *testing.T) func() []clientCancelLogRecord {
	t.Helper()
	var mu sync.Mutex
	var records []clientCancelLogRecord
	previous := slog.Default()
	slog.SetDefault(slog.New(clientCancelLogHandler{mu: &mu, records: &records}))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return func() []clientCancelLogRecord {
		mu.Lock()
		defer mu.Unlock()
		return append([]clientCancelLogRecord(nil), records...)
	}
}

func cancelledByClientError() error {
	return fmt.Errorf("query work unit investments: ClickHouse query failed: %w", &clickhousedriver.Exception{
		Code: 735, Name: "QUERY_WAS_CANCELLED_BY_CLIENT", Message: "Query was cancelled",
	})
}

type clientCancelRequestContext struct {
	name       string
	make       func(t *testing.T) context.Context
	wantCancel bool
}

func clientCancelRequestContexts() []clientCancelRequestContext {
	return []clientCancelRequestContext{
		{"the client closed the request", func(*testing.T) context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx
		}, true},
		{"the request is alive", func(*testing.T) context.Context { return context.Background() }, false},
		{"the deadline of the request passed", func(t *testing.T) context.Context {
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			t.Cleanup(cancel)
			return ctx
		}, false},
	}
}

func countClientCancelLines(records []clientCancelLogRecord, phase string) (cancelLines int, wrongLevel bool) {
	for _, record := range records {
		if record.msg == "analytics: query cancelled by the client" && record.attrs["phase"] == phase {
			cancelLines++
			wrongLevel = wrongLevel || record.level != slog.LevelInfo
		}
	}
	return cancelLines, wrongLevel
}

func countLinesContaining(records []clientCancelLogRecord, part string) int {
	count := 0
	for _, record := range records {
		if strings.Contains(record.msg, part) {
			count++
		}
	}
	return count
}

// cancelledWorkUnitsClient fails every statement with the error ClickHouse
// gives a statement the client's cancel ended.
type cancelledWorkUnitsClient struct{}

func (cancelledWorkUnitsClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return nil, cancelledByClientError()
}

// A /api/v1/work-units read that the client's cancel ended is a cancel: one
// INFO cancel line of the work_units phase and no "degraded to 503" line. The
// same error on a live request, or on one whose deadline passed, stays the
// degradation it was.
func TestAWorkUnitsReadTheClientCancelledIsNotADegradation(t *testing.T) {
	reader, err := investmentexplain.NewReader(cancelledWorkUnitsClient{})
	if err != nil {
		t.Fatalf("investmentexplain.NewReader: %v", err)
	}
	routes := []struct {
		name    string
		handler http.HandlerFunc
		request func() *http.Request
	}{
		{"GET", newWorkUnitsGetHandler(reader), func() *http.Request {
			return httptest.NewRequest(http.MethodGet, "/api/v1/work-units", nil)
		}},
		{"POST", newWorkUnitsPostHandler(reader), func() *http.Request {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/work-units", strings.NewReader(`{"filters":{}}`))
			req.Header.Set("Content-Type", "application/json")
			return req
		}},
	}
	for _, route := range routes {
		for _, request := range clientCancelRequestContexts() {
			t.Run(route.name+"/"+request.name, func(t *testing.T) {
				logs := captureClientCancelLogs(t)
				req := route.request()
				ctx := authctx.WithClaims(request.make(t), authctx.Claims{OrgID: "org-1", Scope: pepgate.Legacy("org-1")})
				rec := httptest.NewRecorder()
				route.handler(rec, req.WithContext(ctx))

				records := logs()
				cancelLines, wrongLevel := countClientCancelLines(records, "work_units")
				degradedLines := countLinesContaining(records, "work_units: degraded to 503")
				if wrongLevel {
					t.Errorf("the cancel line is not at INFO")
				}
				if rec.Code != http.StatusServiceUnavailable || rec.Body.String() != `{"detail":"Data unavailable"}` {
					t.Errorf("response = %d %q, want 503 {\"detail\":\"Data unavailable\"}", rec.Code, rec.Body.String())
				}
				wantCancel, wantDegraded := 0, 1
				if request.wantCancel {
					wantCancel, wantDegraded = 1, 0
				}
				if cancelLines != wantCancel || degradedLines != wantDegraded {
					t.Errorf("cancel lines = %d, degraded lines = %d; want %d and %d (logs %+v)", cancelLines, degradedLines, wantCancel, wantDegraded, records)
				}
			})
		}
	}
}

// A resolver error that the client's cancel caused writes no unwrap chain; the
// presenter reports the cancel under its label when no analytics phase did.
// Any other resolver error, also one on a request the client has left since,
// keeps its chain and is no cancel.
func TestTheGraphQLPresenterWritesNoChainForAClientCancel(t *testing.T) {
	present := safeGraphQLErrorPresenter("test", nil)
	for _, test := range []struct {
		name string
		err  error
		// wantChain on the closed request; a live or expired request always logs it.
		wantChain bool
	}{
		{"ClickHouse: cancelled by the client (735)", fmt.Errorf("analytics: breakdown[0]: %w", cancelledByClientError()), false},
		{"the context's own cancel error", fmt.Errorf("analytics: breakdown[0]: rows: %w", context.Canceled), false},
		{"ClickHouse: unknown table (60)", fmt.Errorf("analytics: breakdown[0]: %w", &clickhousedriver.Exception{Code: 60, Name: "UNKNOWN_TABLE"}), true},
		{"another error", errors.New(driverText), true},
	} {
		for _, request := range clientCancelRequestContexts() {
			t.Run(test.name+"/"+request.name, func(t *testing.T) {
				logs := captureClientCancelLogs(t)
				presented := present(request.make(t), test.err)
				if presented.Message != graphQLInternalMessage {
					t.Errorf("message = %q, want the internal message", presented.Message)
				}
				records := logs()
				chainLines := countLinesContaining(records, "query-api: test resolver error")
				cancelLines, wrongLevel := countClientCancelLines(records, "graphql_test")
				wantChain, wantCancel := 1, 0
				if request.wantCancel && !test.wantChain {
					wantChain, wantCancel = 0, 1
				}
				if chainLines != wantChain || cancelLines != wantCancel || wrongLevel {
					t.Errorf("chain lines = %d, cancel lines = %d (wrong level %v); want %d and %d", chainLines, cancelLines, wrongLevel, wantChain, wantCancel)
				}
			})
		}
	}
}

// readFunc is a ClickHouse client made of one function.
type readFunc func(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)

func (f readFunc) Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return f(ctx, statement, bindings)
}

func clientCancelledCount(t *testing.T) int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := testMeterReader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "devhealth_query_api_analytics_client_cancelled_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("counter data shape = %+v", m.Data)
			}
			for _, point := range sum.DataPoints {
				total += point.Value
			}
		}
	}
	return total
}

// Through the served GraphQL route: each analytics read that the client's
// cancel ends (the client leaves while the statement runs) is ONE cancel
// report (an INFO line and a count) and no resolver chain, whichever layer
// reports it; two cancelled statements of one batch are two reports. A
// statement that fails for its own reason on a live request keeps its chain.
func TestAGraphQLAnalyticsReadTheClientCancelledIsOneCancelReport(t *testing.T) {
	dates := map[string]any{"startDate": "2026-07-01", "endDate": "2026-07-04"}
	breakdowns := []any{map[string]any{"dimension": "REPO", "measure": "COUNT", "topN": 10, "dateRange": dates}}
	timeseries := []any{map[string]any{"dimension": "REPO", "measure": "COUNT", "interval": "DAY", "dateRange": dates}}
	withSankey := sankeyBatch([]string{"TEAM", "REPO"}, "2026-07-01", "2026-07-04")
	quality := map[string]any{"useInvestment": true, "breakdowns": breakdowns, "evidenceQualityGroupBy": "THEME"}
	cases := []struct {
		name, selection string
		batch           map[string]any
		// statements holds a part of each statement the client's cancel ends.
		statements []string
		// reports is the cancel reports wanted, one per statement read.
		reports int
	}{
		{"repository names of the filter", `breakdowns { dimension }`, map[string]any{
			"breakdowns": breakdowns, "filters": map[string]any{"what": map[string]any{"repos": []string{"acme/repo"}}},
		}, []string{"lower(repo) IN"}, 1},
		{"evidence quality stats", `evidenceQualityStats { total }`, quality, []string{"quality_known_count"}, 1},
		{"evidence quality by group", `evidenceQualityByGroup { key }`, quality, []string{"evidence_quality_group_key"}, 1},
		{"team ownership", `filterEmptyReason`, map[string]any{
			"useInvestment": true, "breakdowns": breakdowns,
			"filters": map[string]any{"scope": map[string]any{"level": "TEAM", "ids": []string{"team-a"}}},
		}, []string{"AS owned_repositories"}, 1},
		{"timeseries", `timeseries { dimension }`, map[string]any{"timeseries": timeseries}, []string{"AS dimension_value"}, 1},
		{"breakdown", `breakdowns { dimension }`, map[string]any{"breakdowns": breakdowns}, []string{"LIMIT {top_n:UInt32}"}, 1},
		{"sankey", `sankey { nodes { id } }`, withSankey, []string{"AS grouping_set,"}, 1},
		{"sankey and its coverage", `sankey { nodes { id } coverage { teamCoverage } }`, withSankey, []string{"AS grouping_set,", "AS assigned_team"}, 2},
		{"timeseries and breakdown of one batch", `timeseries { dimension } breakdowns { dimension }`,
			map[string]any{"timeseries": timeseries, "breakdowns": breakdowns}, []string{"AS dimension_value"}, 2},
	}
	run := func(t *testing.T, selection string, batch map[string]any, client readFunc, ctx context.Context) string {
		t.Helper()
		body, err := json.Marshal(map[string]any{
			"query":     `query Q($orgId: String!, $batch: AnalyticsRequestInput!) { analytics(orgId: $orgId, batch: $batch) { ` + selection + ` } }`,
			"variables": map[string]any{"orgId": "org-1", "batch": batch},
		})
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(string(body)))
		request.Header.Set("Content-Type", "application/json")
		ctx = authctx.WithClaims(ctx, authctx.Claims{OrgID: "org-1", Scope: pepgate.Legacy("org-1")})
		rec := httptest.NewRecorder()
		newGraphQLServer(&graph.Resolver{ClickHouse: client}).ServeHTTP(rec, request.WithContext(ctx))
		return rec.Body.String()
	}
	for _, cell := range cases {
		t.Run(cell.name, func(t *testing.T) {
			logs := captureClientCancelLogs(t)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			var reads atomic.Int64
			client := readFunc(func(statementCtx context.Context, statement string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
				for _, part := range cell.statements {
					if strings.Contains(statement, part) {
						reads.Add(1)
						cancel()
						return nil, fmt.Errorf("ClickHouse query failed: %w", statementCtx.Err())
					}
				}
				return &oneRow{}, nil
			})
			before := clientCancelledCount(t)
			body := run(t, cell.selection, cell.batch, client, ctx)
			records := logs()
			if int(reads.Load()) != cell.reports || !strings.Contains(body, graphQLInternalMessage) {
				t.Fatalf("cancelled statements = %d (want %d), body %s: the cell did not reach its statements and the presenter", reads.Load(), cell.reports, body)
			}
			var lines int
			for _, record := range records {
				if record.msg == "analytics: query cancelled by the client" {
					lines++
				}
			}
			chains := countLinesContaining(records, "query-api: query resolver error")
			if delta := clientCancelledCount(t) - before; lines != cell.reports || delta != int64(cell.reports) || chains != 0 {
				t.Errorf("cancel lines = %d, count moved by %d, resolver chains = %d; want %d, %d and 0 (logs %+v)", lines, delta, chains, cell.reports, cell.reports, records)
			}
		})
	}
	t.Run("a statement that fails for its own reason on a live request", func(t *testing.T) {
		logs := captureClientCancelLogs(t)
		client := readFunc(func(_ context.Context, statement string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
			if strings.Contains(statement, "LIMIT {top_n:UInt32}") {
				return nil, fmt.Errorf("ClickHouse query failed: %w", &clickhousedriver.Exception{Code: 60, Name: "UNKNOWN_TABLE"})
			}
			return &oneRow{}, nil
		})
		before := clientCancelledCount(t)
		run(t, `breakdowns { dimension }`, map[string]any{"breakdowns": breakdowns}, client, context.Background())
		records := logs()
		chains := countLinesContaining(records, "query-api: query resolver error")
		cancels := countLinesContaining(records, "analytics: query cancelled by the client")
		if delta := clientCancelledCount(t) - before; chains != 1 || cancels != 0 || delta != 0 {
			t.Errorf("resolver chains = %d, cancel lines = %d, count moved by %d; want 1, 0 and 0", chains, cancels, delta)
		}
	})
}
