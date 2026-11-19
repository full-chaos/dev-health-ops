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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/reports"
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

// A resolver error that the client's cancel caused writes no unwrap chain. One
// that no analytics phase reported gets ONE presenter line of its own kind and
// no analytics cancel line. Any other resolver error, also one on a request
// the client has left since, keeps its chain and is no cancel.
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
				presenterLines := countPresenterCancelLines(records, "test")
				analyticsLines := countLinesContaining(records, "analytics: query cancelled by the client")
				wantChain, wantPresenter := 1, 0
				if request.wantCancel && !test.wantChain {
					wantChain, wantPresenter = 0, 1
				}
				if chainLines != wantChain || presenterLines != wantPresenter || analyticsLines != 0 {
					t.Errorf("chain lines = %d, presenter cancel lines = %d, analytics cancel lines = %d; want %d, %d and 0", chainLines, presenterLines, analyticsLines, wantChain, wantPresenter)
				}
			})
		}
	}
}

// countPresenterCancelLines counts the INFO lines the GraphQL presenter writes
// for a cancel no analytics phase reported.
func countPresenterCancelLines(records []clientCancelLogRecord, presenter string) int {
	count := 0
	for _, record := range records {
		if record.msg == "query-api: resolver ended by the client's cancel" && record.attrs["presenter"] == presenter && record.level == slog.LevelInfo {
			count++
		}
	}
	return count
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
	run := func(t *testing.T, selection string, batch map[string]any, client analytics.QueryClient, ctx context.Context) string {
		t.Helper()
		return serveClientCancelGraphQL(t, ctx, &graph.Resolver{ClickHouse: client},
			`query Q($orgId: String!, $batch: AnalyticsRequestInput!) { analytics(orgId: $orgId, batch: $batch) { `+selection+` } }`,
			map[string]any{"orgId": "org-1", "batch": batch})
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
			presenter := countPresenterCancelLines(records, "query")
			if delta := clientCancelledCount(t) - before; lines != cell.reports || delta != int64(cell.reports) || chains != 0 || presenter != 0 {
				t.Errorf("cancel lines = %d, count moved by %d, resolver chains = %d, presenter lines = %d; want %d, %d, 0 and 0 (logs %+v)", lines, delta, chains, presenter, cell.reports, cell.reports, records)
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

func serveClientCancelGraphQL(t *testing.T, ctx context.Context, resolver *graph.Resolver, document string, variables map[string]any) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{"query": document, "variables": variables})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	ctx = authctx.WithClaims(ctx, authctx.Claims{OrgID: "org-1", Scope: pepgate.Legacy("org-1")})
	rec := httptest.NewRecorder()
	newGraphQLServer(resolver).ServeHTTP(rec, request.WithContext(ctx))
	return rec.Body.String()
}

// One request shares ONE membership-scope lookup between its phases
// (analytics.WithInvestmentMembershipScopeRequest). When the client's cancel
// ends that lookup, every phase gets its error, but it is one read: exactly
// one cancel report and no resolver chain, whichever phases read it.
func TestASharedMembershipLookupTheClientCancelledIsOneCancelReport(t *testing.T) {
	dates := map[string]any{"startDate": "2026-07-01", "endDate": "2026-07-04"}
	breakdown := map[string]any{"dimension": "THEME", "measure": "COUNT", "topN": 10, "dateRange": dates}
	series := map[string]any{"dimension": "THEME", "measure": "COUNT", "interval": "DAY", "dateRange": dates}
	withSankey := sankeyBatch([]string{"THEME", "SUBCATEGORY"}, "2026-07-01", "2026-07-04")
	for _, cell := range []struct {
		name, selection string
		batch           map[string]any
	}{
		{"timeseries and breakdown", `timeseries { dimension } breakdowns { dimension }`,
			map[string]any{"useInvestment": true, "timeseries": []any{series}, "breakdowns": []any{breakdown}}},
		{"two breakdowns", `breakdowns { dimension }`, map[string]any{"useInvestment": true, "breakdowns": []any{breakdown, breakdown}}},
		{"sankey and its coverage", `sankey { nodes { id } coverage { teamCoverage } }`, withSankey},
	} {
		t.Run(cell.name, func(t *testing.T) {
			logs := captureClientCancelLogs(t)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			var lookups, scopedReads atomic.Int64
			client := analytics.PinInvestmentMembershipScope(readFunc(func(statementCtx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
				if strings.Contains(statement, "SELECT scope_mode, lag_seconds, latest_run_id") {
					lookups.Add(1)
					cancel()
					return nil, fmt.Errorf("ClickHouse query failed: %w", statementCtx.Err())
				}
				for _, binding := range bindings {
					if binding.Name == "investment_scope_run_id" {
						scopedReads.Add(1)
					}
				}
				return &oneRow{}, nil
			}))
			before := clientCancelledCount(t)
			body := serveClientCancelGraphQL(t, analytics.WithInvestmentMembershipScopeRequest(ctx), &graph.Resolver{ClickHouse: client},
				`query Q($orgId: String!, $batch: AnalyticsRequestInput!) { analytics(orgId: $orgId, batch: $batch) { `+cell.selection+` } }`,
				map[string]any{"orgId": "org-1", "batch": cell.batch})
			records := logs()
			if lookups.Load() != 1 || scopedReads.Load() != 0 || !strings.Contains(body, graphQLInternalMessage) {
				t.Fatalf("lookups = %d, scoped reads = %d, body %s: the cell did not reach one cancelled lookup and the presenter", lookups.Load(), scopedReads.Load(), body)
			}
			var phases []string
			for _, record := range records {
				if record.msg == "analytics: query cancelled by the client" {
					phases = append(phases, record.attrs["phase"])
				}
			}
			chains := countLinesContaining(records, "query-api: query resolver error")
			presenter := countPresenterCancelLines(records, "query")
			if delta := clientCancelledCount(t) - before; len(phases) != 1 || delta != 1 || chains != 0 || presenter != 0 {
				t.Errorf("cancel lines %v, count moved by %d, resolver chains = %d, presenter lines = %d; want 1 line, 1, 0 and 0", phases, delta, chains, presenter)
			}
		})
	}
}

type cancelledCommitBegin func(context.Context) (pgx.Tx, error)

func (f cancelledCommitBegin) Begin(ctx context.Context) (pgx.Tx, error) { return f(ctx) }

// cancelledCommitTx deletes the row, then the client leaves during the commit.
type cancelledCommitTx struct {
	pgx.Tx
	cancel           context.CancelFunc
	deletes, commits int
}

func (tx *cancelledCommitTx) Exec(_ context.Context, statement string, _ ...any) (pgconn.CommandTag, error) {
	if !strings.HasPrefix(statement, "DELETE FROM saved_reports") {
		return pgconn.CommandTag{}, fmt.Errorf("unexpected statement %q", statement)
	}
	tx.deletes++
	return pgconn.NewCommandTag("DELETE 1"), nil
}

func (tx *cancelledCommitTx) Commit(ctx context.Context) error {
	tx.commits++
	tx.cancel()
	return ctx.Err()
}

func (tx *cancelledCommitTx) Rollback(context.Context) error { return nil }

// A mutation whose Postgres commit the client's cancel ended is not an
// analytics read: one presenter line of kind "mutation", no resolver chain,
// no analytics cancel line and no move of the analytics read counter.
func TestACancelledMutationCommitIsNotAnAnalyticsCancel(t *testing.T) {
	logs := captureClientCancelLogs(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tx := &cancelledCommitTx{cancel: cancel}
	writer := &reports.Writer{Pool: cancelledCommitBegin(func(context.Context) (pgx.Tx, error) { return tx, nil })}
	before := clientCancelledCount(t)
	body := serveClientCancelGraphQL(t, ctx, &graph.Resolver{ReportWriter: writer},
		`mutation D { deleteSavedReport(orgId: "org-1", reportId: "13a0c122-d750-425d-9e59-0b5b7c1c383f") }`, nil)
	records := logs()
	if tx.deletes != 1 || tx.commits != 1 || !strings.Contains(body, graphQLInternalMessage) {
		t.Fatalf("deletes = %d, commits = %d, body %s: the cell did not reach the commit and the presenter", tx.deletes, tx.commits, body)
	}
	kinds := 0
	for _, record := range records {
		if record.msg == "query-api: resolver ended by the client's cancel" && record.attrs["presenter"] == "query" && record.attrs["kind"] == "mutation" {
			kinds++
		}
	}
	analyticsLines := countLinesContaining(records, "analytics: query cancelled by the client")
	chains := countLinesContaining(records, "query-api: query resolver error")
	if delta := clientCancelledCount(t) - before; kinds != 1 || analyticsLines != 0 || chains != 0 || delta != 0 {
		t.Errorf("mutation cancel lines = %d, analytics cancel lines = %d, resolver chains = %d, count moved by %d; want 1, 0, 0 and 0 (logs %+v)", kinds, analyticsLines, chains, delta, records)
	}
}
