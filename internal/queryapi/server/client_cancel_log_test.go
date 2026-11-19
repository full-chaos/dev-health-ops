package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
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
				if request.wantCancel {
					if cancelLines != 1 || degradedLines != 0 {
						t.Errorf("cancel lines = %d, degraded lines = %d; want 1 and 0 (logs %+v)", cancelLines, degradedLines, records)
					}
					return
				}
				if cancelLines != 0 || degradedLines != 1 {
					t.Errorf("cancel lines = %d, degraded lines = %d; want 0 and 1 (logs %+v)", cancelLines, degradedLines, records)
				}
				if rec.Code != http.StatusServiceUnavailable {
					t.Errorf("status = %d, want 503", rec.Code)
				}
			})
		}
	}
}

// A resolver error that the client's cancel caused writes no unwrap chain:
// the read that ended reports the cancel in its own phase. Any other resolver
// error, also one on a request the client has left since, keeps its chain.
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
				chainLines := countLinesContaining(logs(), "query-api: test resolver error")
				want := 1
				if request.wantCancel && !test.wantChain {
					want = 0
				}
				if chainLines != want {
					t.Errorf("chain lines = %d, want %d", chainLines, want)
				}
			})
		}
	}
}
