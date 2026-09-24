package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/explain"
)

// emptyRowsExplainClient answers every ClickHouse call with zero rows --
// same convention as drilldown_prs_route_test.go's emptyRowsDrilldownClient.
type emptyRowsExplainClient struct{}

func (emptyRowsExplainClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return emptyRowsExplainScanner{}, nil
}

type emptyRowsExplainScanner struct{}

func (emptyRowsExplainScanner) Next() bool        { return false }
func (emptyRowsExplainScanner) Scan(...any) error { return nil }
func (emptyRowsExplainScanner) Err() error        { return nil }
func (emptyRowsExplainScanner) Close() error      { return nil }

func newEmptyRowsExplainReader(t *testing.T) *explain.Reader {
	t.Helper()
	reader, err := explain.NewReader(emptyRowsExplainClient{})
	if err != nil {
		t.Fatalf("explain.NewReader: %v", err)
	}
	return reader
}

func TestExplainSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := explainSwitchFromEnv(os.Getenv)
	if sw.Enabled(explainGetOperation) || sw.Enabled(explainPostOperation) {
		t.Fatal("expected both explain operations disabled with no env var set")
	}
}

func TestExplainSwitchFromEnvEnablesBothOperations(t *testing.T) {
	t.Setenv(explainEnabledEnvVar, "true")
	sw := explainSwitchFromEnv(os.Getenv)
	if !sw.Enabled(explainGetOperation) {
		t.Fatal("expected explainGetOperation enabled with GO_API_EXPLAIN_ENABLED=true")
	}
	if !sw.Enabled(explainPostOperation) {
		t.Fatal("expected explainPostOperation enabled with GO_API_EXPLAIN_ENABLED=true")
	}
}

// TestNewExplainGetHandlerRequiresAuthContext pins that the work handler
// (reached only after buildExplainRoute's entryHandler already
// authenticated) itself still refuses a request with no claims attached,
// same defensive check every sibling REST route's work handler makes --
// and that this route's OWN 401 envelope ({"detail":{"message":...}},
// not plain text) applies here too.
func TestNewExplainGetHandlerRequiresAuthContext(t *testing.T) {
	handler := newExplainGetHandler(newEmptyRowsExplainReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explain?metric=cycle_time", nil)
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var body restErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
}

// TestNewExplainGetHandlerHappyPathSetsDeprecatedHeader pins the GET
// route's Content-Type, its Python-parity X-DevHealth-Deprecated
// response header (main.py:562-563), and that drivers/contributors are
// [] (present), never omitted/null, for a supported, empty-data request.
func TestNewExplainGetHandlerHappyPathSetsDeprecatedHeader(t *testing.T) {
	handler := newExplainGetHandler(newEmptyRowsExplainReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explain?metric=cycle_time&scope_type=repo&scope_id=repo-a&range_days=7", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	if got := rec.Header().Get("X-DevHealth-Deprecated"); got != "use POST with filters" {
		t.Fatalf("X-DevHealth-Deprecated = %q, want %q", got, "use POST with filters")
	}
	var decoded struct {
		Metric       string           `json:"metric"`
		Drivers      []map[string]any `json:"drivers"`
		Contributors []map[string]any `json:"contributors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if decoded.Metric != "cycle_time" {
		t.Fatalf("metric = %q, want cycle_time", decoded.Metric)
	}
	if decoded.Drivers == nil {
		t.Fatal("drivers must be [] (present), not omitted/null")
	}
	if decoded.Contributors == nil {
		t.Fatal("contributors must be [] (present), not omitted/null")
	}
}

// TestNewExplainPostHandlerRequiresAuthContext mirrors the GET handler's
// own auth-context guard.
func TestNewExplainPostHandlerRequiresAuthContext(t *testing.T) {
	handler := newExplainPostHandler(newEmptyRowsExplainReader(t))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/explain", bytes.NewReader([]byte(`{"metric":"cycle_time","filters":{}}`)))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestNewExplainPostHandlerHappyPathNoDeprecatedHeader pins that POST
// never sets the GET-only deprecation header, and echoes back an unknown
// metric string verbatim while still answering 200 (api/services/
// explain.py:146's own cycle_time fallback -- unknown metric is not a
// validation error).
func TestNewExplainPostHandlerHappyPathNoDeprecatedHeader(t *testing.T) {
	handler := newExplainPostHandler(newEmptyRowsExplainReader(t))
	body := `{"metric":"totally_bogus","filters":{"scope":{"level":"org"},"time":{"range_days":7}}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/explain", bytes.NewReader([]byte(body)))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get("X-DevHealth-Deprecated"); got != "" {
		t.Fatalf("X-DevHealth-Deprecated = %q, want unset on POST", got)
	}
	var decoded struct {
		Metric string `json:"metric"`
		Label  string `json:"label"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if decoded.Metric != "totally_bogus" || decoded.Label != "Cycle Time" {
		t.Fatalf("decoded = %+v, want metric=totally_bogus label=Cycle Time", decoded)
	}
}

// TestExplainErrorEnvelopesMatchPython pins the three non-2xx envelope
// shapes this route answers with (401/503/405) against bytes captured
// live from FastAPI/Starlette's own default handlers (see this file's
// own package doc comment in explain_route.go for the capture method) --
// through this binary's one shared REST error-response path
// (rest_error_response.go), the same helper every sibling REST route
// uses; this route keeps no local copy of it.
func TestExplainErrorEnvelopesMatchPython(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explain", nil)

	t.Run("401 not authenticated", func(t *testing.T) {
		rec := httptest.NewRecorder()
		writeRESTUnauthorized(rec, req, "explain", "Not authenticated")
		assertExplainJSONBody(t, rec, http.StatusUnauthorized, `{"detail":{"message":"Not authenticated"}}`)
		if got := rec.Header().Get("WWW-Authenticate"); got != "Bearer" {
			t.Fatalf("WWW-Authenticate = %q, want Bearer", got)
		}
	})
	t.Run("401 invalid authorization header", func(t *testing.T) {
		rec := httptest.NewRecorder()
		writeRESTUnauthorized(rec, req, "explain", "Invalid authorization header")
		assertExplainJSONBody(t, rec, http.StatusUnauthorized, `{"detail":{"message":"Invalid authorization header"}}`)
	})
	t.Run("401 invalid or expired token", func(t *testing.T) {
		rec := httptest.NewRecorder()
		writeRESTUnauthorized(rec, req, "explain", "Invalid or expired token")
		assertExplainJSONBody(t, rec, http.StatusUnauthorized, `{"detail":{"message":"Invalid or expired token"}}`)
	})
	t.Run("503 data unavailable", func(t *testing.T) {
		rec := httptest.NewRecorder()
		writeRESTDataUnavailable(rec, req, "explain", "org-1", errors.New("simulated downstream failure"))
		assertExplainJSONBody(t, rec, http.StatusServiceUnavailable, `{"detail":"Data unavailable"}`)
	})
	t.Run("405 method not allowed", func(t *testing.T) {
		rec := httptest.NewRecorder()
		writeRESTMethodNotAllowed(rec, req, "explain", "POST")
		assertExplainJSONBody(t, rec, http.StatusMethodNotAllowed, `{"detail":"Method Not Allowed"}`)
		if got := rec.Header().Get("Allow"); got != "POST" {
			t.Fatalf("Allow = %q, want POST (Starlette reports the FIRST registered route's methods for this path -- POST is registered before GET, main.py:521,537)", got)
		}
	})
}

func assertExplainJSONBody(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantBody string) {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d", rec.Code, wantStatus)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	got := rec.Body.String()
	// json.Encoder appends a trailing newline; insignificant JSON
	// whitespace (RFC 8259 SS2), same tolerance writeKeepAliveJSON's own
	// doc comment (investment_explain_route.go) already establishes.
	if got != wantBody+"\n" {
		t.Fatalf("body = %s, want %s", got, wantBody)
	}
}
