package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

// TestNewExplainGetHandlerEmptyWindowStatesBothDataFlags is a REST-contract
// regression check for CHAOS-8491. The empty reader models a metric query
// whose requested and comparison windows contain no stored values. Both
// placeholder zeroes must be identified in the JSON response; a client must
// not infer their meaning from value or delta_pct.
func TestNewExplainGetHandlerEmptyWindowStatesBothDataFlags(t *testing.T) {
	handler := newExplainGetHandler(newEmptyRowsExplainReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explain?metric=cycle_time&range_days=7", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	for _, key := range []string{"has_data", "has_prior_data"} {
		raw, ok := body[key]
		if !ok {
			t.Errorf("response has no %q key: %s", key, rec.Body.String())
			continue
		}
		var got bool
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Errorf("decode %s: %v", key, err)
			continue
		}
		if got {
			t.Errorf("%s = true, want false for an empty window", key)
		}
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
// never sets the GET-only deprecation header.
func TestNewExplainPostHandlerHappyPathNoDeprecatedHeader(t *testing.T) {
	handler := newExplainPostHandler(newEmptyRowsExplainReader(t))
	body := `{"metric":"churn","filters":{"scope":{"level":"org"},"time":{"range_days":7}}}`
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
		Metric       string `json:"metric"`
		Label        string `json:"label"`
		HasData      *bool  `json:"has_data"`
		HasPriorData *bool  `json:"has_prior_data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if decoded.Metric != "churn" || decoded.Label != "Code Churn" {
		t.Fatalf("decoded = %+v, want metric=churn label=Code Churn", decoded)
	}
	if decoded.HasData == nil || decoded.HasPriorData == nil {
		t.Fatalf("response must contain has_data and has_prior_data: %s", rec.Body.String())
	}
	if *decoded.HasData || *decoded.HasPriorData {
		t.Fatalf("empty response flags = has_data:%t has_prior_data:%t, want both false", *decoded.HasData, *decoded.HasPriorData)
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

// The explain route answers a metric with ITS OWN config, or a client error,
// never another metric's config (CHAOS-9136, D5869). Executed through both
// handlers for the 11 Home metrics and an invented name: the nine metrics with a
// config answer 200 with their own label; the three Home metrics without one
// (rework_ratio, pr_rework_ratio, ci_success) and the invented name answer the
// route's parameter error (422 literal_error on the metric), until a config
// exists. (The Python original answered all of them with cycle_time's config; a
// known bug, not pinned.)
func TestExplainAnswersTheMetricsOwnConfigOrAClientError(t *testing.T) {
	labels := map[string]string{
		"cycle_time": "Cycle Time", "review_latency": "Review Latency", "throughput": "Throughput",
		"deploy_freq": "Deploy Frequency", "churn": "Code Churn", "wip_saturation": "WIP Saturation",
		"blocked_work": "Blocked Work", "change_failure_rate": "Change Failure Rate",
	}
	names := []string{"cycle_time", "review_latency", "throughput", "deploy_freq", "churn", "wip_saturation",
		"blocked_work", "change_failure_rate", "rework_ratio", "pr_rework_ratio", "ci_success", "totally_bogus"}
	get, post := newExplainGetHandler(newEmptyRowsExplainReader(t)), newExplainPostHandler(newEmptyRowsExplainReader(t))
	for _, name := range names {
		for surface, request := range map[string]func() *http.Request{
			"GET": func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/api/v1/explain?metric="+name+"&range_days=7", nil)
			},
			"POST": func() *http.Request {
				return httptest.NewRequest(http.MethodPost, "/api/v1/explain",
					bytes.NewReader([]byte(`{"metric":"`+name+`","filters":{"scope":{"level":"org"},"time":{"range_days":7}}}`)))
			},
		} {
			req := request()
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
			rec := httptest.NewRecorder()
			handler := get
			if surface == "POST" {
				handler = post
			}
			serveRoute(t, handler, rec, req)
			wantLabel, known := labels[name]
			if !known {
				if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), `"type":"literal_error"`) ||
					strings.Contains(rec.Body.String(), "Cycle Time") {
					t.Errorf("%s %s: status %d body %s, want 422 literal_error and no config", surface, name, rec.Code, rec.Body.String())
				}
				continue
			}
			var decoded struct {
				Metric string `json:"metric"`
				Label  string `json:"label"`
			}
			if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &decoded) != nil || decoded.Metric != name || decoded.Label != wantLabel {
				t.Errorf("%s %s: status %d body %s, want 200 with label %q", surface, name, rec.Code, rec.Body.String(), wantLabel)
			}
		}
	}
}
