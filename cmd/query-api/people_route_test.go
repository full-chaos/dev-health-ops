package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/people"
)

// emptyRowsPeopleClient answers every ClickHouse call with zero rows --
// same convention as quadrant_route_test.go/drilldown_prs_route_test.go's
// own empty-rows fakes.
type emptyRowsPeopleClient struct{}

func (emptyRowsPeopleClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return emptyRowsPeopleScanner{}, nil
}

type emptyRowsPeopleScanner struct{}

func (emptyRowsPeopleScanner) Next() bool        { return false }
func (emptyRowsPeopleScanner) Scan(...any) error { return nil }
func (emptyRowsPeopleScanner) Err() error        { return nil }
func (emptyRowsPeopleScanner) Close() error      { return nil }

func newEmptyRowsPeopleReader(t *testing.T) *people.Reader {
	t.Helper()
	reader, err := people.NewReader(emptyRowsPeopleClient{})
	if err != nil {
		t.Fatalf("people.NewReader: %v", err)
	}
	return reader
}

// failingPeopleClient always errors -- used to pin the outer 503 fallback.
type failingPeopleClient struct{}

func (failingPeopleClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return nil, os.ErrClosed
}

func newFailingPeopleReader(t *testing.T) *people.Reader {
	t.Helper()
	reader, err := people.NewReader(failingPeopleClient{})
	if err != nil {
		t.Fatalf("people.NewReader: %v", err)
	}
	return reader
}

func TestPeopleSearchSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := peopleSearchSwitchFromEnv()
	if sw.Enabled(peopleSearchOperation) {
		t.Fatal("expected the people search operation disabled with no env var set")
	}
}

func TestPeopleSearchSwitchFromEnvEnablesOperation(t *testing.T) {
	t.Setenv(peopleSearchEnabledEnvVar, "true")
	sw := peopleSearchSwitchFromEnv()
	if !sw.Enabled(peopleSearchOperation) {
		t.Fatal("expected the people search operation enabled with GO_API_PEOPLE_SEARCH_ENABLED=true")
	}
}

// TestNewPeopleSearchHandlerRequiresAuthContext pins that the work
// handler (reached only after buildPeopleSearchRoute's entryHandler
// already authenticated) itself still refuses a request with no claims
// attached, same defensive check every sibling REST route's work handler
// makes.
func TestNewPeopleSearchHandlerRequiresAuthContext(t *testing.T) {
	handler := newPeopleSearchHandler(newEmptyRowsPeopleReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people?q=ali", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestNewPeopleSearchHandlerHappyPathEmptyResult pins Content-Type and
// the "[]" (present, not omitted/null) shape for a supported,
// empty-data request.
func TestNewPeopleSearchHandlerHappyPathEmptyResult(t *testing.T) {
	handler := newPeopleSearchHandler(newEmptyRowsPeopleReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people?q=ali", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var decoded []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if decoded == nil {
		t.Fatal("body must be [] (present), not omitted/null")
	}
}

// TestNewPeopleSearchHandlerEmptyQueryNeverTouchesClickHouse pins the
// route-level plumbing for search_people_response's `if not trimmed:
// return []` short circuit (services/people.py:405-406): even an
// unauthenticated-ClickHouse-would-fail client must not error, because
// the query never reaches it.
func TestNewPeopleSearchHandlerEmptyQueryNeverTouchesClickHouse(t *testing.T) {
	handler := newPeopleSearchHandler(newFailingPeopleReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if rec.Body.String() != "[]\n" {
		t.Fatalf("body = %q, want []\\n", rec.Body.String())
	}
}

// TestNewPeopleSearchHandlerDataUnavailable pins the outer `except
// Exception: raise HTTPException(503, "Data unavailable")` fallback
// (main.py:1064-1065), body shape confirmed live via FastAPI's own
// http_exception_handler ({"detail": exc.detail}) -- see
// writePeopleDetailError's own doc comment.
func TestNewPeopleSearchHandlerDataUnavailable(t *testing.T) {
	handler := newPeopleSearchHandler(newFailingPeopleReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people?q=ali", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var decoded map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if decoded["detail"] != "Data unavailable" {
		t.Fatalf("detail = %q, want %q", decoded["detail"], "Data unavailable")
	}
}

// TestNewPeopleSearchHandlerRejectsComparativeParams pins
// _reject_comparative_params (main.py:202-208) for every key in
// _FORBIDDEN_QUERY_PARAMS (main.py:167-175), body captured live via
// FastAPI TestClient against the real /api/v1/people route
// (dependency-overridden auth, ENVIRONMENT=development) -- see this
// PR's own TEST-EVIDENCE for the exact capture command.
func TestNewPeopleSearchHandlerRejectsComparativeParams(t *testing.T) {
	for _, key := range []string{"compare_to", "rank", "percentile", "score", "leaderboard", "top", "bottom"} {
		t.Run(key, func(t *testing.T) {
			handler := newPeopleSearchHandler(newFailingPeopleReader(t))
			req := httptest.NewRequest(http.MethodGet, "/api/v1/people?q=ali&"+key+"=x", nil)
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
			rec := httptest.NewRecorder()
			handler(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("Content-Type = %q, want application/json", ct)
			}
			var decoded map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
				t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
			}
			if decoded["detail"] != "Comparative parameters are not supported." {
				t.Fatalf("detail = %q, want %q", decoded["detail"], "Comparative parameters are not supported.")
			}
		})
	}
}

// TestNewPeopleSearchHandlerLimitValidationBeatsComparativeParamRejection
// pins the precedence confirmed live: FastAPI/Pydantic resolves a
// malformed `limit` query parameter at the framework level, before
// _reject_comparative_params (main.py:1056) ever runs inside the route
// function body -- so a request carrying BOTH a malformed limit and a
// forbidden comparative param answers 422 (limit's error), never 400.
func TestNewPeopleSearchHandlerLimitValidationBeatsComparativeParamRejection(t *testing.T) {
	handler := newPeopleSearchHandler(newFailingPeopleReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people?limit=not-a-number&compare_to=x", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}

	data, err := os.ReadFile("testdata/people_422/get_non_numeric_limit_with_comparative_param.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var want pydanticValidationErrorBody
	if err := json.NewDecoder(bytes.NewReader(data)).Decode(&want); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	var got pydanticValidationErrorBody
	if err := json.NewDecoder(bytes.NewReader(rec.Body.Bytes())).Decode(&got); err != nil {
		t.Fatalf("decode response: %v (body=%s)", err, rec.Body.String())
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("body = %+v, want %+v", got, want)
	}
}

// TestNewPeopleSearchHandlerNonNumericLimitValidationError replays
// testdata/people_422/get_non_numeric_limit.json against
// newPeopleSearchHandler: a non-numeric limit must answer 422 with
// FastAPI's own default RequestValidationError body, byte-for-byte on
// every field this Go type declares -- same shape
// drilldown_prs_422_test.go's own range_days case already establishes
// for this binary's shared intQueryParamError helper, captured fresh for
// this route's own field name ("limit").
func TestNewPeopleSearchHandlerNonNumericLimitValidationError(t *testing.T) {
	handler := newPeopleSearchHandler(newFailingPeopleReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people?limit=not-a-number", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}

	data, err := os.ReadFile("testdata/people_422/get_non_numeric_limit.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var want pydanticValidationErrorBody
	if err := json.NewDecoder(bytes.NewReader(data)).Decode(&want); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	var got pydanticValidationErrorBody
	if err := json.NewDecoder(bytes.NewReader(rec.Body.Bytes())).Decode(&got); err != nil {
		t.Fatalf("decode response: %v (body=%s)", err, rec.Body.String())
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("body = %+v, want %+v", got, want)
	}
}
