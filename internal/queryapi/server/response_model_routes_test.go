package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// TestMarkResponseModelRoutesCountsAWrongWriter drives the production
// wrapper (markResponseModelRoutes over a ServeMux, as Run builds it): a
// 2xx success body on a response_model route that is not written by
// writeModelResponse is counted, the model writer is not, and the flag is
// resolved per method and through a wildcard pattern.
func TestMarkResponseModelRoutesCountsAWrongWriter(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/meta", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]float64{"x": 1e-7})
	})
	mux.HandleFunc("/api/v1/people/{person_id}/summary", func(w http.ResponseWriter, r *http.Request) {
		if err := writeModelResponse(w, map[string]float64{"x": 1e-7}); err != nil {
			t.Fatal(err)
		}
	})
	mux.HandleFunc("/api/v1/people/{person_id}/drilldown/prs", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode([]int{})
	})
	server := httptest.NewServer(markResponseModelRoutes(mux))
	defer server.Close()
	for _, test := range []struct {
		method, path string
		counted      int64
	}{
		{http.MethodGet, "/api/v1/meta", 1},
		{http.MethodPost, "/api/v1/meta", 0},
		{http.MethodGet, "/api/v1/people/p-1/summary", 0},
		{http.MethodGet, "/api/v1/people/p-1/drilldown/prs", 1},
	} {
		before := policy.WriterViolations()
		request, err := http.NewRequest(test.method, server.URL+test.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if got := policy.WriterViolations() - before; got != test.counted {
			t.Errorf("%s %s: %d writer violations, want %d", test.method, test.path, got, test.counted)
		}
	}
}

// TestStreamErrorChunksAreNotAWrongWriter: the investment explain stream's
// error chunks are Python's own body on a 200, not a wrong writer.
func TestStreamErrorChunksAreNotAWrongWriter(t *testing.T) {
	before := policy.WriterViolations()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(" "))
		writeStreamErrorChunks(w, nil)
	})
	recorder := httptest.NewRecorder()
	serveModelRoute(handler, "POST /api/v1/investment/explain", true, recorder,
		httptest.NewRequest(http.MethodPost, "/api/v1/investment/explain", nil))
	if got := policy.WriterViolations() - before; got != 0 {
		t.Fatalf("stream error chunks counted %d writer violations, want 0", got)
	}
	if want := ` {"error": "Streaming error", "detail": "An internal error has occurred."}{"error": "Streaming error", "detail": "An internal streaming error occurred."}`; recorder.Body.String() != want {
		t.Fatalf("body = %q, want %q", recorder.Body.String(), want)
	}
}
