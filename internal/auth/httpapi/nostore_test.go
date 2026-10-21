package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNoStoreSetsHeaderOnSuccessAndErrorAnswers(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusUnauthorized, http.StatusForbidden, http.StatusInternalServerError} {
		handler := NoStore(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/x", nil))
		if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
			t.Fatalf("status %d: Cache-Control = %q, want no-store", status, got)
		}
	}
}

func TestNoStoreNegativeControlUnwrappedHandlerSendsNoCacheControl(t *testing.T) {
	recorder := httptest.NewRecorder()
	http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }).
		ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/x", nil))
	if got := recorder.Header().Get("Cache-Control"); got != "" {
		t.Fatalf("unwrapped handler sent Cache-Control %q", got)
	}
}
