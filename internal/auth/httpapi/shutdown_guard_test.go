package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// A request that reaches a route chain after the drain deadline answered the
// others is answered 503 at once; its handler never runs.
func TestARequestAfterTheDrainDeadlineIsAnswered503WithoutItsHandler(t *testing.T) {
	answers := newShutdownAnswers()
	answers.answerAll()
	ran := false
	handler := answers.guard(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		ran = true
		w.WriteHeader(http.StatusInternalServerError)
	}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/late", nil))
	if ran {
		t.Error("the handler ran after the drain deadline")
	}
	if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") != "1" {
		t.Errorf("a request after the drain deadline answered %d (Retry-After %q), want 503 with Retry-After 1",
			recorder.Code, recorder.Header().Get("Retry-After"))
	}
}

// The guard gives the handler a header of its own until it answers: a header the
// outer middleware set is visible to it, its own changes reach the answer, and a
// header it deletes is gone.
func TestTheGuardKeepsTheHandlersHeaderSemantics(t *testing.T) {
	answers := newShutdownAnswers()
	handler := answers.guard(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if got := w.Header().Get("X-Outer"); got != "outer" {
			t.Errorf("the handler sees X-Outer = %q, want outer", got)
		}
		w.Header().Del("X-Outer")
		w.Header().Set("X-Inner", "inner")
		w.WriteHeader(http.StatusAccepted)
		w.Header().Set("X-After", "after")
		_, _ = w.Write([]byte("ok"))
	}))
	recorder := httptest.NewRecorder()
	recorder.Header().Set("X-Outer", "outer")
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/h", nil))
	if recorder.Code != http.StatusAccepted || recorder.Body.String() != "ok" {
		t.Errorf("answer = %d %q, want 202 ok", recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("X-Inner") != "inner" || recorder.Header().Get("X-Outer") != "" {
		t.Errorf("answer header = %v, want X-Inner and no X-Outer", recorder.Header())
	}
	if len(answers.pending) != 0 {
		t.Errorf("%d guards still pending after the handler returned", len(answers.pending))
	}
}
