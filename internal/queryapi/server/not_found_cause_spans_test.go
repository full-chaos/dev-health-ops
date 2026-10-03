package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// CHAOS-7932: the query-api's /graphql 404s say WHY on the server span, from a
// closed enum, and never carry the document, its digest or any request value.
func TestGraphQLEdge404SpansCarryTheirCause(t *testing.T) {
	handler, _, member, _, _ := edgeHarness(t)
	for _, tc := range []struct {
		name      string
		request   func() *http.Request
		status    int
		wantCause string // "" = the span must carry NO cause
	}{
		{"unregistered document (POST)", func() *http.Request {
			return edgeRequest(t, edgeCell{method: http.MethodPost, carrier: "member", document: "unregistered"}, member, "", "")
		}, http.StatusNotFound, "unregistered_document"},
		{"unregistered document (GET)", func() *http.Request {
			return edgeRequest(t, edgeCell{method: http.MethodGet, carrier: "member", document: "unregistered"}, member, "", "")
		}, http.StatusNotFound, "unregistered_document"},
		{"browser GET while the IDE is off", func() *http.Request {
			return edgeRequest(t, edgeCell{method: http.MethodGet, accept: "text/html", carrier: "none", document: "query"}, member, "", "")
		}, http.StatusNotFound, "ide_off"},
		{"GET with no query and an HTML-ish Accept", func() *http.Request {
			request := httptest.NewRequest(http.MethodGet, "/graphql?org_id="+ecOrg.String(), nil)
			request.Header.Set("Accept", "*/*")
			request.Header.Set("Authorization", "Bearer "+member)
			return request
		}, http.StatusNotFound, "ide_off"},
		{"a registered document (no 404, no cause)", func() *http.Request {
			return edgeRequest(t, edgeCell{method: http.MethodPost, carrier: "member", document: "query"}, member, "", "")
		}, http.StatusOK, ""},
		{"an unauthenticated POST (401, no cause)", func() *http.Request {
			return edgeRequest(t, edgeCell{method: http.MethodPost, carrier: "none", document: "query"}, member, "", "")
		}, http.StatusUnauthorized, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			read := installSpanTracing(t, "1")
			traced := httpapi.TraceHandler(handler, httpapi.TraceOptions{Listener: "public", ProbePaths: queryProbePaths})
			recorder := httptest.NewRecorder()
			traced.ServeHTTP(recorder, tc.request())
			if recorder.Code != tc.status {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, tc.status, recorder.Body.String())
			}
			spans := read()
			if len(spans) != 1 {
				t.Fatalf("got %d spans, want 1", len(spans))
			}
			cause, present := spanString(spans[0], httpapi.NotFoundCauseAttribute)
			if tc.wantCause == "" {
				if present {
					t.Errorf("a non-404 span carries cause %q", cause)
				}
				return
			}
			if !present || cause != tc.wantCause {
				t.Errorf("%s = %q (present %v), want %q", httpapi.NotFoundCauseAttribute, cause, present, tc.wantCause)
			}
			text := spanFieldTextForServer(spans[0])
			for _, leak := range []string{"query Other", "InternalAuthProbe", digestHex("query Other { probe }"), digestHex(iaDocument), ecOrg.String(), member} {
				if strings.Contains(text, leak) {
					t.Errorf("the span carries request/document text %q: %s", leak, text)
				}
			}
		})
	}
}

// The /api/v1/ catch-all answers 404 for a route that does not exist, and says so.
func TestAPIV1CatchAllNotFoundSaysNotFound(t *testing.T) {
	read := installSpanTracing(t, "1")
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/", apiV1CatchAllNotFound)
	traced := httpapi.TraceHandler(mux, httpapi.TraceOptions{Listener: "public", ProbePaths: queryProbePaths})
	recorder := httptest.NewRecorder()
	traced.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/nothing-here", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", recorder.Code)
	}
	spans := read()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	if cause, _ := spanString(spans[0], httpapi.NotFoundCauseAttribute); cause != "not_found" {
		t.Errorf("cause = %q, want not_found", cause)
	}
}

// spanFieldTextForServer is the name and every attribute (key + value) of the
// span, never its random ids or timestamps.
func spanFieldTextForServer(span *tracepb.Span) string {
	parts := []string{span.GetName()}
	for _, kv := range span.GetAttributes() {
		parts = append(parts, kv.GetKey(), fmt.Sprint(kv.GetValue()))
	}
	return strings.Join(parts, "\n")
}

// Q7 (vetter-3): the NON-edge dispatch (the internal /query listener) records the
// same cause for an unregistered document; the edge is not the only door.
func TestInternalQueryUnregisteredDocument404SpanSaysUnregisteredDocument(t *testing.T) {
	read := installSpanTracing(t, "1")
	verifier, _ := iaVerifier(t)
	handler, _ := iaDispatch(t, verifier)
	traced := httpapi.TraceHandler(handler, httpapi.TraceOptions{Listener: "internal", TrustRemoteSampling: true, ProbePaths: queryProbePaths})
	body := strings.NewReader(`{"query":"query { notRegistered { id } }"}`)
	req := httptest.NewRequest(http.MethodPost, "/query", body)
	iaHeaders("org-1", "member", false, false)(req)
	recorder := httptest.NewRecorder()
	traced.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", recorder.Code)
	}
	spans := read()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	if cause, _ := spanString(spans[0], httpapi.NotFoundCauseAttribute); cause != "unregistered_document" {
		t.Errorf("cause = %q, want unregistered_document", cause)
	}
}

// Q12 (vetter-3): the REAL server mux (Build) answers /api/v1/<nothing> through
// its catch-all and the span says not_found; an inline handler that records
// nothing would read "other".
func TestBuildMuxAPIV1CatchAll404SpanSaysNotFound(t *testing.T) {
	read := installSpanTracing(t, "1")
	plane, err := Build(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	defer plane.Close()
	traced := traceListener(plane.Handler, "public", false)
	recorder := httptest.NewRecorder()
	traced.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/not-a-route", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", recorder.Code)
	}
	spans := read()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	if cause, _ := spanString(spans[0], httpapi.NotFoundCauseAttribute); cause != "not_found" {
		t.Errorf("cause = %q, want not_found (the Build mux's /api/v1/ catch-all)", cause)
	}
}
