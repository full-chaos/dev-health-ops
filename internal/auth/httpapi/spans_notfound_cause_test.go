package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// CHAOS-7932: a 404 server span carries a CLOSED-enum cause; no other status
// does; nothing but the enum can reach it.

func causeOf(span *tracepb.Span) (string, bool) { return attrString(span, NotFoundCauseAttribute) }

func routeAnswering(pattern string, status int, record func(r *http.Request)) Route {
	return Route{Method: http.MethodGet, Pattern: pattern, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if record != nil {
			record(r)
		}
		w.WriteHeader(status)
	})}
}

func TestA404SpanCarriesTheCauseTheHandlerRecordedAndOnlyFromTheEnum(t *testing.T) {
	for _, cause := range NotFoundCauses {
		cause := cause
		t.Run(string(cause), func(t *testing.T) {
			read := tracedEnv(t, "1")
			route := routeAnswering("/v1/gone", http.StatusNotFound, func(r *http.Request) { RecordNotFoundCause(r.Context(), cause) })
			get(spanHandler(t, "public", false, route), "/v1/gone", nil)
			spans := read()
			if len(spans) != 1 {
				t.Fatalf("got %d spans, want 1", len(spans))
			}
			if got, ok := causeOf(spans[0]); !ok || got != string(cause) {
				t.Errorf("%s = %q (present %v), want %q", NotFoundCauseAttribute, got, ok, cause)
			}
		})
	}
}

func TestAnUnknownOrFreeTextCauseIsRecordedAsOtherNeverVerbatim(t *testing.T) {
	read := tracedEnv(t, "1")
	route := routeAnswering("/v1/gone", http.StatusNotFound, func(r *http.Request) {
		RecordNotFoundCause(r.Context(), NotFoundCause("document query { secret9183 } digest abc123"))
	})
	get(spanHandler(t, "public", false, route), "/v1/gone", nil)
	spans := read()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	if got, _ := causeOf(spans[0]); got != string(NotFoundOther) {
		t.Errorf("cause = %q, want %q", got, NotFoundOther)
	}
	if strings.Contains(spanFieldTextForCause(spans[0]), "secret9183") {
		t.Errorf("free text reached the span")
	}
}

func TestA404WithNoRecordedCauseIsOtherOnAMatchedRouteAndNotFoundWhenNoRouteMatched(t *testing.T) {
	read := tracedEnv(t, "1")
	handler := spanHandler(t, "public", false, routeAnswering("/v1/gone", http.StatusNotFound, nil))
	get(handler, "/v1/gone", nil)
	get(handler, "/no/such/path", nil)
	spans := read()
	if len(spans) != 2 {
		t.Fatalf("got %d spans, want 2", len(spans))
	}
	got := map[string]string{}
	for _, span := range spans {
		cause, _ := causeOf(span)
		got[span.GetName()] = cause
	}
	if got["GET /v1/gone"] != string(NotFoundOther) || got["GET "+UnmatchedRoute] != string(NotFoundNoRoute) {
		t.Errorf("causes by span = %v, want matched=other and unmatched=not_found", got)
	}
}

func TestOnlyA404SpanCarriesACause(t *testing.T) {
	read := tracedEnv(t, "1")
	record := func(r *http.Request) { RecordNotFoundCause(r.Context(), NotFoundIDEOff) }
	handler := spanHandler(t, "public", false,
		routeAnswering("/v1/ok", http.StatusNoContent, record),
		routeAnswering("/v1/boom", http.StatusInternalServerError, record),
		routeAnswering("/v1/denied", http.StatusForbidden, record),
	)
	for _, path := range []string{"/v1/ok", "/v1/boom", "/v1/denied"} {
		get(handler, path, nil)
	}
	spans := read()
	if len(spans) != 3 {
		t.Fatalf("got %d spans, want 3", len(spans))
	}
	for _, span := range spans {
		if cause, ok := causeOf(span); ok {
			t.Errorf("span %s carries a not-found cause %q although its status is not 404", span.GetName(), cause)
		}
	}
}

func TestTheLastRecordedCauseWins(t *testing.T) {
	read := tracedEnv(t, "1")
	route := routeAnswering("/v1/gone", http.StatusNotFound, func(r *http.Request) {
		RecordNotFoundCause(r.Context(), NotFoundUnregisteredDocument)
		RecordNotFoundCause(r.Context(), NotFoundIDEOff)
	})
	get(spanHandler(t, "public", false, route), "/v1/gone", nil)
	if got, _ := causeOf(read()[0]); got != string(NotFoundIDEOff) {
		t.Errorf("cause = %q, want the last recorded (ide_off)", got)
	}
}

// RecordNotFoundCause on a context with no observer (a probe path, an untraced
// listener) is a no-op, never a panic.
func TestRecordNotFoundCauseWithoutAnObserverIsANoOp(t *testing.T) {
	RecordNotFoundCause(httptest.NewRequest(http.MethodGet, "/x", nil).Context(), NotFoundIDEOff)
}

// The TraceHandler listeners (the query-api's) stamp the cause the same way,
// including NotFoundNoRoute for a path no mux pattern matched.
func TestTraceHandlerStampsTheCauseToo(t *testing.T) {
	read := tracedEnv(t, "1")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/gone", func(w http.ResponseWriter, r *http.Request) {
		RecordNotFoundCause(r.Context(), NotFoundUnregisteredDocument)
		w.WriteHeader(http.StatusNotFound)
	})
	handler := TraceHandler(mux, TraceOptions{Listener: "public"})
	get(handler, "/v1/gone", nil)
	get(handler, "/nope", nil)
	spans := read()
	if len(spans) != 2 {
		t.Fatalf("got %d spans, want 2", len(spans))
	}
	got := map[string]string{}
	for _, span := range spans {
		cause, _ := causeOf(span)
		got[span.GetName()] = cause
	}
	if got["GET /v1/gone"] != string(NotFoundUnregisteredDocument) || got["GET "+UnmatchedRoute] != string(NotFoundNoRoute) {
		t.Errorf("causes by span = %v", got)
	}
}

// spanFieldTextForCause: every attribute value + name of the span (no ids).
func spanFieldTextForCause(span *tracepb.Span) string {
	parts := []string{span.GetName()}
	for _, kv := range span.GetAttributes() {
		parts = append(parts, kv.GetKey(), kv.GetValue().GetStringValue())
	}
	return strings.Join(parts, "\n")
}
