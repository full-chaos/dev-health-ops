package categorize

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
	return recorder
}

func spanAttrs(s sdktrace.ReadOnlySpan) map[string]attribute.Value {
	out := map[string]attribute.Value{}
	for _, kv := range s.Attributes() {
		out[string(kv.Key)] = kv.Value
	}
	return out
}

// The census: the span name and the whole attribute vocabulary. A new key or
// class fails here until it is listed on purpose.
func TestLLMSpanNameAttributeKeysAndClassesAreTheCensus(t *testing.T) {
	if llmSpanName != "dev_health.llm.request" {
		t.Fatalf("span name = %q", llmSpanName)
	}
	keys := []string{llmAttrProvider, llmAttrModel, llmAttrRole, llmAttrAttempt, llmAttrStatus, llmAttrClass}
	want := []string{"error.class", "http.response.status_code", "llm.attempt", "llm.model", "llm.provider", "llm.role"}
	sort.Strings(keys)
	if strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Fatalf("attribute keys = %v, want %v", keys, want)
	}
	var classes []string
	for _, c := range llmSpanClasses {
		classes = append(classes, string(c))
	}
	sort.Strings(classes)
	wantClasses := "auth,canceled,invalid_answer,invalid_request,model_not_found,other,rate_limit,refused,server,timeout"
	if strings.Join(classes, ",") != wantClasses {
		t.Fatalf("classes = %v, want %s", classes, wantClasses)
	}
}

func TestLLMSpanOnePerAttemptWithClassStatusAndAttributes(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		body       string
		wantClass  string
		wantSpans  int
		wantStatus int
	}{
		{"rate limit retried once", 429, `{}`, "rate_limit", 2, 429},
		{"server retried once", 503, `{}`, "server", 2, 503},
		{"auth not retried", 401, `{}`, "auth", 1, 401},
		{"payment is auth", 402, `{}`, "auth", 1, 402},
		{"forbidden is auth", 403, `{}`, "auth", 1, 403},
		{"unknown model", 404, `{}`, "model_not_found", 1, 404},
		{"bad request", 400, `{}`, "invalid_request", 1, 400},
		{"unprocessable", 422, `{}`, "invalid_request", 1, 422},
		{"other 4xx", 409, `{}`, "other", 1, 409},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := recordSpans(t)
			h := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})
			ctx, parent := otel.Tracer("test").Start(WithLLMRole(context.Background(), LLMRoleShadow), "job")
			_, _ = h.client.SendBody(ctx, tsBody)
			parent.End()
			spans := llmSpans(recorder)
			if len(spans) != tc.wantSpans {
				t.Fatalf("spans = %d, want %d", len(spans), tc.wantSpans)
			}
			for i, s := range spans {
				a := spanAttrs(s)
				if s.Status().Code != codes.Error || s.Status().Description != tc.wantClass {
					t.Errorf("span %d status = %+v", i, s.Status())
				}
				if a[llmAttrClass].AsString() != tc.wantClass || a[llmAttrStatus].AsInt64() != int64(tc.wantStatus) {
					t.Errorf("span %d attrs = %v", i, a)
				}
				if a[llmAttrProvider].AsString() != "typesafe" || a[llmAttrModel].AsString() != h.client.cfg.Model ||
					a[llmAttrRole].AsString() != "shadow" || a[llmAttrAttempt].AsInt64() != int64(i+1) {
					t.Errorf("span %d attrs = %v", i, a)
				}
				if len(a) != 6 {
					t.Errorf("span %d has %d attributes: %v", i, len(a), a)
				}
				if s.Parent().SpanID() != parent.SpanContext().SpanID() {
					t.Errorf("span %d is not a child of the job span", i)
				}
			}
		})
	}
}

func llmSpans(recorder *tracetest.SpanRecorder) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		if s.Name() == llmSpanName {
			out = append(out, s)
		}
	}
	return out
}

func TestLLMSpanSuccessHasNoErrorAndNoRoleWhenUnknown(t *testing.T) {
	recorder := recordSpans(t)
	h := newTS(t, okHandler(t, "req-1"))
	if _, err := h.client.SendBody(context.Background(), tsBody); err != nil {
		t.Fatal(err)
	}
	spans := llmSpans(recorder)
	if len(spans) != 1 {
		t.Fatalf("spans = %d", len(spans))
	}
	a := spanAttrs(spans[0])
	if spans[0].Status().Code == codes.Error {
		t.Fatalf("status = %+v", spans[0].Status())
	}
	if _, has := a[llmAttrClass]; has {
		t.Fatalf("error.class on a success: %v", a)
	}
	if _, has := a[llmAttrRole]; has {
		t.Fatalf("role set without a role: %v", a)
	}
	if a[llmAttrStatus].AsInt64() != 200 || a[llmAttrAttempt].AsInt64() != 1 {
		t.Fatalf("attrs = %v", a)
	}
}

func TestLLMSpanTimeoutRefusedCanceledAndInvalidAnswer(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		recorder := recordSpans(t)
		h := newTS(t, func(_ http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
		}, func(c *TypeSafeClientConfig) { c.Timeout = 60 * time.Millisecond })
		_, _ = h.client.SendBody(context.Background(), tsBody)
		assertAllClass(t, llmSpans(recorder), 2, "timeout")
	})
	t.Run("refused", func(t *testing.T) {
		recorder := recordSpans(t)
		dead := httptest.NewServer(http.NotFoundHandler())
		url := dead.URL
		dead.Close()
		h := newTS(t, okHandler(t, "r"), func(c *TypeSafeClientConfig) { c.BaseURL = url })
		_, _ = h.client.SendBody(context.Background(), tsBody)
		spans := llmSpans(recorder)
		assertAllClass(t, spans, 2, "refused")
		if _, has := spanAttrs(spans[0])[llmAttrStatus]; has {
			t.Fatal("a refused connection has no status code")
		}
	})
	t.Run("canceled", func(t *testing.T) {
		recorder := recordSpans(t)
		h := newTS(t, func(_ http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
		})
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, _ = h.client.SendBody(ctx, tsBody)
		assertAllClass(t, llmSpans(recorder), 1, "canceled")
	})
	t.Run("invalid answer", func(t *testing.T) {
		recorder := recordSpans(t)
		h := newTS(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("not json")) })
		_, _ = h.client.SendBody(context.Background(), tsBody)
		spans := llmSpans(recorder)
		assertAllClass(t, spans, 1, "invalid_answer")
		if spanAttrs(spans[0])[llmAttrStatus].AsInt64() != 200 {
			t.Fatal("an invalid answer keeps its 200")
		}
	})
}

func assertAllClass(t *testing.T, spans []sdktrace.ReadOnlySpan, n int, class string) {
	t.Helper()
	if len(spans) != n {
		t.Fatalf("spans = %d, want %d", len(spans), n)
	}
	for _, s := range spans {
		if s.Status().Code != codes.Error || spanAttrs(s)[llmAttrClass].AsString() != class {
			t.Fatalf("status = %+v attrs = %v, want class %s", s.Status(), spanAttrs(s), class)
		}
	}
}

// The seam is shared: OpenAI goes through it with no code of its own.
func TestLLMSpanOpenAIProviderUsesTheSameSeam(t *testing.T) {
	recorder := recordSpans(t)
	provider, _ := newTestOpenAIProvider(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})
	_, _ = provider.Complete(context.Background(), CategorizationRequest("p"))
	spans := llmSpans(recorder)
	assertAllClass(t, spans, 1, "auth")
	a := spanAttrs(spans[0])
	if a[llmAttrProvider].AsString() != "openai" || a[llmAttrModel].AsString() != "gpt-5-nano" || a[llmAttrAttempt].AsInt64() != 1 {
		t.Fatalf("attrs = %v", a)
	}
}

// No prompt, key, URL or peer-controlled string reaches a span.
func TestLLMSpanCarriesNoSecretNoURLNoPeerText(t *testing.T) {
	recorder := recordSpans(t)
	h := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(typeSafeRequestIDHeader, "peer-req-id-zzz")
		w.Header().Set("X-Peer", "peer-header-zzz")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("peer-body-zzz " + tsKey))
	})
	_, _ = h.client.SendBody(context.Background(), tsBody)
	for _, s := range llmSpans(recorder) {
		var text strings.Builder
		text.WriteString(s.Status().Description)
		for _, kv := range s.Attributes() {
			text.WriteString(" " + string(kv.Key) + "=" + kv.Value.Emit())
		}
		for _, e := range s.Events() {
			text.WriteString(" event:" + e.Name)
		}
		for _, bad := range []string{tsKey, "peer-", "zzz", "127.0.0.1", "://", tsStateText} {
			if strings.Contains(text.String(), bad) {
				t.Fatalf("span text %q holds %q", text.String(), bad)
			}
		}
	}
}

func TestLLMSpanNoRequestNoSpan(t *testing.T) {
	recorder := recordSpans(t)
	_ = newTS(t, okHandler(t, "r"))
	if got := len(recorder.Ended()); got != 0 {
		t.Fatalf("spans without a request = %d", got)
	}
}

func TestLLMSpanModelIsBoundedAndConfigured(t *testing.T) {
	recorder := recordSpans(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) }))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
	resp, err := tracedLLMDo(srv.Client(), req, ProviderKindLocal, strings.Repeat("m", 500))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	spans := llmSpans(recorder)
	if len(spans) != 1 || len(spanAttrs(spans[0])[llmAttrModel].AsString()) != llmSpanMaxModelLen {
		t.Fatalf("spans = %v", spans)
	}
}

func TestLLMSpanClassifyTable(t *testing.T) {
	cases := []struct {
		name   string
		status int
		err    error
		ctxErr error
		want   llmSpanClass
	}{
		{"200", 200, nil, nil, ""},
		{"204 is not an answer", 204, nil, nil, llmSpanClassOther},
		{"redirect", 302, nil, nil, llmSpanClassOther},
		{"network timeout", 0, netTimeout{}, nil, llmSpanClassTimeout},
		{"deadline", 0, context.DeadlineExceeded, nil, llmSpanClassTimeout},
		{"canceled context", 0, context.Canceled, context.Canceled, llmSpanClassCanceled},
		{"canceled transport error without ctx", 0, context.Canceled, nil, llmSpanClassCanceled},
		{"unknown transport", 0, io.ErrUnexpectedEOF, nil, llmSpanClassOther},
	}
	for _, tc := range cases {
		if got := classifyLLMSpan(tc.status, tc.err, tc.ctxErr); got != tc.want {
			t.Errorf("%s: class = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestLLMSpanUnknownRoleIsLeftOff(t *testing.T) {
	recorder := recordSpans(t)
	h := newTS(t, okHandler(t, "r"))
	if _, err := h.client.SendBody(WithLLMRole(context.Background(), LLMRole("bogus")), tsBody); err != nil {
		t.Fatal(err)
	}
	if _, has := spanAttrs(llmSpans(recorder)[0])[llmAttrRole]; has {
		t.Fatal("an unknown role reached the span")
	}
}

func TestLLMSpanBodyReadFailureIsAnErrorSpan(t *testing.T) {
	recorder := recordSpans(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("short"))
	}))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
	resp, err := tracedLLMDo(srv.Client(), req, ProviderKindLocal, "m")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Fatal("expected a read error")
	}
	_ = resp.Body.Close()
	spans := llmSpans(recorder)
	assertAllClass(t, spans, 1, "other")
	if spanAttrs(spans[0])[llmAttrStatus].AsInt64() != 200 {
		t.Fatal("status kept")
	}
}

// Tracing changes nothing on the wire: same requests, same bytes, same header
// names, same result, whether or not a provider records spans.
func TestLLMSpanLeavesTheRequestsAndTheResultUnchanged(t *testing.T) {
	type seen struct {
		body    string
		headers string
	}
	run := func() ([]seen, SystemOneResult) {
		var got []seen
		h := newTS(t, func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			var names []string
			for k := range r.Header {
				names = append(names, k)
			}
			sort.Strings(names)
			got = append(got, seen{string(b), strings.Join(names, ",")})
			okHandler(t, "req-1")(w, r)
		})
		res, err := h.client.SendBody(context.Background(), tsBody)
		if err != nil {
			t.Fatal(err)
		}
		return got, res
	}
	offReqs, offRes := run()
	recordSpans(t)
	onReqs, onRes := run()
	if len(offReqs) != 1 || len(onReqs) != 1 || offReqs[0] != onReqs[0] {
		t.Fatalf("requests differ: off=%v on=%v", offReqs, onReqs)
	}
	if string(offRes.Body) != string(onRes.Body) || offRes.StatusCode != onRes.StatusCode || offRes.RequestID != onRes.RequestID {
		t.Fatal("results differ")
	}
}

// A body that breaks on an error response keeps the class of its status.
func TestLLMSpanBodyReadFailureKeepsTheStatusClass(t *testing.T) {
	recorder := recordSpans(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("short"))
	}))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
	resp, err := tracedLLMDo(srv.Client(), req, ProviderKindLocal, "m")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assertAllClass(t, llmSpans(recorder), 1, "model_not_found")
}
