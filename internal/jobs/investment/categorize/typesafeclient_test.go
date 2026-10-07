package categorize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

const (
	tsKey        = "tsk-sentinel-key-0123456789abcdef"
	tsStateText  = "SENTINEL-STATE-TEXT-do-not-log"
	tsAnswerText = "SENTINEL-ANSWER-TEXT-do-not-log"
)

var tsBody = []byte(`{"model":"jev-1.13.0","state":{"source_block":"` + tsStateText + `"},"questions":{"q":{"type":"noul","instructions":"i"}}}`)

func realResponse(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/typesafe-systemone-real-response.json")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type tsHarness struct {
	client *TypeSafeClient
	logs   *bytes.Buffer
	calls  *atomic.Int32
	delays *[]time.Duration
}

// newTS starts an httptest server in front of a client. The sleep is replaced
// so retry tests run at once and record the delay they asked for.
func newTS(t *testing.T, handler http.HandlerFunc, mutate ...func(*TypeSafeClientConfig)) tsHarness {
	t.Helper()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	logs := &bytes.Buffer{}
	cfg := TypeSafeClientConfig{
		APIKey:                       secrets.NewHidden(tsKey),
		BaseURL:                      server.URL,
		Logger:                       slog.New(slog.NewJSONHandler(&syncWriter{w: logs}, &slog.HandlerOptions{Level: slog.LevelDebug})),
		UnsafeAllowAnyBaseURLForTest: true,
	}
	for _, m := range mutate {
		m(&cfg)
	}
	client, err := NewTypeSafeClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var delays []time.Duration
	client.sleep = func(_ context.Context, d time.Duration) bool {
		delays = append(delays, d)
		return true
	}
	t.Cleanup(func() { client.Close() })
	return tsHarness{client: client, logs: logs, calls: &calls, delays: &delays}
}

type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func okHandler(t *testing.T, requestID string) http.HandlerFunc {
	response := realResponse(t)
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("x-typesafe-request-id", requestID)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(response)
	}
}

func mustSystemOneError(t *testing.T, err error) *SystemOneError {
	t.Helper()
	var got *SystemOneError
	if !errors.As(err, &got) {
		t.Fatalf("error %v is not a *SystemOneError", err)
	}
	return got
}

func TestTypeSafeSuccessSendsTheDocumentedWireAndReturnsUsageRequestIDAndLatency(t *testing.T) {
	var gotBody []byte
	var gotHeader http.Header
	var gotMethod, gotPath string
	response := realResponse(t)
	h := newTS(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotHeader = r.Method, r.URL.Path, r.Header.Clone()
		gotBody, _ = io.ReadAll(r.Body)
		time.Sleep(5 * time.Millisecond)
		w.Header().Set("x-typesafe-request-id", "req_abc-123")
		_, _ = w.Write(response)
	})
	res, err := h.client.SendBody(context.Background(), tsBody)
	if err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodPost || gotPath != "/v1/systemone" {
		t.Fatalf("request line = %s %s", gotMethod, gotPath)
	}
	if gotHeader.Get("Authorization") != "Bearer "+tsKey || gotHeader.Get("Content-Type") != "application/json" {
		t.Fatalf("headers = %v", gotHeader)
	}
	if !bytes.Equal(gotBody, tsBody) {
		t.Fatal("the body on the wire is not the body the caller rendered, byte for byte")
	}
	if !bytes.Equal(res.Body, response) {
		t.Fatal("the raw response body was not returned unchanged")
	}
	if res.Model != "jev-1.13.0" || res.RequestID != "req_abc-123" || res.StatusCode != 200 {
		t.Fatalf("model/request id/status = %q %q %d", res.Model, res.RequestID, res.StatusCode)
	}
	if !res.Usage.Reported || res.Usage.InputTokens != 2383 || res.Usage.OutputTokens != 368 {
		t.Fatalf("usage = %+v, want 2383/368 reported", res.Usage)
	}
	if res.AttemptCount() != 1 || res.Latency() < 5*time.Millisecond || res.RetryWait() != 0 {
		t.Fatalf("attempts=%d latency=%v wait=%v", res.AttemptCount(), res.Latency(), res.RetryWait())
	}
}

func TestSystemOneRequestBodyIsExactAndNotHTMLEscaped(t *testing.T) {
	req := SystemOneRequest{
		State:     json.RawMessage(`{"source_block":"a<b>&c"}`),
		Questions: json.RawMessage(`{"z":{"type":"noul"},"a":{"type":"noul"}}`),
	}
	got, err := req.Body("jev-1.13.0")
	if err != nil {
		t.Fatal(err)
	}
	want := `{"model":"jev-1.13.0","state":{"source_block":"a<b>&c"},"questions":{"z":{"type":"noul"},"a":{"type":"noul"}}}`
	if string(got) != want {
		t.Fatalf("body = %s\nwant   %s", got, want)
	}
	req.Model = "jev-9.9.9"
	if got, _ := req.Body("jev-1.13.0"); !strings.HasPrefix(string(got), `{"model":"jev-9.9.9"`) {
		t.Fatalf("explicit model lost: %s", got)
	}
	if _, err := (SystemOneRequest{State: json.RawMessage(`{`), Questions: json.RawMessage(`{}`)}).Body("m"); err == nil {
		t.Fatal("invalid state accepted")
	}
}

func TestTypeSafeSystemOneRendersAndSendsTheRequest(t *testing.T) {
	var gotBody []byte
	response := realResponse(t)
	h := newTS(t, func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		_, _ = w.Write(response)
	})
	_, err := h.client.SystemOne(context.Background(), SystemOneRequest{State: json.RawMessage(`{"s":1}`), Questions: json.RawMessage(`{"q":{}}`)})
	if err != nil {
		t.Fatal(err)
	}
	if string(gotBody) != `{"model":"jev-1.13.0","state":{"s":1},"questions":{"q":{}}}` {
		t.Fatalf("body = %s", gotBody)
	}
}

func TestTypeSafeAuthFailuresStopAndAreNotRetried(t *testing.T) {
	for _, status := range []int{401, 402, 403} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			h := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("x-typesafe-request-id", "req_auth")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"error":"bad key ` + tsKey + ` ` + tsStateText + `"}`))
			})
			_, err := h.client.SendBody(context.Background(), tsBody)
			se := mustSystemOneError(t, err)
			if se.Class != SystemOneClassAuth || !se.StopsArm() || se.StatusCode != status || se.RequestID != "req_auth" {
				t.Fatalf("error = %+v", se)
			}
			if h.calls.Load() != 1 || len(se.Attempts) != 1 {
				t.Fatalf("calls=%d attempts=%d, want 1 (an auth failure is not retried)", h.calls.Load(), len(se.Attempts))
			}
			if class, ok := ClassifyLLMError(err); !ok || class != LLMErrorClassAuth {
				t.Fatalf("ClassifyLLMError = %v %v, want auth", class, ok)
			}
			assertNoSentinels(t, err.Error(), h.logs.String())
		})
	}
}

func TestTypeSafeInvalidRequestIsNotRetriedAndHoldsNoBodyText(t *testing.T) {
	for _, status := range []int{400, 422} {
		h := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			// A 422 detail may quote the offending request field: that is source text.
			_, _ = w.Write([]byte(`{"detail":[{"loc":["body","state"],"input":"` + tsStateText + `"}]}`))
		})
		_, err := h.client.SendBody(context.Background(), tsBody)
		se := mustSystemOneError(t, err)
		if se.Class != SystemOneClassInvalid || se.StatusCode != status || se.StopsArm() {
			t.Fatalf("%d: error = %+v", status, se)
		}
		if h.calls.Load() != 1 {
			t.Fatalf("%d: calls = %d, want 1", status, h.calls.Load())
		}
		assertNoSentinels(t, err.Error(), h.logs.String(), unwrapAll(err))
	}
}

func TestTypeSafe422BodyWithRateLimitWordsIsStillInvalidRequest(t *testing.T) {
	// classifyProviderError matches substrings of the BODY before it looks at the
	// status. A 422 whose detail contains "429" or "rate limit" must not become a
	// rate limit and must not be retried.
	h := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(422)
		_, _ = w.Write([]byte(`{"detail":"field 429 rate limit timeout 401 server error 500"}`))
	})
	_, err := h.client.SendBody(context.Background(), tsBody)
	if se := mustSystemOneError(t, err); se.Class != SystemOneClassInvalid || h.calls.Load() != 1 {
		t.Fatalf("class=%s calls=%d, want invalid_request/1", se.Class, h.calls.Load())
	}
}

func TestTypeSafeRetriesRateLimitHonouringRetryAfterThenSucceeds(t *testing.T) {
	response := realResponse(t)
	var n atomic.Int32
	h := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) == 1 {
			w.Header().Set("Retry-After", "3")
			w.Header().Set("x-typesafe-request-id", "req_first")
			w.WriteHeader(429)
			return
		}
		w.Header().Set("x-typesafe-request-id", "req_second")
		_, _ = w.Write(response)
	})
	res, err := h.client.SendBody(context.Background(), tsBody)
	if err != nil {
		t.Fatal(err)
	}
	if res.AttemptCount() != 2 || res.RequestID != "req_second" {
		t.Fatalf("attempts=%d request id=%q", res.AttemptCount(), res.RequestID)
	}
	if res.Attempts[0].StatusCode != 429 || res.Attempts[0].RequestID != "req_first" || res.Attempts[0].Class != "rate_limit" {
		t.Fatalf("first attempt = %+v", res.Attempts[0])
	}
	if len(*h.delays) != 1 || (*h.delays)[0] != 3*time.Second || res.RetryWait() != 3*time.Second {
		t.Fatalf("delays=%v wait=%v, want one 3s Retry-After", *h.delays, res.RetryWait())
	}
	if !strings.Contains(h.logs.String(), `"retrying":true`) || !strings.Contains(h.logs.String(), `"request_id":"req_first"`) {
		t.Fatalf("the failed first attempt was not logged loudly: %s", h.logs.String())
	}
}

func TestTypeSafeRetryAfterIsCapped(t *testing.T) {
	var n atomic.Int32
	response := realResponse(t)
	h := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) == 1 {
			w.Header().Set("Retry-After", "86400")
			w.WriteHeader(429)
			return
		}
		_, _ = w.Write(response)
	})
	if _, err := h.client.SendBody(context.Background(), tsBody); err != nil {
		t.Fatal(err)
	}
	if (*h.delays)[0] != 60*time.Second {
		t.Fatalf("delay = %v, want the 60 s cap", (*h.delays)[0])
	}
}

func TestTypeSafeRetries529And5xxWithBackoffThenGivesUpAfterOneRetry(t *testing.T) {
	for _, status := range []int{529, 500, 503} {
		t.Run(http.StatusText(status)+"/exhausted", func(t *testing.T) {
			h := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("x-typesafe-request-id", "req_srv")
				w.WriteHeader(status)
			})
			_, err := h.client.SendBody(context.Background(), tsBody)
			se := mustSystemOneError(t, err)
			if se.Class != SystemOneClassServer || se.StatusCode != status || h.calls.Load() != 2 || len(se.Attempts) != 2 {
				t.Fatalf("error=%+v calls=%d", se, h.calls.Load())
			}
			if len(*h.delays) != 1 || (*h.delays)[0] != retryDelay(0) {
				t.Fatalf("delays = %v, want the shared backoff %v", *h.delays, retryDelay(0))
			}
			if class, ok := ClassifyLLMError(err); !ok || class != LLMErrorClassServer {
				t.Fatalf("ClassifyLLMError = %v %v", class, ok)
			}
			if strings.Count(h.logs.String(), "attempt failed") != 2 {
				t.Fatalf("want one WARN per failed attempt:\n%s", h.logs.String())
			}
		})
	}
	t.Run("529/recovers", func(t *testing.T) {
		var n atomic.Int32
		response := realResponse(t)
		h := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
			if n.Add(1) == 1 {
				w.WriteHeader(529)
				return
			}
			_, _ = w.Write(response)
		})
		res, err := h.client.SendBody(context.Background(), tsBody)
		if err != nil || res.AttemptCount() != 2 || res.Attempts[1].WaitBefore != retryDelay(0) {
			t.Fatalf("res=%+v err=%v", res.Attempts, err)
		}
	})
}

func TestTypeSafeTimeoutIsClassifiedRetriedOnceAndLoggedWithTheConfiguredValue(t *testing.T) {
	h := newTS(t, func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}, func(c *TypeSafeClientConfig) { c.Timeout = 60 * time.Millisecond })
	_, err := h.client.SendBody(context.Background(), tsBody)
	se := mustSystemOneError(t, err)
	if se.Class != SystemOneClassTimeout || len(se.Attempts) != 2 || h.calls.Load() != 2 {
		t.Fatalf("error=%+v calls=%d", se, h.calls.Load())
	}
	if class, _ := ClassifyLLMError(err); class != LLMErrorClassOther {
		t.Fatalf("a timeout is Other for ClassifyLLMError, got %v", class)
	}
	// Loud: the timeout that won is logged next to the measured latency.
	if !strings.Contains(h.logs.String(), `"class":"timeout"`) || !strings.Contains(h.logs.String(), `"timeout":60000000`) {
		t.Fatalf("timeout not logged with its configured value:\n%s", h.logs.String())
	}
}

type timeoutRoundTripper struct{}

type netTimeout struct{}

func (netTimeout) Error() string   { return "i/o timeout" }
func (netTimeout) Timeout() bool   { return true }
func (netTimeout) Temporary() bool { return true }

func (timeoutRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, &net.OpError{Op: "read", Net: "tcp", Err: netTimeout{}}
}

// A network timeout that is not the client's own deadline (a handshake or read
// timeout) is a timeout too, and is retried once like one.
func TestTypeSafeNetworkTimeoutIsClassifiedAsTimeout(t *testing.T) {
	client, err := NewTypeSafeClient(TypeSafeClientConfig{APIKey: secrets.NewHidden(tsKey), UnsafeAllowAnyBaseURLForTest: true,
		BaseURL: "http://127.0.0.1:1", HTTPClient: &http.Client{Transport: timeoutRoundTripper{}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	client.sleep = func(context.Context, time.Duration) bool { return true }
	_, err = client.SendBody(context.Background(), tsBody)
	if se := mustSystemOneError(t, err); se.Class != SystemOneClassTimeout || len(se.Attempts) != 2 {
		t.Fatalf("error = %+v", se)
	}
}

func TestTypeSafeNeverFollowsARedirectAndTheOtherOriginSeesNothing(t *testing.T) {
	for _, supplied := range []bool{false, true} {
		name := "default client"
		if supplied {
			name = "supplied client of the default redirect policy"
		}
		t.Run(name, func(t *testing.T) {
			probe := redirectprobe.New(t)
			cfg := TypeSafeClientConfig{APIKey: secrets.NewHidden("SECRET"), BaseURL: probe.Base.URL, UnsafeAllowAnyBaseURLForTest: true,
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			if supplied {
				cfg.HTTPClient = probe.Client()
			}
			client, err := NewTypeSafeClient(cfg)
			if err != nil {
				t.Fatal(err)
			}
			client.sleep = func(context.Context, time.Duration) bool { return true }
			_, err = client.SendBody(context.Background(), tsBody)
			se := mustSystemOneError(t, err)
			if se.Class != SystemOneClassUnexpected || len(se.Attempts) != 1 {
				t.Fatalf("a refused redirect must end as unexpected_status after one attempt, got %+v", se)
			}
			probe.Assert(t)
		})
	}
}

func TestTypeSafeMalformedJSONIsDecodeFailureWithoutRetryOrBodyText(t *testing.T) {
	for name, body := range map[string]string{
		"truncated":  `{"model":"jev-1.13.0","answers":{"q":{"type":"noul","noul":0.5,"x":"` + tsAnswerText,
		"not json":   tsAnswerText,
		"json array": `[` + `"` + tsAnswerText + `"]`,
	} {
		t.Run(name, func(t *testing.T) {
			h := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("x-typesafe-request-id", "req_bad")
				_, _ = w.Write([]byte(body))
			})
			_, err := h.client.SendBody(context.Background(), tsBody)
			se := mustSystemOneError(t, err)
			if se.Class != SystemOneClassDecode || se.StatusCode != 200 || se.RequestID != "req_bad" || h.calls.Load() != 1 {
				t.Fatalf("error=%+v calls=%d", se, h.calls.Load())
			}
			assertNoSentinels(t, err.Error(), h.logs.String(), unwrapAll(err))
		})
	}
}

func TestTypeSafeMissingUsageIsReportedNotZero(t *testing.T) {
	h := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("x-typesafe-request-id", "req_nousage")
		_, _ = w.Write([]byte(`{"model":"jev-1.13.0","answers":{}}`))
	})
	res, err := h.client.SendBody(context.Background(), tsBody)
	if err != nil {
		t.Fatal(err)
	}
	if res.Usage.Reported || res.Usage.InputTokens != 0 {
		t.Fatalf("usage = %+v, want Reported=false", res.Usage)
	}
	if !strings.Contains(h.logs.String(), "carried no usage") || !strings.Contains(h.logs.String(), `"level":"WARN"`) {
		t.Fatalf("missing usage is not loud:\n%s", h.logs.String())
	}
	// A usage object with only output tokens is also "not reported": input is what is billed.
	h2 := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"model":"jev-1.13.0","answers":{},"usage":{"output_tokens":7}}`))
	})
	if res2, err := h2.client.SendBody(context.Background(), tsBody); err != nil || res2.Usage.Reported {
		t.Fatalf("res=%+v err=%v", res2.Usage, err)
	}
}

func TestTypeSafeBoundsTheResponseRead(t *testing.T) {
	h := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"model":"jev-1.13.0","pad":"`))
		chunk := bytes.Repeat([]byte("x"), 1<<20)
		for i := 0; i < 5; i++ {
			_, _ = w.Write(chunk)
		}
		_, _ = w.Write([]byte(`"}`))
	})
	_, err := h.client.SendBody(context.Background(), tsBody)
	if se := mustSystemOneError(t, err); se.Class != SystemOneClassTooLarge || h.calls.Load() != 1 {
		t.Fatalf("error=%+v calls=%d", se, h.calls.Load())
	}
	// Exactly at the bound is accepted.
	h2 := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
		head, tail := `{"model":"jev-1.13.0","pad":"`, `"}`
		_, _ = w.Write([]byte(head))
		_, _ = w.Write(bytes.Repeat([]byte("x"), maxSystemOneResponseBytes-len(head)-len(tail)))
		_, _ = w.Write([]byte(tail))
	})
	if res, err := h2.client.SendBody(context.Background(), tsBody); err != nil || len(res.Body) != maxSystemOneResponseBytes {
		t.Fatalf("a body of exactly the bound must pass: len=%d err=%v", len(res.Body), err)
	}
}

// The bound is on what is READ, not only on what is judged afterwards: a peer
// that never stops sending must be cut off, not buffered to its end.
func TestTypeSafeStopsReadingAnEndlessBody(t *testing.T) {
	const capBytes = 48 << 20
	var written atomic.Int64
	h := newTS(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		chunk := bytes.Repeat([]byte("x"), 64<<10)
		for written.Load() < capBytes {
			n, err := w.Write(chunk)
			written.Add(int64(n))
			if err != nil {
				return
			}
		}
	})
	_, err := h.client.SendBody(context.Background(), tsBody)
	if se := mustSystemOneError(t, err); se.Class != SystemOneClassTooLarge {
		t.Fatalf("error = %+v", se)
	}
	if got := written.Load(); got >= capBytes/2 {
		t.Fatalf("the server wrote %d bytes before the client hung up; the read is not bounded", got)
	}
}

func TestTypeSafeCancelDuringBackoffReturnsCanceledAndTheContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	h := newTS(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(529) })
	h.client.sleep = func(ctx context.Context, _ time.Duration) bool {
		cancel()
		return sleepForRetry(ctx, time.Hour)
	}
	_, err := h.client.SendBody(ctx, tsBody)
	se := mustSystemOneError(t, err)
	if se.Class != SystemOneClassCanceled || !errors.Is(err, context.Canceled) || h.calls.Load() != 1 || len(se.Attempts) != 1 {
		t.Fatalf("error=%+v is-canceled=%v calls=%d attempts=%d", se, errors.Is(err, context.Canceled), h.calls.Load(), len(se.Attempts))
	}
}

func TestTypeSafeCancelDuringRequestIsNotRetried(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	h := newTS(t, func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // the server notices a closed connection only after the body is read
		cancel()
		<-r.Context().Done()
	})
	_, err := h.client.SendBody(ctx, tsBody)
	se := mustSystemOneError(t, err)
	if se.Class != SystemOneClassCanceled || !errors.Is(err, context.Canceled) || h.calls.Load() != 1 {
		t.Fatalf("error=%+v calls=%d", se, h.calls.Load())
	}
}

func TestTypeSafeTransportFailureIsRetriedAndNamesNoURL(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	url := server.URL
	server.Close()
	logs := &bytes.Buffer{}
	client, err := NewTypeSafeClient(TypeSafeClientConfig{APIKey: secrets.NewHidden(tsKey), BaseURL: url, UnsafeAllowAnyBaseURLForTest: true,
		Logger: slog.New(slog.NewJSONHandler(logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	client.sleep = func(context.Context, time.Duration) bool { return true }
	_, err = client.SendBody(context.Background(), tsBody)
	se := mustSystemOneError(t, err)
	if se.Class != SystemOneClassTransport || len(se.Attempts) != 2 {
		t.Fatalf("error=%+v", se)
	}
	assertNoSentinels(t, err.Error(), logs.String(), unwrapAll(err))
	if strings.Contains(err.Error()+logs.String(), url) {
		t.Fatalf("the request URL leaked: %s %s", err, logs)
	}
}

func TestTypeSafeRequestIDIsSanitizedBeforeItIsLogged(t *testing.T) {
	h := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("x-typesafe-request-id", "req_1\"}{ injected "+tsKey+strings.Repeat("a", 200))
		w.WriteHeader(422)
	})
	_, err := h.client.SendBody(context.Background(), tsBody)
	se := mustSystemOneError(t, err)
	if len(se.RequestID) > maxLoggedRequestIDLen || strings.ContainsAny(se.RequestID, "\"{} ") {
		t.Fatalf("request id = %q", se.RequestID)
	}
}

func TestTypeSafeSendBodyRefusesInvalidJSONWithoutSending(t *testing.T) {
	h := newTS(t, func(w http.ResponseWriter, _ *http.Request) {})
	_, err := h.client.SendBody(context.Background(), []byte(`{"state":`+tsStateText))
	if se := mustSystemOneError(t, err); se.Class != SystemOneClassBuildRequest || h.calls.Load() != 0 {
		t.Fatalf("error=%+v calls=%d", se, h.calls.Load())
	}
	assertNoSentinels(t, err.Error())
}

// No key, request text or answer text reaches a log line or an error text on
// any path: success, every failure class, a hostile peer that echoes them.
func TestTypeSafeNeverLogsTheKeyOrAnyRequestOrResponseText(t *testing.T) {
	hostile := func(status int, body string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			echoed, _ := io.ReadAll(r.Body)
			w.Header().Set("x-typesafe-request-id", "req_h")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body + string(echoed) + r.Header.Get("Authorization")))
		}
	}
	for name, handler := range map[string]http.HandlerFunc{
		"success": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"model":"jev-1.13.0","answers":{"q":{"note":"` + tsAnswerText + `"}},"usage":{"input_tokens":1,"output_tokens":1}}`))
		},
		"401": hostile(401, tsAnswerText), "422": hostile(422, tsAnswerText), "429": hostile(429, tsAnswerText),
		"529": hostile(529, tsAnswerText), "500": hostile(500, tsAnswerText), "302": hostile(302, tsAnswerText),
		"200 malformed": hostile(200, tsAnswerText),
	} {
		t.Run(name, func(t *testing.T) {
			h := newTS(t, handler)
			var buf bytes.Buffer
			res, err := h.client.SendBody(context.Background(), tsBody)
			if err != nil {
				buf.WriteString(err.Error() + unwrapAll(err))
			}
			buf.WriteString(h.logs.String())
			// The result's own printed forms must not hold the key either.
			buf.WriteString(strings.TrimSpace(string(mustJSON(t, res.Attempts))))
			assertNoSentinels(t, buf.String())
			if h.logs.Len() == 0 {
				t.Fatal("nothing was logged: the test measured nothing")
			}
		})
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func assertNoSentinels(t *testing.T, texts ...string) {
	t.Helper()
	for _, text := range texts {
		for _, sentinel := range []string{tsKey, tsStateText, tsAnswerText, "Bearer "} {
			if strings.Contains(text, sentinel) {
				t.Fatalf("%q leaked into %q", sentinel, text)
			}
		}
	}
}

// unwrapAll renders every layer of an error chain, as a caller that unwraps
// (errors.As to the status error, as the classifier does) would see it.
func unwrapAll(err error) string {
	var b strings.Builder
	for err != nil {
		b.WriteString(err.Error())
		b.WriteByte('|')
		var hs *httpStatusError
		if errors.As(err, &hs) {
			b.WriteString(hs.body)
		}
		err = errors.Unwrap(err)
	}
	return b.String()
}

func TestNewTypeSafeClientRefusesAnUnsafeConfig(t *testing.T) {
	key := secrets.NewHidden(tsKey)
	bad := map[string]TypeSafeClientConfig{
		"no key":          {},
		"other host":      {APIKey: key, BaseURL: "https://evil.example"},
		"http":            {APIKey: key, BaseURL: "http://api.typesafe.ai"},
		"userinfo":        {APIKey: key, BaseURL: "https://user:pw@api.typesafe.ai"},
		"port":            {APIKey: key, BaseURL: "https://api.typesafe.ai:8443"},
		"path":            {APIKey: key, BaseURL: "https://api.typesafe.ai/v1"},
		"suffix host":     {APIKey: key, BaseURL: "https://api.typesafe.ai.evil.example"},
		"unparseable":     {APIKey: key, BaseURL: "https://api.typesafe.ai\x7f"},
		"query":           {APIKey: key, BaseURL: "https://api.typesafe.ai?next=https://evil.example"},
		"fragment":        {APIKey: key, BaseURL: "https://api.typesafe.ai#frag"},
		"prefixed":        {APIKey: key, Model: "my-jev-1.13.0"},
		"alias latest":    {APIKey: key, Model: "jev-latest"},
		"alias preview":   {APIKey: key, Model: "jev-preview"},
		"not versioned":   {APIKey: key, Model: "jev-1.13"},
		"another family":  {APIKey: key, Model: "gpt-5-nano"},
		"trailing suffix": {APIKey: key, Model: "jev-1.13.0-beta"},
	}
	for name, cfg := range bad {
		t.Run(name, func(t *testing.T) {
			client, err := NewTypeSafeClient(cfg)
			if err == nil || client != nil {
				t.Fatalf("accepted %+v", cfg)
			}
			if strings.Contains(err.Error(), "evil.example") || strings.Contains(err.Error(), "pw@") || strings.Contains(err.Error(), tsKey) {
				t.Fatalf("error repeats the configured value: %v", err)
			}
		})
	}
	for name, cfg := range map[string]TypeSafeClientConfig{
		"defaults":      {APIKey: key},
		"trailing /":    {APIKey: key, BaseURL: "https://api.typesafe.ai/"},
		"upper host":    {APIKey: key, BaseURL: "https://API.typesafe.ai"},
		"pinned model":  {APIKey: key, Model: "jev-2.0.1"},
		"explicit base": {APIKey: key, BaseURL: DefaultTypeSafeBaseURL, Model: DefaultTypeSafeModel},
	} {
		t.Run("ok/"+name, func(t *testing.T) {
			client, err := NewTypeSafeClient(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if client.cfg.BaseURL != DefaultTypeSafeBaseURL {
				t.Fatalf("base = %q", client.cfg.BaseURL)
			}
		})
	}
}

func TestTypeSafeClientFromEnvReadsOnlyTheTypeSafeNames(t *testing.T) {
	for _, name := range []string{"TYPESAFE_API_KEY", "TYPESAFE_BASE_URL", "TYPESAFE_MODEL", "LLM_API_KEY", "LLM_BASE_URL", "LLM_MODEL", "OPENAI_API_KEY"} {
		t.Setenv(name, "")
	}
	if _, err := NewTypeSafeClientFromEnv("", nil); err == nil || !strings.Contains(err.Error(), "TYPESAFE_API_KEY") {
		t.Fatalf("no key: err = %v", err)
	}
	// The served provider's generic overrides must not steer the shadow client.
	t.Setenv("LLM_API_KEY", "generic-key-must-not-be-used")
	t.Setenv("OPENAI_API_KEY", "openai-key-must-not-be-used")
	t.Setenv("LLM_MODEL", "gpt-5-nano")
	t.Setenv("LLM_BASE_URL", "https://evil.example")
	if _, err := NewTypeSafeClientFromEnv("", nil); err == nil {
		t.Fatal("built a client without TYPESAFE_API_KEY from another provider's key")
	}
	t.Setenv("TYPESAFE_API_KEY", tsKey)
	client, err := NewTypeSafeClientFromEnv("", nil)
	if err != nil {
		t.Fatal(err)
	}
	if client.Model() != DefaultTypeSafeModel || client.cfg.BaseURL != DefaultTypeSafeBaseURL || client.cfg.APIKey.Reveal() != tsKey {
		t.Fatalf("model=%q base=%q", client.Model(), client.cfg.BaseURL)
	}
	t.Setenv("TYPESAFE_MODEL", "jev-1.14.0")
	if c, _ := NewTypeSafeClientFromEnv("", nil); c == nil || c.Model() != "jev-1.14.0" {
		t.Fatal("TYPESAFE_MODEL ignored")
	}
	if c, _ := NewTypeSafeClientFromEnv("jev-1.15.0", nil); c == nil || c.Model() != "jev-1.15.0" {
		t.Fatal("an explicit model must win over TYPESAFE_MODEL")
	}
	t.Setenv("TYPESAFE_MODEL", "jev-latest")
	if _, err := NewTypeSafeClientFromEnv("", nil); err == nil {
		t.Fatal("jev-latest accepted from the environment")
	}
	t.Setenv("TYPESAFE_MODEL", "")
	t.Setenv("TYPESAFE_BASE_URL", "https://evil.example")
	if _, err := NewTypeSafeClientFromEnv("", nil); err == nil {
		t.Fatal("another host accepted from the environment; there is no env switch for the test-only override")
	}
}

func TestTypeSafeStructsRedactTheAPIKey(t *testing.T) {
	cfg := TypeSafeClientConfig{APIKey: secrets.NewHidden(llmKeySentinel), Model: DefaultTypeSafeModel}
	client, err := NewTypeSafeClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for name, subject := range map[string]any{
		"TypeSafeClientConfig": cfg, "*TypeSafeClientConfig": &cfg,
		"TypeSafeClient": client, "TypeSafeClient(value)": *client,
	} {
		assertNoKey(t, name, subject)
	}
}

func TestTypeSafeKindIsNamedButNeverAProviderNorAutoDetected(t *testing.T) {
	if IsProviderKindImplemented(ProviderKindTypeSafe) {
		t.Fatal("typesafe must not be in goImplementedProviderKinds: the served path would try to build it")
	}
	for _, kind := range ImplementedProviderKinds() {
		if kind == ProviderKindTypeSafe {
			t.Fatal("ImplementedProviderKinds lists typesafe")
		}
	}
	if _, err := NewProviderFromEnv(ProviderKindTypeSafe); err == nil || !strings.Contains(err.Error(), "NewTypeSafeClientFromEnv") {
		t.Fatalf("NewProviderFromEnv(typesafe) err = %v", err)
	}
	if _, err := NewProviderFromCredentials(ProviderKindTypeSafe, "k", "", ""); err == nil {
		t.Fatal("NewProviderFromCredentials(typesafe) built a Provider")
	}
	// A present key never selects Jev by auto-detection.
	for _, name := range []string{"LLM_PROVIDER", "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "LOCAL_LLM_BASE_URL", "DASHSCOPE_API_KEY", "QWEN_API_KEY", "OLLAMA_MODEL", "OLLAMA_BASE_URL"} {
		t.Setenv(name, "")
	}
	t.Setenv("TYPESAFE_API_KEY", tsKey)
	if kind, err := ResolveProviderKind(""); err == nil {
		t.Fatalf("auto resolved to %q from a TypeSafe key alone", kind)
	}
	if kind, err := ResolveProviderKind("typesafe"); err != nil || kind != ProviderKindTypeSafe {
		t.Fatalf("an explicit request names the kind: %q %v", kind, err)
	}
}

// decisionTransport is the interface the decision adapter (T1) declares in its
// own package; the client satisfies it with stdlib types only.
type decisionTransport interface {
	PostSystemOne(ctx context.Context, body []byte) (response []byte, header http.Header, err error)
}

var _ decisionTransport = (*TypeSafeClient)(nil)

func TestPostSystemOneReturnsTheBodyAndHeaderOfTheFinal200(t *testing.T) {
	response := realResponse(t)
	var n atomic.Int32
	h := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) == 1 {
			w.Header().Set("x-typesafe-request-id", "req_first")
			w.WriteHeader(529)
			return
		}
		w.Header().Set("X-Typesafe-Request-Id", "req_final")
		_, _ = w.Write(response)
	})
	got, header, err := h.client.PostSystemOne(context.Background(), tsBody)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, response) || header.Get("X-Typesafe-Request-Id") != "req_final" || h.calls.Load() != 2 {
		t.Fatalf("header=%v calls=%d", header, h.calls.Load())
	}
}

func TestPostSystemOneHandsANonJSON200BackWithoutJudgingIt(t *testing.T) {
	h := newTS(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("<html>gateway page</html>")) })
	got, header, err := h.client.PostSystemOne(context.Background(), tsBody)
	if err != nil || string(got) != "<html>gateway page</html>" || header == nil || h.calls.Load() != 1 {
		t.Fatalf("got=%q err=%v calls=%d", got, err, h.calls.Load())
	}
	// SendBody stays strict.
	if _, err := h.client.SendBody(context.Background(), tsBody); mustSystemOneError(t, err).Class != SystemOneClassDecode {
		t.Fatalf("SendBody err = %v", err)
	}
}

func TestPostSystemOneErrorsFeedTheSharedFailureVocabulary(t *testing.T) {
	for _, tc := range []struct {
		status        int
		class         string
		deterministic bool
		calls         int32
	}{
		{401, "auth", true, 1}, {402, "missing_key", true, 1}, {403, "missing_key", true, 1},
		{429, "rate_limit", false, 2}, {529, "server_error", false, 2}, {422, "llm_error", false, 1},
	} {
		h := newTS(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(tc.status) })
		_, _, err := h.client.PostSystemOne(context.Background(), tsBody)
		if err == nil || IsDeterministicFailure(err) != tc.deterministic || h.calls.Load() != tc.calls {
			t.Fatalf("%d: err=%v deterministic=%v calls=%d", tc.status, err, IsDeterministicFailure(err), h.calls.Load())
		}
		if got := FailureClass(err); got != tc.class && !(tc.status == 401 && got == "missing_key") {
			t.Fatalf("%d: FailureClass = %q, want %q", tc.status, got, tc.class)
		}
	}
}

func TestPostSystemOneCancelAndDeadlineAreTheContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	h := newTS(t, func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		cancel()
		<-r.Context().Done()
	})
	if _, _, err := h.client.PostSystemOne(ctx, tsBody); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	dctx, dcancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer dcancel()
	h2 := newTS(t, func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	})
	if _, _, err := h2.client.PostSystemOne(dctx, tsBody); !errors.Is(err, context.DeadlineExceeded) || h2.calls.Load() != 1 {
		t.Fatalf("err = %v calls=%d", err, h2.calls.Load())
	}
}
