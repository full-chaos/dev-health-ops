package providersync

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// wireCount sends each request through a counting Doer over a real net/http
// transport and returns what the ledger counted.
func wireCount(t *testing.T, doer *http.Client, requests ...func(context.Context) *http.Request) RequestUsageTotals {
	t.Helper()
	ledger := NewRequestLedger()
	ctx := WithRequestLedger(context.Background(), ledger)
	counting := CountRequests(doer)
	for _, build := range requests {
		response, err := counting.Do(build(ctx))
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
	}
	return ledger.Totals()
}

func getRequest(t *testing.T, url string) func(context.Context) *http.Request {
	return func(ctx context.Context) *http.Request {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		return request
	}
}

// TestWireCountMatchesWhatTheServerReceived runs the transport behaviours
// below the Doer against real servers: each row's counted requests equal the
// requests the server read.
func TestWireCountMatchesWhatTheServerReceived(t *testing.T) {
	t.Parallel()

	t.Run("transport re-sends on a dead reused connection", func(t *testing.T) {
		t.Parallel()
		var received atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if received.Add(1) == 2 {
				connection, _, err := writer.(http.Hijacker).Hijack()
				if err == nil {
					_ = connection.Close()
				}
				return
			}
			writer.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(server.Close)
		got := wireCount(t, server.Client(), getRequest(t, server.URL), getRequest(t, server.URL))
		if int(received.Load()) != 3 || got.Requests != 3 || got.Responses != 2 {
			t.Fatalf("server received %d, counted %+v", received.Load(), got)
		}
	})

	t.Run("redirect followed and not followed", func(t *testing.T) {
		t.Parallel()
		var received atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			received.Add(1)
			if request.URL.Path == "/from" {
				http.Redirect(writer, request, "/to", http.StatusFound)
				return
			}
			writer.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(server.Close)
		followed := wireCount(t, server.Client(), getRequest(t, server.URL+"/from"))
		if received.Load() != 2 || followed != (RequestUsageTotals{Requests: 2, Responses: 1}) {
			t.Fatalf("followed: server received %d, counted %+v", received.Load(), followed)
		}
		stopping := &http.Client{Transport: server.Client().Transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		notFollowed := wireCount(t, stopping, getRequest(t, server.URL+"/from"))
		if received.Load() != 3 || notFollowed != (RequestUsageTotals{Requests: 1, Responses: 1}) {
			t.Fatalf("not followed: server received %d, counted %+v", received.Load(), notFollowed)
		}
	})

	// A named limit: whether the transport reports the write of a request the
	// server refuses on Expect: 100-continue before its body depends on
	// timing, so the send counts 0 or 1 times, never more. No provider request
	// sets Expect.
	t.Run("expect-continue refused before the body counts at most once", func(t *testing.T) {
		t.Parallel()
		var received atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			received.Add(1)
			writer.WriteHeader(http.StatusExpectationFailed)
		}))
		t.Cleanup(server.Close)
		client := server.Client()
		client.Transport.(*http.Transport).ExpectContinueTimeout = time.Second
		got := wireCount(t, client, func(ctx context.Context) *http.Request {
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, strings.NewReader(`{"large":"body"}`))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Expect", "100-continue")
			return request
		})
		if received.Load() != 1 || got.Requests > 1 || got.Responses != 1 {
			t.Fatalf("server received %d, counted %+v", received.Load(), got)
		}
	})

	t.Run("rewindable body re-sent on a dead reused connection", func(t *testing.T) {
		t.Parallel()
		var received atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			_, _ = io.Copy(io.Discard, request.Body)
			if received.Add(1) == 2 {
				connection, _, err := writer.(http.Hijacker).Hijack()
				if err == nil {
					_ = connection.Close()
				}
				return
			}
			writer.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(server.Close)
		post := func(ctx context.Context) *http.Request {
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader([]byte(`{"query":"q"}`)))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Idempotency-Key", "ABC-123")
			return request
		}
		got := wireCount(t, server.Client(), post, post)
		if got.Requests != int(received.Load()) || got.Requests < 2 {
			t.Fatalf("server received %d, counted %+v", received.Load(), got)
		}
	})

	t.Run("HTTP/2 over TLS", func(t *testing.T) {
		t.Parallel()
		var received atomic.Int32
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			received.Add(1)
			if request.ProtoMajor != 2 {
				writer.WriteHeader(http.StatusHTTPVersionNotSupported)
				return
			}
			writer.WriteHeader(http.StatusOK)
		}))
		server.EnableHTTP2 = true
		server.StartTLS()
		t.Cleanup(server.Close)
		got := wireCount(t, server.Client(), getRequest(t, server.URL), getRequest(t, server.URL+"/graphql"), getRequest(t, server.URL))
		if received.Load() != 3 || got != (RequestUsageTotals{Requests: 3, Responses: 3}) {
			t.Fatalf("server received %d, counted %+v", received.Load(), got)
		}
	})

	t.Run("refused dial writes nothing", func(t *testing.T) {
		t.Parallel()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		_ = listener.Close()
		got := wireCount(t, &http.Client{}, getRequest(t, "http://"+address+"/"))
		if got != (RequestUsageTotals{}) {
			t.Fatalf("counted %+v for a request that never reached a server", got)
		}
	})

	t.Run("response lost after the request was written", func(t *testing.T) {
		t.Parallel()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })
		var received atomic.Int32
		go func() {
			for {
				connection, err := listener.Accept()
				if err != nil {
					return
				}
				_, _ = http.ReadRequest(bufio.NewReader(connection))
				received.Add(1)
				_ = connection.Close()
			}
		}()
		got := wireCount(t, &http.Client{}, getRequest(t, "http://"+listener.Addr().String()+"/"))
		if got.Requests != int(received.Load()) || got.Requests < 1 || got.Responses != 0 {
			t.Fatalf("server read %d, counted %+v", received.Load(), got)
		}
	})
}

// passThroughDoer stands for any Doer between two counting Doers.
type passThroughDoer struct {
	delegate interface {
		Do(*http.Request) (*http.Response, error)
	}
}

func (doer passThroughDoer) Do(request *http.Request) (*http.Response, error) {
	return doer.delegate.Do(request)
}

func TestNestedCountingDoersCountOneSendOnce(t *testing.T) {
	t.Parallel()
	var received atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		received.Add(1)
		writer.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	ledger := NewRequestLedger()
	ctx := WithRequestLedger(context.Background(), ledger)
	nested := CountRequests(passThroughDoer{delegate: CountRequests(server.Client())})
	for range 3 {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := nested.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
	}
	if received.Load() != 3 || ledger.Totals() != (RequestUsageTotals{Requests: 3, Responses: 3}) {
		t.Fatalf("server received %d, counted %+v", received.Load(), ledger.Totals())
	}
}

// blockingWriter never returns from Write until released: a log pipe nobody
// reads.
type blockingWriter struct{ release chan struct{} }

func (output blockingWriter) Write(p []byte) (int, error) {
	<-output.release
	return len(p), nil
}

// TestUsageRecordingNeverHoldsAUnitsPath gives an execution a writer whose
// store hangs, whose own log output is blocked, and whose queue is already
// full. The execution still returns at the speed of the same execution
// without usage recording, with the route's own result; the spend it could
// not hand off is dropped and counted.
func TestUsageRecordingNeverHoldsAUnitsPath(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	descriptor, _ := Descriptor("launchdarkly", "feature-flags")
	run := func(writer *RequestUsageWriter) (time.Duration, CompleteRouteExecutionResult, error) {
		claim, session := completeRouteSession(t, now, false)
		executor := completeRouteExecutor(now, &spendingCompleteRouteHandler{
			calls: 2, batch: completeRouteFixture(t, claim),
		}, &memoryEffectLedger{}, &memoryEffectSink{})
		executor.Doer = &scriptedDoer{}
		executor.RequestUsage = writer
		started := time.Now()
		result, err := executor.Execute(context.Background(), session, descriptor)
		return time.Since(started), result, err
	}
	baseline, baseResult, baseErr := run(nil)

	hung := &hangingRequestUsageSink{release: make(chan struct{}), started: make(chan struct{})}
	pipe := blockingWriter{release: make(chan struct{})}
	t.Cleanup(func() {
		close(hung.release)
		close(pipe.release)
	})
	writer := NewRequestUsageWriter(hung, slog.New(slog.NewTextHandler(pipe, nil)))
	writer.cadence = time.Millisecond
	writer.writeTimeout = 10 * time.Millisecond
	writer.Start()
	writer.Offer([]RequestUsageRow{{Transport: requestTransportREST, Requests: 1}})
	<-hung.started
	for len(writer.batches) < cap(writer.batches) {
		writer.Offer([]RequestUsageRow{{Transport: requestTransportREST, Requests: 1}})
	}
	droppedBefore := writer.droppedRequests.Load()

	elapsed, result, err := run(writer)
	if err != nil || baseErr != nil || result.Effects != baseResult.Effects || !result.Comparison.Match ||
		result.RequestUsage != (RequestUsageTotals{Requests: 2, Responses: 2}) {
		t.Fatalf("unit outcome changed: err=%v result=%+v baseline=%+v", err, result, baseResult)
	}
	if elapsed > baseline+100*time.Millisecond {
		t.Fatalf("execution took %s with a stuck writer, %s without one", elapsed, baseline)
	}
	if dropped := writer.droppedRequests.Load() - droppedBefore; dropped != 2 {
		t.Fatalf("hand-off not dropped and counted: %d", dropped)
	}
}
