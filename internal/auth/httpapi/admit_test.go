package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type admitCountingBody struct {
	r io.Reader
	n int
}

func (b *admitCountingBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n += n
	return n, err
}

func (b *admitCountingBody) Close() error { return nil }

type admitKey struct{}

// admitRequest is a POST whose body counts the bytes read from it; chunked is
// an undeclared length (ContentLength -1).
func admitRequest(payload string, chunked bool) (*http.Request, *admitCountingBody) {
	body := &admitCountingBody{r: strings.NewReader(payload)}
	req := httptest.NewRequest(http.MethodPost, "/x", body)
	req.ContentLength = int64(len(payload))
	if chunked {
		req.ContentLength = -1
	}
	return req, body
}

func admitChain(route Route, limit int64, burst int) http.Handler {
	options := ServerOptions{MaxBodyBytes: limit, RateLimit: 0.001, RateLimitBurst: burst, RequestTimeout: time.Second}
	return routeChain(route, options, slog.New(slog.DiscardHandler), WriteError, make(chan struct{}, 4))
}

func TestAdmitRefusalAnswersBeforeAnyBodyByteIsRead(t *testing.T) {
	for _, chunked := range []bool{true, false} {
		served := 0
		route := Route{Method: http.MethodPost, Pattern: "/x",
			Admit: func(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
				w.WriteHeader(http.StatusTeapot)
				return nil, false
			},
			Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) { served++ })}
		req, body := admitRequest(strings.Repeat("a", 10), chunked)
		rec := httptest.NewRecorder()
		admitChain(route, 64, 10).ServeHTTP(rec, req)
		if rec.Code != http.StatusTeapot || served != 0 || body.n != 0 {
			t.Errorf("chunked=%v: a refusing Admit gave status %d, served %d, body bytes read %d; want the gate's status, 0, 0", chunked, rec.Code, served, body.n)
		}
	}
}

func TestAdmitPassesItsRequestOnAndTheBoundStillHolds(t *testing.T) {
	var seen any
	route := Route{Method: http.MethodPost, Pattern: "/x",
		Admit: func(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
			return r.WithContext(context.WithValue(r.Context(), admitKey{}, "caller")), true
		},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seen = r.Context().Value(admitKey{})
			io.Copy(io.Discard, r.Body)
		})}
	req, _ := admitRequest("0123456789", true)
	rec := httptest.NewRecorder()
	admitChain(route, 64, 10).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || seen != "caller" {
		t.Fatalf("an admitted request: status %d, context value %v; want 200 and the gate's value", rec.Code, seen)
	}
	for _, chunked := range []bool{true, false} {
		served := 0
		route.Handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) { served++ })
		req, _ := admitRequest(strings.Repeat("a", 65), chunked)
		rec := httptest.NewRecorder()
		admitChain(route, 64, 10).ServeHTTP(rec, req)
		if rec.Code != http.StatusRequestEntityTooLarge || served != 0 {
			t.Errorf("chunked=%v: an admitted oversize body gave status %d, served %d; want 413 and 0", chunked, rec.Code, served)
		}
	}
}

func TestAdmitRunsAfterTheRateLimit(t *testing.T) {
	calls := 0
	route := Route{Method: http.MethodPost, Pattern: "/x",
		Admit: func(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
			calls++
			return r, true
		},
		Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})}
	chain := admitChain(route, 64, 1)
	for i := 0; i < 2; i++ {
		req, _ := admitRequest("{}", false)
		rec := httptest.NewRecorder()
		chain.ServeHTTP(rec, req)
		if want := []int{http.StatusOK, http.StatusTooManyRequests}[i]; rec.Code != want {
			t.Fatalf("request %d: status %d, want %d", i, rec.Code, want)
		}
	}
	if calls != 1 {
		t.Fatalf("Admit ran %d times for 2 requests, one of them rate limited; want 1 (the gate runs after the rate limit)", calls)
	}
}

// A route without Admit is unchanged: an undeclared-length body is buffered
// before the handler runs, and over the bound it never runs.
func TestRouteWithoutAdmitStillReadsTheBodyBeforeTheHandler(t *testing.T) {
	var read int
	route := Route{Method: http.MethodPost, Pattern: "/x",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			read = len(b)
		})}
	req, body := admitRequest("0123456789", true)
	rec := httptest.NewRecorder()
	admitChain(route, 64, 10).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || read != 10 || body.n != 10 {
		t.Fatalf("a route without Admit: status %d, handler read %d, source read %d; want 200, 10, 10", rec.Code, read, body.n)
	}
	served := 0
	route.Handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) { served++ })
	req, _ = admitRequest(strings.Repeat("a", 65), true)
	rec = httptest.NewRecorder()
	admitChain(route, 64, 10).ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge || served != 0 {
		t.Fatalf("a route without Admit, oversize chunked body: status %d, served %d; want 413 and 0", rec.Code, served)
	}
}
