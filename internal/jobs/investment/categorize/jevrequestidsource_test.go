package categorize

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

// A jev attempt names where its request id came from: the jev response header. An empty source means unknown only.
func TestJevAttemptNamesTheHeaderAsTheSourceOfItsRequestID(t *testing.T) {
	h := newTS(t, okHandler(t, "req_abc-123"))
	res, err := h.client.SendBody(context.Background(), tsBody)
	if err != nil {
		t.Fatal(err)
	}
	att := res.Attempts[0]
	if att.RequestID != "req_abc-123" || att.RequestIDSource != "header" {
		t.Fatalf("id %q source %q, want req_abc-123 and header", att.RequestID, att.RequestIDSource)
	}
}

// A retried call: every attempt that carries a header id names the header, the failed one too.
func TestJevAttemptsOfARetriedCallEachNameTheHeader(t *testing.T) {
	response := realResponse(t)
	var n atomic.Int32
	h := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) == 1 {
			w.Header().Set("x-typesafe-request-id", "req_first")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("x-typesafe-request-id", "req_second")
		_, _ = w.Write(response)
	})
	res, err := h.client.SendBody(context.Background(), tsBody)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"req_first", "req_second"} {
		if got := res.Attempts[i]; got.RequestID != want || got.RequestIDSource != "header" {
			t.Fatalf("attempt %d: id %q source %q, want %s and header", i, got.RequestID, got.RequestIDSource, want)
		}
	}
}

// No header, an unusable header: there is no id, so the source stays empty (unknown).
func TestJevAttemptWithNoUsableRequestIDHasNoSource(t *testing.T) {
	response := realResponse(t)
	cases := map[string]string{
		"no header":        "",
		"carries the key":  "req_" + tsKey,
		"not id-shaped":    "not an id!",
		"longer than 64 b": "r" + strings.Repeat("a", 64),
	}
	for name, header := range cases {
		t.Run(name, func(t *testing.T) {
			h := newTS(t, func(w http.ResponseWriter, _ *http.Request) {
				if header != "" {
					w.Header().Set("x-typesafe-request-id", header)
				}
				_, _ = w.Write(response)
			})
			res, err := h.client.SendBody(context.Background(), tsBody)
			if err != nil {
				t.Fatal(err)
			}
			att := res.Attempts[0]
			if att.RequestID != "" || att.RequestIDSource != "" {
				t.Fatalf("id %q source %q, want both empty", att.RequestID, att.RequestIDSource)
			}
		})
	}
}
