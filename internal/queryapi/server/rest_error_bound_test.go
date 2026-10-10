package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
)

// CHAOS-9126 (round 98): the named cause of a bound hit, for the shared writer and
// for the work-unit explain route's own writer.
func TestRESTBoundHitCauses(t *testing.T) {
	exception := func(code int32) error {
		return fmt.Errorf("ClickHouse row iteration failed: %w", &chdriver.Exception{Code: code, Message: "bound"})
	}
	capture := func() (*bytes.Buffer, func()) {
		var buf bytes.Buffer
		log.SetOutput(&buf)
		return &buf, func() { log.SetOutput(os.Stderr) }
	}
	serve := func(t *testing.T, write func(http.ResponseWriter, *http.Request), ctx context.Context) (int, string) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/x?scope_id=a&scope_id=b", nil).WithContext(ctx)
		rec := httptest.NewRecorder()
		write(rec, req)
		return rec.Code, rec.Body.String()
	}
	shared := func(err error) func(http.ResponseWriter, *http.Request) {
		return func(w http.ResponseWriter, r *http.Request) { writeRESTDataUnavailable(w, r, "probe", "org-1", err) }
	}

	for _, tc := range []struct {
		name       string
		err        error
		wantDetail string
		wantLimit  string
	}{
		{"rows 396", exception(396), "Result too large", "limit=max_result_rows=500000"},
		{"rows 158", exception(158), "Result too large", "limit=max_result_rows=500000"},
		{"bytes 307", exception(307), "Query read limit exceeded", "limit=max_bytes_to_read"},
		{"code 396 may be max_result_bytes", exception(396), "Result too large", "code 396 also covers max_result_bytes"},
		{"time 159", exception(159), "Query time limit exceeded", "limit=max_execution_time"},
		{"client deadline", fmt.Errorf("row iteration: %w", context.DeadlineExceeded), "Query time limit exceeded", "limit=max_execution_time"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs, restore := capture()
			code, body := serve(t, shared(tc.err), context.Background())
			restore()
			if code != 503 || !strings.Contains(body, tc.wantDetail) {
				t.Errorf("HTTP %d %s, want 503 %q", code, body, tc.wantDetail)
			}
			if l := logs.String(); !strings.Contains(l, "WARN probe: read hit the") || !strings.Contains(l, tc.wantLimit) || !strings.Contains(l, "scope_ids=2") {
				t.Errorf("WARN line = %q, want the route, %q and scope_ids=2", l, tc.wantLimit)
			}
			if (tc.wantLimit == "limit=max_bytes_to_read" || tc.wantLimit == "limit=max_execution_time") && strings.Contains(logs.String(), "max_result_rows") {
				t.Errorf("a %s bound is logged as a row bound: %q", tc.name, logs.String())
			}
		})
	}

	t.Run("a deadline of the caller's own request is not a bound", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		_, restore := capture()
		code, body := serve(t, shared(fmt.Errorf("row iteration: %w", context.DeadlineExceeded)), ctx)
		restore()
		if code != 503 || !strings.Contains(body, "Data unavailable") {
			t.Errorf("HTTP %d %s, want 503 Data unavailable for the caller's own expired deadline", code, body)
		}
	})
	t.Run("a store that is down is not a bound", func(t *testing.T) {
		_, restore := capture()
		code, body := serve(t, shared(errors.New("connection refused")), context.Background())
		restore()
		if code != 503 || !strings.Contains(body, "Data unavailable") {
			t.Errorf("HTTP %d %s, want 503 Data unavailable", code, body)
		}
	})
	t.Run("work-unit explain names a bound too", func(t *testing.T) {
		_, restore := capture()
		code, body := serve(t, func(w http.ResponseWriter, r *http.Request) {
			writeWorkUnitExplainUnavailable(w, r, "org-1", exception(396))
		}, context.Background())
		restore()
		if code != 503 || !strings.Contains(body, "Result too large") {
			t.Errorf("HTTP %d %s, want 503 Result too large", code, body)
		}
		_, restore = capture()
		code, body = serve(t, func(w http.ResponseWriter, r *http.Request) {
			writeWorkUnitExplainUnavailable(w, r, "org-1", errors.New("boom"))
		}, context.Background())
		restore()
		if code != 503 || !strings.Contains(body, "Explanation unavailable") {
			t.Errorf("HTTP %d %s, want 503 Explanation unavailable for any other failure", code, body)
		}
	})
}
