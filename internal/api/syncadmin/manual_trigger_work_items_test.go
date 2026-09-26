package syncadmin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type fakeCounter struct {
	rows int64
	err  error
}

func (f *fakeCounter) count(context.Context, string) (int64, error) { return f.rows, f.err }

func workItemHandlers(fake *fakeCounter) (*handlers, *bytes.Buffer) {
	var logs bytes.Buffer
	h := &handlers{logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	if fake != nil {
		h.workItems = fake
	}
	return h, &logs
}

func openCount(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != workItemsLimitOpenName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is not an int64 sum", m.Name)
			}
			var total int64
			for _, point := range sum.DataPoints {
				total += point.Value
			}
			return total
		}
	}
	return 0
}

// D2619: the work items check has three answers to a count it cannot read: no
// login at all skips the check (Python without CLICKHOUSE_URI), an UNREACHABLE
// ClickHouse lets the sync through loudly (a warning naming the org and a
// counter), and a login that is not ALLOWED to read the table refuses the sync.
func TestWorkItemsVerdict(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	refusal := func(t *testing.T, err error, status int, contains string) {
		t.Helper()
		var raised *answer
		if !errors.As(err, &raised) || raised.status != status || !strings.Contains(fmt.Sprint(raised.detail), contains) {
			t.Fatalf("want a %d refusal containing %q, got %v", status, contains, err)
		}
	}

	t.Run("no login skips the check", func(t *testing.T) {
		h, _ := workItemHandlers(nil)
		if err := h.workItemsVerdict(context.Background(), "org-1", 10); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("under the cap", func(t *testing.T) {
		h, _ := workItemHandlers(&fakeCounter{rows: 9})
		if err := h.workItemsVerdict(context.Background(), "org-1", 10); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("at the cap is refused (Python: count >= max)", func(t *testing.T) {
		h, _ := workItemHandlers(&fakeCounter{rows: 10})
		refusal(t, h.workItemsVerdict(context.Background(), "org-1", 10), http.StatusForbidden,
			"Work items limit exceeded: 10/10. Upgrade your tier to sync more work items.")
	})
	t.Run("unreachable ClickHouse allows the sync, loudly", func(t *testing.T) {
		before := openCount(t, reader)
		h, logs := workItemHandlers(&fakeCounter{err: &net.OpError{Op: "dial", Err: errors.New("connection refused")}})
		if err := h.workItemsVerdict(context.Background(), "org-unreachable", 10); err != nil {
			t.Fatalf("an unreachable ClickHouse must allow the sync (Python parity): %v", err)
		}
		if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "org-unreachable") {
			t.Errorf("the fall-open must be a warning naming the org, got %q", logs.String())
		}
		if got := openCount(t, reader) - before; got != 1 {
			t.Errorf("the fall-open must count once, counted %d", got)
		}
	})
	t.Run("a timeout allows the sync, loudly", func(t *testing.T) {
		before := openCount(t, reader)
		h, _ := workItemHandlers(&fakeCounter{err: context.DeadlineExceeded})
		if err := h.workItemsVerdict(context.Background(), "org-1", 10); err != nil {
			t.Fatal(err)
		}
		if got := openCount(t, reader) - before; got != 1 {
			t.Errorf("counted %d", got)
		}
	})
	t.Run("a login without the grant fails closed", func(t *testing.T) {
		for _, code := range []int32{497, 516} {
			before := openCount(t, reader)
			h, logs := workItemHandlers(&fakeCounter{err: &clickhouse.Exception{Code: code, Name: "DB::Exception", Message: "not enough privileges"}})
			refusal(t, h.workItemsVerdict(context.Background(), "org-denied", 10), http.StatusServiceUnavailable, "not allowed to read work_items")
			if !strings.Contains(logs.String(), "level=ERROR") || !strings.Contains(logs.String(), "org-denied") {
				t.Errorf("code %d: the refusal must be an ERROR naming the org, got %q", code, logs.String())
			}
			if got := openCount(t, reader) - before; got != 0 {
				t.Errorf("code %d: a refused sync is not a fall-open, counted %d", code, got)
			}
		}
	})
	t.Run("another server error allows the sync, loudly", func(t *testing.T) {
		before := openCount(t, reader)
		h, _ := workItemHandlers(&fakeCounter{err: &clickhouse.Exception{Code: 60, Name: "DB::Exception", Message: "unknown table"}})
		if err := h.workItemsVerdict(context.Background(), "org-1", 10); err != nil {
			t.Fatal(err)
		}
		if got := openCount(t, reader) - before; got != 1 {
			t.Errorf("counted %d", got)
		}
	})
}
