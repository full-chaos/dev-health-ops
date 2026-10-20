package httpapi

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// quotingStore is a CounterStore whose backend fails with an error that
// quotes a value it was handed.
type quotingStore struct{}

func (quotingStore) Increment(context.Context, Hit) (int64, error) {
	return 0, errors.New("driver bind value someone@example.com: connection reset")
}
func (quotingStore) Peek(context.Context, Hit) (int64, error) {
	return 0, errors.New("driver bind value someone@example.com: connection reset")
}
func (quotingStore) Backend() string { return "redis" }

// TestRateLimitStoreFailureLogsNoErrorText: the store failure log of
// AllowCounted and TestCounted names the limit, the backend and the error's
// class, never the error text.
func TestRateLimitStoreFailureLogsNoErrorText(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(logging.NewJSON(&buf, slog.LevelDebug))
	t.Cleanup(func() { slog.SetDefault(previous) })
	limiter := NewKeyedLimiter(quotingStore{}, Limit{ID: "probe", Count: 1, Window: time.Hour})
	if _, err := limiter.AllowCounted(context.Background(), "k", "/p"); err == nil {
		t.Fatal("AllowCounted returned no error")
	}
	if _, err := limiter.TestCounted(context.Background(), "k", "/p"); err == nil {
		t.Fatal("TestCounted returned no error")
	}
	out := buf.String()
	if strings.Count(out, "rate limit store failed") != 2 {
		t.Fatalf("want two store failure logs: %s", out)
	}
	if strings.Contains(out, "someone@example.com") || strings.Contains(out, "driver bind value") {
		t.Fatalf("the store failure log carries the error text: %s", out)
	}
}
