package llmbudgetstale

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
)

type fakeLister struct {
	rows      []llmbudget.StaleReservation
	err       error
	olderThan time.Duration
}

func (lister *fakeLister) ListStale(_ context.Context, olderThan time.Duration, _ time.Time) ([]llmbudget.StaleReservation, error) {
	lister.olderThan = olderThan
	return lister.rows, lister.err
}

func execution(scheduledFor string) *jobruntime.Execution[jobruntime.LLMBudgetStaleReservationsArgs] {
	return &jobruntime.Execution[jobruntime.LLMBudgetStaleReservationsArgs]{
		Args: jobruntime.LLMBudgetStaleReservationsArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.LLMBudgetStaleReservationsPayload]{
			Payload: jobcontract.LLMBudgetStaleReservationsPayload{ScheduledFor: scheduledFor},
		}},
	}
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestHandlerPassesTheThresholdToTheListing(t *testing.T) {
	t.Parallel()
	lister := &fakeLister{}
	handler, err := NewHandler(lister, 6*time.Hour, quiet())
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), execution("2026-10-13T12:00:00Z")); err != nil {
		t.Fatal(err)
	}
	if lister.olderThan != 6*time.Hour {
		t.Fatalf("threshold = %v, want 6h", lister.olderThan)
	}
}

func TestHandlerRetriesAFailedListing(t *testing.T) {
	t.Parallel()
	handler, _ := NewHandler(&fakeLister{err: errors.New("postgres unavailable")}, time.Hour, quiet())
	err := handler.Work(context.Background(), execution("2026-10-13T12:00:00Z"))
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("Work() = %v, want retryable", err)
	}
}

func TestHandlerRejectsABadRequestOrConfiguration(t *testing.T) {
	t.Parallel()
	if _, err := NewHandler(nil, time.Hour, quiet()); err == nil {
		t.Fatal("nil lister accepted")
	}
	if _, err := NewHandler(&fakeLister{}, 0, quiet()); err == nil {
		t.Fatal("zero threshold accepted")
	}
	if _, err := NewHandler(&fakeLister{}, time.Hour, nil); err == nil {
		t.Fatal("nil logger accepted")
	}
	handler, _ := NewHandler(&fakeLister{}, time.Hour, quiet())
	err := handler.Work(context.Background(), execution("not a time"))
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf("Work() = %v, want permanent", err)
	}
}

func TestThresholdFromEnv(t *testing.T) {
	t.Parallel()
	for raw, want := range map[string]time.Duration{"3600": time.Hour, " 86400 ": 24 * time.Hour} {
		got, err := ThresholdFromEnv(func(string) (string, bool) { return raw, true })
		if err != nil || got != want {
			t.Errorf("ThresholdFromEnv(%q) = %v, %v, want %v", raw, got, err, want)
		}
	}
	if got, err := ThresholdFromEnv(func(string) (string, bool) { return "", false }); err != nil || got != DefaultThreshold {
		t.Errorf("unset = %v, %v", got, err)
	}
	for _, raw := range []string{"", "  ", "abc", "0", "-5", "59", "1.5", "99999999999"} {
		if _, err := ThresholdFromEnv(func(string) (string, bool) { return raw, true }); err == nil {
			t.Errorf("ThresholdFromEnv(%q) accepted", raw)
		}
	}
}
