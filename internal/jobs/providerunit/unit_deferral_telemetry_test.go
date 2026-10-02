package providerunit

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// deferralLine runs one Work attempt whose executor fails with buildErr and
// returns the sync_provider_unit_finished record plus the rendered metrics
// (CHAOS-7434). The unit is the shared providerUnit() fixture
// (launchdarkly / feature-flags).
func deferralLine(t *testing.T, repository *memoryUnitRepository, unit providersync.Unit, buildErr error) (map[string]any, string) {
	t.Helper()
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	var logs bytes.Buffer
	metrics := providerfoundation.NewMetrics()
	handler := &Handler{
		Repository:      repository,
		LeaseDuration:   time.Minute,
		Heartbeat:       10 * time.Second,
		Now:             func() time.Time { return now },
		ProviderMetrics: metrics,
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			return providersync.CompleteRouteExecutor{}, buildErr
		},
	}
	execution := providerExecution(unit, now, 5)
	execution.Definition.MaxAttempts = 5
	execution.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	if err := handler.Work(context.Background(), execution); err == nil {
		t.Fatal("Work() = nil; want an attempt-neutral snooze")
	}
	var found map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "sync_provider_unit_finished" {
			found = record
		}
	}
	if found == nil {
		t.Fatalf("no sync_provider_unit_finished line in:\n%s", logs.String())
	}
	var rendered bytes.Buffer
	if err := metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	return found, rendered.String()
}

func TestBudgetContentionLineCarriesTheUnitsDeferralCountAndReason(t *testing.T) {
	t.Parallel()
	unit := providerUnit()
	repository := newMemoryUnitRepository(unit)
	// The unit already lost the reservation four times: this is the fifth.
	repository.contentionDeferrals = 4
	line, rendered := deferralLine(t, repository, unit, providerfoundation.ErrBudgetContended)
	if line["result"] != "deferred" || line["deferral_reason"] != "budget_contention" ||
		line["deferrals"] != float64(5) {
		t.Fatalf("budget contention line=%v; want result=deferred reason=budget_contention deferrals=5", line)
	}
	delay, _ := line["delay_ms"].(float64)
	if delay < 1000 || delay >= 2000 {
		t.Fatalf("delay_ms=%v; want the 1-2 s contention snooze", line["delay_ms"])
	}
	want := `dev_health_provider_unit_deferred_total{provider="launchdarkly",dataset="feature-flags",reason="budget_contention"} 1`
	if !strings.Contains(rendered, want) {
		t.Fatalf("missing %q in:\n%s", want, rendered)
	}
}

func TestRateLimitLineCarriesTheReasonAndCounts(t *testing.T) {
	t.Parallel()
	unit := providerUnit()
	repository := newMemoryUnitRepository(unit)
	line, rendered := deferralLine(t, repository, unit, rateLimitError(2*time.Minute))
	if line["result"] != "rate_limited" || line["deferral_reason"] != "rate_limited" || line["deferrals"] != float64(1) {
		t.Fatalf("rate limit line=%v; want result=rate_limited reason=rate_limited deferrals=1", line)
	}
	if delay, _ := line["delay_ms"].(float64); delay < 120000 || delay > 125000 {
		t.Fatalf("rate limit delay_ms=%v; want the provider's 2 min window plus up to 5 s jitter", line["delay_ms"])
	}
	want := `dev_health_provider_unit_deferred_total{provider="launchdarkly",dataset="feature-flags",reason="rate_limited"} 1`
	if !strings.Contains(rendered, want) {
		t.Fatalf("missing %q in:\n%s", want, rendered)
	}
}

func TestChunkContinuationLineCarriesTheReasonAndCounts(t *testing.T) {
	t.Parallel()
	unit := providerUnit()
	repository := newMemoryUnitRepository(unit)
	buildErr := providersync.ChunkContinuationError{
		Next: time.Now().Add(time.Hour), Reason: providersync.ChunkStopChunkBound,
		Chunks: 8, Elapsed: 5 * time.Second,
	}
	line, rendered := deferralLine(t, repository, unit, buildErr)
	if line["result"] != "continued" || line["deferral_reason"] != "chunk_continuation" {
		t.Fatalf("chunk continuation line=%v; want result=continued reason=chunk_continuation", line)
	}
	// The delay is Next minus the wall clock (an hour away here), never 0.
	if delay, _ := line["delay_ms"].(float64); delay < 3500000 || delay > 3600000 {
		t.Fatalf("chunk continuation delay_ms=%v; want about one hour", line["delay_ms"])
	}
	want := `dev_health_provider_unit_deferred_total{provider="launchdarkly",dataset="feature-flags",reason="chunk_continuation"} 1`
	if !strings.Contains(rendered, want) {
		t.Fatalf("missing %q in:\n%s", want, rendered)
	}
}

func TestOrdinaryAttemptFailureIsNotCountedAsADeferral(t *testing.T) {
	t.Parallel()
	unit := providerUnit()
	repository := newMemoryUnitRepository(unit)
	metrics := providerfoundation.NewMetrics()
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	handler := &Handler{
		Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
		Now: func() time.Time { return now }, ProviderMetrics: metrics,
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			return providersync.CompleteRouteExecutor{}, providerfoundation.ErrBudgetUnavailable
		},
	}
	execution := providerExecution(unit, now, 5)
	execution.Definition.MaxAttempts = 5
	_ = handler.Work(context.Background(), execution)
	var rendered bytes.Buffer
	if err := metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(rendered.String(), "dev_health_provider_unit_deferred_total{") {
		t.Fatalf("a budget-store outage was counted as a deferral:\n%s", rendered.String())
	}
}

// failingDeferRepository fails to persist a budget-contention deferral.
type failingDeferRepository struct{ *memoryUnitRepository }

func (failingDeferRepository) DeferForBudgetContention(context.Context, providersync.Claim, time.Time, time.Time) (int, error) {
	return 0, providersync.ErrLeaseLost
}

func TestADeferralThatFailedToPersistIsNotCounted(t *testing.T) {
	t.Parallel()
	unit := providerUnit()
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	metrics := providerfoundation.NewMetrics()
	handler := &Handler{
		Repository: failingDeferRepository{newMemoryUnitRepository(unit)}, LeaseDuration: time.Minute,
		Heartbeat: 10 * time.Second, Now: func() time.Time { return now }, ProviderMetrics: metrics,
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			return providersync.CompleteRouteExecutor{}, providerfoundation.ErrBudgetContended
		},
	}
	execution := providerExecution(unit, now, 5)
	execution.Definition.MaxAttempts = 5
	if err := handler.Work(context.Background(), execution); err == nil {
		t.Fatal("Work() = nil; want a retryable error when the deferral cannot be persisted")
	}
	var rendered bytes.Buffer
	if err := metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(rendered.String(), "dev_health_provider_unit_deferred_total{") {
		t.Fatalf("a deferral that was not persisted was counted:\n%s", rendered.String())
	}
}
