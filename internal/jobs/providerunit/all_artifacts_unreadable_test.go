package providerunit

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// observeAllArtifactsUnreadable direct unit test, mirroring
// TestTerminalizationWithCommittedRowsIsCounted's structure: it must count
// ONLY its own category, proving the branch does not fire for every
// deterministic-terminal failure the handler ever sees (CHAOS-4185 standing
// telemetry order).
func TestAllArtifactsUnreadableIsCountedOnlyForItsOwnCategory(t *testing.T) {
	t.Parallel()
	claim := providersync.Claim{}
	claim.Provider, claim.Dataset = "github", "cicd"
	claim.ID = "11111111-1111-4111-8111-111111111111"

	cases := []struct {
		name     string
		category string
		want     bool
	}{
		{name: "its own category", category: AllArtifactsUnreadableCategory, want: true},
		{name: "a different deterministic category", category: AuthCategory, want: false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			metrics := providerfoundation.NewMetrics()
			handler := &Handler{ProviderMetrics: metrics}
			handler.observeAllArtifactsUnreadable(claim, testCase.category)
			var output bytes.Buffer
			if err := metrics.WritePrometheus(&output); err != nil {
				t.Fatal(err)
			}
			line := `dev_health_provider_all_artifacts_unreadable_total{provider="github",dataset="cicd"} 1`
			if got := strings.Contains(output.String(), line); got != testCase.want {
				t.Fatalf("counter recorded=%v, want %v:\n%s", got, testCase.want, output.String())
			}
		})
	}

	// A deployment without the shared registry must not panic on the way to
	// a terminalization it still has to report.
	(&Handler{}).observeAllArtifactsUnreadable(claim, AllArtifactsUnreadableCategory)
}

// allArtifactsUnreadableRouteHandler is a fake CompleteRouteHandler whose
// Collect always returns the totality sentinel, so Work can be driven into
// the deterministic-terminal branch without any real HTTP traffic -- the
// non-chunked executor path successfulExecutor already wires is enough to
// reach deterministicTerminalCategory; the chunked route's own end-to-end
// behavior is covered separately in internal/providersync.
type allArtifactsUnreadableRouteHandler struct{}

func (allArtifactsUnreadableRouteHandler) Collect(
	context.Context, providersync.Claim, providerfoundation.Credential,
	*providerfoundation.HTTPClient, time.Time,
) (providersync.CompleteRouteBatch, error) {
	return providersync.CompleteRouteBatch{}, providersync.ErrGitHubTestsAllArtifactsUnreadable
}

// Wiring proof (CHAOS-4185 codex round 1, MEDIUM finding): the counter is
// only worth having if the terminal branch actually calls it AFTER the
// durable Fail transition succeeds -- this drives Handler.Work end to end
// into the deterministic-terminal branch and reads the metric the process
// would scrape, the same reachability discipline
// TestDeterministicTerminalFailureWithRowsReachesTheCounter applies to
// CHAOS-4130's counter.
func TestHandlerRecordsAllArtifactsUnreadableOnlyAfterDurableFail(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	repository := newMemoryUnitRepository(unit)
	metrics := providerfoundation.NewMetrics()
	build := successfulExecutor(t, now)
	handler := &Handler{
		Repository:      repository,
		LeaseDuration:   time.Minute,
		Heartbeat:       10 * time.Second,
		Now:             func() time.Time { return now },
		ProviderMetrics: metrics,
		BuildExecutor: func(session *providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			executor, err := build(session)
			if err != nil {
				return executor, err
			}
			executor.Handler = allArtifactsUnreadableRouteHandler{}
			return executor, nil
		},
	}
	execution := providerExecution(unit, now, 1)
	execution.Definition.MaxAttempts = 5

	err := handler.Work(context.Background(), execution)
	if !errors.Is(err, providersync.ErrGitHubTestsAllArtifactsUnreadable) {
		t.Fatalf("Work()=%v, want ErrGitHubTestsAllArtifactsUnreadable", err)
	}
	if repository.lastFailCategory != AllArtifactsUnreadableCategory {
		t.Fatalf("category=%q, want %q", repository.lastFailCategory, AllArtifactsUnreadableCategory)
	}
	var output bytes.Buffer
	if writeErr := metrics.WritePrometheus(&output); writeErr != nil {
		t.Fatal(writeErr)
	}
	line := `dev_health_provider_all_artifacts_unreadable_total{provider="launchdarkly",dataset="feature-flags"} 1`
	if !strings.Contains(output.String(), line) {
		t.Fatalf("totality failure was not counted:\n%s", output.String())
	}
}

// A unit whose Fail CAS is refused must not be counted -- the same
// discipline TestTerminalizationRefusedByTheRepositoryIsNotCounted pins for
// CHAOS-4130's counter, and specifically the gap codex round 1 found here:
// this attempt stays retryable rather than terminal, and only a LATER
// attempt whose Fail durably succeeds may ever record it, capping the
// series at exactly one increment per real unit failure.
func TestAllArtifactsUnreadableRefusedByTheRepositoryIsNotCounted(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	repository := &lostLeaseFailRepository{memoryUnitRepository: newMemoryUnitRepository(unit)}
	metrics := providerfoundation.NewMetrics()
	build := successfulExecutor(t, now)
	handler := &Handler{
		Repository:      repository,
		LeaseDuration:   time.Minute,
		Heartbeat:       10 * time.Second,
		Now:             func() time.Time { return now },
		ProviderMetrics: metrics,
		BuildExecutor: func(session *providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			executor, err := build(session)
			if err != nil {
				return executor, err
			}
			executor.Handler = allArtifactsUnreadableRouteHandler{}
			return executor, nil
		},
	}
	execution := providerExecution(unit, now, 1)
	execution.Definition.MaxAttempts = 5

	if err := handler.Work(context.Background(), execution); err == nil {
		t.Fatal("Work() succeeded; the premise needs a failing execution")
	}
	if repository.failCalls == 0 {
		t.Fatal("premise broken: the terminal branch never attempted the durable Fail")
	}
	var output bytes.Buffer
	if writeErr := metrics.WritePrometheus(&output); writeErr != nil {
		t.Fatal(writeErr)
	}
	if strings.Contains(output.String(), "dev_health_provider_all_artifacts_unreadable_total{provider=") {
		t.Fatalf("counted a totality failure the repository refused:\n%s", output.String())
	}
}
