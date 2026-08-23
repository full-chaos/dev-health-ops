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

// A unit destroyed while it already holds committed rows is the CHAOS-4130
// signature: for days a page-budget refusal cancelled units holding ~9,500
// durable rows, deleted their checkpoints, and let the run re-plan the same
// window from page one. Every individual signal looked ordinary; only the
// combination was diagnostic.
func TestTerminalizationWithCommittedRowsIsCounted(t *testing.T) {
	t.Parallel()
	claim := providersync.Claim{}
	claim.Provider, claim.Dataset = "github", "cicd"
	claim.ID = "11111111-1111-4111-8111-111111111111"

	cases := []struct {
		name   string
		result providersync.CompleteRouteExecutionResult
		want   bool
	}{
		{
			name:   "rows from earlier attempts",
			result: providersync.CompleteRouteExecutionResult{CommittedRows: 9526},
			want:   true,
		},
		{
			name: "rows written by this attempt only",
			result: providersync.CompleteRouteExecutionResult{
				Effects: providersync.EffectCommitResult{Written: 12},
			},
			want: true,
		},
		{
			name:   "nothing durable yet",
			result: providersync.CompleteRouteExecutionResult{},
			want:   false,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			metrics := providerfoundation.NewMetrics()
			handler := &Handler{ProviderMetrics: metrics}
			handler.observeTerminalWithCommittedRows(
				claim, testCase.result, PaginationIncompleteCategory, errors.New("cap"),
			)
			var output bytes.Buffer
			if err := metrics.WritePrometheus(&output); err != nil {
				t.Fatal(err)
			}
			line := `dev_health_provider_unit_terminal_with_rows_total{provider="github",dataset="cicd"} 1`
			if got := strings.Contains(output.String(), line); got != testCase.want {
				t.Fatalf("counter recorded=%v, want %v:\n%s", got, testCase.want, output.String())
			}
		})
	}

	// A deployment without the shared registry must not panic on the way to a
	// terminalization it still has to report.
	(&Handler{}).observeTerminalWithCommittedRows(
		claim, providersync.CompleteRouteExecutionResult{CommittedRows: 1}, AuthCategory, nil,
	)
}

// ambiguousAfterFirstCommitLedger commits the first effect batch and then
// reports an unrecoverable ambiguity, so Execute returns BOTH a
// deterministic-terminal error and a result holding written rows.
type ambiguousAfterFirstCommitLedger struct {
	testEffectLedger
	commits int
}

func (ledger *ambiguousAfterFirstCommitLedger) CommitEffect(
	_ context.Context, _ providersync.Claim, _ int, _ string, _ time.Time,
) error {
	ledger.commits++
	if ledger.commits > 1 {
		return providersync.ErrEffectRecoveryAmbiguous
	}
	return nil
}

// Wiring proof. The counter above is only worth having if the terminal branch
// actually calls it, and the CHAOS-4130 lesson is that a cited call site is
// not proof of reachability: this drives Handler.Work end to end into the
// deterministic-terminal branch and reads the metric the process would scrape.
func TestDeterministicTerminalFailureWithRowsReachesTheCounter(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
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
			executor.Committer.Ledger = &ambiguousAfterFirstCommitLedger{}
			return executor, nil
		},
	}
	execution := providerExecution(unit, now, 1)
	execution.Definition.MaxAttempts = 5

	err := handler.Work(context.Background(), execution)
	if !errors.Is(err, providersync.ErrEffectRecoveryAmbiguous) {
		t.Fatalf("Work()=%v, want the deterministic-terminal ambiguity", err)
	}
	if repository.lastFailCategory != EffectRecoveryAmbiguousCategory {
		t.Fatalf("category=%q, want %q", repository.lastFailCategory, EffectRecoveryAmbiguousCategory)
	}
	var output bytes.Buffer
	if writeErr := metrics.WritePrometheus(&output); writeErr != nil {
		t.Fatal(writeErr)
	}
	line := `dev_health_provider_unit_terminal_with_rows_total{provider="launchdarkly",dataset="feature-flags"} 1`
	if !strings.Contains(output.String(), line) {
		t.Fatalf("terminalization with committed rows was not counted:\n%s", output.String())
	}
}
