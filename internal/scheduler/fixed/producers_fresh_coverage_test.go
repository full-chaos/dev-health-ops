package fixed

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/jackc/pgx/v5"
)

// freshRemainingStore is the recording store plus RemainingMetricsFreshCoverageStore (CHAOS-9587).
type freshRemainingStore struct {
	recordingRemainingStore
	result map[string]remaining.FreshCoverageResult
	calls  []freshCall
}

type freshCall struct {
	org, family string
	cutoff, now time.Time
	budget      time.Duration
}

func (store *freshRemainingStore) FreshCoverage(
	_ context.Context, _ pgx.Tx, organizationID, family string, _ json.RawMessage, cutoff, now time.Time, stageBudget time.Duration,
) (remaining.FreshCoverageResult, error) {
	store.calls = append(store.calls, freshCall{organizationID, family, cutoff, now, stageBudget})
	return store.result[organizationID], nil
}

func freshProducer(t *testing.T, result map[string]remaining.FreshCoverageResult) (*RemainingMetricsFanoutProducer, *freshRemainingStore) {
	t.Helper()
	store := &freshRemainingStore{result: result}
	producer, err := NewRemainingMetricsFanoutProducer(store, nopPartitionPublisher{},
		fixedOrganizationLister{identifiers: []string{testOrgA, testOrgB}}, &recordingGraphWriter{})
	if err != nil {
		t.Fatal(err)
	}
	return producer.(*RemainingMetricsFanoutProducer), store
}

func capturedLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buffer bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buffer, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buffer
}

// A covered organization is skipped (and only it), with a loud line naming the covering run; the others run.
// The cutoff and the stage budget each family hands the store are pinned here. What counts as covering
// (created after the cutoff, younger than the budget, not a dead handoff, rows written) is the store's
// decision and is pinned against real Postgres in remaining/fresh_coverage_integration_test.go.
func TestFreshCoverageSkipsOnlyTheCoveredOrganizationAndLogsTheCover(t *testing.T) {
	for _, tc := range []struct {
		schedule   string
		due        string
		wantCutoff string
	}{
		{"recommendations_daily_fanout", "2026-11-18T02:00:00Z", "2026-11-18T00:00:00Z"},
		{"capacity_forecast_weekly_fanout", "2026-11-23T01:00:00Z", "2026-11-23T01:00:00Z"},
	} {
		for _, branch := range []remaining.FreshCoverageBranch{remaining.FreshCoverageSucceeded, remaining.FreshCoverageInFlight} {
			t.Run(tc.schedule+"/"+string(branch), func(t *testing.T) {
				logs := capturedLogs(t)
				schedule := scheduleByID(t, tc.schedule)
				due := mustTime(t, tc.due)
				status := "succeeded"
				if branch == remaining.FreshCoverageInFlight {
					status = "running"
				}
				producer, store := freshProducer(t, map[string]remaining.FreshCoverageResult{
					testOrgA: {Covered: true, Branch: branch, RunID: "run-123", Status: status, CreatedAt: due.Add(-time.Minute)},
				})
				outcome, err := producer.Produce(context.Background(), &stubTx{}, schedule, NewOccurrence(schedule, due, due.Add(5*time.Second)))
				if err != nil || outcome.Handoffs != 1 || len(store.requests) != 1 || store.requests[0].OrganizationID != testOrgB {
					t.Fatalf("outcome=%+v err=%v requests=%+v: want only the uncovered organization to run", outcome, err, store.requests)
				}
				if len(store.calls) != 2 {
					t.Fatalf("FreshCoverage called %d times, want once per organization", len(store.calls))
				}
				if got := store.calls[0].cutoff; !got.Equal(mustTime(t, tc.wantCutoff)) {
					t.Fatalf("cutoff = %s, want %s", got, tc.wantCutoff)
				}
				if got, want := store.calls[0].now, due.Add(5*time.Second); !got.Equal(want) {
					t.Fatalf("now = %s, want the occurrence observation %s", got, want)
				}
				if store.calls[0].budget != remainingStageBudget {
					t.Fatalf("budget = %s, want %s", store.calls[0].budget, remainingStageBudget)
				}
				line := logs.String()
				for _, want := range []string{
					"occurrence skipped as covered", "covering_run_id=run-123", "covering_run_status=" + status,
					"covering_run_created_at=", "rule_branch=" + string(branch), "organization_id=" + testOrgA,
				} {
					if !strings.Contains(line, want) {
						t.Fatalf("skip log lacks %q: %s", want, line)
					}
				}
			})
		}
	}
}

// Nothing covers: both organizations run as today, and nothing is logged as a skip.
func TestFreshCoverageRunsWhenNothingCovers(t *testing.T) {
	logs := capturedLogs(t)
	schedule := scheduleByID(t, "recommendations_daily_fanout")
	due := mustTime(t, "2026-11-18T02:00:00Z")
	producer, store := freshProducer(t, nil)
	outcome, err := producer.Produce(context.Background(), &stubTx{}, schedule, NewOccurrence(schedule, due, due))
	if err != nil || outcome.Handoffs != 2 || len(store.requests) != 2 {
		t.Fatalf("outcome=%+v err=%v requests=%d", outcome, err, len(store.requests))
	}
	if strings.Contains(logs.String(), "skipped as covered") {
		t.Fatalf("an uncovered occurrence logged a skip: %s", logs.String())
	}
}

// The other bindings keep the day-label check: a store with FreshCoverage is never asked for them.
func TestFreshCoverageIsOnlyForRecommendationsAndCapacity(t *testing.T) {
	for _, id := range []string{"dora_daily_fanout", "release_impact_daily_fanout"} {
		schedule := scheduleByID(t, id)
		producer, store := freshProducer(t, nil)
		due := mustTime(t, "2026-11-18T02:00:00Z")
		if _, err := producer.Produce(context.Background(), &stubTx{}, schedule, NewOccurrence(schedule, due, due)); err != nil {
			t.Fatal(err)
		}
		if len(store.calls) != 0 {
			t.Fatalf("%s: FreshCoverage called %d times", id, len(store.calls))
		}
	}
}

// remainingStageBudget is the kind timeout in the job registry, not a number of its own.
func TestRemainingStageBudgetIsTheKindTimeout(t *testing.T) {
	registry := testRegistry(t)
	for _, kind := range []string{jobcontract.KindRemainingRecommendations, jobcontract.KindRemainingCapacity} {
		descriptor, ok := registry.Descriptor(kind)
		if !ok {
			t.Fatalf("%s is not in the job registry", kind)
		}
		if descriptor.Timeout != remainingStageBudget {
			t.Fatalf("%s timeout = %s, remainingStageBudget = %s", kind, descriptor.Timeout, remainingStageBudget)
		}
	}
}
