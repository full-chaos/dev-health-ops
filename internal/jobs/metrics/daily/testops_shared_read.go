package daily

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// testopsTestMetricShare lets every family that needs a repo's test metric
// record for the partition day -- testops_test writes it, testops_risk scores
// it -- compute it once per partition pass instead of once each. The record's
// case half comes from test_case_results, whose sorting key has no time
// column, so each computation scans the repo's whole case history; doing that
// once per family was the dominant ClickHouse cost of the daily run.
//
// What sharing does not change:
//
//   - Scope is one computeNativeFamilies call. A retried or re-driven
//     partition is a new pass with an empty share and reads the tables as
//     they are then.
//   - Only a successful computation is stored. A family whose computation
//     fails returns its own error as before, and the next family makes its
//     own attempt, so neither family's failure, retry or blocked state
//     depends on the other.
//   - Each family still loads its own teams, writes its own tables and
//     reports its own row count, so the partition ledger and the per-family
//     telemetry still see two independent families.
//   - The stored record is finished without a team resolver and each family
//     resolves team_id with its own, so a team edit landing between the two
//     families' team reads resolves exactly as it did when each read alone.
type testopsTestMetricShare struct {
	mu      sync.Mutex
	records map[testopsTestMetricKey][]testops.TestMetric
}

type testopsTestMetricKey struct {
	orgID                    string
	repoID                   uuid.UUID
	historyStart, start, end int64
}

type testopsTestMetricShareContextKey struct{}

// withTestopsTestMetricShare returns ctx carrying a fresh, empty share. A
// context without one computes on every call.
func withTestopsTestMetricShare(ctx context.Context) context.Context {
	return context.WithValue(ctx, testopsTestMetricShareContextKey{}, &testopsTestMetricShare{
		records: make(map[testopsTestMetricKey][]testops.TestMetric),
	})
}

// recordFor returns the stored record for key, or runs compute and stores
// its result when it succeeds. The lock is held across compute so two callers
// can never both pay for the same read.
func (share *testopsTestMetricShare) recordFor(
	key testopsTestMetricKey, compute func() ([]testops.TestMetric, error),
) ([]testops.TestMetric, error) {
	if share == nil {
		return compute()
	}
	share.mu.Lock()
	defer share.mu.Unlock()
	if record, ok := share.records[key]; ok {
		return record, nil
	}
	record, err := compute()
	if err != nil {
		return nil, err
	}
	share.records[key] = record
	return record, nil
}

// loadTestopsTestMetrics returns one repo's test metric records for [start,
// end), with failure recurrence measured against [historyStart, start), team
// ids resolved through the caller's resolver.
func loadTestopsTestMetrics(
	ctx context.Context,
	conn testopsNativeConn,
	orgID string,
	repoID uuid.UUID,
	repoName string,
	resolver testops.RepoTeamResolver,
	historyStart, start, end time.Time,
) ([]testops.TestMetric, error) {
	share, _ := ctx.Value(testopsTestMetricShareContextKey{}).(*testopsTestMetricShare)
	key := testopsTestMetricKey{
		orgID: orgID, repoID: repoID,
		historyStart: historyStart.UnixNano(), start: start.UnixNano(), end: end.UnixNano(),
	}
	unresolved, err := share.recordFor(key, func() ([]testops.TestMetric, error) {
		return computeTestopsTestMetricsWithoutTeams(ctx, conn, orgID, repoID, historyStart, start, end)
	})
	if err != nil {
		return nil, err
	}
	resolved := make([]testops.TestMetric, len(unresolved))
	for index, metric := range unresolved {
		resolved[index] = metric.ResolveTeam(repoName, resolver)
	}
	return resolved, nil
}

func computeTestopsTestMetricsWithoutTeams(
	ctx context.Context,
	conn testopsNativeConn,
	orgID string,
	repoID uuid.UUID,
	historyStart, start, end time.Time,
) ([]testops.TestMetric, error) {
	accumulator := testops.NewTestAccumulator(repoID, "", nil)
	if err := loadNativeTestopsSuites(ctx, conn, accumulator, orgID, repoID, start, end); err != nil {
		return nil, err
	}
	historicalFailedNames, err := loadNativeTestopsCaseAggregate(
		ctx, conn, accumulator, orgID, repoID, historyStart, start, end)
	if err != nil {
		return nil, err
	}
	return accumulator.Finish(historicalFailedNames), nil
}
