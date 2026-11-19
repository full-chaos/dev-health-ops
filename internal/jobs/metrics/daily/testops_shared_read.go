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
	records map[testopsTestMetricKey]testopsSharedRecord
}

// testopsSharedRecord is a stored record with the instant its inputs were read. A family that takes the record later
// carries that instant as the snapshot of what it writes: the read happened then, whatever its own clock says now.
type testopsSharedRecord struct {
	metrics []testops.TestMetric
	readAt  time.Time
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
		records: make(map[testopsTestMetricKey]testopsSharedRecord),
	})
}

// readFor returns the stored record for key and the instant its inputs were read, or runs compute and stores its result
// when it succeeds; now is taken just before compute. The lock is held across compute so two callers can never both pay
// for the same read.
func (share *testopsTestMetricShare) readFor(
	key testopsTestMetricKey, now func() time.Time, compute func() ([]testops.TestMetric, error),
) ([]testops.TestMetric, time.Time, error) {
	if share == nil {
		readAt := now().UTC()
		metrics, err := compute()
		return metrics, readAt, err
	}
	share.mu.Lock()
	defer share.mu.Unlock()
	if stored, ok := share.records[key]; ok {
		return stored.metrics, stored.readAt, nil
	}
	readAt := now().UTC()
	metrics, err := compute()
	if err != nil {
		return nil, time.Time{}, err
	}
	share.records[key] = testopsSharedRecord{metrics: metrics, readAt: readAt}
	return metrics, readAt, nil
}

// loadTestopsTestMetrics returns one repo's test metric records for [start,
// end), with failure recurrence measured against [historyStart, start), team
// ids resolved through the caller's resolver. The instant returned is when the inputs were read: the first family of the
// pass that needed them took it, and a later family gets the same instant with the same record.
func loadTestopsTestMetrics(
	ctx context.Context,
	now func() time.Time,
	conn testopsNativeConn,
	orgID string,
	repoID uuid.UUID,
	repoName string,
	resolver testops.RepoTeamResolver,
	historyStart, start, end time.Time,
) ([]testops.TestMetric, time.Time, error) {
	share, _ := ctx.Value(testopsTestMetricShareContextKey{}).(*testopsTestMetricShare)
	key := testopsTestMetricKey{
		orgID: orgID, repoID: repoID,
		historyStart: historyStart.UnixNano(), start: start.UnixNano(), end: end.UnixNano(),
	}
	unresolved, readAt, err := share.readFor(key, now, func() ([]testops.TestMetric, error) {
		return computeTestopsTestMetricsWithoutTeams(ctx, conn, orgID, repoID, historyStart, start, end)
	})
	if err != nil {
		return nil, time.Time{}, err
	}
	resolved := make([]testops.TestMetric, len(unresolved))
	for index, metric := range unresolved {
		resolved[index] = metric.ResolveTeam(repoName, resolver)
	}
	return resolved, readAt, nil
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
