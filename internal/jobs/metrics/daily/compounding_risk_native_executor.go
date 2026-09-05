package daily

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/compoundingrisk"
)

// CompoundingRiskExecutor is the NATIVE implementation of the compounding_risk
// metrics.daily family (CHAOS-4287), REPO scope only. Fidelity notes -- the
// repo/team split, the append-only write mode, and why team rows are still
// Python -- live on internal/jobs/metrics/daily/compoundingrisk's package doc
// comment; this type is a thin ClickHouse-connection adapter over that
// package's pure Compute kernel and its loader/writer.
//
// # It is registered POST_BRIDGE, and that is load-bearing
//
// This family's only input is repo_metrics_daily, which the repo_user_commit
// family writes in the SAME partition. repo_user_commit is already native and
// runs pre_bridge, but computeNativeFamilies walks nativeFamilyNames in SORTED
// order, and "compounding_risk" sorts BEFORE "repo_user_commit" -- so a
// pre_bridge registration here would read the table before this partition's
// rows were written and compute from stale or absent data, silently. Worse,
// when repo_user_commit's own executor refuses, Python writes repo_metrics_daily
// during the bridge call, which is later still.
//
// post_bridge is the phase that is after BOTH: every pre_bridge native family
// has run, and the compatibility bridge has returned. This is exactly
// work_item_state's situation and precedent (CHAOS-4278, caught there by a
// codex round-1 P1 on a pre_bridge placement reading stale data).
//
// Unlike a pre_bridge family this is NOT fail-open end to end: Python is told
// to skip compounding_risk unconditionally (skipFamiliesForBridge appends every
// post_bridge name), so if this executor fails, nothing writes the family for
// that partition. That tradeoff is inherent to the phase -- see
// computePostBridgeNativeFamilies' doc comment -- not an oversight here.
type CompoundingRiskExecutor struct {
	loader *compoundingrisk.ClickHouseLoader
	writer *compoundingrisk.Writer
	nowUTC func() time.Time
}

var errCompoundingRiskUnavailable = fmt.Errorf("compounding_risk native executor unavailable")

// NewCompoundingRiskExecutor fails closed on a nil connection, matching
// NewCICDExecutor's construction-time policy: a refused executor never enters
// PartitionHandler's post-bridge family map, and compounding_risk stays on the
// Python compatibility bridge for every partition until the worker restarts
// with a healthy connection.
func NewCompoundingRiskExecutor(conn driver.Conn) (*CompoundingRiskExecutor, error) {
	if conn == nil {
		return nil, errCompoundingRiskUnavailable
	}
	loader, err := compoundingrisk.NewClickHouseLoader(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errCompoundingRiskUnavailable, err)
	}
	writer, err := compoundingrisk.NewWriter(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errCompoundingRiskUnavailable, err)
	}
	return &CompoundingRiskExecutor{
		loader: loader, writer: writer,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// ComputeFamily implements NativeFamilyExecutor.
func (executor *CompoundingRiskExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.loader == nil || executor.writer == nil {
		return 0, errCompoundingRiskUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: partition %s run has no organization or target day", ErrInvalidState, partition.ID)
	}

	// Validated before any ClickHouse round-trip: a malformed durable
	// partition scope is a precondition failure, not a transient dependency
	// error, and must not spend a query proving that.
	repoIDs, err := parseRepositoryUUIDs(partition.RepoIDs)
	if err != nil {
		return 0, fmt.Errorf("%w: partition %s repo_ids: %v", ErrInvalidState, partition.ID, err)
	}
	if len(repoIDs) == 0 {
		return 0, nil
	}

	targetDay := run.TargetDay.UTC()
	day := time.Date(targetDay.Year(), targetDay.Month(), targetDay.Day(), 0, 0, 0, 0, time.UTC)

	rows, err := executor.loader.LoadRepoMetrics(ctx, run.OrganizationID, repoIDs, day)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		// No repo_metrics_daily rows for this partition's repos: nothing to
		// score, and not an error. Python's own degenerate path broadens to an
		// org-wide refetch here; see repoMetricsQuery's doc comment for why
		// this port does not.
		return 0, nil
	}

	// Python fetches the complexity delta inside its per-repo loop over the
	// repo_metrics rows (compounding_risk.py:503-505), one query per repo, and
	// only for repos that HAVE a repo_metrics row. Iterating `rows` rather than
	// the partition's full repo list mirrors that exactly and skips queries for
	// repos that could not produce a record anyway.
	//
	// Kept as one query per repo rather than batched: the window/midpoint
	// arithmetic and the two avg()s are per-repo, and a single grouped query
	// would change which rows each average sees.
	complexityDeltas := make(map[string]*float64, len(rows))
	for _, row := range rows {
		// The id came from a scanned uuid.UUID a moment ago, so this cannot
		// fail; treated as a precondition failure rather than ignored, so a
		// future loader change that widens RepoID does not silently degrade
		// every repo to a nil delta (severity "unknown") instead of erroring.
		repoUUID, parseErr := uuid.Parse(row.RepoID)
		if parseErr != nil {
			return 0, fmt.Errorf("%w: repo_metrics_daily repo_id %q: %v", ErrInvalidState, row.RepoID, parseErr)
		}
		delta, deltaErr := executor.loader.LoadComplexityDelta(
			ctx, run.OrganizationID, repoUUID, day, compoundingrisk.ComplexityWindowDays,
		)
		if deltaErr != nil {
			return 0, deltaErr
		}
		complexityDeltas[row.RepoID] = delta
	}

	computedAt := executor.nowUTC()
	records := compoundingrisk.ComputeForRepos(
		day, run.OrganizationID, rows, complexityDeltas, computedAt,
		compoundingrisk.DefaultWeights,
		compoundingrisk.DefaultThresholds,
		compoundingrisk.DefaultReferences,
	)

	rowsWritten, err := executor.writer.WriteRecords(ctx, records, run.OrganizationID)
	if err != nil {
		return 0, err
	}
	return rowsWritten, nil
}

var _ NativeFamilyExecutor = (*CompoundingRiskExecutor)(nil)
