package daily

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/benchmarking"
)

// BenchmarkingExecutor is the NATIVE implementation of the benchmarking
// metrics.daily family (CHAOS-4288).
//
// # IT COMPUTES ONCE PER ORG PER DAY, WHICH PYTHON DOES NOT
//
// `run_benchmarking_for_day(sink, as_of_day, computed_at, org_id)` takes no
// repo_id (runner.py:259), yet job_daily.py:2037 calls it inside
// run_daily_metrics_job's per-day loop, above the `if not skip_finalize:` gate.
// The compatibility bridge fans out one partition per repo, so an org with N
// repos recomputes the whole org's benchmarks N times and appends N identical
// row sets to six APPEND-ONLY tables every night. Nothing deduplicates them on
// read.
//
// Team-lead ruled this fixed rather than mirrored (the same ruling class as
// compounding_risk's org-wide fallback). The mechanism: this executor runs the
// org's benchmarks only on the partition that holds the org's
// LEXICOGRAPHICALLY-FIRST repository id, and no-ops on every other partition.
// One partition per org/day therefore does the work, and the row sets stop
// multiplying by N.
//
// Why that anchor: it needs to be a property every partition can evaluate
// independently, with no coordination and no extra state, and it must select
// exactly one partition. The org's minimum repo id is derived from the SAME
// query ClickHouseRepositoryDiscoverer uses to enumerate an org's repos, so
// "the first repo" means the same thing here as everywhere else in this
// pipeline. If that query returns nothing the executor no-ops -- an org with no
// repos has no partitions either.
//
// The tradeoff is stated rather than hidden: if the anchor partition fails, the
// org gets no benchmarking rows that day even though other partitions
// succeeded. Under Python's behaviour it would have got N copies from the
// others. Zero-with-a-refused-family-counter is a better failure than silently
// multiplied rows, and computeNativeFamilies reports the refusal through
// ObserveDailyMetricsNativeFamily either way.
type BenchmarkingExecutor struct {
	conn   driver.Conn
	writer *benchmarking.Writer
	nowUTC func() time.Time
	logger *slog.Logger
}

var errBenchmarkingUnavailable = fmt.Errorf("benchmarking native executor unavailable")

// NewBenchmarkingExecutor fails closed on a nil connection, matching
// NewCICDExecutor's construction-time policy.
func NewBenchmarkingExecutor(conn driver.Conn, logger *slog.Logger) (*BenchmarkingExecutor, error) {
	if conn == nil {
		return nil, errBenchmarkingUnavailable
	}
	writer, err := benchmarking.NewWriter(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errBenchmarkingUnavailable, err)
	}
	return &BenchmarkingExecutor{
		conn:   conn,
		writer: writer,
		nowUTC: func() time.Time { return time.Now().UTC() },
		logger: logger,
	}, nil
}

// anchorFromDiscoveredSet picks the org/day's single benchmarking partition:
// the lexicographically smallest repository id in the RUN's discovered set.
//
// An EMPTY discovered set is an ERROR, not a no-op. By the time this is called
// the partition under execution has already been shown to carry repositories,
// and the discovered set is the UNION of the run's partition scopes -- so it
// must contain at least those. Empty therefore means the Run was built without
// its partitions being read, which is a caller bug, and returning zero rows
// with a nil error would reproduce exactly the silent-success failure this
// change exists to remove (CHAOS-4288).
//
// An unparseable id is an ERROR for the same reason: silently dropping one
// could move the minimum and hand the run to a different partition, or to none.
func anchorFromDiscoveredSet(run Run) (uuid.UUID, error) {
	if len(run.DiscoveredRepoIDs) == 0 {
		return uuid.Nil, fmt.Errorf(
			"%w: run %s has an empty discovered repository set while executing a "+
				"partition that carries repositories -- Run was built without its "+
				"partitions", ErrInvalidState, run.ID)
	}
	discovered, err := parseRepositoryUUIDs(run.DiscoveredRepoIDs)
	if err != nil {
		return uuid.Nil, fmt.Errorf(
			"%w: run %s discovered repo_ids: %v", ErrInvalidState, run.ID, err)
	}
	if len(discovered) == 0 {
		return uuid.Nil, fmt.Errorf(
			"%w: run %s discovered repository set parsed to nothing", ErrInvalidState, run.ID)
	}
	anchor := discovered[0]
	for _, candidate := range discovered[1:] {
		if candidate.String() < anchor.String() {
			anchor = candidate
		}
	}
	return anchor, nil
}

// ComputeFamily implements NativeFamilyExecutor.
func (executor *BenchmarkingExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil || executor.writer == nil {
		return 0, errBenchmarkingUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: partition %s run has no organization or target day", ErrInvalidState, partition.ID)
	}

	repoIDs, err := parseRepositoryUUIDs(partition.RepoIDs)
	if err != nil {
		return 0, fmt.Errorf("%w: partition %s repo_ids: %v", ErrInvalidState, partition.ID, err)
	}
	if len(repoIDs) == 0 {
		return 0, nil
	}

	// The anchor comes from the RUN's discovered set -- the union of this run's
	// partition scopes -- never from a live `min(id)` read of `repos`
	// (CHAOS-4288, codex r1 on #2235).
	//
	// The live read consulted a DIFFERENT set from the one partitions were cut
	// from. A run over a subset of the org's repos, or a repo inserted between
	// discovery and execution, could name an anchor that no partition contained;
	// every partition then answered "not mine", returned zero rows AND SUCCESS,
	// and the org silently produced no benchmarking output. Success with zero
	// rows is indistinguishable from "correctly nothing to do", which is why
	// nothing downstream could notice.
	//
	// Choosing from Run.DiscoveredRepoIDs makes "some partition holds the
	// anchor" true BY CONSTRUCTION -- the anchor is the minimum of the union of
	// the partition scopes, so it lies in one of them by definition. The
	// "not mine" branch below therefore stays the NORMAL path for every
	// non-anchor partition; it is not an error and never was.
	anchor, err := anchorFromDiscoveredSet(run)
	if err != nil {
		return 0, err
	}
	if anchor == uuid.Nil {
		// Defensive only: anchorFromDiscoveredSet returns an error rather than
		// uuid.Nil for every empty/unparseable case, so this is unreachable
		// today. Kept as a guard against a future change making it returnable
		// again -- silently benchmarking against the nil UUID would be worse
		// than a no-op.
		return 0, nil
	}
	if !containsRepository(repoIDs, anchor) {
		// Another partition owns this org/day's single benchmarking run. Normal.
		return 0, nil
	}

	targetDay := run.TargetDay.UTC()
	asOfDay := time.Date(targetDay.Year(), targetDay.Month(), targetDay.Day(), 0, 0, 0, 0, time.UTC)

	loader, err := benchmarking.NewClickHouseLoader(executor.conn, run.OrganizationID)
	if err != nil {
		return 0, err
	}

	computedAt := executor.nowUTC()
	outputs, err := benchmarking.ComputeBenchmarkingForDay(
		ctx, loader, asOfDay, computedAt, run.OrganizationID,
		benchmarking.DefaultBenchmarkMetrics,
		benchmarking.DefaultCorrelationPairs,
		executor.logger,
	)
	if err != nil {
		// Only insight generation propagates; every per-metric failure was
		// already swallowed inside, matching Python.
		return 0, err
	}

	rowsWritten, err := executor.writer.WriteOutputs(ctx, outputs, run.OrganizationID)
	if err != nil {
		return 0, err
	}
	return rowsWritten, nil
}

func containsRepository(repoIDs []uuid.UUID, target uuid.UUID) bool {
	for _, repoID := range repoIDs {
		if repoID == target {
			return true
		}
	}
	return false
}

var _ NativeFamilyExecutor = (*BenchmarkingExecutor)(nil)
