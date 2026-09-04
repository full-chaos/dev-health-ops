package daily

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
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

// orgAnchorRepositoryQuery is ClickHouseRepositoryDiscoverer.RepositoryIDs'
// query reduced to its minimum id. Deriving the anchor from the same source of
// truth is what makes "the org's first repo" mean the same thing here as in
// partition fan-out and in ci/assert_metrics_executed_proof.py's live_repo_ids.
const orgAnchorRepositoryQuery = `
SELECT min(id) FROM (
    SELECT id, argMax(tuple(repo, settings, provider), last_synced) AS latest
    FROM repos
    WHERE org_id = {org_id:String}
    GROUP BY org_id, id
)`

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

	anchor, err := executor.orgAnchorRepository(ctx, run.OrganizationID)
	if err != nil {
		return 0, err
	}
	if anchor == uuid.Nil {
		// The org has no repos in ClickHouse: nothing to benchmark, and not an
		// error.
		return 0, nil
	}
	if !containsRepository(repoIDs, anchor) {
		// Another partition owns this org/day's single benchmarking run.
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

func (executor *BenchmarkingExecutor) orgAnchorRepository(ctx context.Context, orgID string) (uuid.UUID, error) {
	rows, err := executor.conn.Query(ctx, orgAnchorRepositoryQuery, clickhouse.Named("org_id", orgID))
	if err != nil {
		return uuid.Nil, fmt.Errorf("resolve org anchor repository: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return uuid.Nil, err
		}
		return uuid.Nil, nil
	}
	var anchor uuid.UUID
	if err := rows.Scan(&anchor); err != nil {
		return uuid.Nil, fmt.Errorf("scan org anchor repository: %w", err)
	}
	if err := rows.Err(); err != nil {
		return uuid.Nil, err
	}
	return anchor, nil
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
