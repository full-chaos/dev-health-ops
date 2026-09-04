package daily

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aigovernance"
)

// AIGovernanceExecutor is the NATIVE implementation of the ai_governance
// metrics.daily family (CHAOS-4285). It is a thin ClickHouse adapter over the
// pure internal/jobs/metrics/aigovernance kernel; all fidelity notes for the
// compute itself live on that package, and the query's notes live on
// LoadGovernanceArtifacts.
//
// # This family is ORG-scoped, and that is not a bug in this executor
//
// Every other native daily family reads partition.RepoIDs and computes
// per-repo rows. This one does NOT, because its Python original does not:
// build_governance_rows_for_day takes only (client, org_id, day)
// (loaders.py:113) and job_daily.py:1671 calls it with exactly those, never
// passing repo_id -- while job_daily.py has no per-repo loop at all, only a
// per-DAY one (:1350). So Python computes the same org-wide rows once per
// PARTITION and writes them once per partition; an org with N repo partitions
// writes N copies of an identical row set every day.
//
// This executor reproduces that call shape exactly rather than trying to
// deduplicate across partitions, which would require cross-partition state no
// daily executor has. What makes the repetition harmless HERE, and did not in
// Python, is the deterministic event_id (aigovernance.newViolation's doc
// comment): with it, all N writes produce byte-identical rows on identical
// ReplacingMergeTree keys and collapse to one. With Python's uuid4() they
// could not, which is why the two decisions are load-bearing for each other
// and neither should be reverted alone.
//
// RepoIDs is therefore validated but not used for scoping -- see ComputeFamily.
type AIGovernanceExecutor struct {
	conn   driver.Conn
	nowUTC func() time.Time
}

var errAIGovernanceUnavailable = fmt.Errorf("ai_governance native executor unavailable")

// NewAIGovernanceExecutor fails closed on a nil connection, matching every
// other native family's construction-time policy: a refused executor never
// enters PartitionHandler's native map, and ai_governance stays on the Python
// compatibility bridge until the worker restarts with a healthy connection.
func NewAIGovernanceExecutor(conn driver.Conn) (*AIGovernanceExecutor, error) {
	if conn == nil {
		return nil, errAIGovernanceUnavailable
	}
	return &AIGovernanceExecutor{
		conn:   conn,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// ComputeFamily implements NativeFamilyExecutor.
func (executor *AIGovernanceExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil {
		return 0, errAIGovernanceUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: partition %s run has no organization or target day", ErrInvalidState, partition.ID)
	}

	// Validated, then deliberately NOT used as a query filter (see the type's
	// doc comment). A malformed durable partition scope is still a
	// precondition failure worth refusing on: this family being org-scoped is
	// no reason to accept a partition whose recorded scope is corrupt.
	if _, err := parseRepositoryUUIDs(partition.RepoIDs); err != nil {
		return 0, fmt.Errorf("%w: partition %s repo_ids: %v", ErrInvalidState, partition.ID, err)
	}

	targetDay := run.TargetDay.UTC()
	dayStart := time.Date(targetDay.Year(), targetDay.Month(), targetDay.Day(), 0, 0, 0, 0, time.UTC)
	// Python's window is datetime.combine(day, time.max) -- an INCLUSIVE bound
	// at 23:59:59.999999, not the next midnight. See LoadGovernanceArtifacts.
	dayEndInclusive := dayStart.Add(24*time.Hour - time.Microsecond)
	computedAt := executor.nowUTC()

	artifacts, err := LoadGovernanceArtifacts(ctx, executor.conn, run.OrganizationID, dayStart, dayEndInclusive)
	if err != nil {
		return 0, err
	}

	// build_governance_rows_for_day returns BOTH lists from ONE artifact load
	// (loaders.py:120-123). Loading twice would be a different query against a
	// moving table and could produce a violation set inconsistent with the
	// coverage rollup computed beside it.
	violations := aigovernance.EvaluateArtifacts(artifacts)
	coverage := aigovernance.RollupCoverageDaily(artifacts, dayStart)

	writtenEvents, err := WriteAIPolicyEvents(ctx, executor.conn, violations, computedAt)
	if err != nil {
		return 0, err
	}
	writtenCoverage, err := WriteAIGovernanceCoverageDaily(ctx, executor.conn, coverage, computedAt)
	if err != nil {
		return 0, err
	}
	// Both tables count toward this family's rows-written telemetry, matching
	// how Python's two unconditional writes (job_daily.py:1904-1905) both
	// belong to ai_governance.
	return writtenEvents + writtenCoverage, nil
}

var _ NativeFamilyExecutor = (*AIGovernanceExecutor)(nil)
