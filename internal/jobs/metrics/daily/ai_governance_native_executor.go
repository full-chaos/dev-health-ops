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

	// WRITE ORDER IS LOAD-BEARING: the SELF-MERGING table goes FIRST, the
	// non-mergeable one LAST. Do not reorder these two calls.
	//
	// ClickHouse gives no cross-table transaction, so a two-table write can
	// always commit the first and fail the second. What happens then is decided
	// by computeNativeFamilies (daily.go:588-595): on ANY error the family is
	// NOT added to skipFamilies, so the Python compatibility bridge computes
	// and writes BOTH tables for this partition. That fallback is fail-open BY
	// DESIGN (chris's ruling, CHAOS-4276) -- one family degrading must not fail
	// the partition -- so the executor has to be safe under it rather than try
	// to prevent it.
	//
	// The two tables behave differently under a duplicate write:
	//
	//   ai_governance_coverage_daily -- ORDER BY (org_id, team_id, repo_id, day),
	//     no random component. Python's rewrite lands on the SAME key and the
	//     ReplacingMergeTree collapses it. Duplicating this is harmless.
	//
	//   ai_policy_events -- ORDER BY (..., event_id), and Python's event_id is
	//     uuid4 (models.py:113) while ours is derived. The two can NEVER merge,
	//     so a Go row plus a Python fallback row is a PERMANENT duplicate
	//     policy event for one artifact.
	//
	// Writing coverage first makes every failure mode safe: if coverage fails,
	// nothing was committed at all; if coverage succeeds and policy events
	// fail, the only committed rows are ones Python's rewrite merges away. The
	// non-mergeable table is only ever written once the mergeable one is
	// already durable, and if it fails it has written nothing.
	//
	// Found by codex round 1 on #2229 (P1), which correctly traced the fallback
	// path; the original order wrote policy events first.
	// Pinned by TestGovernanceWritesTheMergeableTableFirst.
	writtenCoverage, err := WriteAIGovernanceCoverageDaily(ctx, executor.conn, coverage, computedAt)
	if err != nil {
		// #2276 confirmation-pass P1: WriteAIGovernanceCoverageDaily's own
		// Send() branch already reports its true row count on an ambiguous
		// network error (the F1 sweep) -- `return 0, err` here discarded
		// that count a second time, exactly the class this whole PR closes,
		// one layer up.
		return wrapAIGovernanceCoverageWritePartialWrite(writtenCoverage, err)
	}
	writtenEvents, err := WriteAIPolicyEvents(ctx, executor.conn, violations, computedAt)
	if err != nil {
		// codex sweep (CHAOS-5190 r3 follow-up, team-lead-requested): the
		// coverage table above already landed writtenCoverage rows durably
		// (it is written FIRST specifically so this branch's failure mode is
		// safe -- see the write-order comment above). Returning `0, err`
		// here would tell this family's dispatcher "refused, 0 rows" despite
		// real rows already on disk -- the same class already fixed in
		// work_item_state/work_item/work_item_estimate/work_graph_edges.
		return wrapAIGovernancePartialWrite(writtenCoverage, err)
	}
	// Both tables count toward this family's rows-written telemetry, matching
	// how Python's two unconditional writes (job_daily.py:1904-1905) both
	// belong to ai_governance.
	return writtenEvents + writtenCoverage, nil
}

// wrapAIGovernancePartialWrite mirrors wrapWorkGraphEdgesPartialWrite's/
// wrapWorkItemPartialWrite's exact shape for this executor's two-table
// sequential write (not a per-repo loop, but the same "a later step failed
// after an earlier one already landed rows" hazard): writtenCoverage == 0
// returns the error unwrapped (nothing landed, ordinary fail-open to the
// Python bridge is correct); writtenCoverage > 0 wraps ErrPartialWrite
// naming the count, so the caller reports PartialWrite/N-rows instead of
// Refused/0-rows.
func wrapAIGovernancePartialWrite(writtenCoverage int, err error) (int, error) {
	if writtenCoverage == 0 {
		return 0, err
	}
	return writtenCoverage, fmt.Errorf(
		"%w: ai_governance failed writing ai_policy_events after %d ai_governance_coverage_daily row(s) already landed: %w",
		ErrPartialWrite, writtenCoverage, err)
}

// wrapAIGovernanceCoverageWritePartialWrite mirrors wrapAIGovernancePartialWrite's
// exact shape, for the OTHER failure site: the ai_governance_coverage_daily
// write itself failing (its own batch.Send() ambiguity), rather than the
// ai_policy_events write that follows it. written==0 returns the error
// unwrapped (ordinary refusal, nothing landed); written>0 wraps
// ErrPartialWrite naming the true row count already durable on
// ai_governance_coverage_daily.
func wrapAIGovernanceCoverageWritePartialWrite(written int, err error) (int, error) {
	if written == 0 {
		return 0, err
	}
	return written, fmt.Errorf(
		"%w: ai_governance failed writing ai_governance_coverage_daily itself after %d row(s) potentially already landed: %w",
		ErrPartialWrite, written, err)
}

var _ NativeFamilyExecutor = (*AIGovernanceExecutor)(nil)
