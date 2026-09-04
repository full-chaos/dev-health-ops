package daily

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// WorkItemAttributionExecutor is the NATIVE implementation of the
// `work_item_attribution` metrics.daily family (CHAOS-4283) -- ports
// compute_work_item_team_attributions
// (src/dev_health_ops/metrics/compute_work_items.py:1498) and writes
// work_item_team_attributions.
//
// # This is the family the other three WAIT for
//
// work_item, work_item_estimate and work_item_state all READ
// work_item_team_attributions in the same partition this family WRITES it.
// That dependency is declared in families.json as `after` and enforced by
// FamilyRunOrder -- see families.go. Registering this executor without that
// ordering would be worse than not registering it at all: the readers would
// silently consume the PREVIOUS run's snapshot while appearing to work.
//
// # Nothing here re-implements the cascade
//
// The 9-source resolve_team_attribution cascade lives in
// internal/teamattribution (extracted by CHAOS-3092 PR-A), the row building in
// remaining.BuildWorkItemAttributionRows, the fact composition in
// remaining.LoadWorkItemDerivationFacts, and the write in
// remaining.WorkItemAttributionClickHouseWriter. This executor supplies only
// what is genuinely different about the DAILY call site: its scope.
//
// # Scope: given, not detected
//
// The remaining-family backstop answers "what changed since my last run" --
// watermarks, detectScope, closure promotion. This family never asks that. Its
// scope is handed to it by the partition (org, repos, day), and it re-derives
// every work item in that window, exactly as Python does: job_daily.py calls
// compute_work_item_team_attributions over the same `work_items` list it just
// loaded for the day. None of the backstop's staleness machinery applies, and
// borrowing it would be answering a question the daily job does not have.
//
// # Dual writers (team-lead ruling, CHAOS-4283 PR2)
//
// This family owns work_item_team_attributions for its (org, repo, day)
// window; the remaining backstop narrows to staleness-only OUTSIDE that window.
// The table is ReplacingMergeTree(computed_at) ORDER BY (org_id, repo_id,
// work_item_id, ifNull(team_id,”), source) and appends EVERY candidate, so two
// writers on one key do not last-write-wins -- both snapshots stay resident and
// readers depend on the (work_item_id, max(computed_at)) fence to choose. That
// is why the ownership split is a real constraint and not a tidy-up.
type WorkItemAttributionExecutor struct {
	conn   driver.Conn
	writer *remaining.WorkItemAttributionClickHouseWriter
	nowUTC func() time.Time
}

var errWorkItemAttributionUnavailable = errors.New("work_item_attribution native executor unavailable")

// NewWorkItemAttributionExecutor fails closed on a nil connection, matching
// every other native family executor's construction contract.
func NewWorkItemAttributionExecutor(conn driver.Conn) (*WorkItemAttributionExecutor, error) {
	if conn == nil {
		return nil, errWorkItemAttributionUnavailable
	}
	writer, err := remaining.NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		return nil, err
	}
	return &WorkItemAttributionExecutor{
		conn:   conn,
		writer: writer,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// ComputeFamily runs the work_item_attribution computation for one partition.
func (executor *WorkItemAttributionExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil || executor.writer == nil {
		return 0, errWorkItemAttributionUnavailable
	}
	scope, err := newWorkItemPartitionScope(run, partition, "work_item_attribution")
	if err != nil {
		return 0, err
	}

	// Facts are ORG-scoped and read ONCE per partition, not once per repo.
	// They describe team/project/repo ownership and membership for the whole
	// org, so a per-repo reload would issue the same six queries N times for
	// an identical answer. `asOf` is the partition's day: ownership rows are
	// bitemporal, and resolving a historical day against today's validity
	// window would attribute that day using ownership that did not yet apply.
	facts, err := remaining.LoadWorkItemDerivationFacts(ctx, executor.conn, run.OrganizationID, scope.day)
	if err != nil {
		return 0, err
	}
	derived := teamattribution.NewGitHubWorkItemDerivationContext(facts)

	computedAt := executor.nowUTC()
	total := 0
	for _, repoID := range scope.repoIDs {
		subjects, err := executor.loadPartitionSubjects(ctx, run.OrganizationID, repoID, scope.start, scope.end)
		if err != nil {
			return total, err
		}
		if len(subjects) == 0 {
			continue
		}
		affected := make(map[string]struct{}, len(subjects))
		for workItemID := range subjects {
			affected[workItemID] = struct{}{}
		}
		rows := remaining.BuildWorkItemAttributionRows(
			run.OrganizationID, computedAt, affected, subjects, derived,
		)
		if len(rows) == 0 {
			continue
		}
		written, err := executor.writer.WriteAttributions(ctx, rows)
		if err != nil {
			return total, err
		}
		total += written
	}
	return total, nil
}

// loadPartitionSubjects reads one partition's (org, repo, day-window) work
// items as cascade subjects.
//
// The PREDICATE is this family's own -- byte-identical to
// LoadWorkItemMetricsWorkItems' and to Python's load_work_items, because
// job_daily.py hands compute_work_item_team_attributions the very same list it
// hands the other work-item computes. The COLUMN LIST and its positional scan
// are shared with the remaining-family backstop
// (remaining.WorkItemDerivationSubjectColumns /
// QueryWorkItemDerivationSubjects), because that binding is the part that must
// not drift between callers: most of these columns are strings, so a column
// added to one spelling and not the other would mis-bind with no type error.
func (executor *WorkItemAttributionExecutor) loadPartitionSubjects(
	ctx context.Context, organizationID string, repoID uuid.UUID, start, end time.Time,
) (map[string]teamattribution.GithubWorkItemDerivationSubject, error) {
	if executor.conn == nil || organizationID == "" || !start.Before(end) {
		return nil, ErrInvalidState
	}
	query := `SELECT ` + remaining.WorkItemDerivationSubjectColumns + `
FROM work_items FINAL
WHERE org_id = ? AND repo_id = ?
  AND created_at < ?
  AND (status != 'done' OR completed_at >= ?)`
	subjects, err := remaining.QueryWorkItemDerivationSubjects(
		ctx, executor.conn, query, organizationID, repoID.String(), end.UTC(), start.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("load work_item_attribution subjects: %w", err)
	}
	return subjects, nil
}

var _ NativeFamilyExecutor = (*WorkItemAttributionExecutor)(nil)
