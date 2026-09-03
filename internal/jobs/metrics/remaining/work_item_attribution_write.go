package remaining

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// ErrWorkItemAttributionWriteInvalidState is returned when a write is
// attempted without an organization scope -- the write-side counterpart of
// a caller bug in THIS package, matching membership_backfill's
// ErrMembershipWriteInvalidState shape.
var ErrWorkItemAttributionWriteInvalidState = errors.New(
	"work_item_attribution: organization id is required to write attribution data")

// workItemAttributionStampPrecision matches the sync-time deriver's own
// work_item_team_attributions column precision (githubTeamAttributionStampPrecision,
// internal/providersync/github_work_item_derived_surfaces.go) -- both writers
// target the same ReplacingMergeTree(computed_at) table, so a mismatched
// truncation would make the two writers' versions non-comparable.
const workItemAttributionStampPrecision = time.Millisecond

// WorkItemAttributionRow is one work_item_team_attributions candidate row --
// the row shape the sync-time deriver's writer already targets
// (githubWorkItemTeamAttributionRow, internal/providersync). Field order and
// meaning are identical; this is a separate type (not an import) because the
// sync-time type is providersync-private and this backstop's org-wide,
// multi-provider run does not go through providersync at all.
type WorkItemAttributionRow struct {
	WorkItemID string
	Provider   string
	Source     string
	IsPrimary  int
	Confidence string
	Evidence   string
	ComputedAt time.Time
	RepoID     *uuid.UUID
	TeamID     *string
	TeamName   *string
	OrgID      string
}

// WorkItemAttributionRunRecord is one work_item_attribution_backstop_runs
// row -- the org-wide completion marker, published as the LAST step of an
// org-wide run (CHAOS-2433 protocol, same as membership_backfill's
// MembershipRunRecord).
//
// PromotedReason is empty for a run that was org-wide from detectScope's own
// decision (an identities/teams change). It is non-empty ONLY when a scoped
// run's linked_issue closure exceeded the promotion bound and was widened to
// org-wide instead of writing a scoped marker for a set that was effectively
// the whole org anyway (team-lead's PR-B ruling) -- the one field this table
// does NOT mirror from #2177's work_unit_membership_runs, since membership
// has no equivalent closure-promotion concept.
type WorkItemAttributionRunRecord struct {
	OrgID          string
	RunID          string
	CompletedAt    time.Time
	PromotedReason string
}

// WorkItemAttributionScopedRunRecord is one
// work_item_attribution_backstop_scoped_runs row -- a repo- or
// project-scoped run's own marker, which never supersedes the org-wide
// "latest complete run" (CHAOS-2433 finding #2, mirrored from
// membership_backfill's MembershipScopedRunRecord).
type WorkItemAttributionScopedRunRecord struct {
	OrgID       string
	ScopeKind   string
	ScopeID     string
	RunID       string
	CompletedAt time.Time
}

// WorkItemAttributionWriter is the write side of the backstop: batched
// attribution-row inserts plus the two run-marker tables. An INTERFACE
// rather than a concrete type bound to *WorkItemAttributionExecutor, so
// ComputeOrg can be exercised in a unit test against a fake, matching
// membership_backfill's MembershipWriter shape.
type WorkItemAttributionWriter interface {
	// WriteAttributions inserts work_item_team_attributions rows, deduped by
	// the same ClickHouse sorting key the sync-time writer uses (repo_id,
	// work_item_id, team_id, source), keeping the highest computed_at and
	// breaking an equal-version tie in favor of the primary row. Returns the
	// number of rows written.
	WriteAttributions(ctx context.Context, rows []WorkItemAttributionRow) (int, error)
	// WriteAttributionRun publishes the org-wide completion marker. Callers
	// MUST call this only after WriteAttributions for the same run has
	// returned successfully (CHAOS-2433 protocol: rows first, marker last).
	WriteAttributionRun(ctx context.Context, record WorkItemAttributionRunRecord) error
	// WriteScopedAttributionRuns publishes one marker per repo/project
	// scope. Never publishes (or substitutes for) the org-wide marker.
	WriteScopedAttributionRuns(ctx context.Context, records []WorkItemAttributionScopedRunRecord) error
}

// workItemAttributionWriterConn is the narrow ClickHouse capability this
// file needs, matching membership_write.go's own conn interface shape.
type workItemAttributionWriterConn interface {
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

// WorkItemAttributionClickHouseWriter is the concrete ClickHouse-backed
// WorkItemAttributionWriter.
type WorkItemAttributionClickHouseWriter struct {
	conn workItemAttributionWriterConn
}

// NewWorkItemAttributionClickHouseWriter constructs the writer. conn is
// accepted as the interface (not driver.Conn) so a caller passing anything
// satisfying it -- production's real connection or a test double -- works
// unchanged.
func NewWorkItemAttributionClickHouseWriter(conn workItemAttributionWriterConn) (*WorkItemAttributionClickHouseWriter, error) {
	if conn == nil {
		return nil, ErrWorkItemAttributionUnavailable
	}
	return &WorkItemAttributionClickHouseWriter{conn: conn}, nil
}

// WriteAttributions inserts work_item_team_attributions rows. Column order
// matches the sync-time deriver's own INSERT
// (internal/providersync/github_work_item_derived_effects_clickhouse.go)
// exactly, so the two writers are byte-for-byte interchangeable at the
// storage layer.
func (w *WorkItemAttributionClickHouseWriter) WriteAttributions(
	ctx context.Context, rows []WorkItemAttributionRow,
) (int, error) {
	if w == nil || w.conn == nil {
		return 0, ErrWorkItemAttributionUnavailable
	}
	if len(rows) == 0 {
		return 0, nil
	}
	// Collisions are genuinely reachable here, same reason as the sync-time
	// writer: the resolver emits one candidate per ownership fact, so two
	// facts naming the same team differently produce two rows with an
	// identical sorting key that differ only in team_name.
	rows = workItemAttributionSortingKeyDedupe(rows)
	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO work_item_team_attributions
(org_id, repo_id, work_item_id, provider, team_id, team_name, source,
is_primary, confidence, evidence, computed_at)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_item_team_attributions batch: %w", err)
	}
	for _, row := range rows {
		if strings.TrimSpace(row.OrgID) == "" {
			return 0, ErrWorkItemAttributionWriteInvalidState
		}
		repoID := uuid.Nil
		if row.RepoID != nil {
			repoID = *row.RepoID
		}
		if err := batch.Append(
			row.OrgID, repoID, row.WorkItemID,
			row.Provider, row.TeamID, row.TeamName, row.Source,
			uint8(row.IsPrimary), row.Confidence, row.Evidence,
			row.ComputedAt.UTC().Truncate(workItemAttributionStampPrecision),
		); err != nil {
			return 0, fmt.Errorf("append work_item_team_attributions row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send work_item_team_attributions batch: %w", err)
	}
	return len(rows), nil
}

// WriteAttributionRun publishes the org-wide completion marker.
func (w *WorkItemAttributionClickHouseWriter) WriteAttributionRun(
	ctx context.Context, record WorkItemAttributionRunRecord,
) error {
	if w == nil || w.conn == nil {
		return ErrWorkItemAttributionUnavailable
	}
	if strings.TrimSpace(record.OrgID) == "" {
		return ErrWorkItemAttributionWriteInvalidState
	}
	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO work_item_attribution_backstop_runs (
		org_id, run_id, completed_at, promoted_reason
	)`)
	if err != nil {
		return fmt.Errorf("prepare work_item_attribution_backstop_runs batch: %w", err)
	}
	if err := batch.Append(
		record.OrgID, record.RunID, record.CompletedAt.UTC(), record.PromotedReason,
	); err != nil {
		return fmt.Errorf("append work_item_attribution_backstop_runs row: %w", err)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send work_item_attribution_backstop_runs batch: %w", err)
	}
	return nil
}

// WriteScopedAttributionRuns publishes repo/project-scoped markers. Never
// touches the org-wide table.
func (w *WorkItemAttributionClickHouseWriter) WriteScopedAttributionRuns(
	ctx context.Context, records []WorkItemAttributionScopedRunRecord,
) error {
	if w == nil || w.conn == nil {
		return ErrWorkItemAttributionUnavailable
	}
	if len(records) == 0 {
		return nil
	}
	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO work_item_attribution_backstop_scoped_runs (
		org_id, scope_kind, scope_id, run_id, completed_at
	)`)
	if err != nil {
		return fmt.Errorf("prepare work_item_attribution_backstop_scoped_runs batch: %w", err)
	}
	for _, record := range records {
		if strings.TrimSpace(record.OrgID) == "" {
			return ErrWorkItemAttributionWriteInvalidState
		}
		if err := batch.Append(
			record.OrgID, record.ScopeKind, record.ScopeID,
			record.RunID, record.CompletedAt.UTC(),
		); err != nil {
			return fmt.Errorf("append work_item_attribution_backstop_scoped_runs row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send work_item_attribution_backstop_scoped_runs batch: %w", err)
	}
	return nil
}

// workItemAttributionSortingKey mirrors githubTeamAttributionSortingKey
// (internal/providersync): the ClickHouse ORDER BY key for
// work_item_team_attributions.
func workItemAttributionSortingKey(row WorkItemAttributionRow) string {
	repoID := uuid.Nil
	if row.RepoID != nil {
		repoID = *row.RepoID
	}
	teamID := ""
	if row.TeamID != nil {
		teamID = *row.TeamID
	}
	return strings.Join([]string{repoID.String(), row.WorkItemID, teamID, row.Source}, "\x00")
}

// workItemAttributionSortingKeyDedupe mirrors githubWorkItemDerivedSortingKeyDedupe
// (internal/providersync/github_work_item_derived_effects_clickhouse.go):
// collapses rows that share a full ClickHouse sorting key, keeping the
// HIGHEST computed_at version, and breaking an equal-version tie in favor of
// the PRIMARY row (CHAOS-4244 codex round-3, HIGH) before falling back to
// last-wins. See that function's doc comment for why version must win over
// insertion order: this table is a ReplacingMergeTree keyed on computed_at,
// so an order-only dedup can name a row the server discards.
func workItemAttributionSortingKeyDedupe(rows []WorkItemAttributionRow) []WorkItemAttributionRow {
	winner := make(map[string]int, len(rows))
	for index, row := range rows {
		key := workItemAttributionSortingKey(row)
		current, exists := winner[key]
		if !exists {
			winner[key] = index
			continue
		}
		existing := rows[current]
		switch {
		case row.ComputedAt.Before(existing.ComputedAt):
			// existing is strictly newer; keep it.
		case existing.ComputedAt.Before(row.ComputedAt):
			winner[key] = index
		case existing.IsPrimary == 1:
			// equal version, existing already primary; keep it.
		case row.IsPrimary == 1:
			winner[key] = index
		default:
			// equal version, neither (or both) primary: last-wins.
			winner[key] = index
		}
	}
	result := make([]WorkItemAttributionRow, 0, len(winner))
	seen := make(map[string]bool, len(winner))
	for _, row := range rows {
		key := workItemAttributionSortingKey(row)
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, rows[winner[key]])
	}
	return result
}
