package remaining

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// legacyMembershipRunID is the seeded marker migration 048 wrote so every
// tenant has a valid "latest complete run" the moment 047/049 land, before
// any real backfill or materializer run has executed. Membership rows
// written before the run_id protocol existed carry run_id="" (047's column
// default), NOT this string -- see the translation in pruneMembershipRuns.
const legacyMembershipRunID = "__legacy__"

// ErrMembershipWriteInvalidState is returned when a write is attempted
// without an organization scope -- the write-side counterpart of
// ErrInvalidState, kept distinct because a missing org here is a caller bug
// in THIS package, never a malformed upstream partition payload.
var ErrMembershipWriteInvalidState = errors.New(
	"membership_backfill: organization id is required to write membership data")

// MembershipRunRecord is one work_unit_membership_runs row -- the completion
// marker published as the LAST step of an org-wide run (CHAOS-2433).
type MembershipRunRecord struct {
	OrgID       string
	RunID       string
	CompletedAt time.Time
}

// MembershipScopedRunRecord is one work_unit_membership_scoped_runs row -- a
// repo-scoped run's own marker, which never supersedes the org-wide "latest
// complete run" (CHAOS-2433 finding #2).
type MembershipScopedRunRecord struct {
	OrgID       string
	ScopeKind   string
	ScopeID     string
	RunID       string
	CompletedAt time.Time
}

// MembershipWriter is the write side of the membership backfill: batched
// inserts plus the retention prune. An INTERFACE rather than a concrete type
// bound to *MembershipExecutor, so ComputeOrg can be exercised in a unit test
// against a fake, and so the concrete implementation below can be relocated
// (e.g. beside lane-4441's chwrite, #2171) without touching the executor.
type MembershipWriter interface {
	// WriteMemberships inserts membership rows, stamping every row with
	// orgID. Returns the number of rows written.
	WriteMemberships(ctx context.Context, orgID string, records []units.MembershipRecord) (int, error)
	// WriteMembershipRun publishes the org-wide completion marker. Callers
	// MUST call this only after WriteMemberships for the same run has
	// returned successfully (CHAOS-2433 protocol: rows first, marker last).
	WriteMembershipRun(ctx context.Context, record MembershipRunRecord) error
	// WriteScopedMembershipRuns publishes one marker per repo scope. Never
	// publishes (or substitutes for) the org-wide marker.
	WriteScopedMembershipRuns(ctx context.Context, records []MembershipScopedRunRecord) error
	// PruneMembershipRuns retains only the latest `keep` COMPLETE org-wide
	// runs, deleting older generations' membership rows and markers. Returns
	// the number of generations pruned.
	PruneMembershipRuns(ctx context.Context, orgID string, keep int) (int, error)
}

// conn is the narrow ClickHouse capability this file needs, matching
// chwrite's own conn interface shape.
type membershipWriterConn interface {
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
	Exec(ctx context.Context, query string, args ...any) error
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

// MembershipClickHouseWriter is the concrete ClickHouse-backed
// MembershipWriter. It ports write_work_unit_memberships, write_membership_run,
// write_scoped_membership_runs and prune_membership_runs
// (src/dev_health_ops/metrics/sinks/clickhouse/investment.py) -- three tables
// chwrite (#2171) does not own: work_unit_membership,
// work_unit_membership_runs, work_unit_membership_scoped_runs.
type MembershipClickHouseWriter struct {
	conn membershipWriterConn
}

// NewMembershipClickHouseWriter builds a writer over an established
// ClickHouse connection.
func NewMembershipClickHouseWriter(connection membershipWriterConn) (*MembershipClickHouseWriter, error) {
	if connection == nil {
		return nil, ErrMembershipUnavailable
	}
	return &MembershipClickHouseWriter{conn: connection}, nil
}

// WriteMemberships inserts work_unit_membership rows. Column order matches
// write_work_unit_memberships (investment.py) exactly.
func (w *MembershipClickHouseWriter) WriteMemberships(
	ctx context.Context, orgID string, records []units.MembershipRecord,
) (int, error) {
	if w == nil || w.conn == nil {
		return 0, ErrMembershipUnavailable
	}
	if strings.TrimSpace(orgID) == "" {
		return 0, ErrMembershipWriteInvalidState
	}
	if len(records) == 0 {
		return 0, nil
	}
	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO work_unit_membership (
		org_id, node_type, node_id, work_unit_id, category_kind, category,
		weight, is_dominant, categorization_status, computed_at, run_id
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_unit_membership batch: %w", err)
	}
	for _, record := range records {
		if err := batch.Append(
			orgID, record.NodeType, record.NodeID, record.WorkUnitID,
			record.CategoryKind, record.Category, record.Weight,
			uint8(record.IsDominant), record.CategorizationStatus,
			record.ComputedAt.UTC(), record.RunID,
		); err != nil {
			return 0, fmt.Errorf("append work_unit_membership row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send work_unit_membership batch: %w", err)
	}
	return len(records), nil
}

// WriteMembershipRun publishes the org-wide completion marker.
func (w *MembershipClickHouseWriter) WriteMembershipRun(
	ctx context.Context, record MembershipRunRecord,
) error {
	if w == nil || w.conn == nil {
		return ErrMembershipUnavailable
	}
	if strings.TrimSpace(record.OrgID) == "" {
		return ErrMembershipWriteInvalidState
	}
	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO work_unit_membership_runs (
		org_id, run_id, completed_at
	)`)
	if err != nil {
		return fmt.Errorf("prepare work_unit_membership_runs batch: %w", err)
	}
	if err := batch.Append(record.OrgID, record.RunID, record.CompletedAt.UTC()); err != nil {
		return fmt.Errorf("append work_unit_membership_runs row: %w", err)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send work_unit_membership_runs batch: %w", err)
	}
	return nil
}

// WriteScopedMembershipRuns publishes repo-scoped markers. Never touches the
// org-wide table.
func (w *MembershipClickHouseWriter) WriteScopedMembershipRuns(
	ctx context.Context, records []MembershipScopedRunRecord,
) error {
	if w == nil || w.conn == nil {
		return ErrMembershipUnavailable
	}
	if len(records) == 0 {
		return nil
	}
	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO work_unit_membership_scoped_runs (
		org_id, scope_kind, scope_id, run_id, completed_at
	)`)
	if err != nil {
		return fmt.Errorf("prepare work_unit_membership_scoped_runs batch: %w", err)
	}
	for _, record := range records {
		if strings.TrimSpace(record.OrgID) == "" {
			return ErrMembershipWriteInvalidState
		}
		if err := batch.Append(
			record.OrgID, record.ScopeKind, record.ScopeID,
			record.RunID, record.CompletedAt.UTC(),
		); err != nil {
			return fmt.Errorf("append work_unit_membership_scoped_runs row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send work_unit_membership_scoped_runs batch: %w", err)
	}
	return nil
}

// PruneMembershipRuns ports prune_membership_runs (investment.py). Retains
// the latest `keep` COMPLETE org-wide runs; deletes older generations' rows
// from BOTH work_unit_membership and work_unit_membership_runs, scoped to
// orgID and the explicit dropped run_id set so the pass is idempotent and
// safe to run concurrently across orgs.
//
// keep-latest-2 (the caller's default, not enforced here): keeps the current
// latest run PLUS one prior, so a reader/overlap mid-flight against the
// immediately-previous complete run is not pulled out from under it.
//
// IN-FLIGHT SAFETY: the candidate set comes EXCLUSIVELY from
// work_unit_membership_runs (markered runs). A markerless in-flight run is
// therefore never in the delete set.
func (w *MembershipClickHouseWriter) PruneMembershipRuns(
	ctx context.Context, orgID string, keep int,
) (int, error) {
	if w == nil || w.conn == nil {
		return 0, ErrMembershipUnavailable
	}
	if strings.TrimSpace(orgID) == "" {
		return 0, ErrMembershipWriteInvalidState
	}
	if keep < 0 {
		return 0, fmt.Errorf("%w: keep must be >= 0", ErrMembershipWriteInvalidState)
	}

	rows, err := w.conn.Query(ctx, `
		SELECT run_id
		FROM work_unit_membership_runs
		WHERE org_id = {org_id:String}
		ORDER BY completed_at DESC, run_id DESC
	`, clickhouse.Named("org_id", orgID))
	if err != nil {
		return 0, fmt.Errorf("query work_unit_membership_runs: %w", err)
	}
	var runIDs []string
	for rows.Next() {
		var runID string
		if scanErr := rows.Scan(&runID); scanErr != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan work_unit_membership_runs row: %w", scanErr)
		}
		runIDs = append(runIDs, runID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("iterate work_unit_membership_runs rows: %w", err)
	}
	_ = rows.Close()

	dropMarkerRunIDs, dropRowRunIDs := membershipRunsToPrune(runIDs, keep)
	if len(dropMarkerRunIDs) == 0 {
		return 0, nil
	}

	if err := w.conn.Exec(ctx,
		"ALTER TABLE work_unit_membership DELETE WHERE org_id = {org_id:String} "+
			"AND run_id IN {run_ids:Array(String)}",
		clickhouse.Named("org_id", orgID), clickhouse.Named("run_ids", dropRowRunIDs),
	); err != nil {
		return 0, fmt.Errorf("prune work_unit_membership: %w", err)
	}
	if err := w.conn.Exec(ctx,
		"ALTER TABLE work_unit_membership_runs DELETE WHERE org_id = {org_id:String} "+
			"AND run_id IN {run_ids:Array(String)}",
		clickhouse.Named("org_id", orgID), clickhouse.Named("run_ids", dropMarkerRunIDs),
	); err != nil {
		return 0, fmt.Errorf("prune work_unit_membership_runs: %w", err)
	}
	return len(dropMarkerRunIDs), nil
}

var _ MembershipWriter = (*MembershipClickHouseWriter)(nil)

// membershipRunsToPrune is PruneMembershipRuns' pure decision logic, split
// out from the SQL so it is testable without a ClickHouse connection: given
// the org's run_ids read in (completed_at DESC, run_id DESC) order (possibly
// containing duplicate versions of the same run_id, since the read has no
// FINAL), return the marker run_ids to delete from work_unit_membership_runs
// and the corresponding row run_ids to delete from work_unit_membership.
//
// The two returned slices are the same length and index-aligned, but are NOT
// the same values: the legacy marker's row run_id is "" (047's column
// default), never the literal "__legacy__" the seed migration stamped onto
// the MARKER.
func membershipRunsToPrune(orderedRunIDs []string, keep int) (dropMarkerRunIDs, dropRowRunIDs []string) {
	// Dedup preserving order (ReplacingMergeTree may surface unmerged
	// duplicate versions of the same run_id; the read has no FINAL, so
	// collapse ourselves) -- same reasoning as the Python sink.
	seen := make(map[string]struct{}, len(orderedRunIDs))
	orderedUnique := make([]string, 0, len(orderedRunIDs))
	for _, id := range orderedRunIDs {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		orderedUnique = append(orderedUnique, id)
	}

	if len(orderedUnique) <= keep {
		return nil, nil
	}
	dropMarkerRunIDs = orderedUnique[keep:]
	dropRowRunIDs = make([]string, len(dropMarkerRunIDs))
	for i, id := range dropMarkerRunIDs {
		if id == legacyMembershipRunID {
			dropRowRunIDs[i] = ""
		} else {
			dropRowRunIDs[i] = id
		}
	}
	return dropMarkerRunIDs, dropRowRunIDs
}
