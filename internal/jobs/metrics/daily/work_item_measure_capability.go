package daily

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// WorkItemMeasureCapabilityFamilyName is the finalize family that writes
// work_item_measure_capability.
const WorkItemMeasureCapabilityFamilyName = "work_item_measure_capability"

// WorkItemMeasureCapabilityExecutor writes, once for each daily run, whether
// each work-item provider of the organization tracks story points and bugs
// (CHAOS-8895). It is RUN-scoped: the answer is the organization's, read from
// every repository, so a partition-scoped family would compute the same rows
// once for each partition. It reads only stored work items, never the output
// of another family, so it has no co-registration dependency.
type WorkItemMeasureCapabilityExecutor struct {
	conn   driver.Conn
	nowUTC func() time.Time
}

var errWorkItemMeasureCapabilityUnavailable = errors.New("work_item_measure_capability native executor unavailable")

// NewWorkItemMeasureCapabilityExecutor fails closed on a nil connection.
func NewWorkItemMeasureCapabilityExecutor(conn driver.Conn) (*WorkItemMeasureCapabilityExecutor, error) {
	if conn == nil {
		return nil, errWorkItemMeasureCapabilityUnavailable
	}
	return &WorkItemMeasureCapabilityExecutor{conn: conn, nowUTC: func() time.Time { return time.Now().UTC() }}, nil
}

// ComputeFinalizeFamily implements NativeFinalizeFamilyExecutor. Every error
// propagates, so the finalize retries; a retry appends rows with a later
// computed_at, which the readers' argMax dedups.
func (executor *WorkItemMeasureCapabilityExecutor) ComputeFinalizeFamily(ctx context.Context, run Run) (int, error) {
	if executor == nil || executor.conn == nil {
		return 0, errWorkItemMeasureCapabilityUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: run has no organization or target day", ErrInvalidState)
	}
	mutantCapabilityRun = run.ID
	return computeWorkItemMeasureCapability(
		ctx, executor.conn, run.OrganizationID, workitemmetrics.UTCDay(run.TargetDay), executor.nowUTC(),
	)
}

var _ NativeFinalizeFamilyExecutor = (*WorkItemMeasureCapabilityExecutor)(nil)

// WorkItemMeasureCapabilityLogMessage is logged once for each compute of the
// capability rows, with the counts of the read and the write.
const WorkItemMeasureCapabilityLogMessage = "daily work item measure capability"

// workItemCapabilityRead is the items of the capability window: each item once, at its current copy.
type workItemCapabilityRead struct {
	items      []workitemmetrics.Item
	duplicates int
	// offWindow is the number of items the window read gave whose current copy is not in the window.
	offWindow int
}

// loadWorkItemCapabilityItems reads every item of the organization in the
// capability window of day, from every repository, each item counted once at
// its current copy (currentWorkItemCopies, the rule of the daily families): an
// item whose current copy is not in the window is not an item of the window,
// whatever an older copy says. The capability answers for (organization,
// provider), so the read takes no partition or scope filter: a subset of the
// items would answer "not tracked" from the items it did not read.
func loadWorkItemCapabilityItems(
	ctx context.Context, conn repositoryRows, organizationID string, day time.Time,
) (workItemCapabilityRead, error) {
	first, last := workitemmetrics.CapabilityWindow(day)
	stored, err := loadWorkItemScopeItems(
		ctx, conn, organizationID, first, last.AddDate(0, 0, 1), nil, false, false, true,
	)
	if err != nil {
		return workItemCapabilityRead{}, fmt.Errorf("load work item capability items: %w", err)
	}
	if kept, ok := mutantCapabilityInputs[mutantCapabilityRun]; ok {
		stored = kept
	} else {
		mutantCapabilityInputs[mutantCapabilityRun] = stored
	}
	winners, duplicates := countWorkItemOncePerProviderAndID(stored)
	copies, err := loadWorkItemCopies(ctx, conn, organizationID, workItemIDsOf(winners))
	if err != nil {
		return workItemCapabilityRead{}, fmt.Errorf("load work item capability items: %w", err)
	}
	current := currentWorkItemCopies(copies)
	read := workItemCapabilityRead{duplicates: duplicates}
	rows := make([]workItemMetricsRow, 0, len(winners))
	for _, row := range winners {
		if currentCopy, found := current[workItemIdentity{row.Provider, row.WorkItemID}]; found && currentCopy.repoID == row.RepoID {
			rows = append(rows, row.workItemMetricsRow)
			continue
		}
		read.offWindow++
	}
	read.items = workItemMetricsItems(rows)
	return read, nil
}

// computeWorkItemMeasureCapability reads the window, derives the rows and
// writes them, then retires each key of the day it did not produce (see
// retireWorkItemMeasureCapability). It returns the true row count on an
// ambiguous Send error, like every Write* of this package.
//
// The live keys are read FIRST, then the items: a key that is right has its
// items stored before its row, so a live key that the items still give is
// produced again, whenever it was written.
func computeWorkItemMeasureCapability(
	ctx context.Context, conn workItemCapabilityConn, organizationID string, day, computedAt time.Time,
) (int, error) {
	live, err := loadLiveWorkItemCapabilityKeys(ctx, conn, organizationID, day)
	if err != nil {
		return 0, err
	}
	read, err := loadWorkItemCapabilityItems(ctx, conn, organizationID, day)
	if err != nil {
		return 0, err
	}
	rows := workitemmetrics.DeriveMeasureCapability(day, read.items)
	written, err := WriteWorkItemMeasureCapability(ctx, conn, organizationID, rows, computedAt)
	retired := 0
	if err == nil {
		// The proof of an empty window is for a read that gave no row; a read whose every item is off the window
		// saw the window's rows.
		retired, err = retireWorkItemMeasureCapability(ctx, conn, organizationID, day, computedAt, live, rows,
			len(read.items)+read.offWindow == 0)
	}
	slog.InfoContext(ctx, WorkItemMeasureCapabilityLogMessage,
		"organization_id", organizationID,
		"target_day", day,
		"items", len(read.items),
		"duplicate_items", read.duplicates,
		"items_off_window", read.offWindow,
		"rows", len(rows),
		"rows_written", written,
		"rows_retired", retired,
		"failed", err != nil)
	return written + retired, err
}

// workItemCapabilityKey is one live key of the day: provider and measure, with
// the newest computed_at stored under it.
type workItemCapabilityKey struct {
	provider, measure string
	stored            time.Time
}

// loadLiveWorkItemCapabilityKeys reads the keys of (organization, window_end =
// day) whose newest row is an answer. A computed row always holds item_count
// >= 1 (a provider with no item gets no row), so item_count 0 marks a retired
// key.
func loadLiveWorkItemCapabilityKeys(
	ctx context.Context, conn repositoryRows, organizationID string, day time.Time,
) ([]workItemCapabilityKey, error) {
	rows, err := conn.Query(ctx, `
SELECT provider, measure, max(computed_at)
FROM work_item_measure_capability
WHERE org_id = ? AND window_end = ?
GROUP BY provider, measure
HAVING argMax(item_count, computed_at) > 0
ORDER BY provider, measure`, organizationID, workitemmetrics.UTCDay(day))
	if err != nil {
		return nil, fmt.Errorf("load work_item_measure_capability live keys: %w", err)
	}
	defer rows.Close()
	var live []workItemCapabilityKey
	for rows.Next() {
		var key workItemCapabilityKey
		if err := rows.Scan(&key.provider, &key.measure, &key.stored); err != nil {
			return nil, fmt.Errorf("scan work_item_measure_capability live key: %w", err)
		}
		live = append(live, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_item_measure_capability live keys: %w", err)
	}
	return live, nil
}

// retireWorkItemMeasureCapability writes a row with tracked 0, evidence_count
// 0 and item_count 0 over each live key of the day that this compute did not
// produce: its provider has no item in the window any more, so the answer for
// the day is unknown, never the answer of an earlier compute. The read takes
// every item of the organization (no partition or scope filter), so the same
// keys are produced by a run of some repositories and by a run of the whole
// organization.
//
// When the read gave no item at all, every live key would be retired: a second
// read of its own (a count of the window's work items) must state that the
// organization has none, else nothing is retired and the run fails.
//
// The row gets computedAt, or one millisecond (the unit of computed_at) after
// the stored row when computedAt is not later: strictly newer than the row it
// retires.
func retireWorkItemMeasureCapability(
	ctx context.Context, conn workItemCapabilityConn, organizationID string, day, computedAt time.Time,
	live []workItemCapabilityKey, produced []workitemmetrics.CapabilityRow, noItem bool,
) (int, error) {
	held := make(map[string]struct{}, len(produced))
	for _, row := range produced {
		held[row.Provider+"\x00"+row.Measure] = struct{}{}
	}
	var stale []workItemCapabilityKey
	for _, key := range live {
		if _, ok := held[key.provider+"\x00"+key.measure]; !ok {
			stale = append(stale, key)
		}
	}
	if len(stale) == 0 {
		return 0, nil
	}
	first, last := workitemmetrics.CapabilityWindow(day)
	if noItem {
		if err := proveOrganizationHasNoWorkItemForDay(ctx, conn, organizationID, first, last.AddDate(0, 0, 1)); err != nil {
			return 0, fmt.Errorf("retire work_item_measure_capability keys: %w", err)
		}
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_measure_capability (
		org_id, provider, measure, tracked, evidence_count, item_count,
		window_start, window_end, computed_at)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_item_measure_capability retirement batch: %w", err)
	}
	for _, key := range stale {
		version := computedAt.UTC().Truncate(time.Millisecond)
		if floor := key.stored.UTC().Truncate(time.Millisecond).Add(time.Millisecond); floor.After(version) {
			version = floor
		}
		if err := batch.Append(
			organizationID, key.provider, key.measure, uint8(0), uint32(0), uint32(0), first, last, version,
		); err != nil {
			return 0, fmt.Errorf("append work_item_measure_capability retirement row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return len(stale), fmt.Errorf("send work_item_measure_capability retirement batch: %w", err)
	}
	return len(stale), nil
}

type workItemCapabilityConn interface {
	repositoryRows
	workItemBatchConn
}

// WriteWorkItemMeasureCapability appends the capability rows of one compute
// to work_item_measure_capability.
func WriteWorkItemMeasureCapability(
	ctx context.Context, conn workItemBatchConn, organizationID string,
	rows []workitemmetrics.CapabilityRow, computedAt time.Time,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_measure_capability (
		org_id, provider, measure, tracked, evidence_count, item_count,
		window_start, window_end, computed_at)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_item_measure_capability batch: %w", err)
	}
	// computed_at is the ReplacingMergeTree version: milliseconds keep two
	// writes of one key in the same second apart.
	computedAtUTC := computedAt.UTC().Truncate(time.Millisecond)
	for _, row := range rows {
		counters, err := workItemUInt32s("work_item_measure_capability",
			fmt.Sprintf("%s %s", row.Provider, row.Measure),
			[]workItemCounter{
				{"evidence_count", row.EvidenceCount},
				{"item_count", row.ItemCount},
			})
		if err != nil {
			return 0, err
		}
		tracked := uint8(0)
		if row.Tracked {
			tracked = 1
		}
		if err := batch.Append(
			organizationID, row.Provider, row.Measure, tracked, counters[0], counters[1],
			workitemmetrics.UTCDay(row.WindowStart), workitemmetrics.UTCDay(row.WindowEnd),
			computedAtUTC,
		); err != nil {
			return 0, fmt.Errorf("append work_item_measure_capability row: %w", err)
		}
	}
	// A Send error is ambiguous: the insert may have committed. The true row
	// count goes back so the caller fails closed.
	if err := batch.Send(); err != nil {
		return len(rows), fmt.Errorf("send work_item_measure_capability batch: %w", err)
	}
	return len(rows), nil
}
