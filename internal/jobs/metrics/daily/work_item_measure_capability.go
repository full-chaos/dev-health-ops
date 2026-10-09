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
	return computeWorkItemMeasureCapability(
		ctx, executor.conn, run.OrganizationID, workitemmetrics.UTCDay(run.TargetDay), executor.nowUTC(),
	)
}

var _ NativeFinalizeFamilyExecutor = (*WorkItemMeasureCapabilityExecutor)(nil)

// WorkItemMeasureCapabilityLogMessage is logged once for each compute of the
// capability rows, with the counts of the read and the write.
const WorkItemMeasureCapabilityLogMessage = "daily work item measure capability"

// loadWorkItemCapabilityItems reads every item of the organization in the
// capability window of day, from every repository, each item counted once.
// The capability answers for (organization, provider), so the read takes no
// partition or scope filter: a subset of the items would answer "not tracked"
// from the items it did not read.
func loadWorkItemCapabilityItems(
	ctx context.Context, conn repositoryRows, organizationID string, day time.Time,
) ([]workitemmetrics.Item, int, error) {
	first, last := workitemmetrics.CapabilityWindow(day)
	stored, err := loadWorkItemScopeItems(
		ctx, conn, organizationID, first, last.AddDate(0, 0, 1), nil, false, false, true,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("load work item capability items: %w", err)
	}
	kept, duplicates := countWorkItemOncePerProviderAndID(stored)
	rows := make([]workItemMetricsRow, 0, len(kept))
	for _, row := range kept {
		rows = append(rows, row.workItemMetricsRow)
	}
	return workItemMetricsItems(rows), duplicates, nil
}

// computeWorkItemMeasureCapability reads the window, derives the rows and
// writes them. It returns the true row count on an ambiguous Send error, like
// every Write* of this package.
func computeWorkItemMeasureCapability(
	ctx context.Context, conn workItemCapabilityConn, organizationID string, day, computedAt time.Time,
) (int, error) {
	items, duplicates, err := loadWorkItemCapabilityItems(ctx, conn, organizationID, day)
	if err != nil {
		return 0, err
	}
	rows := workitemmetrics.DeriveMeasureCapability(day, items)
	written, err := WriteWorkItemMeasureCapability(ctx, conn, organizationID, rows, computedAt)
	slog.InfoContext(ctx, WorkItemMeasureCapabilityLogMessage,
		"organization_id", organizationID,
		"target_day", day,
		"items", len(items),
		"duplicate_items", duplicates,
		"rows", len(rows),
		"rows_written", written,
		"failed", err != nil)
	return written, err
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
