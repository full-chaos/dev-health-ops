package remaining

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// ErrWorkItemAttributionUnavailable is the nil-connection refusal.
var ErrWorkItemAttributionUnavailable = errors.New(
	"work_item_attribution: clickhouse connection unavailable")

// ErrWorkItemAttributionWriterUnavailable is the nil-writer refusal. Distinct
// from the connection refusal so the two report under different reasons --
// a missing writer is a wiring bug (daily.go constructed the executor
// without one), never a database outage. Matches membership_backfill's
// shape (CHAOS-4282).
var ErrWorkItemAttributionWriterUnavailable = errors.New(
	"work_item_attribution: writer unavailable")

// ErrWorkItemAttributionSchemaIncompatible refuses a database this executor
// cannot compute against.
var ErrWorkItemAttributionSchemaIncompatible = errors.New(
	"work_item_attribution: clickhouse schema incompatible")

// workItemAttributionTableRequirements lists what each table must provide,
// checked at CONSTRUCTION so a database this code cannot compute against
// refuses the kind once and loudly, rather than letting the handler claim
// partitions and fail every one of them -- capacity's, recommendations', and
// membership_backfill's shape, for the same reason.
var workItemAttributionTableRequirements = map[string][]string{
	"work_items": {
		"org_id", "work_item_id", "provider", "type", "repo_id", "native_team_key",
		"project_key", "project_id", "project_name", "assignees", "reporter", "last_synced",
	},
	"work_item_dependencies": {
		"org_id", "source_work_item_id", "target_work_item_id", "relationship_type",
		"relationship_type_raw", "last_synced",
	},
	"teams": {
		"org_id", "provider", "id", "name", "project_keys", "members", "manual_members",
		"is_active", "updated_at",
	},
	"team_project_ownership": {
		"org_id", "provider", "project_id", "project_key", "team_id", "is_primary",
		"specificity", "priority", "valid_from", "valid_to", "updated_at",
	},
	"team_repo_ownership": {
		"org_id", "provider", "repo_id", "repo_full_name", "team_id", "is_primary",
		"specificity", "priority", "valid_from", "valid_to", "updated_at",
	},
	"identities": {
		"org_id", "canonical_id", "email", "provider_identities", "team_ids",
		"is_active", "updated_at",
	},
	"team_memberships": {
		"org_id", "provider", "team_id", "member_id", "raw_provider_user_id", "raw_email",
		"identity_facets", "is_primary", "specificity", "priority", "valid_from", "valid_to",
		"updated_at",
	},
	"manual_attribution_fallbacks": {
		"org_id", "provider", "scope_type", "scope_id", "team_id", "team_name", "reason",
		"priority", "valid_from", "valid_to", "updated_at",
	},
	"work_item_team_attributions": {
		"org_id", "repo_id", "work_item_id", "provider", "team_id", "team_name", "source",
		"is_primary", "confidence", "evidence", "computed_at",
	},
	"work_item_attribution_backstop_runs": {
		"org_id", "run_id", "completed_at", "promoted_reason",
	},
	"work_item_attribution_backstop_scoped_runs": {
		"org_id", "scope_kind", "scope_id", "run_id", "completed_at",
	},
}

// WorkItemAttributionExecutor computes the daily work_item_team_attributions
// backstop natively (CHAOS-3092 PR-B): the staleness-window backstop for the
// sync-time deriver's incremental watermark, replacing the retired Python
// daily sweep (job_daily.py's unconditional compute_work_item_team_attributions
// call). Unlike the sync-time deriver (per-provider, re-derives on every item
// sync) and unlike the retired Python sweep (re-derives every work item
// loaded that day, unconditionally), this backstop is SCOPED: it only
// re-derives items whose ownership scope changed since the last backstop
// run -- see ComputeOrg's doc comment for the exact scoping rule.
type WorkItemAttributionExecutor struct {
	conn   driver.Conn
	writer WorkItemAttributionWriter
	nowUTC func() time.Time

	observer WorkItemAttributionObserver
	logger   WorkItemAttributionLogger
}

// WorkItemAttributionLogger is the narrow logging capability ComputeOrg needs
// for non-fatal warnings. Matches membership_backfill's MembershipLogger
// shape; *slog.Logger satisfies it directly.
type WorkItemAttributionLogger interface {
	Warn(msg string, args ...any)
}

// NewWorkItemAttributionExecutor refuses at construction rather than per
// partition, matching capacity's, recommendations', and membership_backfill's
// shape.
func NewWorkItemAttributionExecutor(
	ctx context.Context, conn driver.Conn, writer WorkItemAttributionWriter,
) (*WorkItemAttributionExecutor, error) {
	if conn == nil {
		return nil, ErrWorkItemAttributionUnavailable
	}
	if writer == nil {
		return nil, ErrWorkItemAttributionWriterUnavailable
	}
	if err := verifyWorkItemAttributionSchema(ctx, conn); err != nil {
		return nil, err
	}
	return &WorkItemAttributionExecutor{
		conn:   conn,
		writer: writer,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// SetObserver wires optional run-stats telemetry. Nil is tolerated
// everywhere it is read, matching the readiness gate's own optionality in
// recommendations_native.go.
func (executor *WorkItemAttributionExecutor) SetObserver(observer WorkItemAttributionObserver) {
	executor.observer = observer
}

// SetLogger wires optional logging. Nil is tolerated, same discipline as
// SetObserver.
func (executor *WorkItemAttributionExecutor) SetLogger(logger WorkItemAttributionLogger) {
	executor.logger = logger
}

// verifyWorkItemAttributionSchema checks every table the executor reads or
// writes.
//
// Iterated in SORTED order rather than over the map: a map range reports a
// different table first on different runs, so one broken deployment would
// produce a different refusal message each restart and look like several
// distinct faults -- capacity's, recommendations', and membership_backfill's
// reasoning, repeated here.
func verifyWorkItemAttributionSchema(ctx context.Context, conn driver.Conn) error {
	tables := make([]string, 0, len(workItemAttributionTableRequirements))
	for table := range workItemAttributionTableRequirements {
		tables = append(tables, table)
	}
	sort.Strings(tables)

	for _, table := range tables {
		// Reuses capacity's column reader rather than restating the
		// system.columns query -- the same reuse recommendations and
		// membership_backfill made, for the same reason: a second copy
		// would be free to drift.
		present, err := capacityTableColumns(ctx, conn, table)
		if err != nil {
			return err
		}
		if len(present) == 0 {
			return fmt.Errorf("%w: table %s does not exist",
				ErrWorkItemAttributionSchemaIncompatible, table)
		}
		var missing []string
		for _, column := range workItemAttributionTableRequirements[table] {
			if !present[column] {
				missing = append(missing, column)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("%w: %s is missing %s",
				ErrWorkItemAttributionSchemaIncompatible, table, strings.Join(missing, ", "))
		}
	}
	return nil
}

// nowOrRefuse yields this executor's instant, refusing an uninjected clock.
//
// NOT a nil-safe fallback to time.Now(): NewWorkItemAttributionExecutor sets
// nowUTC unconditionally on the only path that returns a non-nil executor
// (same shape as DORAExecutor/CapacityExecutor), so a fallback here would be
// dead in production and would only ever fire in a test that forgot to
// inject a clock -- converting that into a silent real-wall-clock dependency
// instead of a loud failure. See executor_clock.go's doc comment.
func (executor *WorkItemAttributionExecutor) nowOrRefuse() (time.Time, error) {
	return clockOrRefuse("WorkItemAttributionExecutor", executor.nowUTC)
}

// WorkItemAttributionObserver wires optional run-stats telemetry. Nil is
// tolerated everywhere it is read, matching membership_backfill's
// MembershipObserver optionality.
type WorkItemAttributionObserver interface {
	ObserveWorkItemAttributionRun(orgID string, outcome WorkItemAttributionOutcome)
}

// CollectorWorkItemAttributionObserver adapts the metrics collector to
// WorkItemAttributionObserver, so ComputeOrg stays free of the telemetry
// package's shape -- same pattern as membership_backfill's
// CollectorMembershipObserver.
type CollectorWorkItemAttributionObserver struct {
	Collector *jobruntime.MetricsCollector
}

func (observer CollectorWorkItemAttributionObserver) ObserveWorkItemAttributionRun(
	_ string, outcome WorkItemAttributionOutcome,
) {
	if observer.Collector == nil {
		return
	}
	observer.Collector.ObserveWorkItemAttributionRun(
		outcome.OrgWide, len(outcome.RepoIDs), len(outcome.ProjectKeys),
		outcome.ItemsSeen, outcome.RowsWritten, outcome.SkippedNoop,
	)
}
