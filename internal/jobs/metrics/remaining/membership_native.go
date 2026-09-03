package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ErrMembershipSchemaIncompatible refuses a database this executor cannot
// compute against.
var ErrMembershipSchemaIncompatible = errors.New(
	"membership_backfill: clickhouse schema incompatible")

// ErrMembershipUnavailable is the nil-connection refusal.
var ErrMembershipUnavailable = errors.New(
	"membership_backfill: clickhouse connection unavailable")

// ErrMembershipWriterUnavailable is the nil-writer refusal. Distinct from the
// connection refusal so the two report under different reasons -- a missing
// writer is a wiring bug (daily.go constructed the executor without one),
// never a database outage.
var ErrMembershipWriterUnavailable = errors.New(
	"membership_backfill: writer unavailable")

// membershipTableRequirements lists what each table must provide, checked at
// CONSTRUCTION so a database this code cannot compute against refuses the
// kind once and loudly, rather than letting the handler claim partitions and
// fail every one of them -- capacity's and recommendations' shape, for the
// same reason.
//
// work_graph_edges' column list matches what chquery.FetchWorkGraphEdges
// selects, not this file's own SQL -- the read itself is chquery's, this is
// only the pre-flight check that the table this executor is about to depend
// on (transitively, through chquery) actually looks like it should.
var membershipTableRequirements = map[string][]string{
	"work_graph_edges": {
		"org_id", "repo_id", "source_type", "source_id", "target_type", "target_id",
		"edge_type", "provider", "provenance", "confidence", "evidence", "last_synced",
	},
	"work_unit_investments": {
		"org_id", "work_unit_id", "theme_distribution_json",
		"subcategory_distribution_json", "categorization_status", "computed_at",
	},
	"work_unit_membership": {
		"org_id", "node_type", "node_id", "work_unit_id", "category_kind", "category",
		"weight", "is_dominant", "categorization_status", "computed_at", "run_id",
	},
	"work_unit_membership_runs": {
		"org_id", "run_id", "completed_at",
	},
	"work_unit_membership_scoped_runs": {
		"org_id", "scope_kind", "scope_id", "run_id", "completed_at",
	},
}

// MembershipExecutor computes the no-LLM membership backfill natively
// (CHAOS-2439/2433, CHAOS-4282). It projects work_unit_membership from the
// theme/subcategory distributions ALREADY persisted in work_unit_investments
// by the (still-Python) LLM materializer -- it never categorizes anything
// itself, and it calls no LLM.
//
// It holds the ClickHouse connection (reads work_graph_edges and
// work_unit_investments via chquery/its own reader) and a MembershipWriter
// (writes work_unit_membership, the completion marker, and prunes old
// generations). The writer is an INTERFACE, not a concrete type, because its
// concrete implementation may move into a shared package once lane-4441's
// chwrite (#2171) settles what it owns -- see membership_write.go's doc
// comment.
type MembershipExecutor struct {
	conn          driver.Conn
	edges         chqueryEdgeReader
	distributions membershipDistributionFetcher
	writer        MembershipWriter
	nowUTC        func() time.Time

	observer MembershipObserver
	logger   MembershipLogger
}

// MembershipLogger is the narrow logging capability ComputeOrg needs for a
// non-fatal prune failure. Matches recommendations' ReadinessLogger shape;
// *slog.Logger satisfies it directly.
type MembershipLogger interface {
	Warn(msg string, args ...any)
}

// NewMembershipExecutor refuses at construction rather than per partition,
// matching capacity's and recommendations' shape.
func NewMembershipExecutor(
	ctx context.Context, conn driver.Conn, writer MembershipWriter,
) (*MembershipExecutor, error) {
	if conn == nil {
		return nil, ErrMembershipUnavailable
	}
	if writer == nil {
		return nil, ErrMembershipWriterUnavailable
	}
	if err := verifyMembershipSchema(ctx, conn); err != nil {
		return nil, err
	}
	edges, err := chquery.NewReader(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMembershipUnavailable, err)
	}
	return &MembershipExecutor{
		conn:          conn,
		edges:         edges,
		distributions: chConnDistributionFetcher{conn: conn},
		writer:        writer,
		nowUTC:        func() time.Time { return time.Now().UTC() },
	}, nil
}

// SetObserver wires optional run-stats telemetry. Nil is tolerated everywhere
// it is read, matching the readiness gate's own optionality in
// recommendations_native.go.
func (executor *MembershipExecutor) SetObserver(observer MembershipObserver) {
	executor.observer = observer
}

// SetLogger wires optional logging for a non-fatal prune failure. Nil is
// tolerated, same discipline as SetObserver.
func (executor *MembershipExecutor) SetLogger(logger MembershipLogger) {
	executor.logger = logger
}

// verifyMembershipSchema checks every table the executor reads or writes.
//
// Iterated in SORTED order rather than over the map: a map range reports a
// different table first on different runs, so one broken deployment would
// produce a different refusal message each restart and look like several
// distinct faults -- capacity's and recommendations' reasoning, repeated here.
func verifyMembershipSchema(ctx context.Context, conn driver.Conn) error {
	tables := make([]string, 0, len(membershipTableRequirements))
	for table := range membershipTableRequirements {
		tables = append(tables, table)
	}
	sort.Strings(tables)

	for _, table := range tables {
		// Reuses capacity's column reader rather than restating the
		// system.columns query -- the same reuse recommendations made, for
		// the same reason: a second copy would be free to drift.
		present, err := capacityTableColumns(ctx, conn, table)
		if err != nil {
			return err
		}
		if len(present) == 0 {
			return fmt.Errorf("%w: table %s does not exist",
				ErrMembershipSchemaIncompatible, table)
		}
		var missing []string
		for _, column := range membershipTableRequirements[table] {
			if !present[column] {
				missing = append(missing, column)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("%w: %s is missing %s",
				ErrMembershipSchemaIncompatible, table, strings.Join(missing, ", "))
		}
	}
	return nil
}

// wallClock never returns nil, matching recommendations' wallClock method.
func (executor *MembershipExecutor) wallClock() func() time.Time {
	if executor.nowUTC != nil {
		return executor.nowUTC
	}
	return func() time.Time { return time.Now().UTC() }
}

// ComputePartition satisfies CompatibilityExecutor: the seam the partition
// handler drives.
//
// Unlike recommendations, this kind needs no readiness gate of its own here:
// the scheduler-level RequiresGraphBuild prerequisite
// (internal/scheduler/fixed/producers.go) already withholds every partition
// of this kind from being claimable until its org's work-graph build has
// durably completed. A gate re-implemented inside ComputePartition would be
// redundant with that fence, not a second layer of safety.
func (executor *MembershipExecutor) ComputePartition(
	ctx context.Context, run Run, partition Partition,
) (CompatibilityOutcome, error) {
	if executor == nil || executor.conn == nil {
		return CompatibilityOutcome{}, ErrMembershipUnavailable
	}
	if strings.TrimSpace(run.OrganizationID) == "" {
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s has no organization", ErrInvalidState, partition.ID))
	}

	var scope membershipScope
	if err := json.Unmarshal(partition.Scope, &scope); err != nil {
		// Static format, partition ID plus the decoder's own message; carries
		// no upstream content, so it is safe to surface at WARN -- same
		// reasoning as recommendations' identical guard.
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s scope: %v", ErrInvalidState, partition.ID, err))
	}

	outcome, err := executor.ComputeOrg(ctx, run.OrganizationID, scope.RepoIDs, executor.wallClock()())
	written := outcome.MembershipRows
	if err != nil {
		return CompatibilityOutcome{RowsWritten: &written}, err
	}
	return CompatibilityOutcome{RowsWritten: &written}, nil
}

// A compile-time pin that this executor IS the seam the handler drives.
var _ CompatibilityExecutor = (*MembershipExecutor)(nil)

// unitNodesFor converts one units.Component into the deduplicated node list
// its work_unit_id hashes over. units.BuildComponents already dedupes
// (list(dict.fromkeys(nodes)) is its Go port, applied once, at discovery
// time), so this is a pure re-labeling, not a second dedup pass -- backfill.py
// itself re-runs list(dict.fromkeys(nodes)) on an already-deduped list for the
// same reason: a no-op kept for symmetry with the reference rather than
// dropped as "obviously redundant".
func unitNodesFor(component units.Component) []units.NodeKey {
	return component.Nodes
}

// chqueryEdgeReader is the narrow chquery capability this file needs, so a
// test can substitute a fake without a live ClickHouse connection.
type chqueryEdgeReader interface {
	FetchWorkGraphEdges(ctx context.Context, opts chquery.EdgeQueryOptions) ([]chquery.EdgeRow, error)
}

var _ chqueryEdgeReader = (*chquery.Reader)(nil)
