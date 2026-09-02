package edges

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// dependencyReadSQL is the read `_build_issue_issue_edges` performs
// (builder.py:837-849), reproduced verbatim in shape.
//
// # WHAT IS DELIBERATELY THE SAME
//
// No `FINAL`, no `argMax`, no `ORDER BY` — because Python has none. The table is
// a ReplacingMergeTree, so this returns every unmerged version of a key and the
// last-write-wins dedup downstream decides which one's timestamp survives. That
// is CHAOS-4788, and it is REPLICATED rather than fixed: adding FINAL here would
// change which edges this port produces relative to Python, which is the one
// thing a parity port must not do quietly.
//
// No time bound either. This sub-builder is window-independent — proven, not
// assumed: PR1 freezes this query's text in the golden and asserts structurally
// that no bound appears (TestDependencyReadIsWindowIndependent), and two live
// captures with and without a window produced an identical edge_id set.
//
// # WHAT IS DELIBERATELY DIFFERENT
//
// Python interpolates the org id straight into the SQL (`_org_id_clause`,
// builder.py:167-171). This binds it. Same semantics, different mechanism, and
// the mechanism is not something to reproduce faithfully.
const dependencyReadSQL = `
        SELECT
            source_work_item_id,
            target_work_item_id,
            relationship_type,
            relationship_type_raw,
            relationship_semantics_version,
            last_synced
        FROM work_item_dependencies
        WHERE 1=1 AND org_id = {org_id:String}
`

// ReadDependencies loads every dependency row for an organization.
//
// The caller must have passed RequireOrganizationScope already; this does not
// re-check, because a reader that refuses would diverge from Python's permissive
// reader and the guard belongs at the entry point (see scope handling).
func ReadDependencies(
	ctx context.Context, conn driver.Conn, organizationID string,
) ([]DependencyRow, error) {
	rows, err := conn.Query(ctx, dependencyReadSQL, clickhouse.Named("org_id", organizationID))
	if err != nil {
		return nil, fmt.Errorf("read work_item_dependencies: %w", err)
	}
	defer rows.Close()

	// Row order is load-bearing: the dedup downstream is last-write-wins, so the
	// order ClickHouse returns decides which duplicate version survives. Python
	// has the same property and the same absence of an ORDER BY. Appended in
	// scan order and never re-sorted.
	dependencies := make([]DependencyRow, 0, 4096)
	for rows.Next() {
		var (
			source, target, relationship, raw, semantics string
			lastSynced                                   time.Time
		)
		if err := rows.Scan(&source, &target, &relationship, &raw, &semantics, &lastSynced); err != nil {
			return nil, fmt.Errorf("scan work_item_dependencies row: %w", err)
		}
		dependencies = append(dependencies, DependencyRow{
			SourceWorkItemID: source,
			TargetWorkItemID: target,
			RelationshipType: relationship,
			RelationshipRaw:  raw,
			SemanticsVersion: semantics,
			// work_item_dependencies.last_synced is DateTime64(3) with NO
			// timezone, while work_graph_edges.last_synced is
			// DateTime64(3,'UTC'). The driver hands back a time.Time either way;
			// normalising to UTC here is the same coercion Python applies at
			// builder.py:892-893, and skipping it would shift every event_ts by
			// the driver's local offset.
			LastSynced: lastSynced.UTC().Format(time.RFC3339Nano),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_item_dependencies: %w", err)
	}
	return dependencies, nil
}

// WriteEdges inserts derived edges into work_graph_edges.
//
// Column order matches the live table, and `day` is supplied explicitly rather
// than left to its DEFAULT: an explicit INSERT names every column, so the
// invariant `day = toDate(event_ts)` is ours to hold. PR1 asserts it across the
// whole golden.
func WriteEdges(ctx context.Context, conn driver.Conn, rows []Row) (int, error) {
	if len(rows) == 0 {
		// Python's `if not edges: return 0` (builder.py:222-223). A batch-level
		// no-op, not a per-row outcome — the caller's outcome tally already says
		// why there was nothing to write.
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx,
		"INSERT INTO work_graph_edges ("+
			"edge_id, source_type, source_id, target_type, target_id, edge_type, "+
			"provenance, confidence, evidence, discovered_at, last_synced, event_ts, day, org_id)")
	if err != nil {
		return 0, fmt.Errorf("prepare work_graph_edges batch: %w", err)
	}
	for _, row := range rows {
		if err := ValidateConfidence(row.Confidence); err != nil {
			// Refuse to MINT an ungroupable confidence. A NaN here would vanish
			// from component grouping entirely and shatter a component into
			// singletons (CHAOS-4441, ca0b40349) — silently.
			return 0, fmt.Errorf("edge %s: %w", row.EdgeID, err)
		}
		if err := batch.Append(
			row.EdgeID, row.SourceType, row.SourceID, row.TargetType, row.TargetID,
			row.EdgeType, row.Provenance, row.Confidence, row.Evidence,
			row.DiscoveredAt, row.LastSynced, row.EventTs, row.Day, row.OrgID,
		); err != nil {
			return 0, fmt.Errorf("append edge %s: %w", row.EdgeID, err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send work_graph_edges batch: %w", err)
	}
	return len(rows), nil
}
