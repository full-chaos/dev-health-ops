package edges

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
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
// # WHAT AN UNSCOPED RUN DOES IN PYTHON
//
// `_build_issue_issue_edges` appends the org clause only when it is non-empty
// (builder.py:847-849) and has NO guard of its own, so an unscoped Python run
// issues `SELECT ... FROM work_item_dependencies` with no WHERE at all and
// reads every tenant's dependency graph. The rows then flow through derivation
// into `_edge_to_record`, which stamps `org_id=self.config.org_id`
// unconditionally -- so a cross-tenant read becomes a write of every tenant's
// edges under one empty org id, and those rows are untargetable by any later
// scoped delete.
//
// This port refuses instead, at both database entry points, before any
// statement is issued. That is a DELIBERATE divergence from Python and it makes
// gates 14, 15, 21, 23, 29 and 32 of the audit unreachable rather than
// replicated: every one of them is a distinct behaviour Python exhibits only
// when org_id is empty.
func ReadDependencies(
	ctx context.Context, conn driver.Conn, organizationID string,
) ([]DependencyRow, error) {
	if err := requireEdgeScope(organizationID); err != nil {
		return nil, err
	}
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
			// DateTime64(3,'UTC').
			//
			// Converting here matches Python, though NOT by the mechanism the
			// shape suggests. clickhouse-connect returns the no-tz column as an
			// AWARE datetime in the server's zone, so `_ensure_utc` CONVERTS it;
			// the naive case arises only for the 'UTC'-declared column, where
			// converting and reinterpreting coincide. Convert is therefore
			// uniformly correct and there is no per-column branch to write.
			//
			// Measured on an Asia/Kolkata server (lane-4441 receipt), one
			// literal written to both column types:
			//
			//	                     python _ensure_utc   reinterpret     .UTC()
			//	DateTime64(3)        1788325200000        1788345000000   1788325200000
			//	DateTime64(3,'UTC')  1788345000000        1788345000000   1788345000000
			//
			// Reinterpreting would be worse than wrong-on-a-non-UTC-server:
			// with apply_server_timezone=False the driver attaches the CLIENT's
			// zone, so a reinterpreting port is wrong by a PER-MACHINE offset
			// and never reproduces for whoever is asked to confirm it.
			// Converting never reads the wall clock.
			LastSynced: lastSynced.UTC(),
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
// The organization id is stamped here rather than carried on the row, because
// the scope is a property of the RUN and the deriver is pure -- it has no
// business knowing which tenant it is deriving for.
func WriteEdges(ctx context.Context, conn driver.Conn, organizationID string, rows []Row) (int, error) {
	// Before the length check, not after: a zero-row write under a bad scope is
	// still a bad scope, and letting it return success would mean the guard's
	// coverage depended on how many edges happened to be derived.
	if err := requireEdgeScope(organizationID); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		// Python's `if not edges: return 0` (builder.py:222-223). A batch-level
		// no-op, not a per-row outcome — the caller's outcome tally already says
		// why there was nothing to write.
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx,
		"INSERT INTO work_graph_edges ("+
			"edge_id, source_type, source_id, target_type, target_id, edge_type, "+
			"repo_id, provider, "+
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
			row.EdgeType, row.RepoID, row.Provider,
			row.Provenance, row.Confidence, row.Evidence,
			row.DiscoveredAt, row.LastSynced, row.EventTs, row.Day, organizationID,
		); err != nil {
			return 0, fmt.Errorf("append edge %s: %w", row.EdgeID, err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send work_graph_edges batch: %w", err)
	}
	return len(rows), nil
}

// existingBlockerEdgeIDsSQL is `_delete_dependency_edge_candidates`'s own read
// (builder.py:952-971), reproduced verbatim in shape: every NATIVE issue<->issue
// blocker-family edge id currently live, paged by edge_id after a cursor.
//
// FINAL here is deliberate and NOT the same divergence flagged on
// dependencyReadSQL above -- Python's own read of THIS table uses FINAL too
// (builder.py:959), because this query decides what to DELETE and reading a
// pre-merge duplicate would target an id that may already be gone.
const existingBlockerEdgeIDsSQL = `
        SELECT edge_id
        FROM work_graph_edges FINAL
        WHERE org_id = {org_id:String}
          AND source_type = 'issue'
          AND target_type = 'issue'
          AND edge_type IN ('blocks', 'is_blocked_by')
          AND provenance = 'native'
          AND edge_id > {after:String}
        ORDER BY edge_id
        LIMIT 1000
`

// ReadExistingBlockerEdgeIDs pages through every currently-live native
// issue<->issue blocker-family edge id, for BuildCleanupPlan's
// existingEdgeIDs input (builder.py:953-971).
func ReadExistingBlockerEdgeIDs(ctx context.Context, conn driver.Conn, organizationID string) ([]string, error) {
	if err := requireEdgeScope(organizationID); err != nil {
		return nil, err
	}
	var ids []string
	after := ""
	for {
		rows, err := conn.Query(ctx, existingBlockerEdgeIDsSQL,
			clickhouse.Named("org_id", organizationID), clickhouse.Named("after", after))
		if err != nil {
			return nil, fmt.Errorf("read existing blocker edge ids: %w", err)
		}
		page := make([]string, 0, cleanupPageSize)
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan blocker edge id: %w", err)
			}
			page = append(page, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("iterate blocker edge ids: %w", err)
		}
		rows.Close()
		if len(page) == 0 {
			break
		}
		// Defensive, matching Python's own re-sort (builder.py:962) even though
		// the ORDER BY above already guarantees it: the cursor (`after`) MUST be
		// derived from a sorted page, or a server that returned ids out of order
		// for any reason would silently skip or repeat entries across pages.
		sort.Strings(page)
		ids = append(ids, page...)
		after = page[len(page)-1]
		if len(page) < cleanupPageSize {
			break
		}
	}
	return ids, nil
}

// deleteProjectionRunsSQL removes prior watermark rows for one org+projection+
// rule before a rerun computes and publishes a fresh one
// (`_delete_dependency_edge_candidates`, builder.py:911-920). `mutations_sync=2`
// waits for the mutation to complete on all replicas before returning, matching
// Python's own synchronous wait -- a caller that proceeds to publish a new
// watermark immediately after must not race the delete of the old one.
const deleteProjectionRunsSQL = `
        ALTER TABLE work_graph_projection_runs DELETE WHERE
        org_id = {org_id:String}
        AND projection_name = {projection_name:String}
        AND rule_version = {rule_version:String}
        SETTINGS mutations_sync=2
`

// DeleteProjectionRuns wipes this org+projection+rule's prior watermark rows.
func DeleteProjectionRuns(ctx context.Context, conn driver.Conn, organizationID, projectionName, ruleVersion string) error {
	if err := requireEdgeScope(organizationID); err != nil {
		return err
	}
	if err := conn.Exec(ctx, deleteProjectionRunsSQL,
		clickhouse.Named("org_id", organizationID),
		clickhouse.Named("projection_name", projectionName),
		clickhouse.Named("rule_version", ruleVersion),
	); err != nil {
		return fmt.Errorf("delete work_graph_projection_runs: %w", err)
	}
	return nil
}

// deleteEdgesByIDSQL is one page of `_delete_dependency_edge_candidates`'s
// delete (builder.py:988-1006). `mutations_sync=2` for the same reason as
// above: the edges this deletes are re-created by the very next write in
// Run(), and that write must not race a still-pending delete of the same ids.
const deleteEdgesByIDSQL = `
        ALTER TABLE work_graph_edges DELETE WHERE
        org_id = {org_id:String} AND edge_id IN {edge_ids:Array(String)}
        SETTINGS mutations_sync=2
`

// DeleteEdgesByID executes a CleanupPlan, one page at a time, in the order
// BuildCleanupPlan produced them.
func DeleteEdgesByID(ctx context.Context, conn driver.Conn, organizationID string, plan CleanupPlan) error {
	if err := requireEdgeScope(organizationID); err != nil {
		return err
	}
	for _, page := range plan.Pages {
		if err := conn.Exec(ctx, deleteEdgesByIDSQL,
			clickhouse.Named("org_id", organizationID),
			clickhouse.Named("edge_ids", page),
		); err != nil {
			return fmt.Errorf("delete work_graph_edges page (%d ids): %w", len(page), err)
		}
	}
	return nil
}

// staleDependencyIssueEdgesDeleteSQL ports `_delete_stale_pr_dependency_issue_edges`
// verbatim (work_graph/builder.py, pre-CHAOS-4924 deletion): a legacy stale-row
// shape where a PR-sourced edge was mislabelled with `source_type='issue'`
// (`source_id` still carries its real `ghpr:`/`gitlab:` prefix) but
// `target_type`/`target_id` name a genuine Linear issue via a
// `linear_attachment` evidence tag. `mutations_sync=2` for the same reason
// DeleteEdgesByID uses it: this delete must be visible before anything reads
// work_graph_edges again in the same build.
const staleDependencyIssueEdgesDeleteSQL = `
        ALTER TABLE work_graph_edges DELETE WHERE
        source_type = 'issue' AND target_type = 'issue' AND evidence = 'linear_attachment'
        AND startsWith(target_id, 'linear:')
        AND (startsWith(source_id, 'ghpr:') OR startsWith(source_id, 'gitlab:'))
        AND org_id = {org_id:String}
        SETTINGS mutations_sync=2
`

// DeleteStalePRDependencyIssueEdges runs the stale-edge cleanup
// `_delete_stale_pr_dependency_issue_edges` used to run as the FIRST action
// inside Python's `build()`, before any other stage. Refuses an unscoped
// call rather than replicating Python's silent no-op on an empty org_id --
// same deliberate divergence ReadDependencies/WriteEdges already document
// for this package: an unscoped delete would target every tenant's stale
// rows at once, which is never what a per-org build request means.
func DeleteStalePRDependencyIssueEdges(ctx context.Context, conn driver.Conn, organizationID string) error {
	if err := requireEdgeScope(organizationID); err != nil {
		return err
	}
	if err := conn.Exec(ctx, staleDependencyIssueEdgesDeleteSQL,
		clickhouse.Named("org_id", organizationID),
	); err != nil {
		return fmt.Errorf("delete stale PR-dependency issue edges: %w", err)
	}
	return nil
}

// WriteProjectionRun inserts one `work_graph_projection_runs` watermark row
// (`_publish_blocker_projection`, builder.py:1016-1045).
//
// ScopeRepoID is written NULL whenever empty: the column is `Nullable(UUID)`,
// and this step is deliberately window/repo-scope-independent by design (see
// issueIssueEdgesPreStep.Run's own doc comment) -- there is no repo scope for
// this step to have parsed in the first place, unlike Python's config.repo_id,
// which is only ever non-nil for a repo-scoped legacy CLI invocation with no
// Go pre-step equivalent.
func WriteProjectionRun(ctx context.Context, conn driver.Conn, run ProjectionRun) error {
	if err := requireEdgeScope(run.OrgID); err != nil {
		return err
	}
	var scopeRepoID *uuid.UUID
	if run.ScopeRepoID != "" {
		parsed, err := uuid.Parse(run.ScopeRepoID)
		if err != nil {
			return fmt.Errorf("parse scope_repo_id %q: %w", run.ScopeRepoID, err)
		}
		scopeRepoID = &parsed
	}
	batch, err := conn.PrepareBatch(ctx,
		"INSERT INTO work_graph_projection_runs ("+
			"org_id, projection_name, scope_repo_id, rule_version, input_watermark, row_count, completed_at)")
	if err != nil {
		return fmt.Errorf("prepare work_graph_projection_runs batch: %w", err)
	}
	if err := batch.Append(
		run.OrgID, run.ProjectionName, scopeRepoID, run.RuleVersion,
		run.InputWatermark, uint64(run.RowCount), run.CompletedAt,
	); err != nil {
		return fmt.Errorf("append work_graph_projection_runs row: %w", err)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send work_graph_projection_runs batch: %w", err)
	}
	return nil
}
