//go:build integration

package edges

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestBlockerCleanupAndProjectionRunAgainstLiveClickHouse is the red/green
// proof for CHAOS-5303 r1 P1: codex review found that the first cut of this
// port carried edge derivation only, leaving `_delete_dependency_edge_candidates`
// and `_publish_blocker_projection`'s behavior completely unreplicated --
// `BuildCleanupPlan`/`BuildBlockerProjection` existed but were never called
// against a real database. This test exercises the exact sequence
// issueIssueEdgesPreStep.Run() now performs (DeleteProjectionRuns ->
// ReadExistingBlockerEdgeIDs -> BuildCleanupPlan -> DeleteEdgesByID ->
// WriteEdges -> BuildBlockerProjection -> WriteProjectionRun) against a real
// engine and asserts both halves of what was missing: a stale legacy blocker
// edge is actually deleted, and a fresh projection-run watermark actually
// replaces the prior one.
//
// RED ON THE PRE-FIX TIP: before CHAOS-5303 r1's fix, Run() never called any
// of DeleteProjectionRuns/ReadExistingBlockerEdgeIDs/DeleteEdgesByID/
// WriteProjectionRun -- this test's two assertions below would have failed
// against that code (the stale edge would still be present, and no fresh
// projection_runs row would exist at all, since work_graph_projection_runs
// started empty and nothing wrote to it). It is written directly against the
// public sequence rather than the unexported pre-step type, matching this
// package's own precedent (writeorder_integration_test.go tests the
// package-level functions a pre-step calls, not the pre-step type itself).
func TestBlockerCleanupAndProjectionRunAgainstLiveClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// The REAL migration chain -- see writeorder_integration_test.go's own
	// comment on why a hand-copied schema is a second source of truth this
	// test must not introduce.
	chschema.Apply(ctx, t, instance)

	const org = "70d529e0-3c06-4597-8480-794fd02328b6"
	const staleWorkItemA = "gh:acme/app#101"
	const staleWorkItemB = "gh:acme/app#102"

	// Seed a STALE legacy edge: a `relates`-orientation edge between two work
	// items that current dependency data says should be a `blocks` edge. This
	// is exactly the shape BuildCleanupPlan's doc comment describes -- "a
	// legacy row may have been written under any of those orientations by an
	// older canonicalisation" -- and it must be gone after cleanup runs,
	// because the rewrite that follows only re-creates the CURRENT
	// orientation. This one is reachable through BOTH of BuildCleanupPlan's
	// input paths at once: it is a `relates`-type candidate the CURRENT
	// `blocks` dependency row (seeded below) regenerates directly, so it does
	// NOT prove the existing-ids READ path does anything by itself.
	staleEdgeID := EdgeID(NodeTypeIssue, staleWorkItemA, EdgeTypeRelates, NodeTypeIssue, staleWorkItemB)
	seededAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := conn.Exec(ctx, `INSERT INTO work_graph_edges
(edge_id, source_type, source_id, target_type, target_id, edge_type, provenance,
 confidence, evidence, discovered_at, last_synced, event_ts, day, org_id)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		staleEdgeID, "issue", staleWorkItemA, "issue", staleWorkItemB, "relates", "native",
		float32(0.9), "stale-legacy-orientation", seededAt, seededAt, seededAt, seededAt, org,
	); err != nil {
		t.Fatal(err)
	}

	// Seed a SECOND stale edge for a pair with NO current dependency row at
	// all -- an orphaned edge from a relationship that has since been removed
	// or renamed upstream. BuildCleanupPlan's freshly-generated candidates
	// (from the CURRENT dependency rows) can never name this id, since there
	// is no row to generate it from. The ONLY path that can catch it is
	// ReadExistingBlockerEdgeIDs's live read feeding into
	// BuildCleanupPlan's existingEdgeIDs argument -- so this edge is what
	// proves that read path actually does something, not just the
	// freshly-generated path the first stale edge above also exercises.
	// `blocks`/`is_blocked_by` only, matching the read's own WHERE clause
	// (edge_type IN ('blocks', 'is_blocked_by')) -- a `relates` orphan would
	// never be found by the read either, by design (builder.py:959).
	const orphanWorkItemA = "gh:acme/app#201"
	const orphanWorkItemB = "gh:acme/app#202"
	orphanEdgeID := EdgeID(NodeTypeIssue, orphanWorkItemA, EdgeTypeBlocks, NodeTypeIssue, orphanWorkItemB)
	if err := conn.Exec(ctx, `INSERT INTO work_graph_edges
(edge_id, source_type, source_id, target_type, target_id, edge_type, provenance,
 confidence, evidence, discovered_at, last_synced, event_ts, day, org_id)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		orphanEdgeID, "issue", orphanWorkItemA, "issue", orphanWorkItemB, "blocks", "native",
		float32(0.9), "orphaned-no-current-dependency-row", seededAt, seededAt, seededAt, seededAt, org,
	); err != nil {
		t.Fatal(err)
	}

	// Seed a STALE projection-run watermark for this exact org+projection+rule,
	// dated far in the past. DeleteProjectionRuns must remove it before the
	// fresh one is published, matching Python's delete-before-publish order.
	staleWatermark := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := conn.Exec(ctx, `INSERT INTO work_graph_projection_runs
(org_id, projection_name, scope_repo_id, rule_version, input_watermark, row_count, completed_at)
VALUES (?,?,NULL,?,?,?,?)`,
		org, BlockerProjectionName, BlockerProjectionRuleVersion(),
		staleWatermark, uint64(0), staleWatermark,
	); err != nil {
		t.Fatal(err)
	}

	// Current dependency data: a live `blocks` relationship between the same
	// two work items, which is what makes the seeded `relates` edge above
	// stale (BuildCleanupPlan generates the delete-candidate id for every
	// orientation x edge-type combination from the CURRENT relationship row,
	// including the exact id seeded above).
	dependencyRows := []DependencyRow{{
		SourceWorkItemID: staleWorkItemA, TargetWorkItemID: staleWorkItemB,
		RelationshipType: "blocks", RelationshipRaw: "blocks",
		LastSynced: time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC),
	}}

	buildClock := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	derived, err := DeriveIssueIssueEdges(dependencyRows, buildClock)
	if err != nil {
		t.Fatal(err)
	}
	if len(derived.Edges) != 1 {
		t.Fatalf("expected exactly one derived edge, got %d", len(derived.Edges))
	}

	// The sequence issueIssueEdgesPreStep.Run() now performs, in Python's own
	// order: delete stale projection rows, read existing blocker edge ids,
	// compute and execute the cleanup plan, write the fresh edges, publish the
	// new watermark.
	if err := DeleteProjectionRuns(ctx, conn, org, BlockerProjectionName, BlockerProjectionRuleVersion()); err != nil {
		t.Fatalf("DeleteProjectionRuns: %v", err)
	}
	existingIDs, err := ReadExistingBlockerEdgeIDs(ctx, conn, org)
	if err != nil {
		t.Fatalf("ReadExistingBlockerEdgeIDs: %v", err)
	}
	foundOrphan := false
	for _, id := range existingIDs {
		if id == orphanEdgeID {
			foundOrphan = true
		}
	}
	if !foundOrphan {
		t.Fatalf("ReadExistingBlockerEdgeIDs did not return the seeded orphan edge id %s among %v "+
			"-- this is the only path that can catch an edge with no current dependency row",
			orphanEdgeID, existingIDs)
	}
	plan := BuildCleanupPlan(dependencyRows, existingIDs)
	if err := DeleteEdgesByID(ctx, conn, org, plan); err != nil {
		t.Fatalf("DeleteEdgesByID: %v", err)
	}
	if _, err := WriteEdges(ctx, conn, org, derived.Edges); err != nil {
		t.Fatalf("WriteEdges: %v", err)
	}
	projectionRun := BuildBlockerProjection(org, "", derived.Edges, buildClock)
	if err := WriteProjectionRun(ctx, conn, projectionRun); err != nil {
		t.Fatalf("WriteProjectionRun: %v", err)
	}

	// ASSERTION 1: BOTH stale edges are gone -- the freshly-regenerated-candidate
	// path (staleEdgeID) AND the existing-ids-read path (orphanEdgeID).
	// `mutations_sync=2` inside DeleteEdgesByID means this read does not need
	// to poll for the mutation to land.
	for _, staleID := range []string{staleEdgeID, orphanEdgeID} {
		var staleCount uint64
		if err := conn.QueryRow(ctx,
			`SELECT count() FROM work_graph_edges FINAL WHERE edge_id = ?`, staleID,
		).Scan(&staleCount); err != nil {
			t.Fatal(err)
		}
		if staleCount != 0 {
			t.Errorf("stale edge %s still present after cleanup (count=%d) -- this is "+
				"exactly the CHAOS-5303 r1 P1 regression: the cleanup plan was computed but never "+
				"executed", staleID, staleCount)
		}
	}

	// ASSERTION 2: the watermark advanced. Both the stale row's absence and
	// the fresh row's presence matter -- a bug that appended without deleting
	// would leave the stale row alongside the fresh one, and FINAL alone
	// would not surface that.
	rows, err := conn.Query(ctx,
		`SELECT input_watermark, row_count, completed_at FROM work_graph_projection_runs FINAL
		 WHERE org_id = ? AND projection_name = ? AND rule_version = ?`,
		org, BlockerProjectionName, BlockerProjectionRuleVersion())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var (
		watermarkCount int
		gotWatermark   time.Time
		gotRowCount    uint64
		gotCompletedAt time.Time
	)
	for rows.Next() {
		watermarkCount++
		if err := rows.Scan(&gotWatermark, &gotRowCount, &gotCompletedAt); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if watermarkCount != 1 {
		t.Fatalf("expected exactly one projection_runs row for this org+projection+rule after "+
			"FINAL, got %d -- either the stale delete or the fresh publish did not land as "+
			"a single row", watermarkCount)
	}
	if gotCompletedAt.Equal(staleWatermark) || gotWatermark.Equal(staleWatermark) {
		t.Errorf("projection_runs still shows the STALE watermark (%v) -- "+
			"DeleteProjectionRuns did not remove it before the fresh publish", staleWatermark)
	}
	if !gotCompletedAt.Equal(buildClock) {
		t.Errorf("completed_at = %v, want the build clock %v", gotCompletedAt, buildClock)
	}
}
