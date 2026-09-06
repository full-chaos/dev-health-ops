//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuecommitedges"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// stubIssueEdgesObserver satisfies jobruntime.WorkGraphIssueEdgesObserver
// without a real MetricsCollector -- issueIssueEdgesPreStep requires one
// (newIssueIssueEdgesPreStep refuses a nil observer), and this test only
// needs to prove the call succeeds, not assert on the recorded counts.
type stubIssueEdgesObserver struct{}

func (stubIssueEdgesObserver) ObserveWorkGraphIssueEdges(
	map[jobruntime.WorkGraphIssueEdgeOutcome]int, int, time.Duration,
) error {
	return nil
}

// TestNativePreStepsRunThroughTheProductionWiring is CHAOS-5341's fold-in of
// two review findings: (1) the issue_issue_edges cleanup/projection
// sequence was only ever exercised by inlining DeleteProjectionRuns ->
// ReadExistingBlockerEdgeIDs -> BuildCleanupPlan -> DeleteEdgesByID ->
// WriteEdges -> BuildBlockerProjection by hand (edges package's own
// blocker_cleanup_integration_test.go, kept as-is -- it still tests those
// package-level functions directly, which remains valuable), never through
// the actual `issueIssueEdgesPreStep.Run()` entry point the worker calls in
// production; (2) `issueCommitEdgesPreStep`'s Run/Loader/Service wiring had
// ZERO coverage. `issueIssueEdgesPreStep`/`issueCommitEdgesPreStep` are
// unexported types in `package main`, so a test proving the PRODUCTION
// CONSTRUCTOR + Run() wiring works end-to-end has to live here, not in the
// edges/issuecommitedges packages themselves -- there is precedent for
// package-main integration tests in this file's siblings (e.g.
// provider_sync_entitlement_integration_test.go).
//
// Both cases are deliberately minimal: prove the wiring (constructor -> Run
// -> real ClickHouse read/write) actually works, not re-derive the detailed
// scenario coverage the edges package's own test already owns.
func TestNativePreStepsRunThroughTheProductionWiring(t *testing.T) {
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

	chschema.Apply(ctx, t, instance)

	const org = "5b1a6e2e-6b0a-4e6a-9b7c-workgraph-wire"
	repoID := uuid.MustParse("00000000-0000-4000-8000-00000000ff01")

	t.Run("issue_issue_edges pre-step Run() writes an edge from a live dependency row", func(t *testing.T) {
		if err := conn.Exec(ctx, `INSERT INTO work_item_dependencies
(source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, relationship_semantics_version, last_synced, org_id)
VALUES (?,?,?,?,?,?,?)`,
			"gh:acme/app#201", "gh:acme/app#202", "blocks", "blocks", "legacy.v1",
			time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC), org,
		); err != nil {
			t.Fatal(err)
		}

		step, err := newIssueIssueEdgesPreStep(conn, stubIssueEdgesObserver{})
		if err != nil {
			t.Fatal(err)
		}
		claim := workgraph.Claim{Request: workgraph.Request{OrganizationID: org}}
		result, err := step.Run(ctx, claim)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if result == nil {
			t.Fatal("Run returned a nil result fragment")
		}

		var count uint64
		if err := conn.QueryRow(ctx,
			`SELECT count() FROM work_graph_edges FINAL WHERE org_id = ? AND source_id = ? AND target_id = ?`,
			org, "gh:acme/app#201", "gh:acme/app#202",
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("expected exactly one edge written through the production Run() wiring, got %d", count)
		}
	})

	t.Run("issue_commit_edges pre-step Run() writes an edge from a live commit reference", func(t *testing.T) {
		commitHash := "cafef00dcafef00dcafef00dcafef00dcafef00"
		if err := conn.Exec(ctx, `INSERT INTO work_items
(repo_id, work_item_id, provider, org_id, title, type, status, status_raw, project_key, project_id, reporter,
 created_at, updated_at, sprint_id, sprint_name, parent_id, epic_id, url, last_synced)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			repoID, "jira:WIRE-1", "jira", org, "wire test", "task", "open", "open", "WIRE", "10000", "someone",
			time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			"", "", "", "", "", time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		); err != nil {
			t.Fatal(err)
		}
		if err := conn.Exec(ctx, `INSERT INTO git_commits
(repo_id, hash, message, author_when, parents, last_synced, org_id)
VALUES (?,?,?,?,?,?,?)`,
			repoID, commitHash, "Fixes WIRE-1: wire the pre-step",
			time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC), uint32(1),
			time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC), org,
		); err != nil {
			t.Fatal(err)
		}

		loader, err := issuecommitedges.NewLoader(conn)
		if err != nil {
			t.Fatal(err)
		}
		service, err := issuecommitedges.NewService(loader, conn, nil)
		if err != nil {
			t.Fatal(err)
		}
		step, err := newIssueCommitEdgesPreStep(service)
		if err != nil {
			t.Fatal(err)
		}
		claim := workgraph.Claim{Request: workgraph.Request{OrganizationID: org}}
		result, err := step.Run(ctx, claim)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if result["edges_written"] != 1 {
			t.Fatalf("Run result = %+v, want edges_written=1", result)
		}

		var count uint64
		if err := conn.QueryRow(ctx,
			`SELECT count() FROM work_graph_edges FINAL WHERE org_id = ? AND target_id = ?`,
			org, "jira:WIRE-1",
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("expected exactly one edge written through the production Run() wiring, got %d", count)
		}
	})
}
