//go:build integration

package workerservice

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
// ReadExistingDependencyEdgeIDs -> BuildCleanupPlan -> DeleteEdgesByID ->
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
// issueCommitWiringNow is the pinned clock of the issue_commit_edges cases:
// 2026-09-01T12:00Z seeds are 14 days inside the 30-day default window.
var issueCommitWiringNow = time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)

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
		// relationship_semantics_version is the CURRENT rule version
		// ("canonical-blocks.v2", CanonicalDependency's branch 2) so the
		// stored orientation is trusted as-is. A "legacy.v1" row from a
		// "gh:"-prefixed source with raw==relationship hits the historical
		// GitHub body-parsing quirk (canonical.go branch 3) and swaps
		// source/target -- a deliberate divergence with its own dedicated
		// coverage in canonical_test.go, not something this wiring-proof test
		// needs to reproduce.
		if err := conn.Exec(ctx, `INSERT INTO work_item_dependencies
(source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, relationship_semantics_version, last_synced, org_id)
VALUES (?,?,?,?,?,?,?)`,
			"gh:acme/app#201", "gh:acme/app#202", "blocks", "blocks", "canonical-blocks.v2",
			time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC), org,
		); err != nil {
			t.Fatal(err)
		}

		step, err := newIssueIssueEdgesPreStep(conn, stubIssueEdgesObserver{}, nil)
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
		// Pin the step's clock: with an empty scope the pre-step reads its
		// 30-day default window from step.now, so the fixed author_when above
		// is only "live" relative to a clock chosen with it (CHAOS-7455: the
		// wall clock passed 2026-10-01T12:00Z and the seed aged out).
		step.now = func() time.Time { return issueCommitWiringNow }
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

	t.Run("issue_commit_edges default window lower bound is exactly now-30d", func(t *testing.T) {
		const boundaryOrg = "5b1a6e2e-6b0a-4e6a-9b7c-workgraph-edge"
		from := issueCommitWiringNow.AddDate(0, 0, -30)
		seed := func(key, hash string, authorWhen time.Time) {
			if err := conn.Exec(ctx, `INSERT INTO work_items
(repo_id, work_item_id, provider, org_id, title, type, status, status_raw, project_key, project_id, reporter,
 created_at, updated_at, sprint_id, sprint_name, parent_id, epic_id, url, last_synced)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
				repoID, "jira:"+key, "jira", boundaryOrg, "edge", "task", "open", "open", "EDGE", "10001", "someone",
				time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
				"", "", "", "", "", time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			); err != nil {
				t.Fatal(err)
			}
			if err := conn.Exec(ctx, `INSERT INTO git_commits
(repo_id, hash, message, author_when, parents, last_synced, org_id)
VALUES (?,?,?,?,?,?,?)`,
				repoID, hash, "Fixes "+key+": boundary", authorWhen, uint32(1), authorWhen, boundaryOrg,
			); err != nil {
				t.Fatal(err)
			}
		}
		seed("EDGE-1", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb1", from.Add(-time.Second))
		seed("EDGE-2", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2", from.Add(time.Second))

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
		step.now = func() time.Time { return issueCommitWiringNow }
		result, err := step.Run(ctx, workgraph.Claim{Request: workgraph.Request{OrganizationID: boundaryOrg}})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if result["edges_written"] != 1 || result["commits_scanned"] != 1 {
			t.Fatalf("Run result = %+v, want exactly the from+1s commit (scanned=1, written=1)", result)
		}
		var count uint64
		if err := conn.QueryRow(ctx,
			`SELECT count() FROM work_graph_edges FINAL WHERE org_id = ? AND target_id = ?`,
			boundaryOrg, "jira:EDGE-2",
		).Scan(&count); err != nil || count != 1 {
			t.Fatalf("in-window edge count = %d (err %v), want 1", count, err)
		}
		if err := conn.QueryRow(ctx,
			`SELECT count() FROM work_graph_edges FINAL WHERE org_id = ? AND target_id = ?`,
			boundaryOrg, "jira:EDGE-1",
		).Scan(&count); err != nil || count != 0 {
			t.Fatalf("out-of-window edge count = %d (err %v), want 0", count, err)
		}
	})
}
