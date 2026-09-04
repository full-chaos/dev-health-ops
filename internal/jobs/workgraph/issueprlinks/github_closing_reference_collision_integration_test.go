//go:build integration

package issueprlinks_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

// TestGithubClosingReferenceCollisionSurvivesRealMerge is CHAOS-4757's
// activation COMPOSED proof, closing the least-sure line stated in #2174's
// own review context: a real github_closing_reference DependencyRow through
// Derive, then the collision path on REAL ClickHouse, had only been proven as
// two separate facts (a unit test on Derive's provenance stamping, and
// CHAOS-4769's generic collision test seeded directly into ClickHouse) — never
// as one continuous chain.
//
// # RED-FIRST
//
// With github_closing_reference NOT in the admission table (the exact shape
// it had in ReservedAdmissions before this PR), Derive must admit and write
// NOTHING for this row -- checked directly below, not assumed, using an
// explicit reserved-shape Admissions table before the real
// DefaultAdmissions-driven call. If that check ever passes with Written()>0,
// this test's own red-first premise is broken and everything after it is
// meaningless.
//
// # Why this needs no repos/work_items/git_pull_requests seeding
//
// Derive is a pure function over in-memory Inputs (see derive_test.go's
// baseInputs pattern) -- it never touches ClickHouse. Only the OUTPUT (the
// Link Derive produces) is written to real ClickHouse here, via the real
// Writer, so only work_graph_issue_pr needs seeding -- for the colliding
// explicit_text row a real Python-era build would have written.
func TestGithubClosingReferenceCollisionSurvivesRealMerge(t *testing.T) {
	ctx := context.Background()
	conn := connect(ctx, t)

	const (
		org      = "00000000-4757-0000-0000-000000000001"
		repoSlug = "owner/closing-ref-repo"
		prNumber = 4757
	)
	repoID := uuid.MustParse("00000000-4757-0000-0000-000000000002")
	workItemID := "gh:owner/closing-ref-repo#99"

	dependency := issueprlinks.DependencyRow{
		OrgID:               org,
		SourceWorkItemID:    fmt.Sprintf("ghpr:%s#%d", repoSlug, prNumber),
		TargetWorkItemID:    workItemID,
		RelationshipTypeRaw: "github_closing_reference",
		// Real, EARLIER than the fallback's build-time stamp below -- the
		// exact regime CHAOS-4769 exists for (a real dependency-row timestamp
		// losing on last_synced alone, before rank was added).
		LastSynced: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	inputs := issueprlinks.Inputs{
		OrgID:        org,
		Dependencies: []issueprlinks.DependencyRow{dependency},
		Repos:        []issueprlinks.RepoRow{{OrgID: org, ID: repoID, Repo: repoSlug}},
		PullRequests: []issueprlinks.PullRequestRow{{OrgID: org, RepoID: repoID, Number: prNumber}},
		WorkItems:    []issueprlinks.WorkItemRow{{OrgID: org, WorkItemID: workItemID}},
		// RESERVED shape: the admission table exactly as it stood before this
		// PR (linear_attachment only) -- github_closing_reference deliberately
		// absent.
		Admissions: []issueprlinks.Admission{
			{RelationshipTypeRaw: "linear_attachment", TargetPrefix: "linear:"},
		},
	}
	revertedResult := issueprlinks.Derive(inputs)
	if revertedResult.Written() != 0 {
		t.Fatalf("RED-FIRST CHECK FAILED: with github_closing_reference not admitted, "+
			"Derive wrote %d links, want 0 -- this test's own setup cannot prove anything "+
			"if the pre-activation shape already writes", revertedResult.Written())
	}
	if got := revertedResult.Rejected[issueprlinks.ReasonNotAdmissible]; got != 1 {
		t.Fatalf("RED-FIRST CHECK FAILED: rejected[not_admissible] = %d, want 1", got)
	}

	// GREEN: the real (activated) admission table.
	inputs.Admissions = nil // -> DefaultAdmissions, github_closing_reference included
	result := issueprlinks.Derive(inputs)
	if result.Written() != 1 {
		t.Fatalf("wrote %d links with the activated admission table, want 1 (rejections %v)",
			result.Written(), result.Rejected)
	}
	link := result.Links[0]
	if link.Provenance != issueprlinks.ProvenanceNative {
		t.Fatalf("provenance = %q, want %q -- the version_rank collision proof below is "+
			"meaningless if this row does not enter the native tier", link.Provenance, issueprlinks.ProvenanceNative)
	}

	writer, err := issueprlinks.NewWriter(conn)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := writer.Write(ctx, []issueprlinks.Link{link}); err != nil {
		t.Fatalf("write the derived native row: %v", err)
	}

	// The colliding row: same identity (repo_id, work_item_id, pr_number), the
	// shape Python's fallback text-parse writer (_build_issue_pr_edges) would
	// have produced from a "closes #99" PR body -- LATER last_synced (build
	// time), lower confidence. Before CHAOS-4769 this row would have won on
	// last_synced alone.
	seed(ctx, t, conn, org, repoID.String(), workItemID, prNumber,
		"explicit_text", 0.90, "2026-01-02 00:00:00.000")

	assertRowCount(ctx, t, conn, workItemID, 2)
	mustExec(ctx, t, conn, "OPTIMIZE TABLE work_graph_issue_pr FINAL")
	assertRowCount(ctx, t, conn, workItemID, 1)
	assertOnlyProvenance(ctx, t, conn, workItemID, issueprlinks.ProvenanceNative)
	assertSurvivingRowIntact(ctx, t, conn, workItemID, org, repoID.String(), prNumber,
		1.0, issueprlinks.ProvenanceNative, "github_closing_reference", "2026-01-01 00:00:00.000")

	writeProof(t, "github-closing-reference-composed-collision")
}
