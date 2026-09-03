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

// TestJiraDevStatusCollisionSurvivesRealMerge is PR6's activation COMPOSED
// proof, mirroring TestGithubClosingReferenceCollisionSurvivesRealMerge
// (github_closing_reference_collision_integration_test.go) for jira_dev_status.
// See that test's doc for the full reasoning; this one states only what
// differs.
//
// # RED-FIRST
//
// The reserved-shape Admissions table below is DefaultAdmissions as it stood
// immediately before this PR (linear_attachment + github_closing_reference,
// jira_dev_status deliberately absent) -- not the original pre-#2174 shape,
// since #2174 is merged by the time this test's premise matters.
//
// FIXED (codex round 1 on #2179, P3): this table used to be hand-retyped
// here with only TargetPrefix on the github_closing_reference entry, missing
// TargetValidator: isWellFormedGithubIssueTarget that the real base table
// carries (issueprlinks.go DefaultAdmissions) -- a behaviorally looser
// stand-in for the real predecessor state, silently accepting inputs the
// real base would reject. Since isWellFormedGithubIssueTarget is unexported
// and this is an external (_test) package, the fix is to DERIVE the
// reserved-shape table from the real DefaultAdmissions by filtering out
// jira_dev_status, rather than hand-retyping it -- this can't drift out of
// sync with the real table again, by construction.
func TestJiraDevStatusCollisionSurvivesRealMerge(t *testing.T) {
	ctx := context.Background()
	conn := connect(ctx, t)

	const (
		org      = "00000000-4757-0000-0000-000000000003"
		repoSlug = "owner/dev-status-repo"
		prNumber = 4758
	)
	repoID := uuid.MustParse("00000000-4757-0000-0000-000000000004")
	workItemID := "jira:OPS-99"

	dependency := issueprlinks.DependencyRow{
		OrgID:               org,
		SourceWorkItemID:    fmt.Sprintf("ghpr:%s#%d", repoSlug, prNumber),
		TargetWorkItemID:    workItemID,
		RelationshipTypeRaw: "jira_dev_status",
		// Real, EARLIER than the fallback's build-time stamp below -- the
		// exact regime CHAOS-4769 exists for.
		LastSynced: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	inputs := issueprlinks.Inputs{
		OrgID:        org,
		Dependencies: []issueprlinks.DependencyRow{dependency},
		Repos:        []issueprlinks.RepoRow{{OrgID: org, ID: repoID, Repo: repoSlug}},
		PullRequests: []issueprlinks.PullRequestRow{{OrgID: org, RepoID: repoID, Number: prNumber}},
		WorkItems:    []issueprlinks.WorkItemRow{{OrgID: org, WorkItemID: workItemID}},
		Admissions:   reservedShapeAdmissions(t),
	}
	revertedResult := issueprlinks.Derive(inputs)
	if revertedResult.Written() != 0 {
		t.Fatalf("RED-FIRST CHECK FAILED: with jira_dev_status not admitted, "+
			"Derive wrote %d links, want 0", revertedResult.Written())
	}
	if got := revertedResult.Rejected[issueprlinks.ReasonNotAdmissible]; got != 1 {
		t.Fatalf("RED-FIRST CHECK FAILED: rejected[not_admissible] = %d, want 1", got)
	}

	// GREEN: the real (activated) admission table.
	inputs.Admissions = nil // -> DefaultAdmissions, jira_dev_status included
	result := issueprlinks.Derive(inputs)
	if result.Written() != 1 {
		t.Fatalf("wrote %d links with the activated admission table, want 1 (rejections %v)",
			result.Written(), result.Rejected)
	}
	link := result.Links[0]
	if link.Provenance != issueprlinks.ProvenanceNative {
		t.Fatalf("provenance = %q, want %q", link.Provenance, issueprlinks.ProvenanceNative)
	}

	writer, err := issueprlinks.NewWriter(conn)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := writer.Write(ctx, []issueprlinks.Link{link}); err != nil {
		t.Fatalf("write the derived native row: %v", err)
	}

	seed(ctx, t, conn, org, repoID.String(), workItemID, prNumber,
		"explicit_text", 0.90, "2026-01-02 00:00:00.000")

	assertRowCount(ctx, t, conn, workItemID, 2)
	mustExec(ctx, t, conn, "OPTIMIZE TABLE work_graph_issue_pr FINAL")
	assertRowCount(ctx, t, conn, workItemID, 1)
	assertOnlyProvenance(ctx, t, conn, workItemID, issueprlinks.ProvenanceNative)
	assertSurvivingRowIntact(ctx, t, conn, workItemID, org, repoID.String(), prNumber,
		1.0, issueprlinks.ProvenanceNative, "jira_dev_status", "2026-01-01 00:00:00.000")

	writeProof(t, "jira-dev-status-composed-collision")
}

// reservedShapeAdmissions is DefaultAdmissions with jira_dev_status filtered
// out -- i.e. exactly the admission table as it stood immediately before this
// PR activated jira_dev_status. Derived from the real table rather than
// hand-retyped: isWellFormedGithubIssueTarget is unexported, so this
// (external, _test) package cannot name it to reconstruct
// github_closing_reference's entry by hand, and a hand-typed copy that
// silently drops TargetValidator is exactly the P3 this fixes -- deriving it
// makes that drift impossible by construction.
func reservedShapeAdmissions(t *testing.T) []issueprlinks.Admission {
	t.Helper()
	reserved := make([]issueprlinks.Admission, 0, len(issueprlinks.DefaultAdmissions))
	for _, admission := range issueprlinks.DefaultAdmissions {
		if admission.RelationshipTypeRaw == "jira_dev_status" {
			continue
		}
		reserved = append(reserved, admission)
	}
	if len(reserved) != len(issueprlinks.DefaultAdmissions)-1 {
		t.Fatalf("expected to filter exactly one jira_dev_status entry out of DefaultAdmissions, filtered %d",
			len(issueprlinks.DefaultAdmissions)-len(reserved))
	}
	return reserved
}

// TestReservedShapeAdmissionsMatchesRealValidation is the regression guard
// for codex round 1 on #2179, P3: reservedShapeAdmissions must enforce the
// SAME validation the real DefaultAdmissions entry does, not a looser
// stand-in. Concrete counterexample codex constructed: a
// github_closing_reference row with source ghpr:owner/repo#12, target
// gh:owner/repo#0, and matching work-item/repo/PR lookups is rejected by the
// real base table but was admitted by the old hand-typed fixture
// (TargetPrefix only, no TargetValidator). Pure Derive() logic -- no
// ClickHouse connection needed, so this runs under -tags=integration without
// docker.
func TestReservedShapeAdmissionsMatchesRealValidation(t *testing.T) {
	const org = "00000000-4757-0000-0000-000000000005"
	const repoSlug = "owner/repo"
	const malformedTarget = "gh:owner/repo#0"
	repoID := uuid.MustParse("00000000-4757-0000-0000-000000000006")

	inputs := issueprlinks.Inputs{
		OrgID: org,
		Dependencies: []issueprlinks.DependencyRow{{
			OrgID:               org,
			SourceWorkItemID:    fmt.Sprintf("ghpr:%s#12", repoSlug),
			TargetWorkItemID:    malformedTarget,
			RelationshipTypeRaw: "github_closing_reference",
		}},
		Repos:        []issueprlinks.RepoRow{{OrgID: org, ID: repoID, Repo: repoSlug}},
		PullRequests: []issueprlinks.PullRequestRow{{OrgID: org, RepoID: repoID, Number: 12}},
		WorkItems:    []issueprlinks.WorkItemRow{{OrgID: org, WorkItemID: malformedTarget}},
		Admissions:   reservedShapeAdmissions(t),
	}
	result := issueprlinks.Derive(inputs)
	if result.Written() != 0 {
		t.Fatalf("malformed github_closing_reference target %q admitted: wrote %d links, want 0 "+
			"(reservedShapeAdmissions must reject the same way the real DefaultAdmissions entry does)",
			malformedTarget, result.Written())
	}
	if got := result.Rejected[issueprlinks.ReasonNotAdmissible]; got != 1 {
		t.Fatalf("rejected[not_admissible] = %d, want 1", got)
	}
}
