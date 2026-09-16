package daily

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestAIImpactAttributionsSQLPinsOrgIDInEveryJoin guards the same shape
// TestAIImpactAttributionTenantIsolation (ai_impact_native_integration_test.go)
// proves at runtime: LoadAIImpactAttributions' two UNION ALL arms must each
// carry org_id on both sides of every JOIN predicate, plus an org_id filter
// in the WHERE of each arm. A container test proves the runtime behavior;
// this static assertion catches the same regression without one, matching
// this package's TestGovernanceArtifactSQLUsesFINALWherePythonDoes shape:
// capture the query the loader ACTUALLY issues, strip comments, then assert
// on fragments.
func TestAIImpactAttributionsSQLPinsOrgIDInEveryJoin(t *testing.T) {
	sql := stripSQLComments(captureAttributionsQuery(t))

	for _, required := range []struct {
		fragment string
		why      string
	}{
		{
			"ON link.org_id = pr.org_id",
			"path 1's work_graph_issue_pr join must carry org_id on both sides -- " +
				"omitting it lets a same-repo/same-number PR from a DIFFERENT org's " +
				"work_graph_issue_pr row link into this org's answer",
		},
		{
			"ON wi.org_id = link.org_id",
			"path 1's work_items join must carry org_id on both sides -- otherwise a " +
				"same-work_item_id row from another org could supply the wrong work_type",
		},
	} {
		if !strings.Contains(sql, required.fragment) {
			t.Errorf("the executed query no longer contains %q (outside comments).\n%s",
				required.fragment, required.why)
		}
	}

	// Both UNION arms filter on the SAME org twice over (the anchor PR and the
	// joined/matched attribution row) -- one occurrence per arm, so exactly 2
	// each. A count instead of a bare Contains, because a single surviving
	// occurrence would satisfy Contains while the other arm silently lost its
	// filter -- the same reasoning TestGovernanceArtifactSQLUsesFINALWherePythonDoes
	// applies to the allowlist joins in this package.
	for _, required := range []struct {
		fragment string
		want     int
		why      string
	}{
		{"pr.org_id = ?", 2, "each UNION arm's anchor PR must stay org-scoped"},
		{"toString(attr.org_id) = ?", 2, "each UNION arm's matched attribution row must stay org-scoped"},
	} {
		if got := strings.Count(sql, required.fragment); got != required.want {
			t.Errorf("expected exactly %d occurrences of %q, found %d. %s",
				required.want, required.fragment, got, required.why)
		}
	}
}

// captureAttributionsQuery runs LoadAIImpactAttributions against a connection
// that records the query and refuses, returning the exact string the loader
// passed. Mirrors captureGovernanceQuery (ai_governance_sql_shape_test.go),
// reusing its queryCapturingRows/stripSQLComments helpers -- same package,
// same execution-link technique.
func captureAttributionsQuery(t *testing.T) string {
	t.Helper()
	capture := &queryCapturingRows{}
	start := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	repoIDs := []uuid.UUID{uuid.MustParse("00000000-0000-4000-8000-000000000001")}
	_, err := LoadAIImpactAttributions(
		context.Background(), capture,
		"00000000-0000-4000-8000-0000000000a0",
		repoIDs, start, start.Add(24*time.Hour),
	)
	if err == nil {
		t.Fatal("the capturing connection must make the loader fail; it returned nil error, " +
			"which means the query was never issued and the capture is empty")
	}
	if capture.query == "" {
		t.Fatalf("no query was captured (loader returned %v before querying) -- the execution "+
			"link is not being exercised, so this test proves nothing", err)
	}
	return capture.query
}

// TestAIImpactPRCommitLinkageSQLPinsOrgIDInEveryJoin guards
// LoadAIImpactPRCommitLinkage's doc comment in this file: both the
// git_commit_stats and git_commits joins must carry org_id on both sides,
// and the WHERE must scope the anchor table too.
func TestAIImpactPRCommitLinkageSQLPinsOrgIDInEveryJoin(t *testing.T) {
	sql := stripSQLComments(capturePRCommitLinkageQuery(t))

	for _, required := range []struct {
		fragment string
		why      string
	}{
		{
			"ON s.org_id = p.org_id",
			"the git_commit_stats join must carry org_id on both sides -- two orgs " +
				"sharing a repo_id/commit_hash pair would otherwise join the WRONG " +
				"org's file_path into this org's linkage",
		},
		{
			"c.org_id = p.org_id",
			"the git_commits join must carry org_id on both sides for the same reason",
		},
		{
			"WHERE p.org_id = ?",
			"the anchor work_graph_pr_commit read must stay org-scoped",
		},
	} {
		if !strings.Contains(sql, required.fragment) {
			t.Errorf("the executed query no longer contains %q (outside comments).\n%s",
				required.fragment, required.why)
		}
	}
}

// capturePRCommitLinkageQuery mirrors captureAttributionsQuery for
// LoadAIImpactPRCommitLinkage.
func capturePRCommitLinkageQuery(t *testing.T) string {
	t.Helper()
	capture := &queryCapturingRows{}
	repoIDs := []uuid.UUID{uuid.MustParse("00000000-0000-4000-8000-000000000001")}
	_, err := LoadAIImpactPRCommitLinkage(
		context.Background(), capture,
		"00000000-0000-4000-8000-0000000000a0",
		repoIDs, []uint32{1},
	)
	if err == nil {
		t.Fatal("the capturing connection must make the loader fail; it returned nil error, " +
			"which means the query was never issued and the capture is empty")
	}
	if capture.query == "" {
		t.Fatalf("no query was captured (loader returned %v before querying) -- the execution "+
			"link is not being exercised, so this test proves nothing", err)
	}
	return capture.query
}
