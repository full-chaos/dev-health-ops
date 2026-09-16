package providersync

import (
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

// TestGitHubWorkItemEngineInspectQueriesDedupAndFilterOrgInTheSameStatement
// pins the class ruling this file's inspect() readers hold to: the org
// filter and the version-dedup GROUP BY must sit in the SAME top-level
// statement, never a subquery an outer WHERE only narrows afterward. A
// depth-based check (sqlshape.Depths), not a bare substring search, so a
// future parenthesised addition between the two markers cannot silently
// defeat it without changing nesting depth.
func TestGitHubWorkItemEngineInspectQueriesDedupAndFilterOrgInTheSameStatement(
	t *testing.T,
) {
	day := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)

	issueQuery, _ := githubIssueTypeMetricsInspectQuery(
		"org-acme", day, githubWorkItemEngineEffectIssueRow(),
	)
	classificationQuery, _ := githubInvestmentClassificationInspectQuery(
		"org-acme", day, githubWorkItemEngineEffectClassificationRow(),
	)

	cases := []struct {
		name        string
		query       string
		orgMarker   string
		dedupMarker string
	}{
		{
			"issue_type_metrics_daily", issueQuery,
			"org_id = ?",
			"GROUP BY repo_id, created_count, completed_count, active_count",
		},
		{
			"investment_classifications_daily", classificationQuery,
			"org_id = ?",
			"GROUP BY repo_id, confidence, rule_id, computed_at",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if strings.Count(testCase.query, "SELECT") != 1 {
				t.Fatalf("%s: expected exactly one top-level SELECT, no dedup subquery:\n%s",
					testCase.name, testCase.query)
			}
			depths := sqlshape.Depths(testCase.query)

			orgIdx := strings.Index(testCase.query, testCase.orgMarker)
			if orgIdx < 0 {
				t.Fatalf("%s: org filter marker %q not found:\n%s",
					testCase.name, testCase.orgMarker, testCase.query)
			}
			dedupIdx := strings.Index(testCase.query, testCase.dedupMarker)
			if dedupIdx < 0 {
				t.Fatalf("%s: dedup marker %q not found:\n%s",
					testCase.name, testCase.dedupMarker, testCase.query)
			}
			if depths[orgIdx] != 0 || depths[dedupIdx] != 0 {
				t.Fatalf(
					"%s: org filter (depth %d) and dedup GROUP BY (depth %d) must both sit at the top-level statement:\n%s",
					testCase.name, depths[orgIdx], depths[dedupIdx], testCase.query,
				)
			}
		})
	}
}
