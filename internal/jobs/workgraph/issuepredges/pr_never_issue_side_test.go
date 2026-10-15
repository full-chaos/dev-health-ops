package issuepredges

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
)

// A pull request is never the issue side of an issue <> pull request link
// (entity tree: Repository <> Pull request <> Issue <> Project). The pull
// request work items GitHub (`ghpr:<owner>/<repo>#<n>`, type pr) and GitLab
// (`gitlab:<path>!<n>`, type merge_request) mint are no issues: the text
// matcher and the heuristic never link one, and the build retracts a stored
// row that does.

var changeRequestItems = []issueProvider{
	{"github", "ghpr:full-chaos/deploy#120", ""},
	{"gitlab", "gitlab:full-chaos/deploy!120", ""},
}

func changeRequestWorkItem(item issueProvider, updated time.Time) []any {
	return []any{fallbackRepo, item.workItemID, item.name, updated}
}

// TestTextParseNeverLinksAPullRequestWorkItemAsTheIssue: pull request #121's
// body says "#120", and #120 is a pull request, not an issue. The control
// issue #119 named in the same body keeps its link.
func TestTextParseNeverLinksAPullRequestWorkItemAsTheIssue(t *testing.T) {
	created := time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)
	controls := map[string]issueProvider{
		"github": {"github", "gh:full-chaos/deploy#119", ""},
		"gitlab": {"gitlab", "gitlab:full-chaos/deploy#119", ""},
	}
	for _, item := range changeRequestItems {
		t.Run(item.name, func(t *testing.T) {
			control := controls[item.name]
			conn := newRouteConn()
			conn.pullRequests = [][]any{pullRequest(121, "follow-up of #120, fixes #119", created)}
			conn.workItems = [][]any{changeRequestWorkItem(item, created), workItem(control)}

			service := newFallbackService(t, conn)
			if _, err := service.ProduceTextParseEdges(context.Background(), "org-9828", Window{}); err != nil {
				t.Fatalf("ProduceTextParseEdges: %v", err)
			}
			want := control.workItemID + "#121"
			if pairs := linkedPairs(conn); len(pairs) != 1 || pairs[0] != want {
				t.Fatalf("links %v, want only %s: a pull request is never the issue side", pairs, want)
			}
		})
	}
}

// TestHeuristicNeverLinksAPullRequestWorkItemAsTheIssue: the pull request
// work item sits next to pull request #121 in time; the heuristic links only
// the control issue.
func TestHeuristicNeverLinksAPullRequestWorkItemAsTheIssue(t *testing.T) {
	updated := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	for _, item := range changeRequestItems {
		for _, control := range trackerMatrix() {
			t.Run(item.name+"/"+control.name, func(t *testing.T) {
				conn := newRouteConn()
				conn.pullRequests = [][]any{pullRequest(121, "", updated.Add(time.Hour))}
				conn.workItems = [][]any{changeRequestWorkItem(item, updated), workItem(control)}

				service := newFallbackService(t, conn)
				if _, err := service.ProduceHeuristicEdges(context.Background(), "org-9828",
					Window{HeuristicDaysWindow: 7, HeuristicConfidence: 0.3}); err != nil {
					t.Fatalf("ProduceHeuristicEdges: %v", err)
				}
				want := control.workItemID + "#121"
				if pairs := linkedPairs(conn); len(pairs) != 1 || pairs[0] != want {
					t.Fatalf("links %v, want only %s: a pull request is never the issue side", pairs, want)
				}
			})
		}
	}
}

// TestRetractionRemovesStoredLinksWhoseIssueSideIsAPullRequest is the prod
// shape (`ghpr:full-chaos/dev-health-deploy#120 -> 121`, explicit_text, and
// heuristic self-links): the build retracts every stored lower-tier row whose
// issue side is a pull request work item; the control issue link stays.
func TestRetractionRemovesStoredLinksWhoseIssueSideIsAPullRequest(t *testing.T) {
	conn := newRouteConn()
	conn.issuePR = [][]any{
		{fallbackRepo, "ghpr:full-chaos/deploy#120", uint32(121), "explicit_text"},
		{fallbackRepo, "ghpr:full-chaos/deploy#122", uint32(122), "heuristic"},
		{fallbackRepo, "gitlab:full-chaos/deploy!7", uint32(8), "heuristic"},
		{fallbackRepo, "jira:API-1", uint32(5), "explicit_text"},
		{fallbackRepo, "gh:full-chaos/deploy#119", uint32(121), "explicit_text"},
	}
	service := newFallbackService(t, conn)
	outcome, err := service.RetractOutrankedFallbacks(context.Background(), "org-9828")
	if err != nil {
		t.Fatalf("RetractOutrankedFallbacks: %v", err)
	}
	if outcome.LinksRetracted != 3 || len(conn.execs) != 1 {
		t.Fatalf("outcome %+v execs %d, want 3 rows retracted in one mutation", outcome, len(conn.execs))
	}
	keys := strings.Join(namedStrings(t, conn.execArgs[0][1]), ",")
	for _, want := range []string{"ghpr:full-chaos/deploy#120|121", "ghpr:full-chaos/deploy#122|122", "gitlab:full-chaos/deploy!7|8"} {
		if !strings.Contains(keys, want) {
			t.Fatalf("mutation keys %s lack %s", keys, want)
		}
	}
	for _, kept := range []string{"jira:API-1", "gh:full-chaos/deploy#119"} {
		if strings.Contains(keys, kept) {
			t.Fatalf("mutation keys %s name the issue link %s", keys, kept)
		}
	}
}

// TestEdgePlanRetiresFallbackEdgesWhoseIssueSideIsAPullRequest: no native link
// anywhere; the text and heuristic edges that point at a pull request work
// item are retired, the edge that points at an issue is kept.
func TestEdgePlanRetiresFallbackEdgesWhoseIssueSideIsAPullRequest(t *testing.T) {
	rows := []edges.Row{
		prIssueEdge(fallbackRepo, 121, edges.EdgeTypeReferences, "ghpr:full-chaos/deploy#120", "explicit_text"),
		prIssueEdge(fallbackRepo, 8, edges.EdgeTypeRelates, "gitlab:full-chaos/deploy!7", "heuristic"),
		prIssueEdge(fallbackRepo, 121, edges.EdgeTypeReferences, "gh:full-chaos/deploy#119", "explicit_text"),
	}
	stamp := time.Date(2026, 10, 15, 14, 0, 0, 0, time.UTC)
	got := retiredTargets(PlanOutrankedEdgeTombstones(versionsOf(stamp, nil, rows...), nil, stamp.Add(time.Minute)))
	want := []string{"#pr121 references ghpr:full-chaos/deploy#120", "#pr8 relates gitlab:full-chaos/deploy!7"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("retired:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}
