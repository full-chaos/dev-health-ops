package teamattribution

import "testing"

// The evidence of an issue's repo_ownership row from a link reads back as the
// same link, field by field; other evidence does not read as a link.
func TestLinkedPullRequestRepoEvidenceRoundTrip(t *testing.T) {
	link := GithubWorkItemDerivationLinkedPullRequest{RepoID: linkedRepoA, PRNumber: 42, Tier: "explicit_text"}
	evidence := LinkedPullRequestRepoEvidence(link)
	if want := "repo_ownership=" + linkedRepoA + ";work_graph_issue_pr=42;tier=explicit_text"; evidence != want {
		t.Fatalf("evidence %q, want %q", evidence, want)
	}
	got, ok := ParseLinkedPullRequestRepoEvidence(evidence)
	if !ok || got != link {
		t.Fatalf("parse %q = %+v %v, want %+v", evidence, got, ok, link)
	}
	for _, other := range []string{
		"", "seeded", "repo_ownership=" + linkedRepoA,
		"repo_ownership=;work_graph_issue_pr=1;tier=native",
		"repo_ownership=" + linkedRepoA + ";work_graph_issue_pr=x;tier=native",
		"tier=native;work_graph_issue_pr=1;repo_ownership=" + linkedRepoA,
	} {
		if parsed, ok := ParseLinkedPullRequestRepoEvidence(other); ok {
			t.Errorf("parse %q = %+v, want no link", other, parsed)
		}
	}
}
