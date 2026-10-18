package issuepredges

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

func TestDeriveFastPathEdgesBuildsAnImplementsEdge(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	buildTime := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	prCreated := time.Date(2026, 7, 15, 9, 0, 0, 0, time.UTC)

	rows := []FastPathRow{{
		RepoID:      repoID,
		WorkItemID:  "jira:ABC-123",
		PRNumber:    42,
		Confidence:  1.0,
		Provenance:  "native",
		Evidence:    "closing_reference",
		PRCreatedAt: prCreated,
	}}

	out := DeriveFastPathEdges(rows, buildTime)
	if len(out) != 1 {
		t.Fatalf("got %d edges, want 1", len(out))
	}
	edge := out[0]
	wantPRID := edges.GeneratePRID(repoID, 42)
	if edge.SourceID != wantPRID {
		t.Errorf("SourceID = %q, want %q", edge.SourceID, wantPRID)
	}
	if edge.TargetID != "jira:ABC-123" {
		t.Errorf("TargetID = %q, want jira:ABC-123", edge.TargetID)
	}
	if edge.EdgeType != edges.EdgeTypeImplements {
		t.Errorf("EdgeType = %q, want %q", edge.EdgeType, edges.EdgeTypeImplements)
	}
	if edge.Provenance != "native" {
		t.Errorf("Provenance = %q, want native", edge.Provenance)
	}
	if !edge.EventTs.Equal(prCreated) {
		t.Errorf("EventTs = %v, want %v", edge.EventTs, prCreated)
	}
	if !edge.DiscoveredAt.Equal(buildTime) || !edge.LastSynced.Equal(buildTime) {
		t.Errorf("DiscoveredAt/LastSynced should both be buildTime, got %v/%v", edge.DiscoveredAt, edge.LastSynced)
	}
}

// TestDeriveFastPathEdgesConfidenceZeroQuirk pins Python's
// `float(row.get("confidence") or 1.0)` falsy-or behavior: a genuinely
// zero-confidence row is silently promoted to 1.0, not written as 0.0.
func TestDeriveFastPathEdgesConfidenceZeroQuirk(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	buildTime := time.Now().UTC()

	rows := []FastPathRow{{
		RepoID:     repoID,
		WorkItemID: "jira:ABC-1",
		PRNumber:   1,
		Confidence: 0,
		Provenance: "native",
		Evidence:   "",
	}}

	out := DeriveFastPathEdges(rows, buildTime)
	if len(out) != 1 {
		t.Fatalf("got %d edges, want 1", len(out))
	}
	if out[0].Confidence != 1.0 {
		t.Errorf("Confidence = %v, want 1.0 (falsy-or promotion)", out[0].Confidence)
	}
	if out[0].Evidence != "issue_pr_fast_path" {
		t.Errorf("Evidence = %q, want issue_pr_fast_path (falsy-or promotion)", out[0].Evidence)
	}
}

// TestDeriveFastPathEdgesEventTsFallsBackToBuildTime pins the zero-PRCreatedAt
// fallback (builder.py's `if not event_ts: event_ts = self._now`).
func TestDeriveFastPathEdgesEventTsFallsBackToBuildTime(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	buildTime := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)

	rows := []FastPathRow{{RepoID: repoID, WorkItemID: "jira:ABC-1", PRNumber: 1, Provenance: "native"}}
	out := DeriveFastPathEdges(rows, buildTime)
	if !out[0].EventTs.Equal(buildTime) {
		t.Errorf("EventTs = %v, want buildTime %v", out[0].EventTs, buildTime)
	}
}

func TestDeriveTextParseEdgesMatchesJiraReferenceAgainstLookup(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000002")
	buildTime := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	prCreated := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)

	prs := []PullRequestRow{{
		RepoID:    repoID,
		Number:    7,
		Title:     "Fix ABC-123 crash",
		CreatedAt: prCreated,
	}}
	workItems := []WorkItemRow{{RepoID: repoID, WorkItemID: "jira:ABC-123", Provider: "jira"}}

	result := DeriveTextParseEdges(prs, workItems, issueprlinks.NativeLinks{}, buildTime)
	if result.JiraRefsFound != 1 {
		t.Fatalf("JiraRefsFound = %d, want 1", result.JiraRefsFound)
	}
	if len(result.Edges) != 1 {
		t.Fatalf("got %d edges, want 1", len(result.Edges))
	}
	edge := result.Edges[0]
	if edge.TargetID != "jira:ABC-123" {
		t.Errorf("TargetID = %q, want jira:ABC-123", edge.TargetID)
	}
	if edge.Provenance != edges.ProvenanceExplicitText {
		t.Errorf("Provenance = %q, want %q", edge.Provenance, edges.ProvenanceExplicitText)
	}
	if edge.Confidence != 0.9 {
		t.Errorf("Confidence = %v, want 0.9", edge.Confidence)
	}
	if edge.Provider == nil || *edge.Provider != "jira" {
		t.Errorf("Provider = %v, want jira", edge.Provider)
	}
	if len(result.Links) != 1 {
		t.Fatalf("got %d links, want 1", len(result.Links))
	}
	if result.Links[0].WorkItemID != "jira:ABC-123" || result.Links[0].PRNumber != 7 {
		t.Errorf("Link = %+v, unexpected", result.Links[0])
	}
}

func TestDeriveTextParseEdgesGitHubCloseKeywordImplementsEdge(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000003")
	buildTime := time.Now().UTC()

	prs := []PullRequestRow{{
		RepoID: repoID,
		Number: 10,
		Body:   "Closes #55",
	}}
	workItems := []WorkItemRow{{RepoID: repoID, WorkItemID: "gh:owner/repo#55", Provider: "github"}}

	result := DeriveTextParseEdges(prs, workItems, issueprlinks.NativeLinks{}, buildTime)
	if len(result.Edges) != 1 {
		t.Fatalf("got %d edges, want 1", len(result.Edges))
	}
	if result.Edges[0].EdgeType != edges.EdgeTypeImplements {
		t.Errorf("EdgeType = %q, want %q for a closing keyword", result.Edges[0].EdgeType, edges.EdgeTypeImplements)
	}
}

func TestDeriveTextParseEdgesSkipsPRWithNoText(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000004")
	prs := []PullRequestRow{{RepoID: repoID, Number: 1}}
	workItems := []WorkItemRow{{RepoID: repoID, WorkItemID: "jira:ABC-1", Provider: "jira"}}

	result := DeriveTextParseEdges(prs, workItems, issueprlinks.NativeLinks{}, time.Now())
	if len(result.Edges) != 0 || len(result.Links) != 0 {
		t.Fatalf("a PR with no title/body/head_branch must be skipped, got edges=%d links=%d", len(result.Edges), len(result.Links))
	}
}

// TestExplicitTextTierProviderMatrix is the CHAOS-9457 provider x entity
// matrix for the explicit_text tier: for each provider family, a PR whose text
// names an issue row of that provider gets one explicit_text link; a reference
// that matches no issue row gets none.
func TestExplicitTextTierProviderMatrix(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000005")
	cases := []struct {
		name      string
		title     string
		items     []WorkItemRow
		wantItem  string // "" = no link
		wantEdge  string
		wantAmbig int
	}{
		{"jira row named", "Fix PAY-2 totals", []WorkItemRow{{repoID, "jira:PAY-2", "jira", ""}}, "jira:PAY-2", "references", 0},
		{"jira key no row", "Fix PAY-2 totals", []WorkItemRow{{repoID, "jira:PAY-9", "jira", ""}}, "", "", 0},
		{"linear row named", "MOB-1 checkout layout", []WorkItemRow{{repoID, "linear:MOB-1", "linear", ""}}, "linear:MOB-1", "references", 0},
		{"linear key no row", "MOB-1 checkout layout", []WorkItemRow{{repoID, "linear:MOB-2", "linear", ""}}, "", "", 0},
		{"jira-looking prefix, org has no rows of it", "ABC-77 thing", nil, "", "", 0},
		{"same key in jira and linear is ambiguous", "ABC-1 thing", []WorkItemRow{{repoID, "jira:ABC-1", "jira", ""}, {repoID, "linear:ABC-1", "linear", ""}}, "", "", 1},
		{"github same-repo #n", "Closes #5", []WorkItemRow{{repoID, "gh:o/r#5", "github", ""}}, "gh:o/r#5", "implements", 0},
		{"github #n no row", "Closes #6", []WorkItemRow{{repoID, "gh:o/r#5", "github", ""}}, "", "", 0},
		{"github owner/repo#n", "Relates to other/lib#12", []WorkItemRow{{uuid.New(), "gh:other/lib#12", "github", ""}}, "gh:other/lib#12", "references", 0},
		{"github owner/repo#n no row", "Relates to other/lib#13", []WorkItemRow{{uuid.New(), "gh:other/lib#12", "github", ""}}, "", "", 0},
		{"gitlab same-repo #n", "see #7", []WorkItemRow{{repoID, "gitlab:g/p#7", "gitlab", ""}}, "gitlab:g/p#7", "references", 0},
		{"gitlab group/project#n", "see grp/sub/proj#8", []WorkItemRow{{uuid.New(), "gitlab:grp/sub/proj#8", "gitlab", ""}}, "gitlab:grp/sub/proj#8", "references", 0},
		{"github repo name is case-insensitive", "Closes acme/api#5", []WorkItemRow{{uuid.New(), "gh:Acme/API#5", "github", ""}}, "gh:Acme/API#5", "implements", 0},
		{"github glued suffix is no reference", "see o/r#5abc", []WorkItemRow{{uuid.New(), "gh:o/r#5", "github", ""}}, "", "", 0},
		{"unicode letter before owner is no reference", "see \u00e9o/r#5", []WorkItemRow{{uuid.New(), "gh:o/r#5", "github", ""}}, "", "", 0},
		{"owner/repo#n held by github and gitlab is ambiguous", "see o/r#5", []WorkItemRow{{uuid.New(), "gh:o/r#5", "github", ""}, {uuid.New(), "gitlab:o/r#5", "gitlab", ""}}, "", "", 1},
		{"gitlab !n is a merge request", "see g/p!9", []WorkItemRow{{repoID, "gitlab:g/p#9", "gitlab", ""}}, "", "", 0},
		// CHAOS-3383: a renamed project's items keep their minted path.
		{"gitlab renamed project by its current path", "see team/web#3", []WorkItemRow{{uuid.New(), "gitlab:team/old-name#3", "gitlab", "team/web"}}, "gitlab:team/old-name#3", "references", 0},
		{"gitlab renamed project by its minted path", "see team/old-name#3", []WorkItemRow{{uuid.New(), "gitlab:team/old-name#3", "gitlab", "team/web"}}, "gitlab:team/old-name#3", "references", 0},
		// CHAOS-9879: the same for a renamed GitHub repository.
		{"github renamed repository by its current name", "see platform/api-v2#3", []WorkItemRow{{uuid.New(), "gh:acme/api#3", "github", "platform/api-v2"}}, "gh:acme/api#3", "references", 0},
		{"github renamed repository by its minted name", "see acme/api#3", []WorkItemRow{{uuid.New(), "gh:acme/api#3", "github", "platform/api-v2"}}, "gh:acme/api#3", "references", 0},
		{"github current name names the repository there now", "see acme/api#4", []WorkItemRow{
			{uuid.New(), "gh:acme/api#4", "github", "platform/api-v2"},
			{uuid.New(), "gh:acme/api@456#4", "github", "acme/api"},
		}, "gh:acme/api@456#4", "references", 0},
		{"gitlab current path names the project there now", "see acme/api#4", []WorkItemRow{
			{uuid.New(), "gitlab:acme/api#4", "gitlab", "platform/api-v2"},
			{uuid.New(), "gitlab:acme/api@456#4", "gitlab", "acme/api"},
		}, "gitlab:acme/api@456#4", "references", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prs := []PullRequestRow{{RepoID: repoID, Number: 1, Title: tc.title}}
			result := DeriveTextParseEdges(prs, tc.items, issueprlinks.NativeLinks{}, time.Now())
			if tc.wantItem == "" {
				if len(result.Links) != 0 || len(result.Edges) != 0 {
					t.Fatalf("want no link, got links=%+v", result.Links)
				}
			} else {
				if len(result.Links) != 1 || result.Links[0].WorkItemID != tc.wantItem || result.Links[0].Provenance != edges.ProvenanceExplicitText {
					t.Fatalf("want one explicit_text link to %s, got %+v", tc.wantItem, result.Links)
				}
				if result.Edges[0].EdgeType != tc.wantEdge {
					t.Fatalf("edge type = %s, want %s", result.Edges[0].EdgeType, tc.wantEdge)
				}
			}
			if result.AmbiguousKeys != tc.wantAmbig {
				t.Fatalf("AmbiguousKeys = %d, want %d", result.AmbiguousKeys, tc.wantAmbig)
			}
		})
	}
}

// CHAOS-3383: a GitLab current-path key keeps the "gitlab:" prefix, so it never
// replaces a GitHub item of the same owner/repo, and it is counted when it
// replaces another GitLab project's item.
func TestGitLabCurrentPathKeysStayUnderTheGitLabPrefix(t *testing.T) {
	github := WorkItemRow{RepoID: uuid.New(), WorkItemID: "gh:org/api#4", Provider: "github"}
	renamed := WorkItemRow{RepoID: uuid.New(), WorkItemID: "gitlab:org/old#4", Provider: "gitlab", CurrentPath: "org/api"}
	lookup := buildProviderLookup([]WorkItemRow{github, renamed})
	if lookup.byID["gh:org/api#4"] != "gh:org/api#4" || lookup.byID["gitlab:org/api#4"] != "gitlab:org/old#4" {
		t.Fatalf("byID = %v", lookup.byID)
	}
	if lookup.renamedPathCollisions != 0 {
		t.Fatalf("collisions = %d, want 0 (two providers are two key spaces)", lookup.renamedPathCollisions)
	}
	taken := WorkItemRow{RepoID: uuid.New(), WorkItemID: "gitlab:acme/api#4", Provider: "gitlab", CurrentPath: "platform/api-v2"}
	taker := WorkItemRow{RepoID: uuid.New(), WorkItemID: "gitlab:acme/api@456#4", Provider: "gitlab", CurrentPath: "acme/api"}
	result := DeriveTextParseEdges([]PullRequestRow{{RepoID: uuid.New(), Number: 1, Title: "see acme/api#4"}},
		[]WorkItemRow{taken, taker}, issueprlinks.NativeLinks{}, time.Now())
	if result.RenamedPathCollisions != 1 || len(result.Links) != 1 || result.Links[0].WorkItemID != taker.WorkItemID {
		t.Fatalf("collisions=%d links=%+v, want 1 and one link to %s", result.RenamedPathCollisions, result.Links, taker.WorkItemID)
	}
}

func TestExtractRepoNamedIssueRefsRejectsEmbeddedAndMergeRequests(t *testing.T) {
	for _, text := range []string{"ghttps://x/o/r#1", "a/b!3", "https://host/o/r#4"} {
		if refs := ExtractRepoNamedIssueRefs(text); len(refs) != 0 {
			t.Errorf("%q gave %+v, want none", text, refs)
		}
	}
}
