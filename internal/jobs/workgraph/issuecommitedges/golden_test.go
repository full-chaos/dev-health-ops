package issuecommitedges_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuecommitedges"
)

// TestDeriveMatchesFrozenPythonGolden is CHAOS-5304's identity test: it
// replays the EXACT engineered corpus captured from the real Python producer
// (tests/fixtures/issue_commit_python_golden.json, via a throwaway,
// never-committed generator run against
// _build_issue_commit_edges_from_text_parsing's actual lookup-building and
// edge-derivation logic, using the REAL extract_jira_keys/
// extract_github_issue_refs/extract_gitlab_issue_refs and
// generate_edge_id/generate_commit_id) through this package's Derive, and
// asserts field-for-field identity against what Python actually produced.
//
// The corpus was deliberately engineered, not sampled from live data (no
// live-oracle replay for this producer -- team-lead's ruling, CHAOS-5264,
// applied identically here: wire-and-delete-Python-in-one-PR, no straddle),
// to cover every documented quirk this port must reproduce exactly:
//   - a Jira reference (always RefType.REFERENCES, never CLOSES) -> a
//     REFERENCES edge
//   - a GitHub CLOSES-style ref ("Closes #N") -> an IMPLEMENTS edge
//   - a GitLab CLOSES-style ref ("Fixes #N") -> an IMPLEMENTS edge
//   - an unresolvable ref (number matches no known work item on that repo)
//     -> no edge at all
//   - a single commit producing edges toward TWO DIFFERENT providers (github
//     AND jira) on the same multi-provider repo, proving no cross-provider
//     collision
//   - an empty message -> the commit is skipped entirely
//   - a missing commit hash -> the commit is skipped entirely
//   - a nil/absent author_when -> event_ts falls back to build time
func TestDeriveMatchesFrozenPythonGolden(t *testing.T) {
	golden := loadGolden(t)

	inputs := issuecommitedges.Inputs{
		Commits:   golden.commits(t),
		WorkItems: golden.workItems(t),
	}
	buildTime := frozenNow(t, golden.FrozenNow)
	result := issuecommitedges.Derive(inputs, buildTime)

	if result.JiraRefsFound != golden.Counts.JiraRefsFound {
		t.Errorf("JiraRefsFound = %d, want %d", result.JiraRefsFound, golden.Counts.JiraRefsFound)
	}
	if result.GitHubRefsFound != golden.Counts.GitHubRefsFound {
		t.Errorf("GitHubRefsFound = %d, want %d", result.GitHubRefsFound, golden.Counts.GitHubRefsFound)
	}
	if result.GitLabRefsFound != golden.Counts.GitLabRefsFound {
		t.Errorf("GitLabRefsFound = %d, want %d", result.GitLabRefsFound, golden.Counts.GitLabRefsFound)
	}

	wantEdges := golden.wantEdges(t)
	if len(result.Edges) != len(wantEdges) {
		t.Fatalf("Derive returned %d edges, golden has %d:\n got=%+v\nwant=%+v",
			len(result.Edges), len(wantEdges), result.Edges, wantEdges)
	}
	for index, got := range result.Edges {
		want := wantEdges[index]
		if got.EdgeID != want.EdgeID || got.SourceType != want.SourceType || got.SourceID != want.SourceID ||
			got.TargetType != want.TargetType || got.TargetID != want.TargetID || got.EdgeType != want.EdgeType ||
			got.Provenance != want.Provenance || got.Confidence != want.Confidence || got.Evidence != want.Evidence ||
			!got.DiscoveredAt.Equal(want.DiscoveredAt) || !got.LastSynced.Equal(want.LastSynced) ||
			!got.EventTs.Equal(want.EventTs) || !got.Day.Equal(want.Day) ||
			got.RepoID == nil || want.RepoID == nil || *got.RepoID != *want.RepoID ||
			got.Provider == nil || want.Provider == nil || *got.Provider != *want.Provider {
			t.Errorf("edge %d mismatch:\n got=%+v (repo=%v provider=%v)\nwant=%+v (repo=%v provider=%v)",
				index, got, derefUUID(got.RepoID), derefString(got.Provider),
				want, derefUUID(want.RepoID), derefString(want.Provider))
		}
	}
}

func derefUUID(p *uuid.UUID) any {
	if p == nil {
		return nil
	}
	return *p
}

func derefString(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}

// --- golden document decoding -----------------------------------------

type goldenDocument struct {
	OrgID     string `json:"org_id"`
	FrozenNow string `json:"frozen_now"`
	Counts    struct {
		EdgesWritten    int `json:"edges_written"`
		JiraRefsFound   int `json:"jira_refs_found"`
		GitHubRefsFound int `json:"github_refs_found"`
		GitLabRefsFound int `json:"gitlab_refs_found"`
	} `json:"counts"`
	WorkItems []struct {
		RepoID     string `json:"repo_id"`
		WorkItemID string `json:"work_item_id"`
		Provider   string `json:"provider"`
	} `json:"work_items"`
	Commits []struct {
		RepoID     string  `json:"repo_id"`
		Hash       string  `json:"hash"`
		Message    string  `json:"message"`
		AuthorWhen *string `json:"author_when"`
	} `json:"commits"`
	EdgeRows []struct {
		EdgeID       string  `json:"edge_id"`
		SourceType   string  `json:"source_type"`
		SourceID     string  `json:"source_id"`
		TargetType   string  `json:"target_type"`
		TargetID     string  `json:"target_id"`
		EdgeType     string  `json:"edge_type"`
		RepoID       string  `json:"repo_id"`
		Provider     string  `json:"provider"`
		Provenance   string  `json:"provenance"`
		Confidence   float32 `json:"confidence"`
		Evidence     string  `json:"evidence"`
		DiscoveredAt string  `json:"discovered_at"`
		LastSynced   string  `json:"last_synced"`
		EventTs      string  `json:"event_ts"`
		Day          string  `json:"day"`
	} `json:"edge_rows"`
}

func loadGolden(t *testing.T) goldenDocument {
	t.Helper()
	path := filepath.Join(repositoryRoot(t), "tests", "fixtures", "issue_commit_python_golden.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document goldenDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return document
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// this file lives at internal/jobs/workgraph/issuecommitedges/golden_test.go
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
}

func mustParseTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse time %q: %v", value, err)
	}
	return parsed.UTC()
}

func mustParseDay(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		t.Fatalf("parse day %q: %v", value, err)
	}
	return parsed
}

func frozenNow(t *testing.T, value string) time.Time {
	t.Helper()
	return mustParseTime(t, value)
}

func (g goldenDocument) workItems(t *testing.T) []issuecommitedges.WorkItemRow {
	t.Helper()
	out := make([]issuecommitedges.WorkItemRow, 0, len(g.WorkItems))
	for _, row := range g.WorkItems {
		out = append(out, issuecommitedges.WorkItemRow{
			RepoID:     uuid.MustParse(row.RepoID),
			WorkItemID: row.WorkItemID,
			Provider:   row.Provider,
		})
	}
	return out
}

func (g goldenDocument) commits(t *testing.T) []issuecommitedges.CommitRow {
	t.Helper()
	out := make([]issuecommitedges.CommitRow, 0, len(g.Commits))
	for _, row := range g.Commits {
		var repoID uuid.UUID
		// A missing-hash commit is deliberately given a valid repo_id in the
		// fixture (it must reach Derive's own `commit.Hash == ""` guard, not
		// fail earlier on a bad UUID), so this always parses.
		repoID = uuid.MustParse(row.RepoID)
		var authorWhen time.Time
		if row.AuthorWhen != nil {
			authorWhen = mustParseTime(t, *row.AuthorWhen)
		}
		out = append(out, issuecommitedges.CommitRow{
			RepoID:     repoID,
			Hash:       row.Hash,
			Message:    row.Message,
			AuthorWhen: authorWhen,
		})
	}
	return out
}

func (g goldenDocument) wantEdges(t *testing.T) []wantEdge {
	t.Helper()
	out := make([]wantEdge, 0, len(g.EdgeRows))
	for _, row := range g.EdgeRows {
		repoID := uuid.MustParse(row.RepoID)
		provider := row.Provider
		out = append(out, wantEdge{
			EdgeID:       row.EdgeID,
			SourceType:   row.SourceType,
			SourceID:     row.SourceID,
			TargetType:   row.TargetType,
			TargetID:     row.TargetID,
			EdgeType:     row.EdgeType,
			Provenance:   row.Provenance,
			Confidence:   row.Confidence,
			Evidence:     row.Evidence,
			DiscoveredAt: mustParseTime(t, row.DiscoveredAt),
			LastSynced:   mustParseTime(t, row.LastSynced),
			EventTs:      mustParseTime(t, row.EventTs),
			Day:          mustParseDay(t, row.Day),
			RepoID:       &repoID,
			Provider:     &provider,
		})
	}
	return out
}

// wantEdge mirrors edges.Row's comparable fields (OrgID is stamped by the
// caller/writer, not carried on the row -- see edges.WriteEdges's own doc
// comment -- so it is not compared here).
type wantEdge struct {
	EdgeID       string
	SourceType   string
	SourceID     string
	TargetType   string
	TargetID     string
	EdgeType     string
	Provenance   string
	Confidence   float32
	Evidence     string
	DiscoveredAt time.Time
	LastSynced   time.Time
	EventTs      time.Time
	Day          time.Time
	RepoID       *uuid.UUID
	Provider     *string
}
