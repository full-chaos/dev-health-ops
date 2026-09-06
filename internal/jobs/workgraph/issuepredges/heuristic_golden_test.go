package issuepredges_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuepredges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

// TestDeriveHeuristicEdgesMatchesFrozenPythonGolden is CHAOS-4924's identity
// test for the heuristic issue<->PR builder: it replays the EXACT seeded
// corpus captured from the deployed Python producer
// (tests/fixtures/heuristic_issue_pr_edges_python_golden.json, via a
// throwaway, never-committed generator run directly against the ORIGINAL
// (pre-deletion) _build_heuristic_issue_pr_edges, loaded from git history
// since this PR deletes the method from the live builder.py) through
// DeriveHeuristicEdges, and asserts field-for-field identity against what
// Python actually wrote.
//
// The corpus is deliberately engineered with ONE isolated repo per case
// (a first draft shared one repo across cases and cross-contaminated: a
// decoy PR seeded for one work item fell inside a DIFFERENT work item's
// window purely because they shared a repo's PR pool -- the generator's
// own comment on REPO_NEAREST/REPO_EXCLUDED/etc. records this), covering:
//   - nearest-PR-within-window selection, not closest-by-number
//   - item-level explicit-link exclusion (excluded even against a
//     pr_number the item has NO link to)
//   - a PR strictly outside the window (no match)
//   - a PR EXACTLY at the window boundary (inclusive bound, matches)
//   - a repo with zero PRs at all (skipped via repo lookup miss)
func TestDeriveHeuristicEdgesMatchesFrozenPythonGolden(t *testing.T) {
	golden := loadHeuristicGolden(t)

	inputs := issuepredges.HeuristicInputs{
		WorkItems:           golden.workItems(t),
		PullRequests:        golden.pullRequests(t),
		ExplicitLinks:       golden.explicitLinks(),
		HeuristicDaysWindow: golden.HeuristicDaysWindow,
		HeuristicConfidence: float32(golden.HeuristicConfidence),
	}
	result := issuepredges.DeriveHeuristicEdges(inputs, golden.frozenNow(t))

	wantEdges := golden.wantEdges(t)
	assertEdgesMatch(t, "heuristic", result.Edges, wantEdges)

	wantLinks := golden.wantLinks(t)
	if len(result.Links) != len(wantLinks) {
		t.Fatalf("heuristic returned %d links, golden has %d:\n got=%+v\nwant=%+v",
			len(result.Links), len(wantLinks), result.Links, wantLinks)
	}
	for index, got := range result.Links {
		want := wantLinks[index]
		if got.RepoID != want.RepoID || got.WorkItemID != want.WorkItemID || got.PRNumber != want.PRNumber ||
			got.Confidence != want.Confidence || got.Provenance != want.Provenance || got.Evidence != want.Evidence {
			t.Errorf("link %d mismatch:\n got=%+v\nwant=%+v", index, got, want)
		}
	}
}

type heuristicGoldenWorkItem struct {
	RepoID    string `json:"repo_id"`
	WorkItem  string `json:"work_item_id"`
	UpdatedAt string `json:"updated_at"`
}

type heuristicGoldenPullRequest struct {
	RepoID    string `json:"repo_id"`
	Number    int    `json:"number"`
	CreatedAt string `json:"created_at"`
}

type heuristicGoldenLink struct {
	RepoID     string  `json:"repo_id"`
	WorkItemID string  `json:"work_item_id"`
	PRNumber   int     `json:"pr_number"`
	Confidence float64 `json:"confidence"`
	Provenance string  `json:"provenance"`
	Evidence   string  `json:"evidence"`
	LastSynced string  `json:"last_synced"`
}

type heuristicGoldenDocument struct {
	OrgID               string                       `json:"org_id"`
	FrozenNow           string                       `json:"frozen_now"`
	HeuristicDaysWindow int                          `json:"heuristic_days_window"`
	HeuristicConfidence float64                      `json:"heuristic_confidence"`
	WorkItems           []heuristicGoldenWorkItem    `json:"work_items"`
	PullRequests        []heuristicGoldenPullRequest `json:"pull_requests"`
	ExplicitLinksRaw    [][]json.RawMessage          `json:"explicit_links"`
	Edges               []goldenEdgeRecord           `json:"edges"`
	LinksWritten        []heuristicGoldenLink        `json:"links_written"`
}

func loadHeuristicGolden(t *testing.T) *heuristicGoldenDocument {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "tests", "fixtures", "heuristic_issue_pr_edges_python_golden.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden fixture: %v", err)
	}
	var doc heuristicGoldenDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal golden fixture: %v", err)
	}
	return &doc
}

func (g *heuristicGoldenDocument) frozenNow(t *testing.T) time.Time {
	t.Helper()
	return mustParseTime(t, g.FrozenNow)
}

func (g *heuristicGoldenDocument) workItems(t *testing.T) []issuepredges.HeuristicWorkItemRow {
	t.Helper()
	out := make([]issuepredges.HeuristicWorkItemRow, 0, len(g.WorkItems))
	for _, row := range g.WorkItems {
		out = append(out, issuepredges.HeuristicWorkItemRow{
			RepoID:     uuid.MustParse(row.RepoID),
			WorkItemID: row.WorkItem,
			UpdatedAt:  mustParseTime(t, row.UpdatedAt),
		})
	}
	return out
}

func (g *heuristicGoldenDocument) pullRequests(t *testing.T) []issuepredges.HeuristicPullRequestRow {
	t.Helper()
	out := make([]issuepredges.HeuristicPullRequestRow, 0, len(g.PullRequests))
	for _, row := range g.PullRequests {
		out = append(out, issuepredges.HeuristicPullRequestRow{
			RepoID:    uuid.MustParse(row.RepoID),
			Number:    row.Number,
			CreatedAt: mustParseTime(t, row.CreatedAt),
		})
	}
	return out
}

func (g *heuristicGoldenDocument) explicitLinks() []issuepredges.ExplicitLink {
	out := make([]issuepredges.ExplicitLink, 0, len(g.ExplicitLinksRaw))
	for _, pair := range g.ExplicitLinksRaw {
		if len(pair) != 2 {
			continue
		}
		var workItemID string
		var prNumber int
		_ = json.Unmarshal(pair[0], &workItemID)
		_ = json.Unmarshal(pair[1], &prNumber)
		out = append(out, issuepredges.ExplicitLink{WorkItemID: workItemID, PRNumber: prNumber})
	}
	return out
}

func (g *heuristicGoldenDocument) wantEdges(t *testing.T) []edges.Row {
	t.Helper()
	return g.wantEdgesFrom(t, g.Edges)
}

// wantEdgesFrom reuses the exact decoding golden_test.go's own goldenDocument
// uses (goldenEdgeRecord, mustParseTime, mustParseDay) -- same package,
// same shape, no reason to duplicate it.
func (g *heuristicGoldenDocument) wantEdgesFrom(t *testing.T, records []goldenEdgeRecord) []edges.Row {
	t.Helper()
	out := make([]edges.Row, 0, len(records))
	for _, record := range records {
		row := edges.Row{
			EdgeID:       record.EdgeID,
			SourceType:   record.SourceType,
			SourceID:     record.SourceID,
			TargetType:   record.TargetType,
			TargetID:     record.TargetID,
			EdgeType:     record.EdgeType,
			Provenance:   record.Provenance,
			Confidence:   float32(record.Confidence),
			Evidence:     record.Evidence,
			DiscoveredAt: mustParseTime(t, record.DiscoveredAt),
			LastSynced:   mustParseTime(t, record.LastSynced),
			EventTs:      mustParseTime(t, record.EventTs),
			Day:          mustParseDay(t, record.Day),
		}
		if record.RepoID != nil {
			id := uuid.MustParse(*record.RepoID)
			row.RepoID = &id
		}
		if record.Provider != nil {
			provider := *record.Provider
			row.Provider = &provider
		}
		out = append(out, row)
	}
	return out
}

func (g *heuristicGoldenDocument) wantLinks(t *testing.T) []issueprlinks.Link {
	t.Helper()
	out := make([]issueprlinks.Link, 0, len(g.LinksWritten))
	for _, record := range g.LinksWritten {
		out = append(out, issueprlinks.Link{
			RepoID:     uuid.MustParse(record.RepoID),
			WorkItemID: record.WorkItemID,
			PRNumber:   uint32(record.PRNumber),
			Confidence: float32(record.Confidence),
			Provenance: record.Provenance,
			Evidence:   record.Evidence,
		})
	}
	return out
}
