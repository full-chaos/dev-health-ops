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

// TestDeriveFastPathAndTextParseEdgesMatchFrozenPythonGolden is CHAOS-4924's
// identity test for the issue<->PR edge sub-builders: it replays the EXACT
// seeded corpus captured from the deployed Python producer
// (tests/fixtures/issue_pr_edges_python_golden.json, via a throwaway,
// never-committed generator run against _build_issue_pr_edges_from_fast_path
// + _build_issue_pr_edges) through this package's DeriveFastPathEdges and
// DeriveTextParseEdges, and asserts field-for-field identity against what
// Python actually wrote.
//
// The corpus was deliberately engineered, not sampled from live data (same
// team-lead ruling as CHAOS-5264/CHAOS-4924's other sub-builders:
// wire-and-delete-Python-in-one-PR, no straddle, no provenance-ranked version
// column to make a live snapshot meaningful), to cover every documented
// quirk this port must reproduce exactly:
//   - fast-path confidence=0.0 promoted to 1.0 (builder.py's `or 1.0`)
//   - fast-path empty evidence promoted to "issue_pr_fast_path"
//   - fast-path unrecognized provenance defaulting to native
//   - fast-path missing/null created_at falling back to build time
//   - Jira references always REFERENCES (never IMPLEMENTS, regardless of
//     surrounding text) -- extract_jira_keys' own hardcoded ref_type
//   - GitHub closing keyword ("Closes #N") producing IMPLEMENTS
//   - GitLab plain reference ("#N") producing REFERENCES
//   - a PR with no title/body/head_branch skipped entirely
//   - a Linear work item whose issue-key-shaped text is found by extraction
//     but never resolves (Linear links arrive as native attachments, not
//     text-parse)
func TestDeriveFastPathAndTextParseEdgesMatchFrozenPythonGolden(t *testing.T) {
	golden := loadGolden(t)

	fastPathRows := golden.fastPathRows(t)
	fastPathEdges := issuepredges.DeriveFastPathEdges(fastPathRows, golden.frozenNow(t))
	assertEdgesMatch(t, "fast-path", fastPathEdges, golden.wantEdges(t, golden.FastPathEdges))

	prs := golden.pullRequests(t)
	workItems := golden.workItems(t)
	result := issuepredges.DeriveTextParseEdges(prs, workItems, golden.frozenNow(t))
	assertEdgesMatch(t, "text-parse", result.Edges, golden.wantEdges(t, golden.TextParseEdges))

	wantLinks := golden.wantLinks(t)
	if len(result.Links) != len(wantLinks) {
		t.Fatalf("text-parse returned %d links, golden has %d:\n got=%+v\nwant=%+v",
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

func assertEdgesMatch(t *testing.T, label string, got []edges.Row, want []edges.Row) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d edges, golden has %d:\n got=%+v\nwant=%+v", label, len(got), len(want), got, want)
	}
	for index := range got {
		g, w := got[index], want[index]
		if g.EdgeID != w.EdgeID || g.SourceType != w.SourceType || g.SourceID != w.SourceID ||
			g.TargetType != w.TargetType || g.TargetID != w.TargetID || g.EdgeType != w.EdgeType ||
			g.Provenance != w.Provenance || g.Confidence != w.Confidence || g.Evidence != w.Evidence ||
			!g.DiscoveredAt.Equal(w.DiscoveredAt) || !g.LastSynced.Equal(w.LastSynced) ||
			!g.EventTs.Equal(w.EventTs) || !g.Day.Equal(w.Day) ||
			(g.RepoID == nil) != (w.RepoID == nil) || (g.RepoID != nil && *g.RepoID != *w.RepoID) ||
			(g.Provider == nil) != (w.Provider == nil) || (g.Provider != nil && *g.Provider != *w.Provider) {
			t.Errorf("%s edge %d mismatch:\n got=%+v\nwant=%+v", label, index, g, w)
		}
	}
}

// --- golden document decoding -------------------------------------------

type goldenEdgeRecord struct {
	EdgeID       string  `json:"edge_id"`
	SourceType   string  `json:"source_type"`
	SourceID     string  `json:"source_id"`
	TargetType   string  `json:"target_type"`
	TargetID     string  `json:"target_id"`
	EdgeType     string  `json:"edge_type"`
	Provenance   string  `json:"provenance"`
	Confidence   float64 `json:"confidence"`
	Evidence     string  `json:"evidence"`
	RepoID       *string `json:"repo_id"`
	Provider     *string `json:"provider"`
	DiscoveredAt string  `json:"discovered_at"`
	LastSynced   string  `json:"last_synced"`
	EventTs      string  `json:"event_ts"`
	Day          string  `json:"day"`
}

type goldenFastPathInputRow struct {
	RepoID      string  `json:"repo_id"`
	WorkItemID  string  `json:"work_item_id"`
	PRNumber    int     `json:"pr_number"`
	Confidence  float64 `json:"confidence"`
	Provenance  string  `json:"provenance"`
	Evidence    string  `json:"evidence"`
	PRCreatedAt *string `json:"pr_created_at"`
}

type goldenPullRequest struct {
	RepoID     string  `json:"repo_id"`
	Number     int     `json:"number"`
	Title      *string `json:"title"`
	Body       *string `json:"body"`
	HeadBranch *string `json:"head_branch"`
	CreatedAt  string  `json:"created_at"`
}

type goldenWorkItem struct {
	RepoID     string `json:"repo_id"`
	WorkItemID string `json:"work_item_id"`
	Provider   string `json:"provider"`
}

type goldenLink struct {
	RepoID     string  `json:"repo_id"`
	WorkItemID string  `json:"work_item_id"`
	PRNumber   int     `json:"pr_number"`
	Confidence float64 `json:"confidence"`
	Provenance string  `json:"provenance"`
	Evidence   string  `json:"evidence"`
	LastSynced string  `json:"last_synced"`
}

type goldenDocument struct {
	OrgID                 string                   `json:"org_id"`
	FrozenNow             string                   `json:"frozen_now"`
	FastPathInputRows     []goldenFastPathInputRow `json:"fast_path_input_rows"`
	FastPathEdges         []goldenEdgeRecord       `json:"fast_path_edges"`
	TextParsePullRequests []goldenPullRequest      `json:"text_parse_pull_requests"`
	TextParseWorkItems    []goldenWorkItem         `json:"text_parse_work_items"`
	TextParseEdges        []goldenEdgeRecord       `json:"text_parse_edges"`
	TextParseLinksWritten []goldenLink             `json:"text_parse_links_written"`
}

func loadGolden(t *testing.T) *goldenDocument {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "tests", "fixtures", "issue_pr_edges_python_golden.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden fixture: %v", err)
	}
	var doc goldenDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal golden fixture: %v", err)
	}
	return &doc
}

func (g *goldenDocument) frozenNow(t *testing.T) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, g.FrozenNow)
	if err != nil {
		t.Fatalf("parse frozen_now: %v", err)
	}
	return parsed
}

func mustParseTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse time %q: %v", value, err)
	}
	return parsed
}

func mustParseDay(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		t.Fatalf("parse day %q: %v", value, err)
	}
	return parsed
}

func (g *goldenDocument) fastPathRows(t *testing.T) []issuepredges.FastPathRow {
	t.Helper()
	out := make([]issuepredges.FastPathRow, 0, len(g.FastPathInputRows))
	for _, row := range g.FastPathInputRows {
		var createdAt time.Time
		if row.PRCreatedAt != nil {
			createdAt = mustParseTime(t, *row.PRCreatedAt)
		}
		out = append(out, issuepredges.FastPathRow{
			RepoID:      uuid.MustParse(row.RepoID),
			WorkItemID:  row.WorkItemID,
			PRNumber:    row.PRNumber,
			Confidence:  float32(row.Confidence),
			Provenance:  row.Provenance,
			Evidence:    row.Evidence,
			PRCreatedAt: createdAt,
		})
	}
	return out
}

func (g *goldenDocument) pullRequests(t *testing.T) []issuepredges.PullRequestRow {
	t.Helper()
	out := make([]issuepredges.PullRequestRow, 0, len(g.TextParsePullRequests))
	for _, row := range g.TextParsePullRequests {
		pr := issuepredges.PullRequestRow{
			RepoID:    uuid.MustParse(row.RepoID),
			Number:    row.Number,
			CreatedAt: mustParseTime(t, row.CreatedAt),
		}
		if row.Title != nil {
			pr.Title = *row.Title
		}
		if row.Body != nil {
			pr.Body = *row.Body
		}
		if row.HeadBranch != nil {
			pr.HeadBranch = *row.HeadBranch
		}
		out = append(out, pr)
	}
	return out
}

func (g *goldenDocument) workItems(t *testing.T) []issuepredges.WorkItemRow {
	t.Helper()
	out := make([]issuepredges.WorkItemRow, 0, len(g.TextParseWorkItems))
	for _, row := range g.TextParseWorkItems {
		out = append(out, issuepredges.WorkItemRow{
			RepoID:     uuid.MustParse(row.RepoID),
			WorkItemID: row.WorkItemID,
			Provider:   row.Provider,
		})
	}
	return out
}

func (g *goldenDocument) wantEdges(t *testing.T, records []goldenEdgeRecord) []edges.Row {
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

// wantLinks decodes the golden's expected links, leaving OrgID and LastSynced
// zero -- both are stamped by Service.ProduceTextParseEdges AFTER Derive
// returns (see issuepredges.Service.ProduceTextParseEdges), same reasoning
// as prcommit's golden test leaving LastSynced zero before comparison, so
// this asserts only the fields DeriveTextParseEdges itself sets.
func (g *goldenDocument) wantLinks(t *testing.T) []issueprlinks.Link {
	t.Helper()
	out := make([]issueprlinks.Link, 0, len(g.TextParseLinksWritten))
	for _, record := range g.TextParseLinksWritten {
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
