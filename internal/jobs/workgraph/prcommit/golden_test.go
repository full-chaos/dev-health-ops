package prcommit_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/prcommit"
)

// TestDeriveAndBuildFastPathEdgesMatchFrozenPythonGolden is CHAOS-5264's
// identity test: it replays the EXACT seeded corpus captured from the
// deployed Python producer (tests/fixtures/pr_commit_python_golden.json, via
// a throwaway, never-committed generator run against
// _derive_pr_commit_links + _build_pr_commit_edges_from_fast_path) through
// this package's Derive and BuildFastPathEdges, and asserts field-for-field
// identity against what Python actually wrote.
//
// The corpus was deliberately engineered, not sampled from live data (there
// is no live-oracle replay for this producer -- team-lead's ruling, CHAOS-5264:
// wire-and-delete-Python-in-one-PR, no straddle), to cover every documented
// quirk this port must reproduce exactly:
//   - explicit merge-keyword tier (0.9/explicit_text)
//   - squash-subject tier requiring corroboration (0.6/heuristic), including
//     the UNCORROBORATED case that must NOT link
//   - explicit tier winning the dedup over squash for the same commit
//   - revert-of-merge-commit rejection
//   - a Unicode-digit PR reference (Arabic-Indic ٣٤)
//   - cross-tenant repo_id collision, excluded by the SCOPED read itself
//   - fast-path confidence=0.0 promoted to 1.0 (builder.py:1949's `or 1.0`)
//   - fast-path event_ts falling back to build time on a falsy author_when
//   - an unrecognized fast-path provenance defaulting to native
func TestDeriveAndBuildFastPathEdgesMatchFrozenPythonGolden(t *testing.T) {
	golden := loadGolden(t)

	inputs := prcommit.Inputs{
		OrgID:        golden.OrgID,
		PullRequests: golden.pullRequests(t),
		Commits:      golden.commits(t),
	}
	result := prcommit.Derive(inputs)

	wantLinks := golden.wantLinks(t)
	if len(result.Links) != len(wantLinks) {
		t.Fatalf("Derive returned %d links, golden has %d:\n got=%+v\nwant=%+v",
			len(result.Links), len(wantLinks), result.Links, wantLinks)
	}
	for index, got := range result.Links {
		want := wantLinks[index]
		// Derive leaves LastSynced zero by design (see prcommit.go) -- stamp
		// it as Service.ProduceLinks would before comparing the rest.
		if got.OrgID != want.OrgID || got.RepoID != want.RepoID || got.PRNumber != want.PRNumber ||
			got.CommitHash != want.CommitHash || got.Confidence != want.Confidence ||
			got.Provenance != want.Provenance || got.Evidence != want.Evidence {
			t.Errorf("link %d mismatch:\n got=%+v\nwant=%+v", index, got, want)
		}
	}

	// Feed BuildFastPathEdges the just-derived links (as the fast-path table
	// would now hold them, LastSynced stamped) PLUS the seeded synthetic rows
	// that exercise quirks _derive_pr_commit_links itself can never produce.
	fastPathRows := make([]prcommit.FastPathRow, 0, len(result.Links)+len(golden.ExtraFastpathSeedRows))
	for _, link := range result.Links {
		fastPathRows = append(fastPathRows, prcommit.FastPathRow{
			RepoID:     link.RepoID,
			PRNumber:   link.PRNumber,
			CommitHash: link.CommitHash,
			Confidence: link.Confidence,
			Provenance: link.Provenance,
			Evidence:   link.Evidence,
			AuthorWhen: golden.authorWhenForCommit(t, link.CommitHash),
		})
	}
	fastPathRows = append(fastPathRows, golden.extraFastPathRows(t)...)

	edges := prcommit.BuildFastPathEdges(golden.OrgID, fastPathRows, frozenNow(t, golden.FrozenNow))

	wantEdges := golden.wantEdges(t)
	if len(edges) != len(wantEdges) {
		t.Fatalf("BuildFastPathEdges returned %d edges, golden has %d:\n got=%+v\nwant=%+v",
			len(edges), len(wantEdges), edges, wantEdges)
	}
	for index, got := range edges {
		want := wantEdges[index]
		if got.EdgeID != want.EdgeID || got.SourceType != want.SourceType || got.SourceID != want.SourceID ||
			got.TargetType != want.TargetType || got.TargetID != want.TargetID || got.EdgeType != want.EdgeType ||
			got.Provenance != want.Provenance || got.Confidence != want.Confidence || got.Evidence != want.Evidence ||
			!got.DiscoveredAt.Equal(want.DiscoveredAt) || !got.LastSynced.Equal(want.LastSynced) ||
			!got.EventTs.Equal(want.EventTs) || !got.Day.Equal(want.Day) ||
			(got.RepoID == nil) != (want.RepoID == nil) || (got.RepoID != nil && *got.RepoID != *want.RepoID) {
			t.Errorf("edge %d mismatch:\n got=%+v\nwant=%+v", index, got, want)
		}
	}
}

// --- golden document decoding -------------------------------------------

type goldenDocument struct {
	FrozenNow             string             `json:"frozen_now"`
	OrgID                 string             `json:"org_id"`
	Inputs                goldenInputs       `json:"inputs"`
	PRCommitRows          []goldenPRCommit   `json:"pr_commit_rows"`
	EdgeRows              []goldenEdge       `json:"edge_rows"`
	ExtraFastpathSeedRows []goldenFastPathIn `json:"extra_fastpath_seed_rows"`
}

type goldenInputs struct {
	PullRequests []goldenPR     `json:"pull_requests"`
	Commits      []goldenCommit `json:"commits"`
}

type goldenPR struct {
	OrgID  string `json:"org_id"`
	RepoID string `json:"repo_id"`
	Number int    `json:"number"`
}

type goldenCommit struct {
	OrgID      string `json:"org_id"`
	RepoID     string `json:"repo_id"`
	Hash       string `json:"hash"`
	Message    string `json:"message"`
	AuthorWhen string `json:"author_when"`
}

type goldenPRCommit struct {
	RepoID     string  `json:"repo_id"`
	PRNumber   int     `json:"pr_number"`
	CommitHash string  `json:"commit_hash"`
	Confidence float32 `json:"confidence"`
	Provenance string  `json:"provenance"`
	Evidence   string  `json:"evidence"`
	LastSynced string  `json:"last_synced"`
	OrgID      string  `json:"org_id"`
}

type goldenEdge struct {
	EdgeID       string  `json:"edge_id"`
	SourceType   string  `json:"source_type"`
	SourceID     string  `json:"source_id"`
	TargetType   string  `json:"target_type"`
	TargetID     string  `json:"target_id"`
	EdgeType     string  `json:"edge_type"`
	Provenance   string  `json:"provenance"`
	Confidence   float32 `json:"confidence"`
	Evidence     string  `json:"evidence"`
	RepoID       *string `json:"repo_id"`
	DiscoveredAt string  `json:"discovered_at"`
	LastSynced   string  `json:"last_synced"`
	EventTs      string  `json:"event_ts"`
	Day          string  `json:"day"`
}

type goldenFastPathIn struct {
	RepoID     string  `json:"repo_id"`
	PRNumber   int     `json:"pr_number"`
	CommitHash string  `json:"commit_hash"`
	Confidence float32 `json:"confidence"`
	Provenance string  `json:"provenance"`
	Evidence   string  `json:"evidence"`
	AuthorWhen *string `json:"author_when"`
}

func loadGolden(t *testing.T) goldenDocument {
	t.Helper()
	path := filepath.Join(repositoryRootPath(t), "tests", "fixtures", "pr_commit_python_golden.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var doc goldenDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	return doc
}

func parseTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse time %q: %v", value, err)
	}
	return parsed
}

func frozenNow(t *testing.T, value string) time.Time { return parseTime(t, value) }

func (doc goldenDocument) pullRequests(t *testing.T) []prcommit.PullRequestRow {
	t.Helper()
	out := make([]prcommit.PullRequestRow, 0, len(doc.Inputs.PullRequests))
	for _, pr := range doc.Inputs.PullRequests {
		out = append(out, prcommit.PullRequestRow{
			OrgID: pr.OrgID, RepoID: uuid.MustParse(pr.RepoID), Number: pr.Number,
		})
	}
	return out
}

func (doc goldenDocument) commits(t *testing.T) []prcommit.CommitRow {
	t.Helper()
	out := make([]prcommit.CommitRow, 0, len(doc.Inputs.Commits))
	for _, c := range doc.Inputs.Commits {
		out = append(out, prcommit.CommitRow{
			OrgID: c.OrgID, RepoID: uuid.MustParse(c.RepoID), Hash: c.Hash, Message: c.Message,
		})
	}
	return out
}

func (doc goldenDocument) authorWhenForCommit(t *testing.T, hash string) time.Time {
	t.Helper()
	for _, c := range doc.Inputs.Commits {
		if c.Hash == hash {
			return parseTime(t, c.AuthorWhen)
		}
	}
	t.Fatalf("golden has no commit with hash %q", hash)
	return time.Time{}
}

func (doc goldenDocument) wantLinks(t *testing.T) []prcommit.Link {
	t.Helper()
	out := make([]prcommit.Link, 0, len(doc.PRCommitRows))
	for _, row := range doc.PRCommitRows {
		out = append(out, prcommit.Link{
			OrgID:      row.OrgID,
			RepoID:     uuid.MustParse(row.RepoID),
			PRNumber:   row.PRNumber,
			CommitHash: row.CommitHash,
			Confidence: row.Confidence,
			Provenance: row.Provenance,
			Evidence:   row.Evidence,
			LastSynced: parseTime(t, row.LastSynced),
		})
	}
	return out
}

func (doc goldenDocument) extraFastPathRows(t *testing.T) []prcommit.FastPathRow {
	t.Helper()
	out := make([]prcommit.FastPathRow, 0, len(doc.ExtraFastpathSeedRows))
	for _, row := range doc.ExtraFastpathSeedRows {
		var authorWhen time.Time
		if row.AuthorWhen != nil {
			authorWhen = parseTime(t, *row.AuthorWhen)
		}
		out = append(out, prcommit.FastPathRow{
			RepoID:     uuid.MustParse(row.RepoID),
			PRNumber:   row.PRNumber,
			CommitHash: row.CommitHash,
			Confidence: row.Confidence,
			Provenance: row.Provenance,
			Evidence:   row.Evidence,
			AuthorWhen: authorWhen,
		})
	}
	return out
}

type wantEdge struct {
	EdgeID       string
	SourceType   string
	SourceID     string
	TargetType   string
	TargetID     string
	EdgeType     string
	Provenance   string
	Evidence     string
	Confidence   float32
	RepoID       *uuid.UUID
	DiscoveredAt time.Time
	LastSynced   time.Time
	EventTs      time.Time
	Day          time.Time
}

func (doc goldenDocument) wantEdges(t *testing.T) []wantEdge {
	t.Helper()
	out := make([]wantEdge, 0, len(doc.EdgeRows))
	for _, e := range doc.EdgeRows {
		var repoID *uuid.UUID
		if e.RepoID != nil {
			parsed := uuid.MustParse(*e.RepoID)
			repoID = &parsed
		}
		out = append(out, wantEdge{
			EdgeID: e.EdgeID, SourceType: e.SourceType, SourceID: e.SourceID,
			TargetType: e.TargetType, TargetID: e.TargetID, EdgeType: e.EdgeType,
			Provenance: e.Provenance, Confidence: e.Confidence, Evidence: e.Evidence,
			RepoID:       repoID,
			DiscoveredAt: parseTime(t, e.DiscoveredAt),
			LastSynced:   parseTime(t, e.LastSynced),
			EventTs:      parseTime(t, e.EventTs),
			Day:          parseTime(t, e.Day+"T00:00:00Z"),
		})
	}
	return out
}

func repositoryRootPath(t *testing.T) string {
	t.Helper()
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for directory := working; ; {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("could not find repository root (no go.mod found)")
		}
		directory = parent
	}
}
