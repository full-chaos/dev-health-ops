package writercontract

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuecommitedges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuepredges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/prcommit"
)

// acr's work_graph.v1 read (CHAOS-10730) keeps a pr-sourced edge only when its
// repo_id equals the repository in source_id ('<repo>#pr<n>'); commit-sourced
// edges are '<repo>@<sha>'. A writer that stamps the TARGET's repo_id breaks it.
var (
	repoA = uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	repoB = uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
	now   = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
)

func assertSourceRepo(t *testing.T, rows []edges.Row, source uuid.UUID, wantRows int) {
	t.Helper()
	if len(rows) != wantRows {
		t.Fatalf("derived %d edge(s), want %d (a vacuous fixture proves nothing)", len(rows), wantRows)
	}
	for _, row := range rows {
		var prefix string
		switch row.SourceType {
		case edges.NodeTypePR:
			prefix = source.String() + "#pr"
		case edges.NodeTypeCommit:
			prefix = source.String() + "@"
		default:
			t.Fatalf("edge %s has source_type %q, fixture expects pr or commit", row.EdgeID, row.SourceType)
		}
		if !strings.HasPrefix(row.SourceID, prefix) {
			t.Errorf("edge %s: source_id %q lacks prefix %q", row.EdgeID, row.SourceID, prefix)
		}
		if row.RepoID == nil {
			t.Errorf("edge %s (%s): repo_id is nil, want %s", row.EdgeID, row.SourceID, source)
			continue
		}
		if *row.RepoID != source {
			t.Errorf("edge %s (%s): repo_id %s, want the source repo %s", row.EdgeID, row.SourceID, *row.RepoID, source)
		}
	}
}

func TestIssuePRFastPathRepoIDIsSourceRepo(t *testing.T) {
	rows := issuepredges.DeriveFastPathEdges([]issuepredges.FastPathRow{
		{RepoID: repoA, WorkItemID: "jira:ABC-1", PRNumber: 5, Confidence: 0.9, PRCreatedAt: now},
	}, now)
	assertSourceRepo(t, rows, repoA, 1)
}

func TestIssuePRTextParseRepoIDIsSourceRepo(t *testing.T) {
	result := issuepredges.DeriveTextParseEdges(
		[]issuepredges.PullRequestRow{
			{RepoID: repoA, Number: 5, Title: "Fixes ABC-1", CreatedAt: now},
			{RepoID: repoA, Number: 6, Body: "Closes ownerB/repoB#7", CreatedAt: now},
		},
		[]issuepredges.WorkItemRow{
			{RepoID: repoB, WorkItemID: "jira:ABC-1", Provider: "jira"},
			{RepoID: repoB, WorkItemID: "gh:ownerB/repoB#7", Provider: "github", CurrentPath: "ownerB/repoB"},
		},
		issueprlinks.NativeLinks{}, now)
	assertSourceRepo(t, result.Edges, repoA, 2)
}

func TestIssuePRHeuristicRepoIDIsSourceRepo(t *testing.T) {
	result := issuepredges.DeriveHeuristicEdges(issuepredges.HeuristicInputs{
		WorkItems:           []issuepredges.HeuristicWorkItemRow{{RepoID: repoA, WorkItemID: "jira:ABC-2", UpdatedAt: now}},
		PullRequests:        []issuepredges.HeuristicPullRequestRow{{RepoID: repoA, Number: 9, CreatedAt: now}},
		HeuristicDaysWindow: 7,
		HeuristicConfidence: 0.5,
	}, now)
	assertSourceRepo(t, result.Edges, repoA, 1)
}

func TestPRCommitFastPathRepoIDIsSourceRepo(t *testing.T) {
	rows := prcommit.BuildFastPathEdges("org", []prcommit.FastPathRow{
		{RepoID: repoA, PRNumber: 5, CommitHash: "deadbeef", Confidence: 0.9, AuthorWhen: now},
	}, now)
	assertSourceRepo(t, rows, repoA, 1)
}

func TestIssueCommitEdgesRepoIDIsSourceRepo(t *testing.T) {
	result := issuecommitedges.Derive(issuecommitedges.Inputs{
		Commits:   []issuecommitedges.CommitRow{{RepoID: repoA, Hash: "cafe01", Message: "Fixes ABC-1", AuthorWhen: now}},
		WorkItems: []issuecommitedges.WorkItemRow{{RepoID: repoB, WorkItemID: "jira:ABC-1", Provider: "jira"}},
	}, now)
	assertSourceRepo(t, result.Edges, repoA, 1)
}

// Census guard: operational, flag-guard and issue-issue writers are not
// pr/commit-sourced by census. Every composite literal setting a pr/commit
// SourceType must live in a covered deriver file or a pinned non-writer. A
// SourceType taken from a variable is out of scope.
func TestEveryPRAndCommitSourcedWriterIsCovered(t *testing.T) {
	covered := map[string]bool{
		"jobs/workgraph/issuepredges/issuepredges.go":         true,
		"jobs/workgraph/issuepredges/heuristic.go":            true,
		"jobs/workgraph/prcommit/fastpath.go":                 true,
		"jobs/workgraph/issuecommitedges/issuecommitedges.go": true,
	}
	notWriters := map[string]string{
		"queryapi/investmentexplain/quotetitles.go": "read-side explain quote lookup key, not a work_graph_edges writer",
	}
	root := filepath.Join("..", "..", "..")
	found := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, perr := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if perr != nil {
			return perr
		}
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			for _, elt := range lit.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "SourceType" && isPRorCommit(kv.Value) {
					rel, _ := filepath.Rel(root, path)
					found[filepath.ToSlash(rel)] = true
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var unknown []string
	for file := range found {
		if !covered[file] && notWriters[file] == "" {
			unknown = append(unknown, file)
		}
	}
	sort.Strings(unknown)
	if len(unknown) > 0 {
		t.Errorf("pr/commit-sourced work_graph_edges writer not in the repo_id contract test: %v", unknown)
	}
	for file := range covered {
		if !found[file] {
			t.Errorf("covered deriver %s no longer builds a pr/commit-sourced edges.Row: update the contract test", file)
		}
	}
	for file, reason := range notWriters {
		if !found[file] {
			t.Errorf("pinned non-writer %s (%s) no longer sets a pr/commit SourceType: drop the pin", file, reason)
		}
	}
}

func isPRorCommit(expr ast.Expr) bool {
	switch v := expr.(type) {
	case *ast.SelectorExpr:
		return v.Sel.Name == "NodeTypePR" || v.Sel.Name == "NodeTypeCommit"
	case *ast.Ident:
		return v.Name == "NodeTypePR" || v.Name == "NodeTypeCommit"
	case *ast.BasicLit:
		return v.Value == `"pr"` || v.Value == `"commit"`
	}
	return false
}
