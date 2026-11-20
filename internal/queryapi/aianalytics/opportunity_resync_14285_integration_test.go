//go:build integration

package aianalytics

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

const (
	resyncRepo = "00000000-0000-0000-0000-000000142850"
	resyncOrgA = "00000000-0000-0000-0000-0000001428a0"
	resyncOrgB = "00000000-0000-0000-0000-0000001428b0"
	resyncOrgC = "00000000-0000-0000-0000-0000001428c0"
)

// resyncPR writes one version of a pull request as its own INSERT (its own part while merges are stopped).
func resyncPR(t *testing.T, ctx context.Context, conn chdriver.Conn, org string, number int, title, synced string) {
	t.Helper()
	exec(t, ctx, conn, `INSERT INTO git_pull_requests (repo_id, number, title, author_name, created_at, org_id, last_synced)
        VALUES ('%s', %d, '%s', 'alice', now64(3) - INTERVAL 2 DAY, '%s', now64(3) - INTERVAL %s)`,
		resyncRepo, number, title, org, synced)
}

func resyncAttr(t *testing.T, ctx context.Context, conn chdriver.Conn, org string, number int) {
	t.Helper()
	exec(t, ctx, conn, `INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source, confidence, evidence, observed_at, ingested_at)
        VALUES (generateUUIDv4(), '%s', 'github', 'pull_request', '%d', '%s', 'ai_assisted', 'pr_label', 1.0, '{}', now64(3), now64(3))`,
		org, number, resyncRepo)
}

func resyncCommit(t *testing.T, ctx context.Context, conn chdriver.Conn, org, hash, synced string) {
	t.Helper()
	exec(t, ctx, conn, `INSERT INTO git_commits (repo_id, hash, committer_when, org_id, last_synced)
        VALUES ('%s', '%s', now64(3) - INTERVAL 3 DAY, '%s', now64(3) - INTERVAL %s)`, resyncRepo, hash, org, synced)
	exec(t, ctx, conn, `INSERT INTO git_commit_stats (repo_id, commit_hash, file_path, additions, deletions, org_id, last_synced)
        VALUES ('%s', '%s', 'src/a.go', 1, 1, '%s', now64(3) - INTERVAL %s)`, resyncRepo, hash, org, synced)
}

// resyncCards is the served opportunity list of one org on the shared repository, "<KIND> <rationale>", sorted.
func resyncCards(t *testing.T, ctx context.Context, client QueryClient, org string) []string {
	t.Helper()
	got, err := AiOpportunities(ctx, client, org, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, r := range got.Recommendations {
		if r.RepoID != nil && *r.RepoID == resyncRepo {
			out = append(out, string(r.Kind)+" "+r.Rationale)
		}
	}
	sort.Strings(out)
	return out
}

func hasCard(cards []string, kind model.AIOpportunityKind, text string) bool {
	for _, c := range cards {
		if strings.HasPrefix(c, string(kind)+" ") && strings.Contains(c, text) {
			return true
		}
	}
	return false
}

func hasKind(cards []string, kind model.AIOpportunityKind) bool {
	for _, c := range cards {
		if strings.HasPrefix(c, string(kind)+" ") {
			return true
		}
	}
	return false
}

// TestRealClickHouse_AIOpportunitiesCountAResyncedPullRequestOnce: git_pull_requests and git_commits are
// ReplacingMergeTree(last_synced). A pull request synced twice keeps both versions until a merge. Each served
// workflow card counts a pull request (or a commit) once and reads its current title, before and after the merge,
// and a second org on the same repository id keeps its own count. CHAOS-14285.
func TestRealClickHouse_AIOpportunitiesCountAResyncedPullRequestOnce(t *testing.T) {
	ctx, conn, client := startStore(t)
	for _, tbl := range []string{"git_pull_requests", "ai_attribution", "git_commits", "git_commit_stats", "work_graph_issue_pr", "work_items"} {
		exec(t, ctx, conn, "SYSTEM STOP MERGES %s", tbl)
	}

	dep := "Bump lodash from 1 to 2 (deps)"
	// org A: 4 dependency PRs, PR 1 synced twice (5 rows, 4 pull requests; min 5).
	for n := 1; n <= 4; n++ {
		resyncPR(t, ctx, conn, resyncOrgA, n, dep, "2 HOUR")
	}
	resyncPR(t, ctx, conn, resyncOrgA, 1, dep, "1 HOUR")
	// org A: 4 AI-assisted PRs of one title prefix, PR 11 synced twice (repetitive cluster min 5).
	for n := 11; n <= 14; n++ {
		resyncPR(t, ctx, conn, resyncOrgA, n, "Rename widget helper", "2 HOUR")
		resyncAttr(t, ctx, conn, resyncOrgA, n)
	}
	resyncPR(t, ctx, conn, resyncOrgA, 11, "Rename widget helper", "1 HOUR")
	// org A: 5 migration PRs; PR 25 was retitled on its second sync, so 4 are migrations now (min 5).
	for n := 21; n <= 25; n++ {
		resyncPR(t, ctx, conn, resyncOrgA, n, "Migrate widget api", "2 HOUR")
	}
	resyncPR(t, ctx, conn, resyncOrgA, 25, "Fix login bug", "1 HOUR")
	// org A: 19 code commits, commit h0 synced twice (doc drift min 20; uniqExact on hash).
	for i := 0; i < 19; i++ {
		resyncCommit(t, ctx, conn, resyncOrgA, fmt.Sprintf("a-h%d", i), "2 HOUR")
	}
	resyncCommit(t, ctx, conn, resyncOrgA, "a-h0", "1 HOUR")
	// org B, same repository id: 5 dependency PRs, one version each.
	for n := 1; n <= 5; n++ {
		resyncPR(t, ctx, conn, resyncOrgB, n, dep, "2 HOUR")
	}
	// org C: one dependency PR synced twice.
	resyncPR(t, ctx, conn, resyncOrgC, 1, dep, "2 HOUR")
	resyncPR(t, ctx, conn, resyncOrgC, 1, dep, "1 HOUR")

	parts := func() uint64 {
		var n uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM system.parts WHERE active AND database = currentDatabase() AND table = 'git_pull_requests'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// The count of the one pull request of org C through the dependency statement with a minimum of 1.
	orgCCount := func() int64 {
		rule := titlePatternRules[0]
		st := strings.Replace(withRepo(titleToilStatement, ""), "@@TITLE@@", rule.titleExpr, 1)
		rows, err := loadClusters(ctx, client, st, opportunityBindings(resyncOrgC, "", clickhouse.Binding{Name: "min_prs", Value: uint32(1)}), false)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 {
			t.Fatalf("org C: %d dependency rows, want 1", len(rows))
		}
		t.Logf("org C dependency row: count=%d refs=%v", rows[0].Count, rows[0].Refs)
		return rows[0].Count
	}

	check := func(phase string) {
		t.Logf("%s: active parts of git_pull_requests = %d", phase, parts())
		if c := orgCCount(); c != 1 {
			t.Errorf("%s: org C counts its one pull request synced twice as %d, want 1", phase, c)
		}
		a := resyncCards(t, ctx, client, resyncOrgA)
		t.Logf("%s: org A cards %q", phase, a)
		if hasKind(a, model.AIOpportunityKindDependencyUpdates) {
			t.Errorf("%s: org A serves a DEPENDENCY_UPDATES card from 4 pull requests (min 5)", phase)
		}
		if hasKind(a, model.AIOpportunityKindRepetitiveChange) {
			t.Errorf("%s: org A serves a REPETITIVE_CHANGE card from 4 pull requests (min 5)", phase)
		}
		if hasKind(a, model.AIOpportunityKindMechanicalMigrations) {
			t.Errorf("%s: org A serves a MECHANICAL_MIGRATIONS card from 4 current migration titles (min 5)", phase)
		}
		if hasKind(a, model.AIOpportunityKindDocumentationDrift) {
			t.Errorf("%s: org A serves a DOCUMENTATION_DRIFT card from 19 commits (min 20)", phase)
		}
		b := resyncCards(t, ctx, client, resyncOrgB)
		t.Logf("%s: org B cards %q", phase, b)
		if !hasCard(b, model.AIOpportunityKindDependencyUpdates, "5 dependency-update PRs") {
			t.Errorf("%s: org B cards %q, want its own DEPENDENCY_UPDATES card of 5", phase, b)
		}
	}
	check("separate parts")
	for _, tbl := range []string{"git_pull_requests", "git_commits", "git_commit_stats"} {
		exec(t, ctx, conn, "SYSTEM START MERGES %s", tbl)
		exec(t, ctx, conn, "OPTIMIZE TABLE %s FINAL", tbl)
	}
	check("after OPTIMIZE FINAL")
}
