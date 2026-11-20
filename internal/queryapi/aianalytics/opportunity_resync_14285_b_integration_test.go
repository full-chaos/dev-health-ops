//go:build integration

package aianalytics

import (
	"context"
	"fmt"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

const r14285Repo = "00000000-0000-0000-0000-000000142859"

func r14285PR(t *testing.T, ctx context.Context, conn chdriver.Conn, org string, number int, title, created, merged, synced string) {
	t.Helper()
	exec(t, ctx, conn, `INSERT INTO git_pull_requests (repo_id, number, title, author_name, created_at, merged_at, org_id, last_synced)
        VALUES ('%s', %d, '%s', 'alice', now64(3) - INTERVAL %s, now64(3) - INTERVAL %s, '%s', now64(3) - INTERVAL %s)`,
		r14285Repo, number, title, created, merged, org, synced)
}

func r14285Attr(t *testing.T, ctx context.Context, conn chdriver.Conn, org, subject string) {
	t.Helper()
	exec(t, ctx, conn, `INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source, confidence, evidence, observed_at, ingested_at)
        VALUES (generateUUIDv4(), '%s', 'github', 'pull_request', '%s', '%s', 'ai_assisted', 'pr_label', 1.0, '{}', now64(3), now64(3))`,
		org, subject, r14285Repo)
}

func r14285Link(t *testing.T, ctx context.Context, conn chdriver.Conn, org string, number int, item, synced string) {
	t.Helper()
	exec(t, ctx, conn, `INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
        VALUES ('%s', '%s', %d, 1.0, 'native', 'fixture', now64(3) - INTERVAL %s, '%s')`, r14285Repo, item, number, synced, org)
}

// TestRealClickHouse_AIRepetitiveClusterCountsAPullRequestOnce: Q1 of CHAOS-14285. A repetitive cluster counts pull
// requests. Org D: 4 AI-assisted PRs, the link of PR 31 synced twice (work_graph_issue_pr is
// ReplacingMergeTree(version_rank)). Org E: 4 AI-assisted PRs, PR 41 linked to two issues. Org F, same repository id:
// 5 AI-assisted PRs, one version each. Org J: 5 AI-assisted PRs, PR 65 retitled on its second sync, so 4 share the
// prefix now (pins the FINAL on git_pull_requests when the count is by distinct pull request). Merges stopped.
func TestRealClickHouse_AIRepetitiveClusterCountsAPullRequestOnce(t *testing.T) {
	ctx, conn, client := startStore(t)
	for _, tbl := range []string{"git_pull_requests", "ai_attribution", "work_graph_issue_pr", "work_items"} {
		exec(t, ctx, conn, "SYSTEM STOP MERGES %s", tbl)
	}
	orgD, orgE, orgF := "00000000-0000-0000-0000-0000001428d0", "00000000-0000-0000-0000-0000001428e0", "00000000-0000-0000-0000-0000001428f0"
	orgJ := "00000000-0000-0000-0000-0000001428a9"
	seed := func(org string, from int) {
		for n := from; n < from+4; n++ {
			r14285PR(t, ctx, conn, org, n, "Rename widget helper", "2 DAY", "1 DAY", "2 HOUR")
			r14285Attr(t, ctx, conn, org, fmt.Sprint(n))
		}
	}
	seed(orgD, 31)
	r14285Link(t, ctx, conn, orgD, 31, "jira:D-1", "2 HOUR")
	r14285Link(t, ctx, conn, orgD, 31, "jira:D-1", "1 HOUR")
	seed(orgE, 41)
	r14285Link(t, ctx, conn, orgE, 41, "jira:E-1", "2 HOUR")
	r14285Link(t, ctx, conn, orgE, 41, "jira:E-2", "2 HOUR")
	seed(orgF, 51)
	r14285PR(t, ctx, conn, orgF, 55, "Rename widget helper", "2 DAY", "1 DAY", "2 HOUR")
	r14285Attr(t, ctx, conn, orgF, "55")
	seed(orgJ, 61)
	r14285PR(t, ctx, conn, orgJ, 65, "Rename widget helper", "2 DAY", "1 DAY", "2 HOUR")
	r14285Attr(t, ctx, conn, orgJ, "65")
	r14285PR(t, ctx, conn, orgJ, 65, "Fix login bug", "2 DAY", "1 DAY", "1 HOUR")

	count := func(org string) int64 {
		rows, err := loadClusters(ctx, client, withRepo(repetitiveStatement, ""),
			opportunityBindings(org, "", clickhouse.Binding{Name: "cluster_min", Value: uint32(1)}), true)
		if err != nil {
			t.Fatal(err)
		}
		var total int64
		for _, r := range rows {
			t.Logf("org %s: cluster %q author %q type %q count %d refs %v", org[len(org)-4:], r.Prefix, r.Author, r.WorkType, r.Count, r.Refs)
			if r.Prefix == "rename widget helper" {
				total += r.Count
			}
		}
		return total
	}
	check := func(phase string) {
		for _, c := range []struct {
			org  string
			want int64
			card bool
		}{{orgD, 4, false}, {orgE, 4, false}, {orgF, 5, true}, {orgJ, 4, false}} {
			if got := count(c.org); got != c.want {
				t.Errorf("%s: org %s prs_total of the 'rename widget helper' cluster %d, want %d", phase, c.org[len(c.org)-4:], got, c.want)
			}
			if got := hasKind(resyncCardsOf(t, ctx, client, c.org), model.AIOpportunityKindRepetitiveChange); got != c.card {
				t.Errorf("%s: org %s REPETITIVE_CHANGE card = %v, want %v", phase, c.org[len(c.org)-4:], got, c.card)
			}
		}
	}
	check("separate parts")
	for _, tbl := range []string{"git_pull_requests", "work_graph_issue_pr"} {
		exec(t, ctx, conn, "SYSTEM START MERGES %s", tbl)
		exec(t, ctx, conn, "OPTIMIZE TABLE %s FINAL", tbl)
	}
	check("after OPTIMIZE FINAL")
}

func resyncCardsOf(t *testing.T, ctx context.Context, client QueryClient, org string) []string {
	t.Helper()
	got, err := AiOpportunities(ctx, client, org, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, r := range got.Recommendations {
		if r.RepoID != nil && *r.RepoID == r14285Repo {
			out = append(out, string(r.Kind)+" "+r.Rationale)
		}
	}
	return out
}

// TestRealClickHouse_AIAttributedPRsPageHoldsDistinctPullRequests: B4 of CHAOS-14285. A page of attributed pull
// requests holds `limit` distinct pull requests while one of them has two versions: org G through the number arm,
// org H through the linked-issue arm, org I (same repository id) one version each. Merges stopped.
func TestRealClickHouse_AIAttributedPRsPageHoldsDistinctPullRequests(t *testing.T) {
	ctx, conn, client := startStore(t)
	for _, tbl := range []string{"git_pull_requests", "ai_attribution", "work_graph_issue_pr", "work_items"} {
		exec(t, ctx, conn, "SYSTEM STOP MERGES %s", tbl)
	}
	orgG, orgH, orgI := "00000000-0000-0000-0000-0000001428a1", "00000000-0000-0000-0000-0000001428a2", "00000000-0000-0000-0000-0000001428a3"
	for _, org := range []string{orgG, orgH, orgI} {
		for n := 1; n <= 3; n++ {
			merged := fmt.Sprintf("%d HOUR", n)
			r14285PR(t, ctx, conn, org, n, fmt.Sprintf("PR %d", n), "1 DAY", merged, "3 HOUR")
			if org == orgH {
				item := fmt.Sprintf("jira:H-%d", n)
				r14285Link(t, ctx, conn, org, n, item, "3 HOUR")
				r14285Attr(t, ctx, conn, org, item)
			} else {
				r14285Attr(t, ctx, conn, org, fmt.Sprint(n))
			}
		}
		if org != orgI {
			r14285PR(t, ctx, conn, org, 1, "PR 1 retitled", "1 DAY", "1 HOUR", "2 HOUR")
		}
	}
	end := time.Now().UTC()
	start := end.AddDate(0, 0, -3)
	page := func(org string, limit, offset int) []string {
		rows, err := loadAttributedPRs(ctx, client, org, start, end, "", nil, false, limit, offset)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, r := range rows {
			title := ""
			if r.Title != nil {
				title = *r.Title
			}
			out = append(out, fmt.Sprintf("%d:%s", r.Number, title))
		}
		return out
	}
	check := func(phase string) {
		for _, c := range []struct {
			org           string
			first, second string
		}{
			{orgG, "[1:PR 1 retitled 2:PR 2]", "[3:PR 3]"},
			{orgH, "[1:PR 1 retitled 2:PR 2]", "[3:PR 3]"},
			{orgI, "[1:PR 1 2:PR 2]", "[3:PR 3]"},
		} {
			p1, p2 := fmt.Sprint(page(c.org, 2, 0)), fmt.Sprint(page(c.org, 2, 2))
			t.Logf("%s: org %s page 1 %s page 2 %s", phase, c.org[len(c.org)-4:], p1, p2)
			if p1 != c.first || p2 != c.second {
				t.Errorf("%s: org %s pages %s %s, want %s %s", phase, c.org[len(c.org)-4:], p1, p2, c.first, c.second)
			}
		}
	}
	check("separate parts")
	exec(t, ctx, conn, "SYSTEM START MERGES git_pull_requests")
	exec(t, ctx, conn, "OPTIMIZE TABLE git_pull_requests FINAL")
	check("after OPTIMIZE FINAL")
}
