//go:build integration

package aianalytics

import (
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// TestRealClickHouse_AIAttributedPRsPageCountsPullRequests: the aiAttributedPRs page holds `limit` distinct pull requests
// and hasMore is true only while one is left, when a pull request reaches the read more than once: its link stored in two
// or three versions (merges stopped), the link's provenance moved, two linked issues, both attribution arms, or both
// direct id forms. Each cell is its own org with PRs 1..3 on one repository id; org Z (no extra path) is the control.
// CHAOS-14285 round 1.
func TestRealClickHouse_AIAttributedPRsPageCountsPullRequests(t *testing.T) {
	ctx, conn, client := startStore(t)
	for _, tbl := range []string{"git_pull_requests", "ai_attribution", "work_graph_issue_pr", "work_items"} {
		exec(t, ctx, conn, "SYSTEM STOP MERGES %s", tbl)
	}
	link := func(org string, number int, item, provenance, synced string) {
		exec(t, ctx, conn, `INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
        VALUES ('%s', '%s', %d, 1.0, '%s', 'fixture', now64(3) - INTERVAL %s, '%s')`, r14285Repo, item, number, provenance, synced, org)
	}
	cells := []struct {
		name string
		org  string
		seed func(org string)
	}{
		{"link-native-two-versions", "00000000-0000-0000-0000-0000001428c1", func(org string) {
			link(org, 1, "jira:L-1", "native", "1 HOUR")
		}},
		{"link-three-versions", "00000000-0000-0000-0000-0000001428c2", func(org string) {
			link(org, 1, "jira:L-1", "native", "1 HOUR")
			link(org, 1, "jira:L-1", "native", "30 MINUTE")
		}},
		{"link-tier-change", "00000000-0000-0000-0000-0000001428c3", func(org string) {
			link(org, 1, "jira:L-1", "explicit_text", "1 HOUR")
		}},
		{"linked-two-issues", "00000000-0000-0000-0000-0000001428c4", func(org string) {
			link(org, 1, "jira:L-9", "native", "2 HOUR")
			r14285Attr(t, ctx, conn, org, "jira:L-9")
		}},
		{"both-arms", "00000000-0000-0000-0000-0000001428c5", func(org string) {
			r14285Attr(t, ctx, conn, org, "1")
		}},
		{"direct-two-id-forms", "00000000-0000-0000-0000-0000001428c6", func(org string) {
			exec(t, ctx, conn, `INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source, confidence, evidence, observed_at, ingested_at)
        VALUES (generateUUIDv4(), '%s', 'github', 'pull_request', '%s#1', '%s', 'ai_assisted', 'pr_body', 1.0, '{}', now64(3), now64(3))`,
				org, r14285Repo, r14285Repo)
		}},
		{"control", "00000000-0000-0000-0000-0000001428c9", func(string) {}},
	}
	for _, c := range cells {
		for n := 1; n <= 3; n++ {
			r14285PR(t, ctx, conn, c.org, n, fmt.Sprintf("PR %d", n), "1 DAY", fmt.Sprintf("%d HOUR", n), "3 HOUR")
			if c.name == "direct-two-id-forms" {
				r14285Attr(t, ctx, conn, c.org, fmt.Sprint(n))
				continue
			}
			item := fmt.Sprintf("jira:L-%d", n)
			link(c.org, n, item, "native", "2 HOUR")
			r14285Attr(t, ctx, conn, c.org, item)
		}
		c.seed(c.org)
	}
	end := time.Now().UTC()
	dr := model.AIDateRangeInput{StartDate: day(end.AddDate(0, 0, -3).Format("2006-01-02")), EndDate: day(end.Format("2006-01-02"))}
	page := func(org string, offset int) string {
		got, err := AttributedPrs(ctx, client, org, dr, nil, 2, offset)
		if err != nil {
			t.Fatal(err)
		}
		var numbers []int
		for _, r := range got.Rows {
			numbers = append(numbers, r.Number)
		}
		return fmt.Sprintf("%v/%v", numbers, got.HasMore)
	}
	for _, c := range cells {
		p1, p2 := page(c.org, 0), page(c.org, 2)
		t.Logf("%s: %s, %s", c.name, p1, p2)
		if p1 != "[1 2]/true" || p2 != "[3]/false" {
			t.Errorf("%s: pages %s, %s, want [1 2]/true, [3]/false", c.name, p1, p2)
		}
	}
}
