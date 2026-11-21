//go:build integration

package aianalytics

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// TestRealClickHouse_AIAttributedPRsFilterThenPickThenPage: a scoped aiAttributedPRs page filters the candidates (the AI
// kinds, the requested work type), then keeps one row per pull request, then cuts the page. Each cell is its own org
// on one repository id, merges stopped. CHAOS-14285 round 2.
func TestRealClickHouse_AIAttributedPRsFilterThenPickThenPage(t *testing.T) {
	ctx, conn, client := startStore(t)
	for _, tbl := range []string{"git_pull_requests", "ai_attribution", "work_graph_issue_pr", "work_items"} {
		exec(t, ctx, conn, "SYSTEM STOP MERGES %s", tbl)
	}
	link := func(org string, number int, item string) {
		exec(t, ctx, conn, `INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
        VALUES ('%s', '%s', %d, 1.0, 'native', 'fixture', now64(3) - INTERVAL 2 HOUR, '%s')`, r14285Repo, item, number, org)
	}
	item := func(org, id, typ string) {
		exec(t, ctx, conn, `INSERT INTO work_items (repo_id, work_item_id, provider, type, org_id, last_synced) VALUES ('%s', '%s', 'jira', '%s', '%s', now64(3))`,
			r14285Repo, id, typ, org)
	}
	attr := func(org, subject, kind string) {
		exec(t, ctx, conn, `INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source, confidence, evidence, observed_at, ingested_at)
        VALUES (generateUUIDv4(), '%s', 'github', 'pull_request', '%s', '%s', '%s', 'pr_label', 1.0, '{}', now64(3), now64(3))`, org, subject, r14285Repo, kind)
	}
	// Fixed times inside yesterday, so the served bytes of two runs on one UTC day can be compared.
	pr := func(org string, n int) {
		exec(t, ctx, conn, `INSERT INTO git_pull_requests (repo_id, number, title, author_name, created_at, merged_at, org_id, last_synced)
        VALUES ('%s', %d, 'PR %d', 'alice', toDateTime64(today() - 1, 3, 'UTC') + INTERVAL 1 HOUR,
        toDateTime64(today() - 1, 3, 'UTC') + INTERVAL %d HOUR, '%s', now64(3))`, r14285Repo, n, n, 20-n, org)
	}
	// A pull request n linked to a story and to a second candidate; second(org, n) seeds the second candidate.
	storyAnd := func(org string, n int, second func(org string, n int)) {
		pr(org, n)
		story := fmt.Sprintf("jira:S-%d", n)
		link(org, n, story)
		item(org, story, "story")
		attr(org, story, "ai_assisted")
		second(org, n)
	}
	bug := func(kind string) func(string, int) {
		return func(org string, n int) {
			id := fmt.Sprintf("jira:B-%d", n)
			link(org, n, id)
			item(org, id, "bug")
			attr(org, id, kind)
		}
	}
	noItem := func(org string, n int) {
		id := fmt.Sprintf("jira:N-%d", n)
		link(org, n, id)
		attr(org, id, "ai_assisted")
	}
	emptyType := func(org string, n int) {
		id := fmt.Sprintf("jira:E-%d", n)
		link(org, n, id)
		item(org, id, "")
		attr(org, id, "ai_assisted")
	}
	end := time.Now().UTC()
	dr := model.AIDateRangeInput{StartDate: day(end.AddDate(0, 0, -3).Format("2006-01-02")), EndDate: day(end.Format("2006-01-02"))}
	read := func(org, workType string, limit, offset int) *model.AiAttributedPrsResult {
		var in *model.AIScopeInput
		if workType != "" {
			in = &model.AIScopeInput{WorkType: &workType}
		}
		got, err := AttributedPrs(ctx, client, org, dr, in, limit, offset)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	page := func(org, workType string, limit, offset int) string {
		got := read(org, workType, limit, offset)
		var rows []string
		for _, r := range got.Rows {
			rows = append(rows, fmt.Sprintf("%d:%s:%s", r.Number, derefString(r.Kind), derefString(r.WorkType)))
		}
		return fmt.Sprintf("%v/%v", rows, got.HasMore)
	}

	cells := []struct {
		name, org string
		seed      func(org string)
		workType  string
		want      string
	}{
		{"scope-linked-work-type", "00000000-0000-0000-0000-0000001428d1", func(o string) { storyAnd(o, 1, bug("ai_assisted")) }, "story", "[1:ai_assisted:story]/false"},
		{"scope-linked-kind", "00000000-0000-0000-0000-0000001428d2", func(o string) { storyAnd(o, 1, bug("agent_created")) }, "story", "[1:ai_assisted:story]/false"},
		{"scope-fallback", "00000000-0000-0000-0000-0000001428d3", func(o string) { storyAnd(o, 1, noItem) }, "story", "[1:ai_assisted:story]/false"},
		{"scope-empty-type", "00000000-0000-0000-0000-0000001428d4", func(o string) { storyAnd(o, 1, emptyType) }, "story", "[1:ai_assisted:story]/false"},
		{"kind-before-pick", "00000000-0000-0000-0000-0000001428d5", func(o string) {
			pr(o, 1)
			link(o, 1, "jira:H-1")
			item(o, "jira:H-1", "story")
			attr(o, "jira:H-1", "human")
			attr(o, "1", "ai_assisted")
			pr(o, 2)
			attr(o, "2", "human")
		}, "", "[1:ai_assisted:pull_request]/false"},
	}
	for _, c := range cells {
		c.seed(c.org)
	}
	boundary := "00000000-0000-0000-0000-0000001428d6"
	for n := 1; n <= 3; n++ {
		storyAnd(boundary, n, bug("agent_created"))
	}
	direct := "00000000-0000-0000-0000-0000001428d7"
	pr(direct, 1)
	link(direct, 1, "jira:S-1")
	item(direct, "jira:S-1", "story")
	attr(direct, "jira:S-1", "ai_assisted")
	attr(direct, "1", "ai_assisted")

	for _, c := range cells {
		got := page(c.org, c.workType, 50, 0)
		t.Logf("%s (workType %q): %s", c.name, c.workType, got)
		if got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
	p1, p2 := page(boundary, "story", 2, 0), page(boundary, "story", 2, 2)
	t.Logf("scoped-boundary (workType story, limit 2): %s, %s", p1, p2)
	if p1 != "[1:ai_assisted:story 2:ai_assisted:story]/true" || p2 != "[3:ai_assisted:story]/false" {
		t.Errorf("scoped-boundary: %s, %s, want [1 2 story]/true, [3 story]/false", p1, p2)
	}
	// OUT of this change (ticket 14325): a pull_request page of a PR that has a linked story and a direct attribution.
	t.Logf("direct-v-linked (workType pull_request, recorded, not asserted): %s", page(direct, "pull_request", 50, 0))

	// The unscoped pages, as served bytes, for a byte comparison with the round 2 tip.
	for _, org := range []string{cells[0].org, cells[1].org, cells[2].org, cells[3].org, cells[4].org, boundary, direct} {
		var bodies []byte
		for _, offset := range []int{0, 2} {
			body, err := json.Marshal(read(org, "", 2, offset))
			if err != nil {
				t.Fatal(err)
			}
			bodies = append(bodies, body...)
		}
		t.Logf("UNSCOPED %s sha256=%x", org[len(org)-4:], sha256.Sum256(bodies))
	}
}
