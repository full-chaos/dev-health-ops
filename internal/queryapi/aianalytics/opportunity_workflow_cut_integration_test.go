//go:build integration

package aianalytics

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

var cutTables = []string{"git_pull_requests", "ai_attribution", "git_commits", "git_commit_stats", "testops_test_metrics_daily", "work_graph_issue_pr", "work_items"}

func cutRepo(i int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", 1000+i) }

// cutSeedSite writes 120 groups with the SAME sort key for one workflow read, in the given group order, 12 groups per
// INSERT (each its own part while merges are stopped).
func cutSeedSite(t *testing.T, ctx context.Context, conn chdriver.Conn, site, org string, order []int) {
	for start := 0; start < len(order); start += 12 {
		var prs, attrs, commits, stats, flaky []string
		for _, g := range order[start : start+12] {
			repo := cutRepo(g)
			switch site {
			case "repetitive", "dependency", "migration":
				title := map[string]string{"repetitive": "Rename widget helper", "dependency": "Bump lodash from 1 to 2 (deps)", "migration": "Migrate widget api"}[site]
				for n := 1; n <= 5; n++ {
					prs = append(prs, fmt.Sprintf("('%s', %d, '%s', 'alice', now64(3) - INTERVAL 2 DAY, '%s', now64(3))", repo, n, title, org))
					if site == "repetitive" {
						attrs = append(attrs, fmt.Sprintf("(generateUUIDv4(), '%s', 'github', 'pull_request', '%d', '%s', 'ai_assisted', 'pr_label', 1.0, '{}', now64(3), now64(3))", org, n, repo))
					}
				}
			case "docdrift":
				for n := 0; n < 20; n++ {
					h := fmt.Sprintf("%s-h%d-%d", org[len(org)-4:], g, n)
					commits = append(commits, fmt.Sprintf("('%s', '%s', now64(3) - INTERVAL 3 DAY, '%s', now64(3))", repo, h, org))
					stats = append(stats, fmt.Sprintf("('%s', '%s', 'src/a.go', 1, 1, '%s', now64(3))", repo, h, org))
				}
			case "flaky":
				flaky = append(flaky, fmt.Sprintf("('%s', today() - 1, 100, 0.1, 10, 100, '%s', now())", repo, org))
			}
		}
		cutInsert(t, ctx, conn, "git_pull_requests (repo_id, number, title, author_name, created_at, org_id, last_synced)", prs)
		cutInsert(t, ctx, conn, "ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source, confidence, evidence, observed_at, ingested_at)", attrs)
		cutInsert(t, ctx, conn, "git_commits (repo_id, hash, committer_when, org_id, last_synced)", commits)
		cutInsert(t, ctx, conn, "git_commit_stats (repo_id, commit_hash, file_path, additions, deletions, org_id, last_synced)", stats)
		cutInsert(t, ctx, conn, "testops_test_metrics_daily (repo_id, day, total_cases, flake_rate, flake_cases, distinct_cases, org_id, computed_at)", flaky)
	}
}

func cutInsert(t *testing.T, ctx context.Context, conn chdriver.Conn, target string, rows []string) {
	if len(rows) > 0 {
		exec(t, ctx, conn, "INSERT INTO %s VALUES %s", target, strings.Join(rows, ","))
	}
}

func cutStopMerges(t *testing.T, ctx context.Context, conn chdriver.Conn) {
	for _, tbl := range cutTables {
		exec(t, ctx, conn, "SYSTEM STOP MERGES %s", tbl)
	}
}

func cutOptimize(t *testing.T, ctx context.Context, conn chdriver.Conn) {
	for _, tbl := range cutTables {
		exec(t, ctx, conn, "SYSTEM START MERGES %s", tbl)
		exec(t, ctx, conn, "OPTIMIZE TABLE %s FINAL", tbl)
	}
}

// TestRealClickHouse_AIWorkflowReadsKeepTheTotalOrderAtTheCut: each workflow read keeps 100 rows. With 120 groups of
// one count, the 100 kept (and so the served list and its limit cut) are the first 100 by repository id, the order
// the served list uses, in both insert orders, on every read and after OPTIMIZE FINAL; and each card counts only its
// own org's rows. CHAOS-14258.
func TestRealClickHouse_AIWorkflowReadsKeepTheTotalOrderAtTheCut(t *testing.T) {
	ctx, conn, client := startStore(t)
	cutStopMerges(t, ctx, conn)
	fwd, rev := make([]int, 120), make([]int, 120)
	for i := range fwd {
		fwd[i], rev[i] = i, 119-i
	}
	sites := []struct {
		name, kind, count string
	}{
		{"repetitive", string(model.AIOpportunityKindRepetitiveChange), "5 AI-assisted PRs shared"},
		{"dependency", string(model.AIOpportunityKindDependencyUpdates), "5 dependency-update PRs"},
		{"migration", string(model.AIOpportunityKindMechanicalMigrations), "5 migration-style PRs"},
		{"docdrift", string(model.AIOpportunityKindDocumentationDrift), "20 code commits"},
		{"flaky", string(model.AIOpportunityKindFlakyTestTriage), "across 100 executions"},
	}
	orgs := map[string][2]string{}
	for k, s := range sites {
		f, r := fmt.Sprintf("00000000-0000-0000-0000-0000000142%d1", k), fmt.Sprintf("00000000-0000-0000-0000-0000000142%d2", k)
		orgs[s.name] = [2]string{f, r}
		cutSeedSite(t, ctx, conn, s.name, f, fwd)
		cutSeedSite(t, ctx, conn, s.name, r, rev)
	}
	check := func(phase string) {
		for _, s := range sites {
			for _, org := range orgs[s.name] {
				for read := 0; read < 3; read++ {
					for _, limit := range []int{100, 5} {
						got, err := AiOpportunities(ctx, client, org, nil, limit)
						if err != nil {
							t.Fatal(err)
						}
						if len(got.Recommendations) != limit {
							t.Fatalf("%s %s org %s: served %d cards, want %d", phase, s.name, org, len(got.Recommendations), limit)
						}
						for i, r := range got.Recommendations {
							if want := cutRepo(i); string(r.Kind) != s.kind || *r.RepoID != want || !strings.Contains(r.Rationale, s.count) {
								t.Fatalf("%s %s org %s read %d limit %d: card %d = %s %s %q, want %s %s with %q",
									phase, s.name, org, read, limit, i, r.Kind, *r.RepoID, r.Rationale, s.kind, want, s.count)
							}
						}
					}
				}
			}
		}
	}
	check("separate parts")
	cutOptimize(t, ctx, conn)
	check("after OPTIMIZE FINAL")
}

// TestRealClickHouse_AIRepetitiveCutKeepsClusterOrder: 120 repetitive clusters of ONE repository and one count,
// differing only in title prefix, in author, or in work type. The 100 kept are the first 100 by that term, in both
// insert orders and after OPTIMIZE FINAL. Each cluster's PRs are numbered so its refs sort with the term. CHAOS-14258.
func TestRealClickHouse_AIRepetitiveCutKeepsClusterOrder(t *testing.T) {
	ctx, conn, client := startStore(t)
	cutStopMerges(t, ctx, conn)
	repo := cutRepo(0)
	seed := func(term, org string, order []int) {
		for start := 0; start < len(order); start += 12 {
			var prs, attrs, links, items []string
			for _, c := range order[start : start+12] {
				title, author := "Rename widget helper", "alice"
				switch term {
				case "prefix":
					title = fmt.Sprintf("p%03d widget helper", c)
				case "author":
					author = fmt.Sprintf("a%03d", c)
				}
				for k := 1; k <= 5; k++ {
					n := 10000 + 10*c + k
					prs = append(prs, fmt.Sprintf("('%s', %d, '%s', '%s', now64(3) - INTERVAL 2 DAY, '%s', now64(3))", repo, n, title, author, org))
					attrs = append(attrs, fmt.Sprintf("(generateUUIDv4(), '%s', 'github', 'pull_request', '%d', '%s', 'ai_assisted', 'pr_label', 1.0, '{}', now64(3), now64(3))", org, n, repo))
					if term == "worktype" {
						links = append(links, fmt.Sprintf("('%s', 'wi-%03d', %d, 1.0, 'native', '', now64(3), '%s')", repo, c, n, org))
					}
				}
				if term == "worktype" {
					items = append(items, fmt.Sprintf("('%s', 'wi-%03d', 'github', 'item', 't%03d', 'done', now64(3), now64(3), now64(3), '%s')", repo, c, c, org))
				}
			}
			cutInsert(t, ctx, conn, "git_pull_requests (repo_id, number, title, author_name, created_at, org_id, last_synced)", prs)
			cutInsert(t, ctx, conn, "ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source, confidence, evidence, observed_at, ingested_at)", attrs)
			cutInsert(t, ctx, conn, "work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)", links)
			cutInsert(t, ctx, conn, "work_items (repo_id, work_item_id, provider, title, type, status, created_at, updated_at, last_synced, org_id)", items)
		}
	}
	fwd, rev := make([]int, 120), make([]int, 120)
	for i := range fwd {
		fwd[i], rev[i] = i, 119-i
	}
	terms := []string{"prefix", "author", "worktype"}
	orgs := map[string][2]string{}
	for k, term := range terms {
		f, r := fmt.Sprintf("00000000-0000-0000-0000-0000001428%d1", k), fmt.Sprintf("00000000-0000-0000-0000-0000001428%d2", k)
		orgs[term] = [2]string{f, r}
		seed(term, f, fwd)
		seed(term, r, rev)
	}
	check := func(phase string) {
		for _, term := range terms {
			for _, org := range orgs[term] {
				got, err := AiOpportunities(ctx, client, org, nil, 100)
				if err != nil {
					t.Fatal(err)
				}
				if len(got.Recommendations) != 100 {
					t.Fatalf("%s %s org %s: served %d cards, want 100", phase, term, org, len(got.Recommendations))
				}
				for i, r := range got.Recommendations {
					want := fmt.Sprintf("git_pull_requests:%s:%d", repo, 10000+10*i+1)
					if len(r.EvidenceRefs) == 0 || r.EvidenceRefs[0] != want {
						t.Fatalf("%s %s org %s: card %d starts with %v, want %s (the cluster at position %d of the term order)",
							phase, term, org, i, r.EvidenceRefs, want, i)
					}
				}
			}
		}
	}
	check("separate parts")
	cutOptimize(t, ctx, conn)
	check("after OPTIMIZE FINAL")
}

// uncutClient runs each workflow read without its 100-row cut: the reference for "the rows a read keeps are the
// rule's first rows in the served order".
type uncutClient struct{ QueryClient }

func (u uncutClient) Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	return u.QueryClient.Query(ctx, strings.ReplaceAll(statement, "\nLIMIT 100", "\nLIMIT 100000"), bindings)
}

// TestRealClickHouse_AIWorkflowCutKeepsGoFirstN: for each workflow rule, with more than 100 groups and ties built on
// every term of the rule's sort key, the served answer at N = 1, 5 and 100 equals the answer of the same reads without
// the cut. Cases: a title prefix and its extension, an author by email and by name only, a linked and the fallback
// work type, counts that tie, and two flake shares that round to the same score. CHAOS-14258.
func TestRealClickHouse_AIWorkflowCutKeepsGoFirstN(t *testing.T) {
	ctx, conn, client := startStore(t)
	cutStopMerges(t, ctx, conn)
	pr := func(org, repo string, n int, title, author, email string) string {
		mail := "NULL"
		if email != "" {
			mail = "'" + email + "'"
		}
		return fmt.Sprintf("('%s', %d, '%s', '%s', %s, now64(3) - INTERVAL 2 DAY, '%s', now64(3))", repo, n, title, author, mail, org)
	}
	attr := func(org, repo string, n int) string {
		return fmt.Sprintf("(generateUUIDv4(), '%s', 'github', 'pull_request', '%d', '%s', 'ai_assisted', 'pr_label', 1.0, '{}', now64(3), now64(3))", org, n, repo)
	}
	const prCols = "git_pull_requests (repo_id, number, title, author_name, author_email, created_at, org_id, last_synced)"
	const attrCols = "ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source, confidence, evidence, observed_at, ingested_at)"
	repo := cutRepo(0)
	type scenario struct {
		name string
		seed func(org string, order []int)
	}
	cluster := func(org string, c int, title, author, email string, link string) (prs, attrs, links []string) {
		for k := 1; k <= 5; k++ {
			n := 10000 + 10*c + k
			prs = append(prs, pr(org, repo, n, title, author, email))
			attrs = append(attrs, attr(org, repo, n))
			if link != "" {
				links = append(links, fmt.Sprintf("('%s', 'wi-%03d', %d, 1.0, 'native', '', now64(3), '%s')", repo, c, n, org))
			}
		}
		return
	}
	scenarios := []scenario{
		{"prefix-extension", func(org string, order []int) {
			for start := 0; start < len(order); start += 12 {
				var prs, attrs []string
				for _, c := range order[start : start+12] {
					// Cluster c > 0 even extends the prefix of cluster c-1, so the 100-row cut falls inside a pair.
					title := fmt.Sprintf("Fix t%03d", (c+1)/2)
					if c > 0 && c%2 == 0 {
						title += " on main"
					}
					p, a, _ := cluster(org, c, title, "alice", "", "")
					prs, attrs = append(prs, p...), append(attrs, a...)
				}
				cutInsert(t, ctx, conn, prCols, prs)
				cutInsert(t, ctx, conn, attrCols, attrs)
			}
		}},
		{"repetitive-counts-and-repos", func(org string, order []int) {
			for start := 0; start < len(order); start += 12 {
				var prs, attrs []string
				for _, g := range order[start : start+12] {
					// Counts 5..7 across repositories; the prefix runs against the repository order.
					title := fmt.Sprintf("Fix p%03d", 119-g)
					for k := 1; k <= 5+g%3; k++ {
						prs = append(prs, pr(org, cutRepo(g), k, title, "alice", ""))
						attrs = append(attrs, attr(org, cutRepo(g), k))
					}
				}
				cutInsert(t, ctx, conn, prCols, prs)
				cutInsert(t, ctx, conn, attrCols, attrs)
			}
		}},
		{"author-email-and-name", func(org string, order []int) {
			for start := 0; start < len(order); start += 12 {
				var prs, attrs []string
				for _, c := range order[start : start+12] {
					author, email := fmt.Sprintf("u%03d", 119-c), ""
					if c%2 == 0 {
						email = fmt.Sprintf("u%03d@x.io", c)
					}
					p, a, _ := cluster(org, c, "Rename widget helper", author, email, "")
					prs, attrs = append(prs, p...), append(attrs, a...)
				}
				cutInsert(t, ctx, conn, prCols, prs)
				cutInsert(t, ctx, conn, attrCols, attrs)
			}
		}},
		{"work-type-linked-and-fallback", func(org string, order []int) {
			for start := 0; start < len(order); start += 12 {
				var prs, attrs, links, items []string
				for _, c := range order[start : start+12] {
					link, wtype := "", ""
					if c != 0 {
						link = "yes"
						wtype = []string{"bug", "pull_request_x", "aa", "zz"}[c%4] + fmt.Sprintf("%03d", 119-c)
					}
					p, a, l := cluster(org, c, "Rename widget helper", "alice", "", link)
					prs, attrs, links = append(prs, p...), append(attrs, a...), append(links, l...)
					if link != "" {
						items = append(items, fmt.Sprintf("('%s', 'wi-%03d', 'github', 'item', '%s', 'done', now64(3), now64(3), now64(3), '%s')", repo, c, wtype, org))
					}
				}
				cutInsert(t, ctx, conn, prCols, prs)
				cutInsert(t, ctx, conn, attrCols, attrs)
				cutInsert(t, ctx, conn, "work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)", links)
				cutInsert(t, ctx, conn, "work_items (repo_id, work_item_id, provider, title, type, status, created_at, updated_at, last_synced, org_id)", items)
			}
		}},
		{"dependency-counts", func(org string, order []int) {
			for start := 0; start < len(order); start += 12 {
				var prs []string
				for _, g := range order[start : start+12] {
					for k := 1; k <= 5+g%3; k++ {
						prs = append(prs, pr(org, cutRepo(g), k, "Bump lodash from 1 to 2 (deps)", "alice", ""))
					}
				}
				cutInsert(t, ctx, conn, prCols, prs)
			}
		}},
		{"migration-counts", func(org string, order []int) {
			for start := 0; start < len(order); start += 12 {
				var prs []string
				for _, g := range order[start : start+12] {
					for k := 1; k <= 5+(119-g)%3; k++ {
						prs = append(prs, pr(org, cutRepo(g), k, "Migrate widget api", "alice", ""))
					}
				}
				cutInsert(t, ctx, conn, prCols, prs)
			}
		}},
		{"docdrift-counts", func(org string, order []int) {
			for start := 0; start < len(order); start += 12 {
				var commits, stats []string
				for _, g := range order[start : start+12] {
					for n := 0; n < 20+g%3; n++ {
						h := fmt.Sprintf("%s-h%d-%d", org[len(org)-4:], g, n)
						commits = append(commits, fmt.Sprintf("('%s', '%s', now64(3) - INTERVAL 3 DAY, '%s', now64(3))", cutRepo(g), h, org))
						stats = append(stats, fmt.Sprintf("('%s', '%s', 'src/a.go', 1, 1, '%s', now64(3))", cutRepo(g), h, org))
					}
				}
				cutInsert(t, ctx, conn, "git_commits (repo_id, hash, committer_when, org_id, last_synced)", commits)
				cutInsert(t, ctx, conn, "git_commit_stats (repo_id, commit_hash, file_path, additions, deletions, org_id, last_synced)", stats)
			}
		}},
		{"flaky-equal-rounded-shares", func(org string, order []int) {
			for start := 0; start < len(order); start += 12 {
				var rows []string
				for _, g := range order[start : start+12] {
					flake, distinct := 40000001, 200000004
					if g%2 == 0 {
						flake, distinct = 40000002, 200000009
					}
					rows = append(rows, fmt.Sprintf("('%s', today() - 1, 100, 0.2, %d, %d, '%s', now())", cutRepo(g), flake, distinct, org))
				}
				cutInsert(t, ctx, conn, "testops_test_metrics_daily (repo_id, day, total_cases, flake_rate, flake_cases, distinct_cases, org_id, computed_at)", rows)
			}
		}},
	}
	fwd, rev := make([]int, 120), make([]int, 120)
	for i := range fwd {
		fwd[i], rev[i] = i, 119-i
	}
	orgs := map[string][2]string{}
	for k, s := range scenarios {
		f, r := fmt.Sprintf("00000000-0000-0000-0000-00000014259%d", k), fmt.Sprintf("00000000-0000-0000-0000-00000014258%d", k)
		orgs[s.name] = [2]string{f, r}
		s.seed(f, fwd)
		s.seed(r, rev)
	}
	check := func(phase string) {
		for _, s := range scenarios {
			for _, org := range orgs[s.name] {
				for _, n := range []int{1, 5, 100} {
					got, err := AiOpportunities(ctx, client, org, nil, n)
					if err != nil {
						t.Fatal(err)
					}
					want, err := AiOpportunities(ctx, uncutClient{client}, org, nil, n)
					if err != nil {
						t.Fatal(err)
					}
					if g, w := cutCards(got.Recommendations), cutCards(want.Recommendations); g != w {
						t.Errorf("%s %s org %s N=%d: the cut serves\n %s\nwithout the cut\n %s", phase, s.name, org, n, g, w)
					}
				}
			}
		}
	}
	check("separate parts")
	cutOptimize(t, ctx, conn)
	check("after OPTIMIZE FINAL")
}

func cutCards(recs []model.AIOpportunity) string {
	var out []string
	for _, r := range recs {
		ref := ""
		if len(r.EvidenceRefs) > 0 {
			ref = r.EvidenceRefs[0]
		}
		out = append(out, fmt.Sprintf("%s|%s|%v|%s|%s", r.Kind, *r.RepoID, r.Score, r.Rationale, ref))
	}
	return strings.Join(out, "\n ")
}

// TestRealClickHouse_AIRepetitiveAuthorKeyIsNeverServed: the author key orders repetitive clusters but is never in
// the served payload. CHAOS-14258.
func TestRealClickHouse_AIRepetitiveAuthorKeyIsNeverServed(t *testing.T) {
	ctx, conn, client := startStore(t)
	org, repo := "00000000-0000-0000-0000-000000142590", cutRepo(0)
	var prs, attrs []string
	for c := 0; c < 3; c++ {
		for k := 1; k <= 5; k++ {
			n := 100*c + k
			prs = append(prs, fmt.Sprintf("('%s', %d, 'Rename widget helper', 'Leak Marker Name %d', 'leak.marker.%d@example.invalid', now64(3) - INTERVAL 2 DAY, '%s', now64(3))", repo, n, c, c, org))
			attrs = append(attrs, fmt.Sprintf("(generateUUIDv4(), '%s', 'github', 'pull_request', '%d', '%s', 'ai_assisted', 'pr_label', 1.0, '{}', now64(3), now64(3))", org, n, repo))
		}
	}
	cutInsert(t, ctx, conn, "git_pull_requests (repo_id, number, title, author_name, author_email, created_at, org_id, last_synced)", prs)
	cutInsert(t, ctx, conn, "ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source, confidence, evidence, observed_at, ingested_at)", attrs)
	got, err := AiOpportunities(ctx, client, org, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Recommendations) != 3 {
		t.Fatalf("served %d cards, want the 3 author clusters", len(got.Recommendations))
	}
	body, _ := json.Marshal(got)
	if s := strings.ToLower(string(body)); strings.Contains(s, "leak.marker") || strings.Contains(s, "leak marker") {
		t.Errorf("the served payload holds an author key: %s", body)
	}
	// The id is not hashed from the author key either (CHAOS-14266): it is the hash of the kind, the repository, the
	// title prefix, the work type and the cluster's anchor (its oldest pull request; one INSERT gives every row one
	// created_at, so the lowest number), one id per author cluster.
	want := map[string]bool{}
	for c := 0; c < 3; c++ {
		want[stableOpportunityID("repetitive_change", repo, repetitiveClusterKey("rename widget helper", "pull_request", uint32(100*c+1)))] = true
	}
	served := map[string]bool{}
	for _, r := range got.Recommendations {
		if !want[r.OpportunityID] || served[r.OpportunityID] {
			t.Errorf("opportunityId %s is not one author-free cluster id served once (the author key?)", r.OpportunityID)
		}
		served[r.OpportunityID] = true
	}
}
