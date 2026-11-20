//go:build integration

package aianalytics

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-14266 on the real read: one repository with four repetitive-change clusters (two authors with one title
// prefix, a second prefix of one author, and a cluster whose oldest pull request has its highest number) serves four
// ids, each the hash of its cluster's prefix, work type and anchor (argMin of the number by created_at, then number),
// and the answer holds no author.
func TestRealClickHouse_RepetitiveClustersOfOneRepoHaveTheirOwnIDs(t *testing.T) {
	ctx, conn, client := startStore(t)
	const repo = "44444444-4444-4444-4444-444444444444"
	pr := func(n int, title, authorName, authorEmail string, daysAgo int) {
		exec(t, ctx, conn, `INSERT INTO git_pull_requests (repo_id, number, title, author_name, author_email, created_at, org_id, last_synced)
            SELECT '%s', %d, '%s', '%s', nullIf('%s', ''), toStartOfDay(now()) - INTERVAL %d DAY, '%s', now64(3)`, repo, n, title, authorName, authorEmail, daysAgo, org1)
		exec(t, ctx, conn, `INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source,
            confidence, evidence, observed_at, ingested_at)
            SELECT generateUUIDv4(), '%s', 'github', 'pull_request', '%d', '%s', 'ai_assisted', 'pr_label', 1.0, '{}', now64(3), now64(3)`, org1, n, repo)
	}
	for i := 0; i < 5; i++ {
		pr(301+i, "Rename widget helper", "Alice", secretEmail14266, 2)
		pr(311+i, "Rename widget helper", secretName14266, "", 2)
		pr(321+i, "Bump the lockfile now", "Alice", secretEmail14266, 2)
		// 405 is the oldest pull request of its cluster and has its highest number.
		pr(401+i, "Tidy the docs", "Carol", "", 2+3*(i/4))
	}

	got, err := AiOpportunities(ctx, client, org1, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		// Equal created_at inside these three clusters: the number breaks the tie, so the anchor is the lowest number.
		stableOpportunityID("repetitive_change", repo, repetitiveClusterKey("rename widget helper", "pull_request", 301)): true,
		stableOpportunityID("repetitive_change", repo, repetitiveClusterKey("rename widget helper", "pull_request", 311)): true,
		stableOpportunityID("repetitive_change", repo, repetitiveClusterKey("bump the lockfile", "pull_request", 321)):    true,
		stableOpportunityID("repetitive_change", repo, repetitiveClusterKey("tidy the docs", "pull_request", 405)):        true,
	}
	served := map[string]bool{}
	for _, r := range got.Recommendations {
		if r.Kind != model.AIOpportunityKindRepetitiveChange {
			continue
		}
		if served[r.OpportunityID] {
			t.Errorf("opportunityId %s is served twice", r.OpportunityID)
		}
		served[r.OpportunityID] = true
		if !want[r.OpportunityID] {
			t.Errorf("opportunityId %s (%q) is not the hash of a seeded cluster key", r.OpportunityID, r.Rationale)
		}
	}
	if len(served) != len(want) {
		t.Errorf("served %d repetitive ids, want %d: %+v", len(served), len(want), got.Recommendations)
	}
	body, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{secretEmail14266, "alice.secret", "example.com", secretName14266, "Hidden-Name"} {
		if strings.Contains(string(body), part) {
			t.Errorf("the served answer holds %q", part)
		}
	}
}
