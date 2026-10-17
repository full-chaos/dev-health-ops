//go:build integration

package aianalytics

import (
	"testing"
)

// The raw pull-request engagement is bucketed in the query, and must name the
// same bucket as the daily rollup (aiimpact.ClassifyBucket): a pull request with
// no AI signal is human, one by a [bot] author is bot, and only a stored kind
// that names no bucket is unknown. Before the human default the query put every
// unattributed pull request in unknown, so the human row of the review-load card
// lost its pickup latency and comment density.
func TestRealClickHouse_EngagementBucketsMatchTheRollupClassifier(t *testing.T) {
	ctx, conn, client := startStore(t)
	const repo = "33333333-3333-3333-3333-333333333333"
	exec(t, ctx, conn, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced)
        SELECT '%s', 'acme/gamma', 'github', '%s', now64(3), now64(3)`, repo, org1)

	for _, bucket := range []string{"human", "bot", "unknown", "ai_assisted"} {
		rollup(t, ctx, conn, org1, "", repo, "pull_request", "2026-08-01", bucket, 1, 1, 0, 0.1, "2026-08-02 00:00:00")
	}
	// number, author, first review after 2h (human), 4h (bot), 6h (garbage kind), 8h (AI).
	pr := func(number int, author string, reviewHours int, comments int) {
		exec(t, ctx, conn, `INSERT INTO git_pull_requests (repo_id, number, title, author_name, created_at, merged_at, first_review_at,
            comments_count, additions, deletions, org_id, last_synced)
            SELECT '%s', %d, 't', '%s', toDateTime64('2026-08-01 01:00:00', 3, 'UTC'), toDateTime64('2026-08-01 20:00:00', 3, 'UTC'),
            toDateTime64('2026-08-01 01:00:00', 3, 'UTC') + INTERVAL %d HOUR, %d, 30, 10, '%s', now64(3)`,
			repo, number, author, reviewHours, comments, org1)
	}
	pr(1, "octocat", 2, 10)
	pr(2, "dependabot[bot]", 4, 20)
	pr(3, "octocat", 6, 40)
	pr(4, "octocat", 8, 80)
	attr := func(number int, kind string) {
		exec(t, ctx, conn, `INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source,
            confidence, evidence, observed_at, ingested_at)
            SELECT generateUUIDv4(), '%s', 'github', 'pull_request', '%d', '%s', '%s', 'pr_label', 1.0, '{}',
            toDateTime64('2026-08-01 12:00:00', 3, 'UTC'), now64(3)`, org1, number, repo, kind)
	}
	attr(3, "vibes")
	attr(4, "ai_assisted")

	raw, err := loadEngagement(ctx, client, org1, day("2026-08-01").Time(), day("2026-08-03").Time(), repo, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	pickup := map[string]float64{}
	for _, row := range raw {
		if row.PickupLatency == nil {
			t.Fatalf("bucket %s has no pickup latency: %+v", row.Bucket, row)
		}
		pickup[row.Bucket] = *row.PickupLatency
	}
	want := map[string]float64{"human": 2, "bot": 4, "unknown": 6, "ai_assisted": 8}
	if len(pickup) != len(want) {
		t.Fatalf("engagement buckets = %v, want %v", pickup, want)
	}
	for bucket, hours := range want {
		if pickup[bucket] != hours {
			t.Errorf("bucket %s pickup latency = %v, want %v (all: %v)", bucket, pickup[bucket], hours, pickup)
		}
	}
}
