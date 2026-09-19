//go:build integration

package aianalytics

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	org1 = "00000000-0000-0000-0000-0000000000a1"
	org2 = "00000000-0000-0000-0000-0000000000a2"
	rA   = "11111111-1111-1111-1111-111111111111" // exists in BOTH orgs
	rB   = "22222222-2222-2222-2222-222222222222"
)

func startStore(t *testing.T) (context.Context, chdriver.Conn, QueryClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	t.Cleanup(cancel)
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = inst.Close(context.Background()) })
	chschema.Apply(ctx, t, inst)
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return ctx, conn, client
}

func exec(t *testing.T, ctx context.Context, conn chdriver.Conn, q string, a ...any) {
	t.Helper()
	if err := conn.Exec(ctx, fmt.Sprintf(q, a...)); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// rollup seeds one ai_impact_metrics_daily row version.
func rollup(t *testing.T, ctx context.Context, conn chdriver.Conn, org, team, repo, workType, day, bucket string, prs, merged, aiPrs int, rework float64, computed string) {
	exec(t, ctx, conn, `INSERT INTO ai_impact_metrics_daily (org_id, team_id, repo_id, work_type, day, attribution_bucket,
        prs_total, prs_merged, ai_assisted_prs, rework_drag_rate, reviews_per_pr, computed_at)
        SELECT '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d, %v, 1.5, toDateTime64('%s', 3, 'UTC')`,
		org, team, repo, workType, day, bucket, prs, merged, aiPrs, rework, computed)
}

func seedStore(t *testing.T, ctx context.Context, conn chdriver.Conn) {
	for _, r := range []struct{ org, id, name string }{
		{org1, rA, "acme/alpha"}, {org1, rB, "acme/beta"}, {org2, rA, "other/alpha"},
	} {
		exec(t, ctx, conn, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced)
            SELECT '%s', '%s', 'github', '%s', now64(3), now64(3)`, r.id, r.name, r.org)
	}
	exec(t, ctx, conn, `INSERT INTO teams (id, team_uuid, name, members, repo_patterns, org_id, updated_at)
        SELECT 'team-a', generateUUIDv4(), 'Team A', [], ['acme/alpha'], '%s', now64(6)`, org1)
	exec(t, ctx, conn, `INSERT INTO teams (id, team_uuid, name, members, repo_patterns, org_id, updated_at)
        SELECT 'team-a', generateUUIDv4(), 'Other Team A', [], ['other/*'], '%s', now64(6)`, org2)

	exec(t, ctx, conn, `INSERT INTO teams (id, team_uuid, name, members, repo_patterns, org_id, updated_at)
        SELECT 'team-b', generateUUIDv4(), 'Team B', [], ['other/*'], '%s', now64(6)`, org1)

	// org1: a stale version of the first row (older computed_at) must lose.
	rollup(t, ctx, conn, org1, "team-a", rA, "pull_request", "2026-08-01", "ai_assisted", 99, 99, 99, 0.9, "2026-08-01 00:00:00")
	rollup(t, ctx, conn, org1, "team-a", rA, "pull_request", "2026-08-01", "ai_assisted", 7, 5, 7, 0.25, "2026-08-02 00:00:00")
	rollup(t, ctx, conn, org1, "team-a", rA, "pull_request", "2026-08-01", "human", 3, 3, 0, 0.1, "2026-08-02 00:00:00")
	rollup(t, ctx, conn, org1, "", rB, "issue", "2026-08-02", "agent_created", 4, 0, 0, 0.5, "2026-08-02 00:00:00")
	// outside the window
	rollup(t, ctx, conn, org1, "team-a", rA, "pull_request", "2026-07-15", "human", 50, 50, 0, 0.1, "2026-07-16 00:00:00")
	// org2 shares repo id rA, the day and the bucket: must never reach org1.
	rollup(t, ctx, conn, org2, "team-a", rA, "pull_request", "2026-08-01", "ai_assisted", 1000, 1000, 1000, 0.99, "2026-08-02 00:00:00")

	// raw pull requests: (rA,#1) exists in both orgs with different content.
	pr := func(org, repo string, n int, created, merged, firstReview string, comments, add, del int) {
		exec(t, ctx, conn, `INSERT INTO git_pull_requests (repo_id, number, title, created_at, merged_at, first_review_at,
            comments_count, additions, deletions, org_id, last_synced)
            SELECT '%s', %d, 't', toDateTime64('%s', 3, 'UTC'), toDateTime64('%s', 3, 'UTC'), toDateTime64('%s', 3, 'UTC'),
            %d, %d, %d, '%s', now64(3)`, repo, n, created, merged, firstReview, comments, add, del, org)
	}
	pr(org1, rA, 1, "2026-08-01 01:00:00", "2026-08-01 05:00:00", "2026-08-01 03:00:00", 10, 30, 10)
	pr(org1, rA, 2, "2026-08-01 01:00:00", "2026-08-01 06:00:00", "2026-08-01 02:00:00", 4, 5, 5)
	pr(org1, rB, 7, "2026-08-01 01:00:00", "2026-08-01 05:00:00", "2026-08-01 02:00:00", 777, 100, 0)
	pr(org2, rA, 1, "2026-08-01 01:00:00", "2026-08-01 05:00:00", "2026-08-01 01:30:00", 500, 5000, 5000)
	// attribution: pr #1 of rA is AI-assisted in org1 and HUMAN in org2.
	for _, a := range []struct{ org, kind string }{{org1, "ai_assisted"}, {org2, "human"}} {
		exec(t, ctx, conn, `INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source,
            confidence, evidence, observed_at, ingested_at)
            SELECT generateUUIDv4(), '%s', 'github', 'pull_request', '1', '%s', '%s', 'pr_label', 1.0, '{}', toDateTime64('2026-08-01 12:00:00', 3, 'UTC'), now64(3)`,
			a.org, rA, a.kind)
	}
	// reviewers of repositories with AI attribution: org1 has 2, org2 has 1 huge.
	for _, u := range []struct {
		org, who string
		n        int
	}{{org1, "alice@x", 3}, {org1, "bob@x", 1}, {org2, "eve@x", 9999}} {
		exec(t, ctx, conn, `INSERT INTO user_metrics_daily (repo_id, day, author_email, reviews_given, org_id, computed_at)
            SELECT '%s', '2026-08-01', '%s', %d, '%s', now64(3)`, rA, u.who, u.n, u.org)
	}
}

func dateRange() model.AIDateRangeInput {
	return model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}
}

func sp(s string) *string { return &s }

func TestRealClickHouse_AIRollups(t *testing.T) {
	ctx, conn, client := startStore(t)
	seedStore(t, ctx, conn)

	t.Run("summary reads the newest version inside the window for its own org", func(t *testing.T) {
		got, err := ImpactSummary(ctx, client, org1, dateRange(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if got.TotalPrs != 14 || got.AiAssistedPrs != 7 || got.HumanPrs != 0 && got.HumanPrs != 3 {
			t.Fatalf("totals %+v", got)
		}
		if len(got.Daily) != 3 {
			t.Fatalf("daily rows = %d, want 3", len(got.Daily))
		}
		if len(got.RepoBreakdown) == 0 || got.RepoBreakdown[0].ScopeLabel != "acme/alpha" {
			t.Fatalf("repo label must come from the caller's org: %+v", got.RepoBreakdown)
		}
		other, err := ImpactSummary(ctx, client, org2, dateRange(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if other.TotalPrs != 1000 || other.RepoBreakdown[0].ScopeLabel != "other/alpha" {
			t.Fatalf("org2 view %+v", other)
		}
	})

	t.Run("summary scope branches", func(t *testing.T) {
		cases := []struct {
			name  string
			scope *model.AIScopeInput
			want  int
		}{
			{"repo uuid", &model.AIScopeInput{RepoID: sp(rB)}, 4},
			{"repo name", &model.AIScopeInput{RepoID: sp("acme/beta")}, 4},
			{"repo name of another org", &model.AIScopeInput{RepoID: sp("other/alpha")}, 0},
			{"stored team id", &model.AIScopeInput{TeamID: sp("team-a")}, 10},
			{"work type", &model.AIScopeInput{WorkType: sp("issue")}, 4},
			{"bucket", &model.AIScopeInput{Buckets: []model.AIAttributionBucketInput{model.AIAttributionBucketInputHuman}}, 3},
			{"unknown team", &model.AIScopeInput{TeamID: sp("nope")}, 0},
		}
		for _, c := range cases {
			got, err := ImpactSummary(ctx, client, org1, dateRange(), c.scope)
			if err != nil {
				t.Fatalf("%s: %v", c.name, err)
			}
			if got.TotalPrs != c.want {
				t.Errorf("%s: total prs = %d, want %d", c.name, got.TotalPrs, c.want)
			}
		}
	})

	t.Run("comparison", func(t *testing.T) {
		got, err := Comparison(ctx, client, org1, dateRange(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if got.AiSide.PrsTotal != 11 || got.BaselineSide.PrsTotal != 3 || !got.DataAvailable {
			t.Fatalf("%+v %+v", got.AiSide, got.BaselineSide)
		}
	})

	t.Run("review load engagement and concentration stay inside the org", func(t *testing.T) {
		got, err := ReviewLoad(ctx, client, org1, dateRange(), nil)
		if err != nil {
			t.Fatal(err)
		}
		var ai *model.AIReviewLoadRow
		for i := range got.ByBucket {
			if got.ByBucket[i].Bucket == "ai_assisted" {
				ai = &got.ByBucket[i]
			}
		}
		if ai == nil || ai.PickupLatencyHours == nil || *ai.PickupLatencyHours != 2.0 {
			t.Fatalf("ai_assisted pickup latency must come from org1's row only: %+v", ai)
		}
		if ai.ReviewCommentsPerLoc == nil || *ai.ReviewCommentsPerLoc != 10.0/40.0 {
			t.Fatalf("comments per loc must exclude org2's pull request: %+v", ai)
		}
		if !got.ReviewerConcentration.DataAvailable || got.ReviewerConcentration.ReviewerCount != 2 {
			t.Fatalf("reviewers must be org1's two: %+v", got.ReviewerConcentration)
		}
	})

	t.Run("team scope selects repositories through the caller's own org", func(t *testing.T) {
		ids, ok := teamRepoIDs(ctx, client, org1, "team-a", "aiReviewLoad")
		if !ok || len(ids) != 1 || ids[0] != rA {
			t.Fatalf("org1 team-a = %v %v, want [%s]", ids, ok, rA)
		}
		// team-b's pattern names other/*, which only org2 has: org1 owns nothing for it.
		ids, ok = teamRepoIDs(ctx, client, org1, "team-b", "aiReviewLoad")
		if !ok || len(ids) != 0 {
			t.Fatalf("org1 team-b = %v %v, want none", ids, ok)
		}
		all, err := loadEngagement(ctx, client, org1, mustDay("2026-08-01"), mustDay("2026-08-03"), "", nil, false)
		if err != nil {
			t.Fatal(err)
		}
		scoped, err := loadEngagement(ctx, client, org1, mustDay("2026-08-01"), mustDay("2026-08-03"), "", []string{rA}, true)
		if err != nil {
			t.Fatal(err)
		}
		unknownComments := func(rows []engagementRow) int64 {
			for _, r := range rows {
				if r.Bucket == "unknown" {
					return r.CommentsTotal
				}
			}
			return -1
		}
		if unknownComments(all) != 781 || unknownComments(scoped) != 4 {
			t.Fatalf("repository list must exclude rB's pull request: all=%v scoped=%v", all, scoped)
		}
		byRepo, err := loadEngagement(ctx, client, org1, mustDay("2026-08-01"), mustDay("2026-08-03"), rB, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(byRepo) != 1 || byRepo[0].Bucket != "unknown" || byRepo[0].CommentsTotal != 777 {
			t.Fatalf("repo scope: %+v", byRepo)
		}
	})
}
