//go:build integration

package aianalytics

import (
	"context"
	"strings"
	"testing"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// seedGroupD adds rows the detectors read. Every org-1 row has an org-2 twin
// sharing its keys with different content.
func seedGroupD(t *testing.T, ctx context.Context, conn chdriver.Conn) {
	// rollups inside the last 30 days: org-1's human PRs miss tests, org-2's do not.
	rollup30 := func(org, repo, bucket string, prs, gap int) {
		exec(t, ctx, conn, `INSERT INTO ai_impact_metrics_daily (org_id, team_id, repo_id, work_type, day, attribution_bucket,
            prs_total, prs_merged, test_gap_prs, computed_at)
            SELECT '%s', 'team-a', '%s', 'pull_request', today() - 1, '%s', %d, %d, %d, now64(3)`, org, repo, bucket, prs, prs, gap)
	}
	rollup30(org1, rA, "human", 10, 6)
	rollup30(org2, rA, "human", 10, 0)

	pr := func(org, repo string, n int, title, author string) {
		exec(t, ctx, conn, `INSERT INTO git_pull_requests (repo_id, number, title, author_name, created_at, org_id, last_synced)
            SELECT '%s', %d, '%s', '%s', now64(3) - INTERVAL 2 DAY, '%s', now64(3)`, repo, n, title, author, org)
	}
	attr := func(org, repo string, n int, kind string) {
		exec(t, ctx, conn, `INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source,
            confidence, evidence, observed_at, ingested_at)
            SELECT generateUUIDv4(), '%s', 'github', 'pull_request', '%d', '%s', '%s', 'pr_label', 1.0, '{}', now64(3), now64(3)`, org, n, repo, kind)
	}
	// repetitive: five AI PRs of one author and title prefix in rB (org-1); org-2 twins.
	for i := 101; i <= 105; i++ {
		pr(org1, rB, i, "Rename widget helper", "carol")
		attr(org1, rB, i, "ai_assisted")
		pr(org2, rB, i, "Rename widget helper", "carol")
		attr(org2, rB, i, "ai_assisted")
	}
	// dependency toil: five human bump PRs in rA (org-1); org-2 attributes the same numbers to AI.
	for i := 201; i <= 205; i++ {
		pr(org1, rA, i, "Bump lodash from 1 to 2 (deps)", "alice")
		pr(org2, rA, i, "Bump lodash from 1 to 2 (deps)", "alice")
		attr(org2, rA, i, "ai_assisted")
	}
	pr(org1, rA, 206, "Bump left-pad from 1 to 2 (deps)", "dependabot[bot]")

	// documentation drift: twenty code commits without doc changes in rA (org-1);
	// org-2 has the same hashes touching docs.
	for i := 0; i < 20; i++ {
		h := "c" + string(rune('a'+i))
		exec(t, ctx, conn, `INSERT INTO git_commits (repo_id, hash, committer_when, org_id, last_synced)
            SELECT '%s', '%s', now64(3) - INTERVAL 3 DAY, '%s', now64(3)`, rA, h, org1)
		exec(t, ctx, conn, `INSERT INTO git_commit_stats (repo_id, commit_hash, file_path, additions, deletions, org_id, last_synced)
            SELECT '%s', '%s', 'src/a.go', 1, 1, '%s', now64(3)`, rA, h, org1)
		exec(t, ctx, conn, `INSERT INTO git_commit_stats (repo_id, commit_hash, file_path, additions, deletions, org_id, last_synced)
            SELECT '%s', '%s', 'README.md', 1, 1, '%s', now64(3)`, rA, h, org2)
	}

	// flaky: one stale and one current version of a day in org-1; org-2 twin is stable.
	flaky := func(org string, flake float64, computed string) {
		exec(t, ctx, conn, `INSERT INTO testops_test_metrics_daily (repo_id, day, total_cases, flake_rate, org_id, computed_at)
            SELECT '%s', today() - 1, 100, %v, '%s', toDateTime('%s', 'UTC')`, rB, flake, org, computed)
	}
	flaky(org1, 0.9, "2026-09-01 00:00:00")
	flaky(org1, 0.10, "2026-09-02 00:00:00")
	flaky(org2, 0.0, "2026-09-02 00:00:00")

	// flow: six days of repository and team metrics; org-2 twins are healthy.
	for d := 1; d <= 6; d++ {
		for _, o := range []struct {
			org    string
			review float64
			cycle  float64
		}{{org1, 30.0, 200.0}, {org2, 1.0, 1.0}} {
			exec(t, ctx, conn, `INSERT INTO repo_metrics_daily (repo_id, day, pr_first_review_p50_hours, org_id, computed_at)
                SELECT '%s', today() - %d, %v, '%s', now64(3)`, rA, d, o.review, o.org)
			exec(t, ctx, conn, `INSERT INTO work_item_metrics_daily (day, team_id, cycle_time_p50_hours, items_completed, org_id, computed_at)
                SELECT today() - %d, 'team-a', %v, 5, '%s', now64(3)`, d, o.cycle, o.org)
		}
	}
}

func kinds(recs []model.AIOpportunity) map[model.AIOpportunityKind]model.AIOpportunity {
	out := map[model.AIOpportunityKind]model.AIOpportunity{}
	for _, r := range recs {
		out[r.Kind] = r
	}
	return out
}

func TestRealClickHouse_AIDetectors(t *testing.T) {
	ctx, conn, client := startStore(t)
	seedStore(t, ctx, conn)
	seedGroupD(t, ctx, conn)

	t.Run("AI opportunities read only the org's rows", func(t *testing.T) {
		got, err := AiOpportunities(ctx, client, org1, nil, 100)
		if err != nil {
			t.Fatal(err)
		}
		byKind := kinds(got.Recommendations)
		for _, want := range []model.AIOpportunityKind{
			model.AIOpportunityKindTestGeneration, model.AIOpportunityKindRepetitiveChange,
			model.AIOpportunityKindDependencyUpdates, model.AIOpportunityKindDocumentationDrift,
			model.AIOpportunityKindFlakyTestTriage,
		} {
			if _, ok := byKind[want]; !ok {
				t.Errorf("missing %s: %+v", want, got.Recommendations)
			}
		}
		if r := byKind[model.AIOpportunityKindRepetitiveChange]; !strings.HasPrefix(r.Rationale, "5 AI-assisted PRs") {
			t.Errorf("repetitive count must not include org-2's twins: %q", r.Rationale)
		}
		if r := byKind[model.AIOpportunityKindDependencyUpdates]; !strings.HasPrefix(r.Rationale, "5 dependency-update PRs") {
			t.Errorf("org-2's attribution must not suppress org-1's pull requests, and the bot is excluded: %q", r.Rationale)
		}
		if r := byKind[model.AIOpportunityKindFlakyTestTriage]; !strings.Contains(r.Rationale, "10.0%") {
			t.Errorf("the newest version of the day must win: %q", r.Rationale)
		}
		other, err := AiOpportunities(ctx, client, org2, nil, 100)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := kinds(other.Recommendations)[model.AIOpportunityKindTestGeneration]; ok {
			t.Errorf("org-2 has no test gap: %+v", other.Recommendations)
		}
		if _, ok := kinds(other.Recommendations)[model.AIOpportunityKindDocumentationDrift]; ok {
			t.Errorf("org-2's commits touch documentation: %+v", other.Recommendations)
		}
	})

	t.Run("AI opportunity scope and limit branches", func(t *testing.T) {
		repo, team, none := rA, "team-a", "team-none"
		got, err := AiOpportunities(ctx, client, org1, &model.AIScopeInput{RepoID: &repo}, 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range got.Recommendations {
			if r.RepoID == nil || *r.RepoID != rA {
				t.Errorf("repository scope leaked %+v", r)
			}
		}
		if len(got.Recommendations) == 0 {
			t.Error("repository scope answered nothing")
		}
		teamed, err := AiOpportunities(ctx, client, org1, &model.AIScopeInput{TeamID: &team}, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(teamed.Recommendations) != 1 || teamed.Recommendations[0].Kind != model.AIOpportunityKindTestGeneration {
			t.Errorf("a team scope reads the rollups only: %+v", teamed.Recommendations)
		}
		empty, err := AiOpportunities(ctx, client, org1, &model.AIScopeInput{TeamID: &none}, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(empty.Recommendations) != 0 {
			t.Errorf("unknown team: %+v", empty.Recommendations)
		}
		one, err := AiOpportunities(ctx, client, org1, nil, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(one.Recommendations) != 1 {
			t.Errorf("limit one: %d", len(one.Recommendations))
		}
	})

	t.Run("flow opportunities read only the org's metrics", func(t *testing.T) {
		got, err := FlowOpportunities(ctx, client, org1, nil, 10, 30)
		if err != nil {
			t.Fatal(err)
		}
		var review, cycle bool
		for _, o := range got.Opportunities {
			review = review || o.Kind == model.ImproveOpportunityKindHighReviewLatency
			cycle = cycle || o.Kind == model.ImproveOpportunityKindSlowCycleTime
		}
		if !review || !cycle || !got.DetectorReady || got.TotalCount != len(got.Opportunities) {
			t.Fatalf("org-1: %+v", got)
		}
		other, err := FlowOpportunities(ctx, client, org2, nil, 10, 30)
		if err != nil {
			t.Fatal(err)
		}
		if len(other.Opportunities) != 0 {
			t.Fatalf("org-2 is healthy: %+v", other.Opportunities)
		}
		narrow, err := FlowOpportunities(ctx, client, org1, nil, 10, 3)
		if err != nil {
			t.Fatal(err)
		}
		if len(narrow.Opportunities) != 0 {
			t.Fatalf("three days hold fewer than five days of data: %+v", narrow.Opportunities)
		}
		team, none := "team-a", "team-none"
		teamed, err := FlowOpportunities(ctx, client, org1, &model.AIScopeInput{TeamID: &team}, 10, 30)
		if err != nil {
			t.Fatal(err)
		}
		if len(teamed.Opportunities) != 2 {
			t.Fatalf("team scope narrows only the team read: %+v", teamed.Opportunities)
		}
		absent, err := FlowOpportunities(ctx, client, org1, &model.AIScopeInput{TeamID: &none}, 10, 30)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range absent.Opportunities {
			if o.EntityType == "team" {
				t.Fatalf("unknown team answered %+v", o)
			}
		}
	})
}
