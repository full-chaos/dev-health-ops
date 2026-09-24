//go:build integration

package aianalytics

import (
	"context"
	"testing"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// seedGroupB adds the risk, linkage and attribution rows the second group of
// fields reads. Every org-1 row has an org-2 twin with the same repository,
// key and path but different content.
func seedGroupB(t *testing.T, ctx context.Context, conn chdriver.Conn) {
	// linked attribution leg: work item ABC-123 links PR 2 of rA and is
	// attributed without a repository.
	exec(t, ctx, conn, `INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
        SELECT '%s', 'ABC-123', 2, 1.0, 'native', '', now64(3), '%s'`, rA, org1)
	exec(t, ctx, conn, `INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source,
        confidence, evidence, observed_at, ingested_at)
        SELECT generateUUIDv4(), '%s', 'linear', 'pull_request', 'ABC-123', NULL, 'agent_created', 'issue_label', 0.9, '{}',
        toDateTime64('2026-08-02 09:00:00', 3, 'UTC'), now64(3)`, org1)
	// the work item exists in both orgs with different types
	for _, w := range []struct{ org, typ string }{{org1, "story"}, {org2, "bug"}} {
		exec(t, ctx, conn, `INSERT INTO work_items (repo_id, work_item_id, provider, title, type, status, created_at, updated_at, last_synced, org_id)
            SELECT '%s', 'ABC-123', 'linear', 't', '%s', 'done', now64(3), now64(3), now64(3), '%s'`, rA, w.typ, w.org)
	}
	// commit linkage and files: same commit hash in both orgs
	for _, c := range []struct{ org, file string }{{org1, "a.go"}, {org1, "b.go"}, {org2, "z.go"}} {
		exec(t, ctx, conn, `INSERT INTO git_commit_stats (repo_id, commit_hash, file_path, additions, deletions, org_id, last_synced)
            SELECT '%s', 'h1', '%s', 1, 1, '%s', now64(3)`, rA, c.file, c.org)
	}
	for _, o := range []string{org1, org2} {
		exec(t, ctx, conn, `INSERT INTO work_graph_pr_commit (repo_id, pr_number, commit_hash, confidence, provenance, evidence, last_synced, org_id)
            SELECT '%s', 1, 'h1', 1.0, 'native', '', now64(3), '%s'`, rA, o)
	}
	for _, h := range []struct {
		org, file string
		risk      float64
	}{{org1, "a.go", 5.0}, {org2, "z.go", 9.0}} {
		exec(t, ctx, conn, `INSERT INTO file_hotspot_daily (repo_id, day, file_path, risk_score, org_id, computed_at)
            SELECT '%s', '2026-08-01', '%s', %v, '%s', now()`, rA, h.file, h.risk, h.org)
		exec(t, ctx, conn, `INSERT INTO file_complexity_snapshots (repo_id, as_of_day, ref, file_path, high_complexity_functions, org_id, computed_at)
            SELECT '%s', '2026-08-01', 'main', '%s', 2, '%s', now()`, rA, h.file, h.org)
	}
}

func TestRealClickHouse_AIGroupB(t *testing.T) {
	ctx, conn, client := startStore(t)
	seedStore(t, ctx, conn)
	seedGroupB(t, ctx, conn)
	dr := dateRange()

	t.Run("risk breakdown overlap reads only the org's files", func(t *testing.T) {
		got, err := RiskBreakdown(ctx, client, org1, dr, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.HotspotOverlap) != 1 || got.HotspotOverlap[0].Bucket != "ai_assisted" ||
			got.HotspotOverlap[0].PrsTotal != 1 || got.HotspotOverlap[0].PrsTouchingHotspots != 1 ||
			got.HotspotOverlap[0].AvgHotspotRiskScore == nil || *got.HotspotOverlap[0].AvgHotspotRiskScore != 5.0 {
			t.Fatalf("hotspot overlap %+v", got.HotspotOverlap)
		}
		if len(got.ComplexityOverlap) != 1 || got.ComplexityOverlap[0].PrsTouchingHighComplexity != 1 {
			t.Fatalf("complexity overlap %+v", got.ComplexityOverlap)
		}
		other, err := RiskBreakdown(ctx, client, org2, dr, nil)
		if err != nil {
			t.Fatal(err)
		}
		// org2's only linked pull request is attributed human, so nothing is AI.
		if len(other.HotspotOverlap) != 0 {
			t.Fatalf("org2 must have no AI overlap: %+v", other.HotspotOverlap)
		}
	})

	t.Run("risk breakdown scope branches", func(t *testing.T) {
		wt := "issue"
		team, none := "team-a", "team-none"
		for name, c := range map[string]struct {
			scope   *model.AIScopeInput
			overlap int
			buckets int
		}{
			"work type":    {&model.AIScopeInput{WorkType: &wt}, 0, 1},
			"team":         {&model.AIScopeInput{TeamID: &team}, 1, 2},
			"unknown team": {&model.AIScopeInput{TeamID: &none}, 0, 0},
		} {
			got, err := RiskBreakdown(ctx, client, org1, dr, c.scope)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if len(got.HotspotOverlap) != c.overlap || len(got.ByBucket) != c.buckets {
				t.Errorf("%s: overlap %d buckets %d, want %d %d", name, len(got.HotspotOverlap), len(got.ByBucket), c.overlap, c.buckets)
			}
		}
	})

	t.Run("attributed pull requests: linked and direct legs, one work item per org", func(t *testing.T) {
		got, err := AttributedPrs(ctx, client, org1, dr, nil, 50, 0)
		if err != nil {
			t.Fatal(err)
		}
		byNumber := map[int]model.AiAttributedPr{}
		for _, r := range got.Rows {
			byNumber[r.Number] = r
		}
		if len(got.Rows) != 2 || byNumber[1].WorkType == nil || *byNumber[1].WorkType != "pull_request" ||
			byNumber[2].WorkType == nil || *byNumber[2].WorkType != "story" {
			t.Fatalf("rows %+v", got.Rows)
		}
		if byNumber[1].TeamID == nil || *byNumber[1].TeamID != "team-a" {
			t.Fatalf("team of rA: %+v", byNumber[1])
		}
		other, err := AttributedPrs(ctx, client, org2, dr, nil, 50, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(other.Rows) != 0 {
			t.Fatalf("org2 has no AI-attributed pull request: %+v", other.Rows)
		}
		paged, err := AttributedPrs(ctx, client, org1, dr, nil, 1, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(paged.Rows) != 1 || !paged.HasMore {
			t.Fatalf("paged %+v", paged)
		}
		wt := "story"
		typed, err := AttributedPrs(ctx, client, org1, dr, &model.AIScopeInput{WorkType: &wt}, 50, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(typed.Rows) != 1 || typed.Rows[0].Number != 2 {
			t.Fatalf("work type filter %+v", typed.Rows)
		}
		team := "team-a"
		teamed, err := AttributedPrs(ctx, client, org1, dr, &model.AIScopeInput{TeamID: &team}, 50, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(teamed.Rows) != 2 {
			t.Fatalf("team scope %+v", teamed.Rows)
		}
		repo := rB
		repoScoped, err := AttributedPrs(ctx, client, org1, dr, &model.AIScopeInput{RepoID: &repo}, 50, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(repoScoped.Rows) != 0 {
			t.Fatalf("repo scope %+v", repoScoped.Rows)
		}
	})

	t.Run("attribution overview", func(t *testing.T) {
		got, err := AttributionOverview(ctx, client, org1, dr, nil, 50, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got.TotalAttributed != 2 || len(got.Rows) != 2 {
			t.Fatalf("org1: %+v", got)
		}
		other, err := AttributionOverview(ctx, client, org2, dr, nil, 50, 0)
		if err != nil {
			t.Fatal(err)
		}
		if other.TotalAttributed != 1 || other.Mix[0].Kind != "human" {
			t.Fatalf("org2: %+v", other)
		}
		kinds := []model.AIAttributionBucketInput{model.AIAttributionBucketInputAgentCreated}
		filtered, err := AttributionOverview(ctx, client, org1, dr, &model.AIAttributionScopeInput{Buckets: kinds}, 50, 0)
		if err != nil {
			t.Fatal(err)
		}
		if filtered.TotalAttributed != 1 || filtered.Rows[0].Kind != "agent_created" {
			t.Fatalf("kind filter: %+v", filtered)
		}
		repo := rA
		scoped, err := AttributionOverview(ctx, client, org1, dr, &model.AIAttributionScopeInput{RepoID: &repo}, 50, 0)
		if err != nil {
			t.Fatal(err)
		}
		if scoped.TotalAttributed != 1 {
			t.Fatalf("repo scope: %+v", scoped)
		}
		team := "team-a"
		teamed, err := AttributionOverview(ctx, client, org1, dr, &model.AIAttributionScopeInput{TeamID: &team}, 50, 0)
		if err != nil {
			t.Fatal(err)
		}
		if teamed.TotalAttributed != 1 || len(teamed.Rows) != 1 {
			t.Fatalf("team scope: %+v", teamed)
		}
	})
}
