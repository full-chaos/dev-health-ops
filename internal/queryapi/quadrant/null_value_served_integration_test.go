//go:build integration

package quadrant

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// A team whose stored owner share of its only repository is 0 holds no weight of the repository's reviews: its review
// load and review latency divide by nullIf(sum(share), 0) and are NULL. The point must not be served as a measured (0, 0).
func TestAReviewPointWithNoOwnerWeightIsNotServedAsZero(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	seedOwnerShareRepos(ctx, t, admin)

	const repo4 = "00000000-0000-4000-8000-0000000000d4"
	crossorg.Exec(ctx, t, admin, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, 'acme/four', 'github', ?, ?, ?)`,
		repo4, ownerShareOrg, ownerShareSynced, ownerShareSynced)
	crossorg.Exec(ctx, t, admin, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider) VALUES ('team-c', generateUUIDv4(), 'Name of team-c', [], [], [], 1, ?, ?, ?, 'github')`,
		ownerShareSynced, ownerShareSynced, ownerShareOrg)
	for _, owner := range []struct {
		team    string
		primary int
		share   float64
	}{{"team-b", 1, 1.0}, {"team-c", 2, 0.0}} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share) VALUES (?, 'github', ?, ?, 'acme/four', 'exact', 'inferred', ?, 0, 0, ?, NULL, ?, ?)`,
			ownerShareOrg, owner.team, repo4, owner.primary, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), ownerShareSynced, owner.share)
	}
	crossorg.Exec(ctx, t, admin, `INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, identity_id, team_id, reviews_given, pr_first_review_p50_hours, computed_at) VALUES (?, ?, ?, 'p4', 'p4', 'team-member', 7, 5, now())`,
		ownerShareOrg, repo4, ownerShareDay)

	resp, err := BuildResponse(ctx, client, ownerShareOrg, Params{
		Type: "review_load_latency", ScopeType: "team", Bucket: "week", RangeDays: 30, EndDate: &ownerShareEnd,
	})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	for _, point := range resp.Points {
		if point.EntityID != "team-c" {
			continue
		}
		if point.NoData {
			t.Logf("team-c served as a no-data point (x %v, y %v)", point.X, point.Y)
			continue
		}
		x, y := any(nil), any(nil)
		if point.X != nil {
			x = *point.X
		}
		if point.Y != nil {
			y = *point.Y
		}
		t.Errorf("team-c served as a measured point (x %v, y %v, trajectory %d): its owner weight is 0, so load and latency are NULL", x, y, len(point.Trajectory))
	}
}
