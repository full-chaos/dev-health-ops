//go:build integration

package aianalytics

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-14101: the team scope of the reviewer concentration is the team's owned
// repositories (teamscope.HeldRepoIDs), never user_metrics_daily.team_id, which
// is a person's membership label (acp, CHAOS-4396 option (a)).
func TestRealClickHouse_ReviewerConcentrationTeamScopeIsOwnership14101(t *testing.T) {
	ctx, conn, client := startStore(t)
	for _, r := range []struct{ id, name string }{{rA, "acme/alpha"}, {rB, "acme/beta"}} {
		exec(t, ctx, conn, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced)
            SELECT '%s', '%s', 'github', '%s', now64(3), now64(3)`, r.id, r.name, org1)
	}
	for _, id := range []string{"team-a", "team-x"} {
		exec(t, ctx, conn, `INSERT INTO teams (id, team_uuid, name, repo_patterns, org_id, updated_at)
            SELECT '%s', generateUUIDv4(), '%s', [], '%s', now64(6)`, id, id, org1)
	}
	// team-a owns acme/alpha (rA); team-x owns nothing; nobody owns acme/beta (rB).
	exec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
        SELECT '%s', 'github', 'team-a', NULL, 'acme/alpha', 'exact', 'provider_access', 1, 0, 0, toDateTime64('2026-01-01 00:00:00', 3, 'UTC'), NULL, now64(3)`, org1)
	for _, repo := range []string{rA, rB} {
		exec(t, ctx, conn, `INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source,
            confidence, evidence, observed_at, ingested_at)
            SELECT generateUUIDv4(), '%s', 'github', 'pull_request', '1', '%s', 'ai_assisted', 'pr_label', 1.0, '{}', toDateTime64('2026-08-01 12:00:00', 3, 'UTC'), now64(3)`,
			org1, repo)
	}
	// member@x is a member of team-a and reviews only in rB, which team-a does not own.
	// owner1@x (member of team-x) and owner2@x (no team) review in rA, which team-a owns.
	for _, u := range []struct {
		repo, who, team string
		n               int
	}{{rB, "member@x", "team-a", 5}, {rA, "owner1@x", "team-x", 3}, {rA, "owner2@x", "unassigned", 1}} {
		exec(t, ctx, conn, `INSERT INTO user_metrics_daily (repo_id, day, author_email, reviews_given, team_id, org_id, computed_at)
            SELECT '%s', '2026-08-01', '%s', %d, '%s', '%s', now64(3)`, u.repo, u.who, u.n, u.team, org1)
	}
	concentration := func(t *testing.T, sc *model.AIScopeInput) *model.AIReviewerConcentrationSummary {
		t.Helper()
		got, err := ReviewLoad(ctx, client, org1, dateRange(), sc)
		if err != nil {
			t.Fatal(err)
		}
		return got.ReviewerConcentration
	}

	t.Run("control: no scope counts all three reviewers", func(t *testing.T) {
		if c := concentration(t, nil); !c.DataAvailable || c.ReviewerCount != 3 {
			t.Fatalf("unscoped = %+v, want 3 reviewers", c)
		}
	})
	t.Run("control: repo scope rA counts its two reviewers", func(t *testing.T) {
		if c := concentration(t, &model.AIScopeInput{RepoID: sp(rA)}); !c.DataAvailable || c.ReviewerCount != 2 {
			t.Fatalf("repo rA = %+v, want 2 reviewers", c)
		}
	})
	t.Run("team-a counts the reviewers of the repositories it owns, not its members elsewhere", func(t *testing.T) {
		c := concentration(t, &model.AIScopeInput{TeamID: sp("team-a")})
		if !c.DataAvailable || c.ReviewerCount != 2 {
			t.Fatalf("team-a = %+v, want 2 reviewers (owner1@x, owner2@x in rA), not member@x in rB", c)
		}
		want := gini([]float64{1, 3})
		if c.ReviewerGini == nil || *c.ReviewerGini != want {
			t.Fatalf("team-a gini = %v, want %v (loads 3 and 1 in rA)", c.ReviewerGini, want)
		}
	})
	t.Run("a team that owns no repository has no concentration, whatever its members review", func(t *testing.T) {
		if c := concentration(t, &model.AIScopeInput{TeamID: sp("team-x")}); c.DataAvailable || c.ReviewerCount != 0 {
			t.Fatalf("team-x = %+v, want no data (it owns nothing)", c)
		}
	})
}
