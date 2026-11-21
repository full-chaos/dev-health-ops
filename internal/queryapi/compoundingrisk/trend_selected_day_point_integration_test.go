//go:build integration

package compoundingrisk

import (
	"context"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

type seedCtx struct {
	ctx  context.Context
	conn stdclickhouse.Conn
}

func seedRepos(c seedCtx, t *testing.T, now time.Time, owned, storedTeamRow bool) {
	repo(t, c.ctx, c.conn, "org-1", rA, "acme/web")
	repo(t, c.ctx, c.conn, "org-1", rB, "acme/api")
	if owned {
		exec(t, c.ctx, c.conn, `INSERT INTO teams (id, name, org_id, repo_patterns, updated_at) VALUES ('t1', 'Platform', 'org-1', [], now64(6))`)
		own(t, c.ctx, c.conn, "org-1", "t1", rA, "acme/web")
	}
	put(t, c.ctx, c.conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 1), score: "0.2", sev: "low"})
	put(t, c.ctx, c.conn, risk{org: "org-1", scope: "repo", id: rB, day: day(now, 1), score: "0.8", sev: "high"})
	if storedTeamRow {
		put(t, c.ctx, c.conn, risk{org: "org-1", scope: "team", id: "t1", day: day(now, 1), score: "0.2", sev: "low"})
	}
}

// The trend point of the selected day covers exactly the repositories of the
// rows of that day. One repository per team and distinct scores make the set
// visible: the mean of the row scores equals the point only for the same set.
func TestRealClickHouse_TrendPointOfSelectedDayMatchesRows(t *testing.T) {
	cases := []struct {
		name          string
		owned, stored bool
		filter        *model.CompoundingRiskFilterInput
	}{
		{"repo breakout", false, false, &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeRepo, TrendDays: 30}},
		{"team fallback, every team, one repository unowned", true, false, &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TrendDays: 30}},
		{"team fallback, team named", true, false, &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: []string{"t1"}, TrendDays: 30}},
		{"team stored rows, one repository unowned", true, true, &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TrendDays: 30}},
		{"team stored rows, team named", true, true, &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: []string{"t1"}, TrendDays: 30}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, conn, client := startStore(t)
			now := time.Now().UTC()
			seedRepos(seedCtx{ctx, conn}, t, now, tc.owned, tc.stored)
			got, err := Resolve(ctx, client, "org-1", tc.filter, now)
			if err != nil {
				t.Fatal(err)
			}
			var point *float64
			for _, p := range got.Trend {
				if p.Day.Time().Equal(day(now, 1)) {
					point = p.Score
				}
			}
			if point == nil || len(got.Rows) == 0 {
				t.Fatalf("no selected-day point or no rows: point %v rows %v", point, ids(got.Rows))
			}
			var sum float64
			for _, r := range got.Rows {
				if r.Score == nil {
					t.Fatalf("row %s has no score", r.ScopeID)
				}
				sum += *r.Score
			}
			mean := sum / float64(len(got.Rows))
			t.Logf("rows %v row-mean %v, selected-day trend point %v", ids(got.Rows), mean, *point)
			if math.Abs(mean-*point) > 1e-9 {
				t.Errorf("selected-day trend point %v covers other repositories than the rows %v (row mean %v)", *point, ids(got.Rows), mean)
			}
		})
	}
}

// Stored team rows, no team named: the repository no team owns is listed under
// the no-team value with its own score and label, beside the stored team row.
func TestRealClickHouse_StoredTeamRowsListUnownedRepositoryAsUnassignedTeam(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	seedRepos(seedCtx{ctx, conn}, t, now, true, true)
	got, err := Resolve(ctx, client, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TrendDays: 30}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Rows) != 2 || got.Rows[0].ScopeID != "unassigned" || got.Rows[1].ScopeID != "t1" {
		t.Fatalf("rows %v, want [unassigned t1] ordered by score", ids(got.Rows))
	}
	u := got.Rows[0]
	if u.ScopeLabel != unassignedTeamLabel || u.Score == nil || *u.Score != 0.8 {
		t.Fatalf("unassigned row label %q score %v, want %q 0.8", u.ScopeLabel, u.Score, unassignedTeamLabel)
	}
}

// Stored team rows with a team named: no unassigned row. Every repository
// owned: no unassigned row.
func TestRealClickHouse_StoredTeamRowsNoUnassignedRowWhenNamedOrAllOwned(t *testing.T) {
	t.Run("team named", func(t *testing.T) {
		ctx, conn, client := startStore(t)
		now := time.Now().UTC()
		seedRepos(seedCtx{ctx, conn}, t, now, true, true)
		got, err := Resolve(ctx, client, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: []string{"t1"}, TrendDays: 30}, now)
		if err != nil {
			t.Fatal(err)
		}
		if r := ids(got.Rows); len(r) != 1 || r[0] != "t1" {
			t.Fatalf("rows %v, want [t1]", r)
		}
	})
	t.Run("all repositories owned", func(t *testing.T) {
		ctx, conn, client := startStore(t)
		now := time.Now().UTC()
		seedRepos(seedCtx{ctx, conn}, t, now, true, true)
		own(t, ctx, conn, "org-1", "t1", rB, "acme/api")
		got, err := Resolve(ctx, client, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TrendDays: 30}, now)
		if err != nil {
			t.Fatal(err)
		}
		if r := ids(got.Rows); len(r) != 1 || r[0] != "t1" {
			t.Fatalf("rows %v, want [t1]", r)
		}
	})
}

// The no-team row is classified by ownership at the selected day (the instant
// the daily writer uses), not at request time: a repository owned on the
// selected day whose ownership ended since is still the team's, not unowned.
func TestRealClickHouse_StoredTeamRowsUnassignedUsesOwnershipOfSelectedDay(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	seedRepos(seedCtx{ctx, conn}, t, now, false, true)
	exec(t, ctx, conn, `INSERT INTO teams (id, name, org_id, repo_patterns, updated_at) VALUES ('t1', 'Platform', 'org-1', [], now64(6))`)
	exec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, valid_from, valid_to, updated_at)
      SELECT 'org-1', 'github', 't1', '%s', 'acme/web', 'exact', 'native', toDateTime64('2020-01-01 00:00:00', 3, 'UTC'), toDateTime64('%s 00:00:00', 3, 'UTC'), now64(3)`, rA, day(now, 0).Format("2006-01-02"))
	got, err := Resolve(ctx, client, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TrendDays: 30}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Rows) != 2 || got.Rows[0].ScopeID != "unassigned" || got.Rows[0].Score == nil || *got.Rows[0].Score != 0.8 {
		t.Fatalf("rows %v, want [unassigned t1] with unassigned = 0.8 (repository B only; A was owned on the selected day)", ids(got.Rows))
	}
}

// A stored no-team row keeps its stored values and gets the canonical label.
func TestRealClickHouse_StoredUnassignedRowKeepsScoreAndGetsCanonicalLabel(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	seedRepos(seedCtx{ctx, conn}, t, now, true, true)
	put(t, ctx, conn, risk{org: "org-1", scope: "team", id: "unassigned", day: day(now, 1), score: "0.75", sev: "high"})
	got, err := Resolve(ctx, client, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TrendDays: 30}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Rows) != 2 || got.Rows[0].ScopeID != "unassigned" {
		t.Fatalf("rows %v, want [unassigned t1]", ids(got.Rows))
	}
	u := got.Rows[0]
	if u.ScopeLabel != unassignedTeamLabel || u.ScopeEntity == nil || u.ScopeEntity.DisplayName != unassignedTeamLabel || u.Score == nil || *u.Score != 0.75 {
		t.Fatalf("stored unassigned row label %q entity %v score %v, want %q and 0.75", u.ScopeLabel, u.ScopeEntity, u.Score, unassignedTeamLabel)
	}
}

// An explicit teamIds request keeps the stored row as stored (id as label):
// a client that reuses the returned scopeId "unassigned" as a filter gets the
// answer main gives.
func TestRealClickHouse_StoredUnassignedRowNamedByTeamIdsKeepsStoredLabel(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	seedRepos(seedCtx{ctx, conn}, t, now, true, true)
	put(t, ctx, conn, risk{org: "org-1", scope: "team", id: "unassigned", day: day(now, 1), score: "0.75", sev: "high"})
	got, err := Resolve(ctx, client, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: []string{"unassigned"}, TrendDays: 30}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Rows) != 1 || got.Rows[0].ScopeID != "unassigned" {
		t.Fatalf("rows %v, want [unassigned]", ids(got.Rows))
	}
	u := got.Rows[0]
	if u.ScopeLabel != "unassigned" || u.ScopeEntity == nil || u.ScopeEntity.DisplayName != "unassigned" || u.Score == nil || *u.Score != 0.75 {
		t.Fatalf("named stored unassigned row label %q entity %v, want the stored id as label on main", u.ScopeLabel, u.ScopeEntity)
	}
}
