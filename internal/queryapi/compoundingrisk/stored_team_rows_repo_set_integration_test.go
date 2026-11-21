//go:build integration

package compoundingrisk

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

const (
	rUnknown = "55555555-5555-5555-5555-555555555555"
	rD       = "66666666-6666-6666-6666-666666666666"
)

type scored struct {
	id    string
	score float64
}

func answerText(rows []scored) string {
	parts := make([]string, 0, len(rows))
	for _, r := range rows {
		parts = append(parts, fmt.Sprintf("%s=%.6g", r.id, r.score))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// On the team breakout with stored team rows, the rows and the selected-day
// trend point cover the same repository set. A filter that keeps every
// repository of the day serves the stored team rows; a filter that leaves one
// out derives each team from its selected repository rows, as the derived
// path does. Team t1 owns A (0.25) and C (0.75), t2 owns X (0.625), B (0.875)
// has no owner. The stored t1 row is 0.55 on the "stored" seed, not the 0.5
// mean of A and C, so a stored row and a derived row read apart.
// With no narrowing filter, a team that owns a repository row of the day and
// has no live stored row is derived from its repository rows; a team with a
// live stored row keeps it.
func TestRealClickHouse_StoredTeamRowsCoverTheSelectedRepositorySet(t *testing.T) {
	type seedFn func(seedCtx, *testing.T, time.Time)
	type world struct {
		t1, t2, teamComputed string
		cMovedToT2           bool
	}
	seedWorld := func(w world) seedFn {
		return func(c seedCtx, t *testing.T, now time.Time) {
			for id, name := range map[string]string{rA: "acme/web", rB: "acme/api", rC: "acme/lib", rX: "acme/ops"} {
				repo(t, c.ctx, c.conn, "org-1", id, name)
			}
			exec(t, c.ctx, c.conn, `INSERT INTO teams (id, name, org_id, repo_patterns, updated_at) VALUES ('t1', 'Platform', 'org-1', [], now64(6)), ('t2', 'Infra', 'org-1', [], now64(6))`)
			own(t, c.ctx, c.conn, "org-1", "t1", rA, "acme/web")
			if w.cMovedToT2 {
				own(t, c.ctx, c.conn, "org-1", "t2", rC, "acme/lib")
			} else {
				own(t, c.ctx, c.conn, "org-1", "t1", rC, "acme/lib")
			}
			own(t, c.ctx, c.conn, "org-1", "t2", rX, "acme/ops")
			for id, score := range map[string]string{rA: "0.25", rB: "0.875", rC: "0.75", rX: "0.625"} {
				put(t, c.ctx, c.conn, risk{org: "org-1", scope: "repo", id: id, day: day(now, 1), score: score, sev: "low"})
			}
			put(t, c.ctx, c.conn, risk{org: "org-1", scope: "team", id: "t1", day: day(now, 1), score: w.t1, sev: "elevated", computed: w.teamComputed})
			put(t, c.ctx, c.conn, risk{org: "org-1", scope: "team", id: "t2", day: day(now, 1), score: w.t2, sev: "elevated", computed: w.teamComputed})
		}
	}
	then := func(first, second seedFn) seedFn {
		return func(c seedCtx, t *testing.T, now time.Time) { first(c, t, now); second(c, t, now) }
	}
	full := seedWorld(world{t1: "0.5", t2: "0.625"})
	stored := seedWorld(world{t1: "0.55", t2: "0.625"})
	// t2's newest row of the day is a retraction marker (weights and
	// thresholds 0, no score): t2 has no live stored row.
	retracted := then(seedWorld(world{t1: "0.55", t2: "0.7", teamComputed: "now() - INTERVAL 1 HOUR"}),
		func(c seedCtx, t *testing.T, now time.Time) {
			exec(t, c.ctx, c.conn, `INSERT INTO compounding_risk_daily (org_id, day, scope, scope_id, computed_at) VALUES ('org-1', '%s', 'team', 't2', now())`, day(now, 1).Format("2006-01-02"))
		})
	// Every team has a live stored row, written before the newest repository
	// rows of the day (a run between its partitions and its finalize).
	olderTeamRows := seedWorld(world{t1: "0.55", t2: "0.7", teamComputed: "now() - INTERVAL 1 HOUR"})
	// C moved from t1 to t2 after both stored rows were written.
	moved := seedWorld(world{t1: "0.55", t2: "0.7", teamComputed: "now() - INTERVAL 1 HOUR", cMovedToT2: true})
	// t3 owns D, which has no repository row and no t3 row on the day (only
	// two days before).
	idleTeam := then(stored, func(c seedCtx, t *testing.T, now time.Time) {
		repo(t, c.ctx, c.conn, "org-1", rD, "acme/docs")
		exec(t, c.ctx, c.conn, `INSERT INTO teams (id, name, org_id, repo_patterns, updated_at) VALUES ('t3', 'Docs', 'org-1', [], now64(6))`)
		own(t, c.ctx, c.conn, "org-1", "t3", rD, "acme/docs")
		put(t, c.ctx, c.conn, risk{org: "org-1", scope: "repo", id: rD, day: day(now, 2), score: "0.125", sev: "low"})
		put(t, c.ctx, c.conn, risk{org: "org-1", scope: "team", id: "t3", day: day(now, 2), score: "0.125", sev: "low"})
	})
	// Another organization holds a team t2 that owns B (unowned in org-1) and
	// a repository row of B on the same day.
	otherOrg := then(stored, func(c seedCtx, t *testing.T, now time.Time) {
		repo(t, c.ctx, c.conn, "org-2", rB, "acme/api")
		exec(t, c.ctx, c.conn, `INSERT INTO teams (id, name, org_id, repo_patterns, updated_at) VALUES ('t2', 'Other', 'org-2', [], now64(6))`)
		own(t, c.ctx, c.conn, "org-2", "t2", rB, "acme/api")
		put(t, c.ctx, c.conn, risk{org: "org-2", scope: "repo", id: rB, day: day(now, 1), score: "0.125", sev: "low"})
	})
	// The stored t1 aggregate also took a repository with merged pull requests
	// and no repository row of the day (C): its 0.6 is not over A alone. It is
	// served as stored: a team's review latency covers every repository it
	// owns that merged pull requests in the window.
	reviewOnly := func(c seedCtx, t *testing.T, now time.Time) {
		repo(t, c.ctx, c.conn, "org-1", rA, "acme/web")
		repo(t, c.ctx, c.conn, "org-1", rB, "acme/api")
		repo(t, c.ctx, c.conn, "org-1", rC, "acme/lib")
		exec(t, c.ctx, c.conn, `INSERT INTO teams (id, name, org_id, repo_patterns, updated_at) VALUES ('t1', 'Platform', 'org-1', [], now64(6))`)
		own(t, c.ctx, c.conn, "org-1", "t1", rA, "acme/web")
		own(t, c.ctx, c.conn, "org-1", "t1", rC, "acme/lib")
		put(t, c.ctx, c.conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 1), score: "0.25", sev: "low"})
		put(t, c.ctx, c.conn, risk{org: "org-1", scope: "repo", id: rB, day: day(now, 1), score: "0.875", sev: "high"})
		put(t, c.ctx, c.conn, risk{org: "org-1", scope: "team", id: "t1", day: day(now, 1), score: "0.6", sev: "elevated"})
	}
	// The team rows were finalized before X got its owner t2 and its
	// repository row: the day holds a stored row for t1 only.
	rowsAhead := func(c seedCtx, t *testing.T, now time.Time) {
		repo(t, c.ctx, c.conn, "org-1", rA, "acme/web")
		repo(t, c.ctx, c.conn, "org-1", rB, "acme/api")
		repo(t, c.ctx, c.conn, "org-1", rX, "acme/ops")
		exec(t, c.ctx, c.conn, `INSERT INTO teams (id, name, org_id, repo_patterns, updated_at) VALUES ('t1', 'Platform', 'org-1', [], now64(6)), ('t2', 'Infra', 'org-1', [], now64(6))`)
		own(t, c.ctx, c.conn, "org-1", "t1", rA, "acme/web")
		own(t, c.ctx, c.conn, "org-1", "t2", rX, "acme/ops")
		put(t, c.ctx, c.conn, risk{org: "org-1", scope: "team", id: "t1", day: day(now, 1), score: "0.25", sev: "low", computed: "now() - INTERVAL 1 HOUR"})
		for id, score := range map[string]string{rA: "0.25", rB: "0.875", rX: "0.625"} {
			put(t, c.ctx, c.conn, risk{org: "org-1", scope: "repo", id: id, day: day(now, 1), score: score, sev: "low"})
		}
	}
	cases := []struct {
		name      string
		seed      seedFn
		repoIDs   []string
		teamIDs   []string
		wantRows  []scored
		wantTrend float64
	}{
		{"control: no filter, stored rows of the day's set", full, nil, nil,
			[]scored{{"unassigned", 0.875}, {"t2", 0.625}, {"t1", 0.5}}, 0.625},
		{"control: every repository selected", full, []string{rA, rB, rC, rX}, nil,
			[]scored{{"unassigned", 0.875}, {"t2", 0.625}, {"t1", 0.5}}, 0.625},
		{"one unowned repository selected", full, []string{rB}, nil,
			[]scored{{"unassigned", 0.875}}, 0.875},
		{"one whole team selected", full, []string{rA, rC}, nil,
			[]scored{{"t1", 0.5}}, 0.5},
		{"subset of a team by id", full, []string{rA}, nil,
			[]scored{{"t1", 0.25}}, 0.25},
		{"subset of a team by full name plus an unowned repository", full, []string{"acme/web", rB}, nil,
			[]scored{{"unassigned", 0.875}, {"t1", 0.25}}, 0.5625},
		{"review-only repository inside the stored aggregate is served as stored", reviewOnly, nil, nil,
			[]scored{{"unassigned", 0.875}, {"t1", 0.6}}, 0.5625},
		{"stored: no filter serves the stored rows", stored, nil, nil,
			[]scored{{"unassigned", 0.875}, {"t2", 0.625}, {"t1", 0.55}}, 0.625},
		{"stored: every repository by full name serves the stored rows", stored, []string{"acme/web", "acme/api", "acme/lib", "acme/ops"}, nil,
			[]scored{{"unassigned", 0.875}, {"t2", 0.625}, {"t1", 0.55}}, 0.625},
		{"stored: every repository plus an unknown id serves the stored rows", stored, []string{rA, rB, rC, rX, rUnknown}, nil,
			[]scored{{"unassigned", 0.875}, {"t2", 0.625}, {"t1", 0.55}}, 0.625},
		{"stored: every repository but one derives the teams", stored, []string{rA, rB, rC}, nil,
			[]scored{{"unassigned", 0.875}, {"t1", 0.5}}, (0.25 + 0.875 + 0.75) / 3},
		{"stored: an empty filter gives no row and no trend", stored, []string{}, nil,
			nil, math.NaN()},
		{"stored: a named team and a subset of its repositories", stored, []string{rA}, []string{"t1"},
			[]scored{{"t1", 0.25}}, 0.25},
		{"repository rows newer than the team aggregate: the team with no stored row is derived", rowsAhead, nil, nil,
			[]scored{{"unassigned", 0.875}, {"t2", 0.625}, {"t1", 0.25}}, (0.25 + 0.875 + 0.625) / 3},
		{"a retraction marker is no live stored row: the team is derived", retracted, nil, nil,
			[]scored{{"unassigned", 0.875}, {"t2", 0.625}, {"t1", 0.55}}, 0.625},
		{"teams with live stored rows stay stored under newer repository rows", olderTeamRows, nil, nil,
			[]scored{{"unassigned", 0.875}, {"t2", 0.7}, {"t1", 0.55}}, 0.625},
		{"a repository moved between two stored teams: both stay stored until the finalize", moved, nil, nil,
			[]scored{{"unassigned", 0.875}, {"t2", 0.7}, {"t1", 0.55}}, 0.625},
		{"a team with no repository row and no stored row of the day is not listed", idleTeam, nil, nil,
			[]scored{{"unassigned", 0.875}, {"t2", 0.625}, {"t1", 0.55}}, 0.625},
		{"a team of another organization is never derived", otherOrg, nil, nil,
			[]scored{{"unassigned", 0.875}, {"t2", 0.625}, {"t1", 0.55}}, 0.625},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, conn, client := startStore(t)
			now := time.Now().UTC()
			tc.seed(seedCtx{ctx, conn}, t, now)
			got, err := Resolve(ctx, client, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, RepoIds: tc.repoIDs, TeamIds: tc.teamIDs, TrendDays: 30}, now)
			if err != nil {
				t.Fatal(err)
			}
			var rows []scored
			for _, r := range got.Rows {
				s := math.NaN()
				if r.Score != nil {
					s = *r.Score
				}
				rows = append(rows, scored{r.ScopeID, s})
			}
			trend := math.NaN()
			for _, p := range got.Trend {
				if p.Day.Time().Equal(day(now, 1)) && p.Score != nil {
					trend = *p.Score
				}
			}
			sameTrend := math.Abs(trend-tc.wantTrend) < 1e-9 || (math.IsNaN(tc.wantTrend) && len(got.Trend) == 0)
			same := len(rows) == len(tc.wantRows) && sameTrend
			for i := 0; same && i < len(rows); i++ {
				same = rows[i].id == tc.wantRows[i].id && math.Abs(rows[i].score-tc.wantRows[i].score) < 1e-9
			}
			t.Logf("repoIds %v: rows %s trend %.6g; want rows %s trend %.6g", tc.repoIDs, answerText(rows), trend, answerText(tc.wantRows), tc.wantTrend)
			if !same {
				t.Errorf("team rows and the selected-day trend point cover different repository sets: rows %s trend %.6g, want rows %s trend %.6g",
					answerText(rows), trend, answerText(tc.wantRows), tc.wantTrend)
			}
		})
	}
}
