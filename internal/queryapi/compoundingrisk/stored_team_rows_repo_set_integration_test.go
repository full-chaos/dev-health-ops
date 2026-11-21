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

const rUnknown = "55555555-5555-5555-5555-555555555555"

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
func TestRealClickHouse_StoredTeamRowsCoverTheSelectedRepositorySet(t *testing.T) {
	type seedFn func(seedCtx, *testing.T, time.Time)
	seedAll := func(t1Score string) seedFn {
		return func(c seedCtx, t *testing.T, now time.Time) {
			for id, name := range map[string]string{rA: "acme/web", rB: "acme/api", rC: "acme/lib", rX: "acme/ops"} {
				repo(t, c.ctx, c.conn, "org-1", id, name)
			}
			exec(t, c.ctx, c.conn, `INSERT INTO teams (id, name, org_id, repo_patterns, updated_at) VALUES ('t1', 'Platform', 'org-1', [], now64(6)), ('t2', 'Infra', 'org-1', [], now64(6))`)
			own(t, c.ctx, c.conn, "org-1", "t1", rA, "acme/web")
			own(t, c.ctx, c.conn, "org-1", "t1", rC, "acme/lib")
			own(t, c.ctx, c.conn, "org-1", "t2", rX, "acme/ops")
			for id, score := range map[string]string{rA: "0.25", rB: "0.875", rC: "0.75", rX: "0.625"} {
				put(t, c.ctx, c.conn, risk{org: "org-1", scope: "repo", id: id, day: day(now, 1), score: score, sev: "low"})
			}
			put(t, c.ctx, c.conn, risk{org: "org-1", scope: "team", id: "t1", day: day(now, 1), score: t1Score, sev: "elevated"})
			put(t, c.ctx, c.conn, risk{org: "org-1", scope: "team", id: "t2", day: day(now, 1), score: "0.625", sev: "elevated"})
		}
	}
	full, stored := seedAll("0.5"), seedAll("0.55")
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
