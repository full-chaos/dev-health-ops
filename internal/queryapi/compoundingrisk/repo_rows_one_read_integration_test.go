//go:build integration

package compoundingrisk

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// countingClient counts the statements a request sends and the reads of the
// latest repository rows of one day. before runs ahead of each statement.
type countingClient struct {
	QueryClient
	statements, repoRowReads int
	before                   func(statement string, repoRowRead bool)
}

func isRepoRowRead(statement string, bindings []clickhouse.Binding) bool {
	if !strings.Contains(statement, "day = {day:Date}") {
		return false
	}
	for _, b := range bindings {
		if b.Name == "scope" && b.Value == scopeRepo {
			return true
		}
	}
	return false
}

func (c *countingClient) Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.statements++
	repoRowRead := isRepoRowRead(statement, bindings)
	if repoRowRead {
		c.repoRowReads++
	}
	if c.before != nil {
		c.before(statement, repoRowRead)
	}
	return c.QueryClient.Query(ctx, statement, bindings)
}

func rowsText(got *model.CompoundingRiskResult) string {
	var rows []scored
	for _, r := range got.Rows {
		s := -1.0
		if r.Score != nil {
			s = *r.Score
		}
		rows = append(rows, scored{r.ScopeID, s})
	}
	return answerText(rows)
}

// seedFourRepos: t1 owns A .25 and C .75, t2 owns X .625, B .875 has no
// owner; stored t1 .55 and t2 .625.
func seedFourRepos(c seedCtx, t *testing.T, now time.Time) {
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
	put(t, c.ctx, c.conn, risk{org: "org-1", scope: "team", id: "t1", day: day(now, 1), score: "0.55", sev: "elevated"})
	put(t, c.ctx, c.conn, risk{org: "org-1", scope: "team", id: "t2", day: day(now, 1), score: "0.625", sev: "elevated"})
}

func answerOf(got *model.CompoundingRiskResult) string {
	var b strings.Builder
	for _, r := range got.Rows {
		fmt.Fprintf(&b, "row %s %q", r.ScopeID, r.ScopeLabel)
		if r.Score != nil {
			fmt.Fprintf(&b, " %v", *r.Score)
		}
		b.WriteString("|")
	}
	for _, p := range got.Trend {
		fmt.Fprintf(&b, "trend %s", p.Day.String())
		if p.Score != nil {
			fmt.Fprintf(&b, " %v", *p.Score)
		}
		b.WriteString("|")
	}
	return b.String()
}

// An explicit empty repoIds with no team named selects no repository: no row,
// no trend, and no statement. With teams named it means the same as an absent
// repoIds (the fallback's parity rule, pinned by
// TestRealClickHouse_FallbackRepoFilterIntersectsOwnership), on a day with
// stored team rows and on a derived day alike.
func TestRealClickHouse_EmptyRepoIdsAnswersNothingOnEveryPath(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	seedFourRepos(seedCtx{ctx, conn}, t, now)
	// org-1 day 2: repository rows only, so the team breakout derives there.
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 2), score: "0.25", sev: "low"})
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rC, day: day(now, 2), score: "0.5", sev: "elevated"})
	selected := gqlDate(day(now, 1))
	derivedDay := gqlDate(day(now, 2))
	resolve := func(t *testing.T, in model.CompoundingRiskFilterInput, repoIDs []string) (*model.CompoundingRiskResult, int) {
		t.Helper()
		in.RepoIds, in.TrendDays = repoIDs, 30
		c := &countingClient{QueryClient: client}
		got, err := Resolve(ctx, c, "org-1", &in, now)
		if err != nil {
			t.Fatal(err)
		}
		return got, c.statements
	}
	for _, tc := range []struct {
		name string
		in   model.CompoundingRiskFilterInput
	}{
		{"repo breakout", model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeRepo}},
		{"team, no team named, stored rows", model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam}},
		{"team, no team named, stored rows, explicit day", model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, Day: &selected}},
		{"team, no team named, derived day", model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, Day: &derivedDay}},
	} {
		t.Run("empty/"+tc.name, func(t *testing.T) {
			got, statements := resolve(t, tc.in, []string{})
			t.Logf("rows %s trend %d points, %d statements", rowsText(got), len(got.Trend), statements)
			if len(got.Rows) != 0 || len(got.Trend) != 0 || statements != 0 {
				t.Errorf("repoIds [] answered rows %s and %d trend points with %d statements, want nothing and no statement", rowsText(got), len(got.Trend), statements)
			}
		})
	}
	for _, tc := range []struct {
		name string
		in   model.CompoundingRiskFilterInput
	}{
		{"named team, stored rows", model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: []string{"t1"}}},
		{"named teams, stored rows, explicit day", model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: []string{"t1", "t2"}, Day: &selected}},
		{"named team, derived day", model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: []string{"t1"}, Day: &derivedDay}},
	} {
		t.Run("as absent/"+tc.name, func(t *testing.T) {
			empty, _ := resolve(t, tc.in, []string{})
			absent, _ := resolve(t, tc.in, nil)
			t.Logf("repoIds []: %s; absent: %s", answerOf(empty), answerOf(absent))
			if answerOf(empty) != answerOf(absent) || len(absent.Rows) == 0 {
				t.Errorf("named teams with repoIds [] answered %s, want the answer of an absent repoIds %s", answerOf(empty), answerOf(absent))
			}
		})
	}
}

// The decision "the filter leaves out a repository of the day" and the
// derived team rows come from one read of the day's repository rows. A
// repository row written while the request runs (C, after the first read)
// does not turn a narrowing filter into a stored answer.
func TestRealClickHouse_NarrowingDecisionAndRowsComeFromOneRead(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	for id, name := range map[string]string{rA: "acme/web", rB: "acme/api", rC: "acme/lib"} {
		repo(t, ctx, conn, "org-1", id, name)
	}
	exec(t, ctx, conn, `INSERT INTO teams (id, name, org_id, repo_patterns, updated_at) VALUES ('t1', 'Platform', 'org-1', [], now64(6))`)
	own(t, ctx, conn, "org-1", "t1", rA, "acme/web")
	own(t, ctx, conn, "org-1", "t1", rC, "acme/lib")
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 1), score: "0.25", sev: "low"})
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rB, day: day(now, 1), score: "0.875", sev: "high"})
	put(t, ctx, conn, risk{org: "org-1", scope: "team", id: "t1", day: day(now, 1), score: "0.55", sev: "elevated"})
	read, written := false, false
	c := &countingClient{QueryClient: client, before: func(_ string, repoRowRead bool) {
		if read && !written {
			put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rC, day: day(now, 1), score: "0.75", sev: "elevated"})
			written = true
		}
		read = read || repoRowRead
	}}
	got, err := Resolve(ctx, c, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, RepoIds: []string{rA, rC}, TrendDays: 30}, now)
	if err != nil {
		t.Fatal(err)
	}
	tr := trendMap(got)[day(now, 1).Format("2006-01-02")]
	t.Logf("rows %s selected-day trend %v (C written after the first repository-row read: %v), %d statements, %d repository-row reads", rowsText(got), tr, written, c.statements, c.repoRowReads)
	if rowsText(got) != "[t1=0.25]" {
		t.Errorf("rows %s, want [t1=0.25]: the day's rows at the first read hold A and B, so [A, C] leaves B out and t1 is derived from A", rowsText(got))
	}
}

// Each team breakout request reads the day's repository rows once.
func TestRealClickHouse_TeamBreakoutReadsTheDayRepositoryRowsOnce(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	seedFourRepos(seedCtx{ctx, conn}, t, now)
	for _, tc := range []struct {
		name    string
		repoIDs []string
		teamIDs []string
	}{
		{"no filter", nil, nil},
		{"filter that leaves out a repository", []string{rA}, nil},
		{"filter that keeps every repository", []string{rA, rB, rC, rX}, nil},
		{"named team and a filter", []string{rA}, []string{"t1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &countingClient{QueryClient: client}
			got, err := Resolve(ctx, c, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, RepoIds: tc.repoIDs, TeamIds: tc.teamIDs, TrendDays: 30}, now)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("rows %s: %d statements, %d repository-row reads", rowsText(got), c.statements, c.repoRowReads)
			if c.repoRowReads != 1 {
				t.Errorf("%d reads of the day's repository rows, want 1", c.repoRowReads)
			}
		})
	}
}

// seedStoredTeams stores nRepos repositories, each with a repository row of
// the day (score 1) and its own owner team t<n>; the first nStored teams have a
// stored team row (score 0.5).
func seedStoredTeams(t *testing.T, nRepos, nStored int) (QueryClient, time.Time) {
	t.Helper()
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	d := day(now, 1).Format("2006-01-02")
	exec(t, ctx, conn, `INSERT INTO teams (id, name, org_id, updated_at) SELECT concat('t', toString(number)), concat('Team ', toString(number)), 'org-1', now64(6) FROM numbers(%d)`, nRepos)
	exec(t, ctx, conn, `INSERT INTO repos (id, repo, org_id, created_at, last_synced) SELECT toUUID(`+capRepoID+`), concat('acme/r', toString(number)), 'org-1', now64(3), now64(3) FROM numbers(%d)`, nRepos)
	exec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, valid_from, updated_at)
		SELECT 'org-1', 'github', concat('t', toString(number)), toUUID(`+capRepoID+`), concat('acme/r', toString(number)), 'exact', 'native', toDateTime64('2020-01-01 00:00:00', 3, 'UTC'), now64(3) FROM numbers(%d)`, nRepos)
	exec(t, ctx, conn, `INSERT INTO compounding_risk_daily (org_id, day, scope, scope_id, compounding_risk, severity, w_churn, w_complexity, w_ownership, w_review, threshold_elevated, threshold_high)
		SELECT 'org-1', toDate('%s'), 'repo', `+capRepoID+`, 1.0, 'high', 0.25, 0.25, 0.25, 0.25, 0.4, 0.7 FROM numbers(%d)`, d, nRepos)
	exec(t, ctx, conn, `INSERT INTO compounding_risk_daily (org_id, day, scope, scope_id, compounding_risk, severity, w_churn, w_complexity, w_ownership, w_review, threshold_elevated, threshold_high)
		SELECT 'org-1', toDate('%s'), 'team', concat('t', toString(number)), 0.5, 'elevated', 0.25, 0.25, 0.25, 0.25, 0.4, 0.7 FROM numbers(%d)`, d, nStored)
	return client, now
}

// The stored team read is capped at maxRows. When it returns the cap, the
// read may be cut, so no team is derived beside it: a team cut off by the cap
// would be derived although it has a live stored row.
func TestRealClickHouse_StoredTeamReadAtItsCapDerivesNoTeam(t *testing.T) {
	for _, tc := range []struct {
		name            string
		stored          int
		wantRows        int
		wantDerivedTeam bool
	}{
		{"stored rows at the cap", maxRows, maxRows, false},
		{"stored rows one below the cap", maxRows - 1, maxRows + 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, now := seedStoredTeams(t, maxRows+1, tc.stored)
			got, err := Resolve(context.Background(), client, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TrendDays: 30}, now)
			if err != nil {
				t.Fatal(err)
			}
			derived := 0
			for _, r := range got.Rows {
				if r.Score != nil && *r.Score == 1 {
					derived++
				}
			}
			last := fmt.Sprintf("t%d", maxRows)
			lastListed := false
			for _, r := range got.Rows {
				lastListed = lastListed || r.ScopeID == last
			}
			t.Logf("%d stored team rows: %d rows, %d derived, %s listed %v", tc.stored, len(got.Rows), derived, last, lastListed)
			if len(got.Rows) != tc.wantRows || lastListed != tc.wantDerivedTeam {
				t.Errorf("%d rows (%d derived), %s listed %v; want %d rows, %s listed %v", len(got.Rows), derived, last, lastListed, tc.wantRows, last, tc.wantDerivedTeam)
			}
		})
	}
}
