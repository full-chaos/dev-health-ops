//go:build integration

package compoundingrisk

import (
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

func TestRealClickHouse_EmptyTeamListIsOmittedTeamList(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	repo(t, ctx, conn, "org-1", rA, "acme/web")
	repo(t, ctx, conn, "org-1", rB, "acme/api")
	exec(t, ctx, conn, `INSERT INTO teams (id, name, org_id, updated_at) VALUES ('t1', 'Platform', 'org-1', now64(6))`)
	own(t, ctx, conn, "org-1", "t1", rA, "acme/web")
	put(t, ctx, conn, risk{org: "org-1", scope: "team", id: "t1", day: day(now, 1), score: "0.75", sev: "high"})
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 1), score: "0.2", sev: "low"})
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rB, day: day(now, 1), score: "1", sev: "high"})
	answer := func(teamIDs []string, d *model.CompoundingRiskFilterInput) string {
		t.Helper()
		d.TeamIds = teamIDs
		got, err := Resolve(ctx, client, "org-1", d, now)
		if err != nil {
			t.Fatal(err)
		}
		s := ""
		for _, r := range got.Rows {
			s += fmt.Sprintf("row %s %v|", r.ScopeID, *r.Score)
		}
		for _, p := range got.Trend {
			s += fmt.Sprintf("trend %s %v|", p.Day.String(), *p.Score)
		}
		return s
	}
	base := func() *model.CompoundingRiskFilterInput {
		return &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TrendDays: 30}
	}
	withDay := func() *model.CompoundingRiskFilterInput {
		f := base()
		dd := gqlDate(day(now, 1))
		f.Day = &dd
		return f
	}
	d1 := day(now, 1).Format("2006-01-02")
	omitted := answer(nil, base())
	t.Run("omitted_unchanged", func(t *testing.T) {
		if want := fmt.Sprintf("row unassigned 1|row t1 0.75|trend %s 0.6|", d1); omitted != want {
			t.Fatalf("omitted teamIds changed: %q want %q", omitted, want)
		}
	})
	t.Run("empty_inferred_day_equals_omitted", func(t *testing.T) {
		if got := answer([]string{}, base()); got != omitted {
			t.Errorf("[] = %q, omitted = %q", got, omitted)
		}
	})
	t.Run("empty_explicit_day_equals_omitted", func(t *testing.T) {
		if o, e := answer(nil, withDay()), answer([]string{}, withDay()); o != e || o == "" {
			t.Errorf("[] = %q, omitted = %q", e, o)
		}
	})
	t.Run("named_unchanged", func(t *testing.T) {
		want := fmt.Sprintf("row t1 0.75|trend %s 0.2|", d1)
		if got := answer([]string{"t1"}, base()); got != want {
			t.Errorf("named teamIds changed: %q want %q", got, want)
		}
	})
}

func TestRealClickHouse_EmptyRepoListStaysEmpty(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	repo(t, ctx, conn, "org-1", rA, "acme/web")
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 1), score: "0.2", sev: "low"})
	got, err := Resolve(ctx, client, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeRepo, RepoIds: []string{}, TrendDays: 30}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Rows) != 0 || len(got.Trend) != 0 {
		t.Fatalf("an empty repoIds must stay empty: %#v", got)
	}
}
