package compoundingrisk

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

func repoIdsRefusal(n int) string {
	return fmt.Sprintf("compoundingRisk: filter.repoIds holds %d entries; the limit is %d", n, maxRows)
}

// wantRepoIdsRefused: no answer and a BAD_USER_INPUT error that names the
// field, the limit and the count sent.
func wantRepoIdsRefused(t *testing.T, got *model.CompoundingRiskResult, err error, n int) {
	t.Helper()
	var gqlErr *gqlerror.Error
	if got != nil || !errors.As(err, &gqlErr) {
		served := "none"
		if got != nil {
			served = fmt.Sprintf("rows=%d trend=%d", len(got.Rows), len(got.Trend))
		}
		t.Fatalf("%d repoIds: served %s, err %v; want no result and a *gqlerror.Error", n, served, err)
	}
	if gqlErr.Message != repoIdsRefusal(n) {
		t.Errorf("message %q; want %q", gqlErr.Message, repoIdsRefusal(n))
	}
	if code, field, limit := gqlErr.Extensions["code"], gqlErr.Extensions["field"], gqlErr.Extensions["cap"]; code != "BAD_USER_INPUT" || field != "filter.repoIds" || limit != maxRows {
		t.Errorf("extensions %v; want code BAD_USER_INPUT, field filter.repoIds, cap %d", gqlErr.Extensions, maxRows)
	}
}

type repoIdsArm struct {
	name     string
	breakout model.CompoundingRiskScope
	teamIDs  []string
	day      bool
}

// Every arm of Resolve takes filter.repoIds through the one check at its
// entry: REPO, TEAM with no team named and TEAM with a team named, each with
// the day picked and with the day given.
func repoIdsArms() []repoIdsArm {
	var arms []repoIdsArm
	for _, a := range []repoIdsArm{
		{name: "REPO", breakout: model.CompoundingRiskScopeRepo},
		{name: "TEAM-no-team-named", breakout: model.CompoundingRiskScopeTeam},
		{name: "TEAM-team-named", breakout: model.CompoundingRiskScopeTeam, teamIDs: []string{"big"}},
	} {
		arms = append(arms, a)
		a.name += "-day-given"
		a.day = true
		arms = append(arms, a)
	}
	return arms
}

func (a repoIdsArm) filter(repoIDs []string) *model.CompoundingRiskFilterInput {
	f := &model.CompoundingRiskFilterInput{Breakout: a.breakout, TeamIds: a.teamIDs, RepoIds: repoIDs, TrendDays: 30}
	if a.day {
		d := graphqldate.New(time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC))
		f.Day = &d
	}
	return f
}

func TestCallerRepoIdsOver500IsRefusedAndExactly500IsServedOnEveryArm(t *testing.T) {
	now := sweepNow()
	var tokens []string
	for i := 0; i <= maxRows; i++ {
		tokens = append(tokens, bigRepoID(i))
	}
	for _, a := range repoIdsArms() {
		t.Run(a.name+"/501", func(t *testing.T) {
			rec := &sweepRecorder{sweepStore: &sweepStore{world: bigTeamWorld(now, false)}}
			got, err := Resolve(context.Background(), rec, sweepOrg, a.filter(tokens), now)
			wantRepoIdsRefused(t, got, err, maxRows+1)
			if len(rec.statements) != 0 {
				t.Errorf("%d statements ran; want 0 (refused before any read)", len(rec.statements))
			}
		})
		t.Run(a.name+"/500", func(t *testing.T) {
			store := &sweepStore{world: bigTeamWorld(now, false)}
			got, err := Resolve(context.Background(), store, sweepOrg, a.filter(tokens[:maxRows]), now)
			for _, st := range store.unknown {
				t.Fatalf("a statement the model does not know:\n%s", st)
			}
			if err != nil {
				t.Fatalf("500 repoIds: %v; want a served answer", err)
			}
			want := 1
			if a.breakout == model.CompoundingRiskScopeRepo {
				want = maxRows
			}
			if len(got.Rows) != want || len(got.Trend) != 1 || sweepNum(got.Trend[0].Score) != "0.25" {
				t.Errorf("500 repoIds: rows=%d trend=%v; want rows=%d and trend 0.25 (big-000..big-499)", len(got.Rows), sweepServed(got, nil), want)
			}
		})
	}
}

// With no repoIds the server reads the org's repositories itself: no refusal,
// and the answer of the base. The trend and the team rows cover all 501
// repositories. The REPO rows stop at maxRows by score (big-499 is left out)
// with no notice: that row cap is not a request list and not changed here.
func TestNoCallerRepoIdsOnAnOrgOver500RepositoriesKeepsTheBaseAnswer(t *testing.T) {
	now := sweepNow()
	mean := sweepNum(sweepF(126.0 / 501))
	for _, a := range repoIdsArms() {
		if a.day {
			continue
		}
		t.Run(a.name, func(t *testing.T) {
			store := &sweepStore{world: bigTeamWorld(now, false)}
			got, err := Resolve(context.Background(), store, sweepOrg, a.filter(nil), now)
			for _, st := range store.unknown {
				t.Fatalf("a statement the model does not know:\n%s", st)
			}
			if err != nil {
				t.Fatalf("no repoIds: %v; want the base answer", err)
			}
			if len(got.Trend) != 1 || got.Trend[0].Day.String() != "2026-03-09" || sweepNum(got.Trend[0].Score) != mean {
				t.Errorf("trend %v; want [2026-03-09=%s] (all 501 repositories)", sweepServed(got, nil), mean)
			}
			if a.breakout == model.CompoundingRiskScopeTeam {
				if len(got.Rows) != 1 || got.Rows[0].ScopeID != "big" || sweepNum(got.Rows[0].Score) != mean {
					t.Errorf("rows %v; want [big=%s] (all 501 repositories)", sweepServed(got, nil), mean)
				}
				return
			}
			seen := map[string]bool{}
			for _, r := range got.Rows {
				seen[r.ScopeID] = true
			}
			var missing []string
			for i := 0; i <= maxRows; i++ {
				if !seen[bigRepoID(i)] {
					missing = append(missing, bigRepoID(i))
				}
			}
			first := ""
			if len(got.Rows) > 0 {
				first = got.Rows[0].ScopeID
			}
			if len(got.Rows) != maxRows || first != bigRepoID(maxRows) || strings.Join(missing, ",") != bigRepoID(maxRows-1) {
				t.Errorf("rows=%d first=%s missing=%v; want the base: %d rows, big-500 first, only big-499 missing", len(got.Rows), first, missing, maxRows)
			}
		})
	}
}

// The limit counts the entries as sent, duplicates included: 501 entries that
// name 300 distinct repositories are refused and the message names 501. 500
// entries that name 250 distinct repositories are served, the same answer as
// the 250 distinct ids.
func TestRepoIdsLimitCountsEntriesAsSentWithDuplicates(t *testing.T) {
	now := sweepNow()
	var over []string
	for i := 0; i < maxRows+1; i++ {
		over = append(over, bigRepoID(i%300))
	}
	var twice, distinct []string
	for i := 0; i < maxRows/2; i++ {
		distinct = append(distinct, bigRepoID(i))
	}
	twice = append(append(twice, distinct...), distinct...)
	for _, a := range repoIdsArms() {
		t.Run(a.name+"/501-entries-300-distinct", func(t *testing.T) {
			rec := &sweepRecorder{sweepStore: &sweepStore{world: bigTeamWorld(now, false)}}
			got, err := Resolve(context.Background(), rec, sweepOrg, a.filter(over), now)
			wantRepoIdsRefused(t, got, err, maxRows+1)
			if len(rec.statements) != 0 {
				t.Errorf("%d statements ran; want 0 (refused before any read)", len(rec.statements))
			}
		})
		t.Run(a.name+"/500-entries-250-distinct", func(t *testing.T) {
			got := sweepAnswer(t, bigTeamWorld(now, false), a.filter(twice))
			want := sweepAnswer(t, bigTeamWorld(now, false), a.filter(distinct))
			rows := 1
			if a.breakout == model.CompoundingRiskScopeRepo {
				rows = maxRows / 2
			}
			if got != want || !strings.HasPrefix(got, fmt.Sprintf("  served breakout=%q rows=%d trend=1", string(a.breakout), rows)) {
				t.Errorf("500 entries, 250 distinct:\n%.300s\nwant the answer of the 250 distinct ids (%d rows):\n%.300s", got, rows, want)
			}
		})
	}
}
