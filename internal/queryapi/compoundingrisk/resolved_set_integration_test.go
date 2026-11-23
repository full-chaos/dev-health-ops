//go:build integration

package compoundingrisk

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// CHAOS-14480: a resolved set is bound whole. 50,000 synthetic repository ids;
// only the ids that sort first, in the middle and last have a repository and a
// risk row. Each reader binds all 50,000 in one statement and serves the three.
func TestRealClickHouse_ResolvedSetOf50000IdsIsServedWhole(t *testing.T) {
	ctx, conn, client := startStore(t)
	const n = 50000
	ids := make(resolvedIDs, 0, n)
	for i := 1; i <= n; i++ {
		ids = append(ids, fmt.Sprintf("00000000-0000-4000-8000-%012d", i))
	}
	sort.Strings(ids)
	first, middle, last := ids[0], ids[n/2], ids[n-1]
	now := sweepNow()
	day := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	world := sweepWorld{
		repos: []sweepRepo{{first, "acme/first"}, {middle, "acme/middle"}, {last, "acme/last"}},
		teams: []sweepTeam{{id: "t-none", name: "None"}},
		risk: []sweepRiskRow{
			bigRiskRow(day, first, .25, now),
			bigRiskRow(day, middle, .5, now),
			bigRiskRow(day, last, .75, now),
		},
	}
	uuidOf := map[string]string{first: first, middle: middle, last: last}
	sweepSeed(t, ctx, conn, world, uuidOf)
	t.Logf("joined ids: %d bytes", len(strings.Join(ids, ",")))

	rows, err := latestRowsLimit(ctx, client, sweepOrg, day, scopeRepo, ids, 0)
	if err != nil {
		t.Fatalf("latestRowsLimit with %d ids: the server refused or failed: %v", n, err)
	}
	var got []string
	for _, r := range rows {
		got = append(got, r.scopeID)
	}
	sort.Strings(got)
	if want := []string{first, middle, last}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("latestRowsLimit rows %v; want %v (first, middle and last id of the set)", got, want)
	}

	trend, err := repoTrend(ctx, client, sweepOrg, day, 7, ids)
	if err != nil {
		t.Fatalf("repoTrend with %d ids: the server refused or failed: %v", n, err)
	}
	if len(trend) != 1 || trend[0].day.Format(dateLayout) != day.Format(dateLayout) || trend[0].avgScore == nil || *trend[0].avgScore != .5 {
		var points []string
		for _, p := range trend {
			points = append(points, p.day.Format(dateLayout)+"="+sweepNum(p.avgScore))
		}
		t.Errorf("repoTrend %v; want [%s=0.5] (the mean of the three rows)", points, day.Format(dateLayout))
	}

	picked, err := latestDay(ctx, client, sweepOrg, scopeRepo, ids, day.AddDate(0, 0, -6), day)
	if err != nil {
		t.Fatalf("latestDay with %d ids: the server refused or failed: %v", n, err)
	}
	if picked == nil || picked.Format(dateLayout) != day.Format(dateLayout) {
		t.Errorf("latestDay %v; want %s", picked, day.Format(dateLayout))
	}
}

// CHAOS-14521: stored team rows for 501 named teams on real ClickHouse. "rows":
// every team has a stored row on d; "day": only tm-500, the 501st named team,
// has a stored row on d (every other row is on the day before). Both pick the
// day and serve tm-500's stored row on d.
func TestRealClickHouse_FiveHundredOneNamedTeamsStoredRowsAnswerTheLastTeamsRowAndDay(t *testing.T) {
	_, conn, client := startStore(t)
	// Each answer reads ownership once per named team, more than once.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	d := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		name     string
		lastOnly bool
		wantRows int
	}{
		{"rows", false, maxRows + 1},
		{"day", true, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			world, teamIDs := namedTeamsWorld(maxRows+1, d, c.lastOnly)
			uuidOf, _ := sweepUUIDs(world)
			sweepSeed(t, ctx, conn, world, uuidOf)
			got, err := Resolve(ctx, client, sweepOrg, namedTeamsFilter(teamIDs, nil), sweepNow())
			if err != nil {
				t.Fatal(err)
			}
			if last := servedTeamRow(got, "tm-500"); len(got.Rows) != c.wantRows || last != lastNamedTeamStoredRow {
				t.Errorf("rows=%d tm-500 row %s; want %d rows and tm-500 row %s (its stored row on the picked day)",
					len(got.Rows), last, c.wantRows, lastNamedTeamStoredRow)
			}
		})
	}
}
