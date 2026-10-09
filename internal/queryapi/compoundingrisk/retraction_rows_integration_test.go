//go:build integration

package compoundingrisk

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// TestTeamRiskRowsGiveRetractionRowsNoWeight reads the newest team risk day
// and its rows for two organizations that hold the same measurements. One of
// them also holds the old rows of the retired team ids and the retraction row
// over each (package retractionseed). The rows must be the same: a retraction
// row has a NULL score, like a team measured with too little data, but it is
// not a team and it is not listed.
func TestTeamRiskRowsGiveRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	newest := store.Days[len(store.Days)-1]
	read := func(org string) (time.Time, []storedRow) {
		t.Helper()
		day, err := latestDay(ctx, client, org, "team", nil, store.Days[1], newest)
		if err != nil {
			t.Fatalf("%s latest day: %v", org, err)
		}
		if day == nil {
			t.Fatalf("%s has no scored team risk day", org)
		}
		rows, err := latestRows(ctx, client, org, *day, "team", nil)
		if err != nil {
			t.Fatalf("%s latest rows: %v", org, err)
		}
		return *day, rows
	}

	controlDay, control := read(retractionseed.ControlOrg)

	// The control rows, from the seed: the four keyed ids on the newest day,
	// highest score first (0.625, 0.5, 0.375, 0.25).
	if !controlDay.Equal(newest) {
		t.Fatalf("control newest day = %s, want %s", controlDay, newest)
	}
	var ids []string
	var scores []float64
	for _, row := range control {
		if row.score == nil {
			t.Fatalf("control row %s has no score", row.scopeID)
		}
		ids, scores = append(ids, row.scopeID), append(scores, *row.score)
	}
	if want := []string{"linear:core", "gitlab:ops", "github:platform", "jira:ENG"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("control team rows = %v, want %v", ids, want)
	}
	if want := []float64{0.625, 0.5, 0.375, 0.25}; !reflect.DeepEqual(scores, want) {
		t.Fatalf("control scores = %v, want %v", scores, want)
	}

	retractedDay, retracted := read(retractionseed.RetractedOrg)
	if !retractedDay.Equal(controlDay) || !reflect.DeepEqual(control, retracted) {
		t.Fatalf("the retraction rows changed the team rows:\n control   %s %+v\n retracted %s %+v",
			controlDay, control, retractedDay, retracted)
	}

	// A day whose only newest team rows are retraction rows holds no team
	// row. The team breakout decides on "is there a team row" whether to
	// derive the teams from the repositories, so an empty list matters.
	const org = retractionseed.RetractionOnlyOrg
	for _, team := range retractionseed.Teams {
		retractionseed.Retract(ctx, t, store.Conn, org, newest, team, store.OldComputedAt, store.NewComputedAt)
	}
	rows, err := latestRows(ctx, client, org, newest, "team", nil)
	if err != nil {
		t.Fatalf("retraction-only latest rows: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("a day of retraction rows only lists team rows: %+v", rows)
	}
}

// TestTeamBreakoutListsTheActiveTeams resolves the team breakout of an
// organization whose newest day holds measured risk rows under a keyed id and
// under the retired id it replaced (the state of a day computed again before
// the writers stored retraction rows). With no team named, the breakout lists
// the active teams: a retired id is not a team, also when it holds a score. A
// breakout that names the retired id still gets its row.
func TestTeamBreakoutListsTheActiveTeams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	retractionseed.Double(ctx, t, store.Conn, store.Seed)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	now := store.Days[len(store.Days)-1].Add(12 * time.Hour)
	scopeIDs := func(teamIDs []string) []string {
		t.Helper()
		result, err := Resolve(ctx, client, retractionseed.DoubledOrg,
			&model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: teamIDs, TrendDays: 7}, now)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		var ids []string
		for _, row := range result.Rows {
			ids = append(ids, row.ScopeID)
		}
		sort.Strings(ids)
		return ids
	}
	if got, want := scopeIDs(nil), []string{"github:platform", "gitlab:ops", "jira:ENG", "linear:core"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("team breakout lists %v, want the active teams %v", got, want)
	}
	retired := retractionseed.Teams[0].RetiredID
	if got, want := scopeIDs([]string{retired}), []string{retired}; !reflect.DeepEqual(got, want) {
		t.Fatalf("team breakout of the named retired id lists %v, want %v", got, want)
	}
}
