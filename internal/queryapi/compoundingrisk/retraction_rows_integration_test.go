//go:build integration

package compoundingrisk

import (
	"context"
	"reflect"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

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
	const org = "org-retraction-rows-only"
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
