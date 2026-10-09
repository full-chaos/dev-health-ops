//go:build integration

package cognitiveload

import (
	"context"
	"reflect"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// TestTeamCommitRatiosGiveRetractionRowsNoWeight reads the after-hours and
// weekend commit ratios, for the whole organization and for one team, of two
// organizations that hold the same measurements. One of them also holds the
// old rows of the retired team ids and the retraction row over each (package
// retractionseed). The days must be the same: the pinned query gives a team
// with no commit a ratio of 0.0 and counts it in the mean across teams, and a
// retired team id has no commit because it was not measured.
func TestTeamCommitRatiosGiveRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Inclusive day window: the days computed again.
	since, until := store.Days[1].Format("2006-01-02"), store.Days[len(store.Days)-1].Format("2006-01-02")
	retiredID := retractionseed.Teams[0].RetiredID
	keyedID := retractionseed.Teams[0].KeyedID
	type answers struct{ Org, Keyed, Retired []teamRatioRow }
	read := func(org string) answers {
		t.Helper()
		var out answers
		var err error
		if out.Org, err = fetchTeamMetrics(ctx, client, org, since, until, nil); err != nil {
			t.Fatalf("%s org-wide: %v", org, err)
		}
		if out.Keyed, err = fetchTeamMetrics(ctx, client, org, since, until, &keyedID); err != nil {
			t.Fatalf("%s team %s: %v", org, keyedID, err)
		}
		if out.Retired, err = fetchTeamMetrics(ctx, client, org, since, until, &retiredID); err != nil {
			t.Fatalf("%s team %s: %v", org, retiredID, err)
		}
		return out
	}

	control := read(retractionseed.ControlOrg)

	// The control days, from the seed: four teams, six days. Of 8 commits a
	// day the teams make 2, 4, 6 and 8 after hours (mean ratio 0.625) and 0,
	// 1, 0 and 1 on a weekend (mean ratio 0.0625). The first team alone: 0.25
	// and 0. The retired id of that team holds no row of these days.
	if len(control.Org) != 6 || len(control.Keyed) != 6 {
		t.Fatalf("control days = %d org-wide, %d for %s, want 6 each", len(control.Org), len(control.Keyed), keyedID)
	}
	for i := range control.Org {
		if control.Org[i].afterHoursCommitRatio != 0.625 || control.Org[i].weekendCommitRatio != 0.0625 {
			t.Fatalf("control org-wide day = %+v, want 0.625 and 0.0625", control.Org[i])
		}
		if control.Keyed[i].afterHoursCommitRatio != 0.25 || control.Keyed[i].weekendCommitRatio != 0 {
			t.Fatalf("control day of %s = %+v, want 0.25 and 0", keyedID, control.Keyed[i])
		}
	}
	if len(control.Retired) != 0 {
		t.Fatalf("control days of the retired id = %+v, want none", control.Retired)
	}

	retracted := read(retractionseed.RetractedOrg)
	if !reflect.DeepEqual(control, retracted) {
		t.Fatalf("the retraction rows changed a day:\n control   %+v\n retracted %+v", control, retracted)
	}
}
