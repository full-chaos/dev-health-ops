//go:build integration

package operatingreview

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// reviewRows is what the operating review readers of the team-keyed daily
// tables give for one organization and period.
type reviewRows struct {
	WorkItems      []workItemsRow
	StateDurations []stateDurationRow
	Investment     []investmentRow
	AIImpact       []aiImpactRow
	AIGovernance   []aiGovernanceRawRow
}

// byText orders rows whose query gives no order (GROUP BY with no ORDER BY).
func byText[T any](rows []T) []T {
	sort.Slice(rows, func(i, j int) bool { return fmt.Sprint(rows[i]) < fmt.Sprint(rows[j]) })
	return rows
}

func readReviewRows(ctx context.Context, t *testing.T, client QueryClient, org string, start, end time.Time) reviewRows {
	t.Helper()
	var rows reviewRows
	var err error
	if rows.WorkItems, err = fetchWorkItems(ctx, client, org, nil, start, end); err != nil {
		t.Fatalf("%s work items: %v", org, err)
	}
	if rows.StateDurations, err = fetchStateDurations(ctx, client, org, nil, start, end); err != nil {
		t.Fatalf("%s state durations: %v", org, err)
	}
	if rows.Investment, err = fetchInvestment(ctx, client, org, nil, start, end); err != nil {
		t.Fatalf("%s investment: %v", org, err)
	}
	if rows.AIImpact, err = fetchAIImpact(ctx, client, org, nil, start, end); err != nil {
		t.Fatalf("%s ai impact: %v", org, err)
	}
	rows.StateDurations = byText(rows.StateDurations)
	rows.Investment = byText(rows.Investment)
	rows.AIImpact = byText(rows.AIImpact)
	rows.AIGovernance = byText(rows.AIGovernance)
	return rows
}

// TestOperatingReviewReadersGiveRetractionRowsNoWeight reads the all-teams
// period rows of two organizations that hold the same measurements. One of
// them also holds the old rows of the retired team ids and the retraction row
// over each (package retractionseed). The rows must be the same: a retraction
// row is not a sample of the mean time in a state and not a coverage row.
func TestOperatingReviewReadersGiveRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	start, end := store.Days[1], store.Days[len(store.Days)-1].AddDate(0, 0, 1)
	control := readReviewRows(ctx, t, client, retractionseed.ControlOrg, start, end)

	// The control rows, from the seed: four teams, six days. A team touches
	// 2, 3, 4 and 5 items in each state each day. Blocked: the mean of 6, 8,
	// 10 and 12 hours. In progress: 12 hours for each team. avg_wip is the
	// hours over 24.
	wantStates := []stateDurationRow{
		{itemsTouched: 6 * 14, durationHours: 12, avgWip: 0.5},
		{itemsTouched: 6 * 14, durationHours: 9, avgWip: 0.375},
	}
	if !reflect.DeepEqual(control.StateDurations, wantStates) {
		t.Fatalf("control state durations = %+v, want %+v", control.StateDurations, wantStates)
	}
	if len(control.WorkItems) != 6 {
		t.Fatalf("control work items = %d days, want 6", len(control.WorkItems))
	}
	if want := []investmentRow{{investmentArea: "feature_delivery", deliveryUnits: 6 * (3 + 4 + 5 + 6)}}; !reflect.DeepEqual(control.Investment, want) {
		t.Fatalf("control investment = %+v, want %+v", control.Investment, want)
	}
	if len(control.AIImpact) != 1 || control.AIImpact[0].prsTotal != 6*(4+5+6+7) {
		t.Fatalf("control ai impact = %+v, want one bucket with 132 pull requests", control.AIImpact)
	}

	retracted := readReviewRows(ctx, t, client, retractionseed.RetractedOrg, start, end)
	if !reflect.DeepEqual(control, retracted) {
		t.Fatalf("the retraction rows changed a period row:\n control   %+v\n retracted %+v", control, retracted)
	}
}

// TestOperatingReviewReadsNoRowForRetractionRowsOnly holds a period whose only
// newest rows are retraction rows. It holds no measurement, so no reader
// returns a row, and AI governance coverage is missing (0.0), not "no AI
// artifact, so fully covered" (1.0).
func TestOperatingReviewReadsNoRowForRetractionRowsOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const org = "org-retraction-rows-only"
	day := store.Days[len(store.Days)-1]
	for _, team := range retractionseed.Teams {
		retractionseed.Retract(ctx, t, store.Conn, org, day, team, store.OldComputedAt, store.NewComputedAt)
	}
	rows := readReviewRows(ctx, t, client, org, day, day.AddDate(0, 0, 1))
	if len(rows.WorkItems)+len(rows.StateDurations)+len(rows.Investment)+len(rows.AIImpact)+len(rows.AIGovernance) != 0 {
		t.Errorf("a period of retraction rows only gives rows: %+v", rows)
	}
}
