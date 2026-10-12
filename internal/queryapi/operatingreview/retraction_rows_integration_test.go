//go:build integration

package operatingreview

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// floatTolerance absorbs the summation order of a float mean: the seed days
// are relative to today, so the order of the addends moves with the date.
const floatTolerance = 1e-9

// equalWithinTolerance is reflect.DeepEqual with float fields compared within
// floatTolerance. Every other field must be exactly equal.
func equalWithinTolerance(a, b reflect.Value) bool {
	if a.Kind() != b.Kind() {
		return false
	}
	switch a.Kind() {
	case reflect.Float32, reflect.Float64:
		return math.Abs(a.Float()-b.Float()) <= floatTolerance
	case reflect.Slice, reflect.Array:
		if a.Kind() == reflect.Slice && a.IsNil() != b.IsNil() {
			return false
		}
		if a.Len() != b.Len() {
			return false
		}
		for i := 0; i < a.Len(); i++ {
			if !equalWithinTolerance(a.Index(i), b.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Struct:
		if a.Type() != b.Type() {
			return false
		}
		if a.Type() == reflect.TypeOf(time.Time{}) {
			return a.Interface().(time.Time).Equal(b.Interface().(time.Time))
		}
		for i := 0; i < a.NumField(); i++ {
			if !equalWithinTolerance(a.Field(i), b.Field(i)) {
				return false
			}
		}
		return true
	case reflect.Ptr:
		if a.IsNil() || b.IsNil() {
			return a.IsNil() == b.IsNil()
		}
		return equalWithinTolerance(a.Elem(), b.Elem())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return a.Int() == b.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return a.Uint() == b.Uint()
	case reflect.String:
		return a.String() == b.String()
	case reflect.Bool:
		return a.Bool() == b.Bool()
	default:
		panic(fmt.Sprintf("equalWithinTolerance: unsupported kind %s", a.Kind()))
	}
}

func sameWithinTolerance(a, b any) bool {
	return equalWithinTolerance(reflect.ValueOf(a), reflect.ValueOf(b))
}

// reviewRows is what the operating review readers of the team-keyed daily
// tables give for one organization and period.
type reviewRows struct {
	WorkItems      []workItemsRow
	StateDurations []stateDurationRow
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
	if rows.AIImpact, err = fetchAIImpact(ctx, client, org, nil, start, end); err != nil {
		t.Fatalf("%s ai impact: %v", org, err)
	}
	if rows.AIGovernance, err = fetchAIGovernance(ctx, client, org, nil, start, end); err != nil {
		t.Fatalf("%s ai governance: %v", org, err)
	}
	rows.StateDurations = byText(rows.StateDurations)
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
	if !sameWithinTolerance(control.StateDurations, wantStates) {
		t.Fatalf("control state durations = %+v, want %+v", control.StateDurations, wantStates)
	}
	if len(control.WorkItems) != 6 {
		t.Fatalf("control work items = %d days, want 6", len(control.WorkItems))
	}
	if len(control.AIImpact) != 1 || control.AIImpact[0].prsTotal != 6*(4+5+6+7) {
		t.Fatalf("control ai impact = %+v, want one bucket with 132 pull requests", control.AIImpact)
	}
	if len(control.AIGovernance) != 6*4 {
		t.Fatalf("control ai governance = %d rows, want one for each team and day (24)", len(control.AIGovernance))
	}

	retracted := readReviewRows(ctx, t, client, retractionseed.RetractedOrg, start, end)
	if !sameWithinTolerance(control, retracted) {
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

	const org = retractionseed.RetractionOnlyOrg
	day := store.Days[len(store.Days)-1]
	for _, team := range retractionseed.Teams {
		retractionseed.Retract(ctx, t, store.Conn, org, day, team, store.OldComputedAt, store.NewComputedAt)
	}
	rows := readReviewRows(ctx, t, client, org, day, day.AddDate(0, 0, 1))
	// The AI impact read is not in this check: ai_impact_metrics_daily stores
	// a measured row with 0 in every measure (the bucket 'unknown' of a group
	// with no pull request), so a retraction row cannot be told from a
	// measurement there. That read sums and takes Nullable means, which a
	// row of zeros does not move (held by the test above).
	if len(rows.WorkItems)+len(rows.StateDurations)+len(rows.AIGovernance) != 0 {
		t.Errorf("a period of retraction rows only gives rows: %+v", rows)
	}
	if coverage := aiGovernanceCoverage(rows.AIGovernance); coverage != 0 {
		t.Errorf("AI governance coverage = %v for a period with no measurement, want 0 (missing)", coverage)
	}
}
