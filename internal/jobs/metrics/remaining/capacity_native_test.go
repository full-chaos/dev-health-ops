package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
)

func TestCapacityExecutorFailsClosedWithoutAConnection(t *testing.T) {
	if _, err := NewCapacityExecutor(context.Background(), nil, nil); err == nil {
		t.Fatal("a nil connection must refuse, not degrade to the bridge")
	}
}

func TestEveryColumnTheExecutorTouchesIsRequiredAtStartup(t *testing.T) {
	// The startup probe is only worth having if its required set actually
	// covers what the code uses. These are the columns the queries and the
	// insert in capacity_native_clickhouse.go name; a column added to either
	// without being added here would pass startup and then fail on every
	// partition, which is the retry storm the probe exists to prevent.
	for table, requirement := range capacityTableRequirements {
		if len(requirement.columns) == 0 {
			t.Errorf("%s requires no columns, so its probe asserts nothing", table)
		}
	}
	for _, column := range []string{"items_completed", "wip_count_end_of_day", "day"} {
		if !slices.Contains(
			capacityTableRequirements["work_item_metrics_daily"].columns, column,
		) {
			t.Errorf("the throughput/backlog reads use %q but startup does not require it", column)
		}
	}
	// The writer binds by POSITION, so every inserted column must be required.
	for _, column := range []string{
		"forecast_id", "p50_days", "p95_items", "throughput_stddev", "org_id",
	} {
		if !slices.Contains(
			capacityTableRequirements["capacity_forecasts"].columns, column,
		) {
			t.Errorf("the insert writes %q but startup does not require it", column)
		}
	}
}

func TestEveryTableReadWithFINALRequiresTheReplacingEngine(t *testing.T) {
	// The column half of the probe is not the whole precondition. FINAL is a
	// no-op outside the Replacing family, so a table read with FINAL under a
	// plain MergeTree returns superseded rows that then aggregate into the
	// forecast -- a wrong answer reported as a successful write.
	//
	// This pins the two halves together: any table whose query text carries
	// FINAL must also carry the engine requirement. Adding a FINAL read of a
	// new table without setting readWithFINAL would otherwise reintroduce
	// exactly the gap this replaces.
	source := capacityClickHouseSource(t)
	for table, requirement := range capacityTableRequirements {
		readWithFINAL := strings.Contains(source, table+" FINAL")
		if readWithFINAL && !requirement.readWithFINAL {
			t.Errorf(
				"%s is read with FINAL but its requirement does not demand the "+
					"Replacing family, so a plain MergeTree would pass startup",
				table)
		}
		if !readWithFINAL && requirement.readWithFINAL {
			t.Errorf(
				"%s demands the Replacing family but nothing reads it with FINAL, "+
					"so the probe refuses deployments it does not need to",
				table)
		}
	}
}

// capacityClickHouseSource reads the query file this probe is derived from.
//
// Reading the SOURCE rather than restating its query text keeps one definition
// of what is read with FINAL. A copy here would be free to keep asserting the
// old shape after the queries moved on, which is the failure the requirement
// set itself is designed against.
func capacityClickHouseSource(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("capacity_native_clickhouse.go")
	if err != nil {
		t.Fatalf("read the capacity query source: %v", err)
	}
	return string(raw)
}

func TestNarrowingRefusesValuesTheColumnCannotHold(t *testing.T) {
	// Go narrows silently: uint32(6e9) is 1705032704 with no panic and no
	// error. Two legitimate rows at 3e9 wip therefore used to produce a
	// plausible 1.7e9 backlog that the batch accepted and the row counter
	// reported as healthy. Each case below is a value that MUST refuse.
	for _, testCase := range []struct {
		name  string
		row   capacityRow
		field string
	}{
		{
			name:  "backlog above UInt32",
			row:   capacityRow{BacklogSize: math.MaxUint32 + 1},
			field: "backlog_size",
		},
		{
			name:  "negative backlog",
			row:   capacityRow{BacklogSize: -1},
			field: "backlog_size",
		},
		{
			name:  "target items above UInt32",
			row:   capacityRow{TargetItems: intPointer(math.MaxUint32 + 1)},
			field: "target_items",
		},
		{
			name: "history days above UInt16",
			row: capacityRow{Forecast: numerical.ForecastResult{
				HistoryDays: math.MaxUint16 + 1,
			}},
			field: "history_days",
		},
		{
			name: "p95 items above UInt32",
			row: capacityRow{Forecast: numerical.ForecastResult{
				P95Items: intPointer(math.MaxUint32 + 1),
			}},
			field: "p95_items",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := narrowCapacityRow(testCase.row)
			if err == nil {
				t.Fatal("an unrepresentable value was accepted, so it would be " +
					"written wrapped and counted as a successful write")
			}
			if !errors.Is(err, ErrCapacityValueOutOfRange) {
				t.Fatalf("refusal must be identifiable, got %v", err)
			}
			if !strings.Contains(err.Error(), testCase.field) {
				t.Fatalf("refusal must name the offending field %q, got %v",
					testCase.field, err)
			}
		})
	}
}

func TestNarrowingAcceptsTheWidestRepresentableRow(t *testing.T) {
	// The negative control for the test above: a guard that refused everything
	// would pass it while breaking every real write. These are the largest
	// values each column CAN hold, and they must all survive.
	narrowed, err := narrowCapacityRow(capacityRow{
		BacklogSize: math.MaxUint32,
		TargetItems: intPointer(math.MaxUint32),
		Forecast: numerical.ForecastResult{
			HistoryDays:     math.MaxUint16,
			SimulationCount: math.MaxUint32,
			P50Days:         intPointer(math.MaxUint16),
			P95Items:        intPointer(math.MaxUint32),
		},
	})
	if err != nil {
		t.Fatalf("representable values must be accepted: %v", err)
	}
	if narrowed.backlogSize != math.MaxUint32 {
		t.Errorf("backlog_size = %d, want %d", narrowed.backlogSize, uint32(math.MaxUint32))
	}
	if narrowed.targetItems == nil || *narrowed.targetItems != math.MaxUint32 {
		t.Error("target_items lost its value on the way through")
	}
	if narrowed.p50Days == nil || *narrowed.p50Days != math.MaxUint16 {
		t.Error("p50_days lost its value on the way through")
	}
}

func TestNarrowingKeepsAbsentValuesAbsent(t *testing.T) {
	// nil is not out of range: an unset percentile writes NULL, and turning it
	// into a zero would make "no estimate" indistinguishable from "zero days".
	narrowed, err := narrowCapacityRow(capacityRow{})
	if err != nil {
		t.Fatalf("an empty row must be writable: %v", err)
	}
	for name, value := range map[string]bool{
		"target_items": narrowed.targetItems != nil,
		"p50_days":     narrowed.p50Days != nil,
		"p85_days":     narrowed.p85Days != nil,
		"p95_days":     narrowed.p95Days != nil,
		"p50_items":    narrowed.p50Items != nil,
		"p85_items":    narrowed.p85Items != nil,
		"p95_items":    narrowed.p95Items != nil,
	} {
		if value {
			t.Errorf("%s became present, so a NULL would be written as a number", name)
		}
	}
}

func TestCapacityRefusesARunWithoutASeed(t *testing.T) {
	// An unseeded Monte Carlo does not fail -- it produces plausible numbers
	// that differ on every run, so no comparison against Python could ever
	// hold. Python refuses identically at worker_metrics.py:892, and the run
	// table enforces seed presence for this family alone (postgres.go:557).
	executor := &CapacityExecutor{conn: stubConn{}}
	scope, err := json.Marshal(map[string]any{
		"version": 1, "history_days": 90, "simulations": 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = executor.ComputePartition(
		context.Background(),
		Run{ID: "r", OrganizationID: "org", Family: "capacity", Seed: nil},
		Partition{ID: "p", RunID: "r", Scope: scope},
	)
	if !errors.Is(err, ErrCapacitySeedMissing) {
		t.Fatalf("expected a seed-missing refusal, got: %v", err)
	}
}

func TestBacklogFallbackIsFalsyNotNil(t *testing.T) {
	// job_capacity.py:94 is `items = target_items if target_items else backlog`.
	// A scope carrying target_items = 0 therefore falls back to the BACKLOG,
	// where a nil-check port would forecast zero items and skip the scope. The
	// two disagree on exactly one input, which is the one a hand-written
	// fixture is least likely to include.
	tests := []struct {
		name        string
		targetItems *int
		backlog     int
		want        int
	}{
		{name: "absent falls back", targetItems: nil, backlog: 40, want: 40},
		{name: "ZERO falls back, as Python's falsy check does",
			targetItems: intPointer(0), backlog: 40, want: 40},
		{name: "a real target wins", targetItems: intPointer(12), backlog: 40, want: 12},
		{name: "a negative target is kept, then skipped downstream",
			targetItems: intPointer(-3), backlog: 40, want: -3},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got := resolveTargetItems(test.targetItems, test.backlog)
			if got != test.want {
				t.Errorf("resolved items = %d, want %d", got, test.want)
			}
		})
	}
}

func TestThroughputQueryUsesTheClientSideWindow(t *testing.T) {
	// Python derives the window start from utc_today() on the CLIENT
	// (capacity_queries.py:26), not from ClickHouse's today(). The difference
	// is observable: the two clocks can straddle midnight independently, and
	// the window decides which rows load at all.
	arguments := map[string]any{}
	conditions := capacityScopeFilters("org", capacityTarget{}, arguments)
	if len(conditions) != 1 || !strings.Contains(conditions[0], "org_id") {
		t.Fatalf("an unscoped forecast must still be org-scoped: %v", conditions)
	}
	if _, bound := arguments["team_id"]; bound {
		t.Error("team_id must not be bound when the scope carries none")
	}

	teamID := "team-a"
	arguments = map[string]any{}
	conditions = capacityScopeFilters("org", capacityTarget{TeamID: &teamID}, arguments)
	if len(conditions) != 2 {
		t.Fatalf("a team-scoped forecast needs both filters: %v", conditions)
	}
	if arguments["team_id"] != teamID {
		t.Errorf("team_id = %v", arguments["team_id"])
	}
}

func TestEmptyScopeValuesDoNotBecomeFilters(t *testing.T) {
	// An empty team id means "unscoped", not "a team whose id is the empty
	// string". Binding it would silently return no rows.
	empty := ""
	arguments := map[string]any{}
	conditions := capacityScopeFilters(
		"org", capacityTarget{TeamID: &empty, WorkScopeID: &empty}, arguments)
	if len(conditions) != 1 {
		t.Fatalf("empty scope values must not add filters: %v", conditions)
	}
}

func intPointer(value int) *int { return &value }

// stubConn satisfies just enough of driver.Conn for the seed refusal, which
// returns before any query runs.
type stubConn struct{ driverConnStub }

func TestDiscoveredEmptyScopeIsNotTheSameAsAnAbsentOne(t *testing.T) {
	// Two different things reach the writer as team_id, and they must stay
	// different:
	//
	//   an EXPLICIT scope omitting team_id  -> nil -> writes NULL
	//   DISCOVERY of an unteamed row        -> ""  -> writes ""
	//
	// team_id is LowCardinality(String) in work_item_metrics_daily, so
	// ClickHouse returns "" and never NULL, and Python carries that "" into the
	// forecast row. capacity_forecasts.team_id is Nullable(String), so the two
	// land as distinct values and the comparator sees them as distinct.
	// Normalising "" to nil in discovery reads like tidying and silently
	// rewrites one into the other.
	explicit := capacityTarget{}
	if explicit.TeamID != nil {
		t.Fatal("an explicit scope with no team must carry nil, to write NULL")
	}

	empty := ""
	discovered := capacityTarget{TeamID: &empty, WorkScopeID: &empty}
	if discovered.TeamID == nil {
		t.Fatal("a discovered unteamed row must carry \"\", not nil")
	}
	if *discovered.TeamID != "" {
		t.Fatalf("discovered team id = %q", *discovered.TeamID)
	}

	// ...while the FILTER treats both alike, matching Python's `if team_id:`
	// falsy check. Same value, different question.
	arguments := map[string]any{}
	if conditions := capacityScopeFilters("org", discovered, arguments); len(conditions) != 1 {
		t.Errorf("an empty discovered scope must add no filter: %v", conditions)
	}
	arguments = map[string]any{}
	if conditions := capacityScopeFilters("org", explicit, arguments); len(conditions) != 1 {
		t.Errorf("an absent scope must add no filter: %v", conditions)
	}
}

func TestAggregateNarrowingRefusesValuesTheKernelCannotHold(t *testing.T) {
	// The mirror of TestNarrowingRefusesValuesTheColumnCannotHold. That guard
	// covers values going OUT to ClickHouse; this covers values coming IN.
	// Fixing one direction of a symmetric boundary and leaving the other is how
	// a guarded write path ends up fed by an unguarded read path.
	//
	// sum() over a UInt32 column widens to UInt64, so these arrive as uint64
	// and the kernel works in int. Above MaxInt64 that conversion wraps
	// NEGATIVE, and a negative count is not merely wrong: a negative backlog
	// makes the scope skip and the partition return successfully having
	// forecast nothing, which reads as a quiet day rather than as a fault.
	for _, testCase := range []struct {
		name  string
		field string
		value uint64
	}{
		{
			name:  "backlog above MaxInt64",
			field: "wip_count_end_of_day",
			value: uint64(math.MaxInt64) + 1,
		},
		{
			name:  "throughput above MaxInt64",
			field: "items_completed",
			value: math.MaxUint64,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := capacityCountFromAggregate(testCase.field, testCase.value)
			if err == nil {
				t.Fatalf(
					"an aggregate the kernel cannot represent was accepted and "+
						"became %d; a negative count silently skips the scope and "+
						"reports the partition as successful", got)
			}
			if !errors.Is(err, ErrCapacityValueOutOfRange) {
				t.Fatalf("refusal must be identifiable, got %v", err)
			}
			if !strings.Contains(err.Error(), testCase.field) {
				t.Fatalf("refusal must name the offending aggregate %q, got %v",
					testCase.field, err)
			}
		})
	}
}

func TestAggregateNarrowingAcceptsTheWidestRepresentableCount(t *testing.T) {
	// Negative control: a guard that refused everything would pass the test
	// above while breaking every real read.
	got, err := capacityCountFromAggregate("items_completed", uint64(math.MaxInt64))
	if err != nil {
		t.Fatalf("a representable aggregate must be accepted: %v", err)
	}
	if got != math.MaxInt64 {
		t.Fatalf("value changed on the way through: got %d", got)
	}
	if zero, err := capacityCountFromAggregate("items_completed", 0); err != nil || zero != 0 {
		t.Fatalf("zero must survive unchanged: %d, %v", zero, err)
	}
}
