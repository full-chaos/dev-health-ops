//go:build integration

// CHAOS-5349's recurrence guard for the two capacity resolvers, against a REAL
// ClickHouse engine (internal/testsupport/containers.StartClickHouse plus the
// real migration chain via internal/testsupport/chschema.Apply), never the fake
// RowScanner the capacityforecast package's own unit tests use.
//
// # Why a fake cannot cover this
//
// Every scan destination in capacityforecast/clickhouse.go is a claim about a
// COLUMN TYPE, and a fake scanner hands back whatever Go value its author
// declared -- so it agrees with any claim, including a wrong one. The
// investmentexplain package next door shipped exactly that defect twice in one
// PR: a Map(String, Float64) column scanned into *string passed every fixture
// test and failed outright against the driver.
//
// capacity_forecasts is unusually exposed to this. Migration 023 declares FIVE
// integer widths across 21 columns -- UInt16 for the day percentiles, UInt32
// for backlog/items, UInt8 for the two boolean flags, DateTime64(3) for
// computed_at, Date for four date columns -- and nine of them are Nullable.
// Widening them all to int64, the natural way to write this, compiles cleanly
// and fails only here.
//
// # This file lives in package main deliberately
//
// It exercises capacityforecast from OUTSIDE, through the package's exported
// entry points, and it sits in an ALREADY-SHARDED package (cmd/query-api is in
// ci/go_integration_shards.tsv). Putting it in cmd/query-api/internal/
// capacityforecast would have made that a new integration package, requiring a
// new .tsv row AND an addition to tests/tooling/test_go_integration_sharding.py's
// EXPECTED_PACKAGES -- a three-way pin for no additional coverage.
package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/capacityforecast"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// migratedClickHouse starts a container and applies the REAL migration chain,
// rather than hand-writing DDL for the two tables under test.
//
// Hand-written DDL is a second copy of the schema, and a second copy is exactly
// what this file exists to disprove the existence of: if the DDL here drifted
// from migrations/clickhouse/, the test would keep passing against a schema
// production does not have.
func migratedClickHouse(ctx context.Context, t *testing.T) (*containers.Instance, *dhclickhouse.Client, stdclickhouse.Conn) {
	t.Helper()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })

	chschema.Apply(ctx, t, instance)

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatalf("build the read-only query client: %v", err)
	}

	// dev-health-go's Client rejects anything but a literal leading SELECT
	// (validateReadOnlyStatement), so seeding goes through the driver directly
	// -- the same split query_route_integration_test.go's own seeding uses.
	options, err := stdclickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	raw, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open a raw ClickHouse connection for seeding: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })

	return instance, client, raw
}

func TestCapacityForecastsReadsRealColumnTypes(t *testing.T) {
	ctx := context.Background()
	_, client, raw := migratedClickHouse(ctx, t)

	// One row per org, so the SAME statement that returns the wanted row would
	// also return the other one if the org predicate were missing or bound
	// wrong. A single-org fixture cannot fail that way.
	seed := `
        INSERT INTO capacity_forecasts (
            forecast_id, computed_at, org_id, team_id, work_scope_id,
            backlog_size, target_items, target_date, history_days,
            simulation_count, p50_days, p85_days, p95_days,
            p50_date, p85_date, p95_date, p50_items, p85_items, p95_items,
            throughput_mean, throughput_stddev, insufficient_history, high_variance
        ) VALUES
        ('forecast-mine-old', toDateTime64('2026-08-30 09:00:00.000', 3, 'UTC'), 'org-mine',
         'team-alpha', 'scope-beta', 200, 120, toDate('2026-10-01'), 90, 10000,
         19, 24, 27, toDate('2026-09-20'), toDate('2026-09-25'), toDate('2026-09-28'),
         NULL, NULL, NULL, 4.25, 1.5, 0, 1),
        ('forecast-mine-new', toDateTime64('2026-09-01 12:30:00.000', 3, 'UTC'), 'org-mine',
         NULL, NULL, 50, NULL, NULL, 30, 500,
         NULL, NULL, NULL, NULL, NULL, NULL, 11, 7, 3, 1.75, 0.25, 1, 0),
        ('forecast-theirs', toDateTime64('2026-09-02 12:30:00.000', 3, 'UTC'), 'org-theirs',
         'team-alpha', 'scope-beta', 999, 999, toDate('2026-11-01'), 90, 10000,
         5, 6, 7, toDate('2026-09-10'), toDate('2026-09-11'), toDate('2026-09-12'),
         NULL, NULL, NULL, 9.5, 2.5, 0, 0)
    `
	if err := raw.Exec(ctx, seed); err != nil {
		t.Fatalf("seed capacity_forecasts: %v", err)
	}

	got, err := capacityforecast.ResolveForecasts(ctx, client, "org-mine", nil)
	if err != nil {
		t.Fatalf("ResolveForecasts: %v", err)
	}
	if len(got.Edges) != 2 {
		t.Fatalf("edges: got %d, want the 2 rows belonging to org-mine", len(got.Edges))
	}

	// ORDER BY computed_at DESC, so the newer row leads.
	newest, oldest := got.Edges[0].Node, got.Edges[1].Node
	if newest.ForecastID != "forecast-mine-new" || oldest.ForecastID != "forecast-mine-old" {
		t.Fatalf("order: got %q then %q, want the newest computed_at first",
			newest.ForecastID, oldest.ForecastID)
	}
	for _, edge := range got.Edges {
		if edge.Node.ForecastID == "forecast-theirs" {
			t.Fatal("a row from another org crossed the tenant boundary")
		}
	}

	// The fixed-scope row: every Nullable(UInt16)/Nullable(UInt32)/Nullable(Date)
	// column populated, both UInt8 flags read, and computed_at rendered as
	// RFC 3339 with an explicit offset (CHAOS-5450 / R55) -- this is the
	// END-TO-END pin for the list path, through a real ClickHouse row rather
	// than a fake scanner, so the DateTime64(3, 'UTC') scan and the rendering
	// are proved together.
	if oldest.ComputedAt != "2026-08-30T09:00:00+00:00" {
		t.Errorf("computedAt: got %q, want RFC 3339 with an explicit +00:00 offset", oldest.ComputedAt)
	}
	if oldest.BacklogSize != 200 {
		t.Errorf("backlogSize: got %d, want 200", oldest.BacklogSize)
	}
	if oldest.TargetItems == nil || *oldest.TargetItems != 120 {
		t.Errorf("targetItems: got %v, want 120", oldest.TargetItems)
	}
	if oldest.P50Days == nil || *oldest.P50Days != 19 {
		t.Errorf("p50Days: got %v, want 19", oldest.P50Days)
	}
	if oldest.P95Days == nil || *oldest.P95Days != 27 {
		t.Errorf("p95Days: got %v, want 27", oldest.P95Days)
	}
	if oldest.TargetDate == nil || oldest.TargetDate.String() != "2026-10-01" {
		t.Errorf("targetDate: got %v, want 2026-10-01", oldest.TargetDate)
	}
	if oldest.P85Date == nil || oldest.P85Date.String() != "2026-09-25" {
		t.Errorf("p85Date: got %v, want 2026-09-25", oldest.P85Date)
	}
	if oldest.InsufficientHistory {
		t.Error("insufficientHistory: got true, want false from a stored 0")
	}
	if !oldest.HighVariance {
		t.Error("highVariance: got false, want true from a stored 1")
	}
	if oldest.ThroughputMean != 4.25 || oldest.ThroughputStddev != 1.5 {
		t.Errorf("throughput stats: got %v/%v, want 4.25/1.5",
			oldest.ThroughputMean, oldest.ThroughputStddev)
	}
	if oldest.HistoryDays != 90 {
		t.Errorf("historyDays: got %d, want 90", oldest.HistoryDays)
	}
	// NULLs must stay absent, never become zero: "we did not forecast items" is
	// a different answer from "we forecast zero items", and this is the row
	// where the whole item family is NULL.
	if oldest.P50Items != nil || oldest.P85Items != nil || oldest.P95Items != nil {
		t.Errorf("item percentiles: got %v/%v/%v, want all three nil",
			oldest.P50Items, oldest.P85Items, oldest.P95Items)
	}

	// The fixed-date row exercises the OPPOSITE null pattern -- items present,
	// days and every date absent, and both scope columns NULL rather than "".
	if newest.P50Items == nil || *newest.P50Items != 11 {
		t.Errorf("p50Items: got %v, want 11", newest.P50Items)
	}
	if newest.P50Days != nil || newest.P50Date != nil || newest.TargetDate != nil {
		t.Errorf("fixed-date row: got p50Days=%v p50Date=%v targetDate=%v, want all nil",
			newest.P50Days, newest.P50Date, newest.TargetDate)
	}
	if newest.TeamID != nil || newest.WorkScopeID != nil {
		t.Errorf("scope columns: got team=%v scope=%v, want both nil for an org-wide row",
			newest.TeamID, newest.WorkScopeID)
	}
	if !newest.InsufficientHistory || newest.HighVariance {
		t.Errorf("flags: got insufficient=%v highVariance=%v, want true/false",
			newest.InsufficientHistory, newest.HighVariance)
	}

	// The cursor is the forecast id verbatim, straight off a real row.
	if got.Edges[0].Cursor != "forecast-mine-new" {
		t.Errorf("cursor: got %q, want the forecast id verbatim", got.Edges[0].Cursor)
	}
	if got.TotalCount != 2 {
		t.Errorf("totalCount: got %d, want the page length 2", got.TotalCount)
	}
}

func TestCapacityForecastsFiltersAgainstRealColumns(t *testing.T) {
	ctx := context.Background()
	_, client, raw := migratedClickHouse(ctx, t)

	seed := `
        INSERT INTO capacity_forecasts (
            forecast_id, computed_at, org_id, team_id, work_scope_id,
            backlog_size, history_days, simulation_count,
            throughput_mean, throughput_stddev, insufficient_history, high_variance
        ) VALUES
        ('alpha-aug', toDateTime64('2026-08-10 00:00:00.000', 3, 'UTC'), 'org-mine', 'team-alpha', 'scope-x', 10, 90, 100, 1.0, 0.0, 0, 0),
        ('alpha-sep', toDateTime64('2026-09-10 00:00:00.000', 3, 'UTC'), 'org-mine', 'team-alpha', 'scope-x', 20, 90, 100, 2.0, 0.0, 0, 0),
        ('beta-sep',  toDateTime64('2026-09-11 00:00:00.000', 3, 'UTC'), 'org-mine', 'team-beta',  'scope-x', 30, 90, 100, 3.0, 0.0, 0, 0)
    `
	if err := raw.Exec(ctx, seed); err != nil {
		t.Fatalf("seed capacity_forecasts: %v", err)
	}

	teamID := "team-alpha"
	from := graphqldate.New(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	filters := &model.CapacityForecastFilterInput{TeamID: &teamID, FromDate: &from, Limit: 10}

	got, err := capacityforecast.ResolveForecasts(ctx, client, "org-mine", filters)
	if err != nil {
		t.Fatalf("ResolveForecasts: %v", err)
	}
	// team-alpha excludes beta-sep; fromDate excludes alpha-aug. Both filters
	// must bite, and the toDate(computed_at) cast must work against the real
	// DateTime64 column -- comparing a Date parameter to a DateTime64 without
	// the cast is a type error the fake scanner never sees.
	if len(got.Edges) != 1 || got.Edges[0].Node.ForecastID != "alpha-sep" {
		ids := make([]string, 0, len(got.Edges))
		for _, edge := range got.Edges {
			ids = append(ids, edge.Node.ForecastID)
		}
		t.Fatalf("got %v, want exactly [alpha-sep]", ids)
	}
}

func TestCapacityForecastComputesAgainstRealMetricsRows(t *testing.T) {
	ctx := context.Background()
	_, client, raw := migratedClickHouse(ctx, t)

	// 30 consecutive days ending on a fixed anchor, one row per day, plus a
	// same-scope row for ANOTHER org that must never be summed in.
	anchor := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	values := ""
	for index := range 30 {
		if index > 0 {
			values += ", "
		}
		day := anchor.AddDate(0, 0, -29+index).Format("2006-01-02")
		values += fmt.Sprintf(
			"(toDate('%s'), 'jira', 'scope-x', 'team-alpha', %d, %d, toDateTime('2026-09-01 00:00:00'), 'org-mine')",
			day, 5, 40,
		)
	}
	values += fmt.Sprintf(
		", (toDate('%s'), 'jira', 'scope-x', 'team-alpha', 999, 9999, toDateTime('2026-09-01 00:00:00'), 'org-theirs')",
		anchor.Format("2006-01-02"),
	)
	seed := `
        INSERT INTO work_item_metrics_daily
            (day, provider, work_scope_id, team_id, items_completed, wip_count_end_of_day, computed_at, org_id)
        VALUES ` + values
	if err := raw.Exec(ctx, seed); err != nil {
		t.Fatalf("seed work_item_metrics_daily: %v", err)
	}

	teamID := "team-alpha"
	input := &model.CapacityForecastInput{
		TeamID:      &teamID,
		HistoryDays: 90,
		// Small, because the assertions below are structural: this test proves
		// the READS work against a real engine, and the kernel's arithmetic is
		// pinned bit-for-bit by the golden fixtures instead.
		Simulations: 200,
	}

	got, err := capacityforecast.ResolveForecast(ctx, client, "org-mine", input, anchor)
	if err != nil {
		t.Fatalf("ResolveForecast: %v", err)
	}
	if got == nil {
		t.Fatal("got nil, want a forecast computed from the seeded rows")
	}

	// 30 seeded days, one row each. If the other org's row leaked in, this
	// would be 31 -- and its 999 items would move the mean too.
	if got.HistoryDays != 30 {
		t.Errorf("historyDays: got %d, want the 30 seeded days for this org", got.HistoryDays)
	}
	if got.ThroughputMean != 5 {
		t.Errorf("throughputMean: got %v, want exactly 5", got.ThroughputMean)
	}
	if got.ThroughputStddev != 0 {
		t.Errorf("throughputStddev: got %v, want 0 for a constant series", got.ThroughputStddev)
	}
	// The backlog is the LATEST day's wip sum for this scope: 40, not 9999 and
	// not 40*30.
	if got.BacklogSize != 40 {
		t.Errorf("backlogSize: got %d, want the latest day's 40", got.BacklogSize)
	}
	// No target supplied, so items falls back to the backlog.
	if got.TargetItems == nil || *got.TargetItems != 40 {
		t.Errorf("targetItems: got %v, want the backlog 40", got.TargetItems)
	}
	// 30 days is over the 14-day floor, and a constant series has zero variance.
	if got.InsufficientHistory {
		t.Error("insufficientHistory: got true, want false for 30 days")
	}
	if got.HighVariance {
		t.Error("highVariance: got true, want false for a constant series")
	}
	// 40 items at 5/day is 8 days, and every simulation draws the same value,
	// so all three percentiles land on exactly 8 regardless of the seed. That
	// is what makes this assertable at all despite the per-request random seed.
	for name, value := range map[string]*int{
		"p50Days": got.P50Days, "p85Days": got.P85Days, "p95Days": got.P95Days,
	} {
		if value == nil || *value != 8 {
			t.Errorf("%s: got %v, want 8 (40 items at a constant 5/day)", name, value)
		}
	}
	if got.P50Date == nil || got.P50Date.String() != "2026-09-09" {
		t.Errorf("p50Date: got %v, want the anchor plus 8 days", got.P50Date)
	}
}

func TestCapacityForecastReturnsNilForAnOrgWithNoRows(t *testing.T) {
	ctx := context.Background()
	_, client, _ := migratedClickHouse(ctx, t)

	got, err := capacityforecast.ResolveForecast(
		ctx, client, "org-empty", nil, time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ResolveForecast: %v", err)
	}
	// A tolerated empty against a REAL, migrated but unseeded schema -- not an
	// error, and not a zero-valued forecast.
	if got != nil {
		t.Fatalf("got %+v, want nil for an org with no throughput history", got)
	}
}
