//go:build integration

package home

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// homeAnswers is what the home readers of the team-keyed daily tables give
// for one organization.
type homeAnswers struct {
	Values      map[string]metricValue
	Series      map[string][]dayValueRow
	Drivers     map[string][]string
	Blocked     float64
	BlockedDays int
	BlockedData bool
	Risk        []RiskRow
}

// TestHomeReadersGiveRetractionRowsNoWeight reads the home metrics of two
// organizations that hold the same measurements. One of them also holds, for
// each day computed again, the old rows of the retired team ids and the
// retraction row over each (package retractionseed). The answers must be the
// same: a retraction row is not a sample of a mean, not a row that proves
// data, not a driver to name, and not a risk row with a missing score.
func TestHomeReadersGiveRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	// The window is the days computed again; the compare window of the driver
	// read is the day that was not.
	start, end := store.Days[1], store.Days[len(store.Days)-1].AddDate(0, 0, 1)
	compareStart, compareEnd := store.Days[0], store.Days[1]

	read := func(org string) homeAnswers {
		t.Helper()
		answers := homeAnswers{
			Values: map[string]metricValue{}, Series: map[string][]dayValueRow{}, Drivers: map[string][]string{},
		}
		for _, spec := range metrics {
			if spec.Scope != "team" {
				continue
			}
			value, err := fetchMetricValue(ctx, client, spec.Table, spec.Column, start, end, "", nil, spec.Aggregator, org)
			if err != nil {
				t.Fatalf("%s %s value: %v", org, spec.Metric, err)
			}
			answers.Values[spec.Metric] = value
			series, err := fetchMetricSeries(ctx, client, spec.Table, spec.Column, start, end, "", nil, spec.Aggregator, org)
			if err != nil {
				t.Fatalf("%s %s series: %v", org, spec.Metric, err)
			}
			answers.Series[spec.Metric] = series
			drivers, err := fetchMetricDriverDelta(ctx, client, spec.Table, spec.Column, metricGroup(spec.Metric),
				start, end, compareStart, compareEnd, "", nil, org, 20)
			if err != nil {
				t.Fatalf("%s %s drivers: %v", org, spec.Metric, err)
			}
			ids := make([]string, 0, len(drivers))
			for _, driver := range drivers {
				ids = append(ids, driver.ID)
			}
			sort.Strings(ids)
			answers.Drivers[spec.Metric] = ids
		}
		total, days, hasData, err := fetchBlockedHours(ctx, client, start, end, "", nil, org)
		if err != nil {
			t.Fatalf("%s blocked hours: %v", org, err)
		}
		answers.Blocked, answers.BlockedDays, answers.BlockedData = total, len(days), hasData
		risk, err := fetchRiskSignals(ctx, client, DefaultFilters(), start, end, org)
		if err != nil {
			t.Fatalf("%s risk signals: %v", org, err)
		}
		answers.Risk = risk
		return answers
	}

	control := read(retractionseed.ControlOrg)

	// The control answers, from the seed: four teams, six days.
	// cycle_time: the mean of 8, 16, 24 and 32 hours. wip_saturation: the mean
	// of 0.5, 0.75, 1.0 and 1.25. throughput: 10 items on an even day and 14
	// on an odd day (three odd days: 1, 3, 5). blocked hours: 6 + 8 + 10 + 12
	// a day. The blocked_work value read has no status filter here, so it
	// also holds the 12 hours each team spent in progress: 36 + 48 a day.
	wantValues := map[string]metricValue{
		"cycle_time":     {Value: 20, HasData: true},
		"throughput":     {Value: 3*14 + 3*10, HasData: true},
		"wip_saturation": {Value: 0.875, HasData: true},
		"blocked_work":   {Value: 6 * (36 + 48), HasData: true},
	}
	if !reflect.DeepEqual(control.Values, wantValues) {
		t.Fatalf("control values = %+v, want %+v", control.Values, wantValues)
	}
	keyed := []string{"github:platform", "gitlab:ops", "jira:ENG", "linear:core"}
	for metric, drivers := range control.Drivers {
		if !reflect.DeepEqual(drivers, keyed) {
			t.Fatalf("control drivers of %s = %v, want %v", metric, drivers, keyed)
		}
	}
	if len(control.Drivers) != 4 {
		t.Fatalf("control drivers cover %d metrics, want 4", len(control.Drivers))
	}
	if control.Blocked != 6*36 || control.BlockedDays != 6 || !control.BlockedData {
		t.Fatalf("control blocked hours = %v over %d days (data %v), want 216 over 6 days",
			control.Blocked, control.BlockedDays, control.BlockedData)
	}
	if len(control.Risk) != 4 {
		t.Fatalf("control risk rows = %+v, want the four keyed teams", control.Risk)
	}
	for _, row := range control.Risk {
		if row.Score == nil {
			t.Fatalf("control risk row %s has no score", row.ScopeID)
		}
	}

	retracted := read(retractionseed.RetractedOrg)
	if !reflect.DeepEqual(control, retracted) {
		t.Fatalf("the retraction rows changed an answer:\n control   %+v\n retracted %+v", control, retracted)
	}
}

// TestHomeReadersReportNoDataForRetractionRowsOnly reads a window whose only
// rows are retraction rows. It holds no measurement: the value readers must
// say so, and the blocked-hours reader must list no day.
func TestHomeReadersReportNoDataForRetractionRowsOnly(t *testing.T) {
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
	start, end := day, day.AddDate(0, 0, 1)
	for _, spec := range metrics {
		if spec.Scope != "team" {
			continue
		}
		value, err := fetchMetricValue(ctx, client, spec.Table, spec.Column, start, end, "", nil, spec.Aggregator, org)
		if err != nil {
			t.Fatalf("%s value: %v", spec.Metric, err)
		}
		if value.HasData {
			t.Errorf("%s reports data for a window of retraction rows only: %+v", spec.Metric, value)
		}
		series, err := fetchMetricSeries(ctx, client, spec.Table, spec.Column, start, end, "", nil, spec.Aggregator, org)
		if err != nil {
			t.Fatalf("%s series: %v", spec.Metric, err)
		}
		if len(series) != 0 {
			t.Errorf("%s series lists a day of retraction rows only: %+v", spec.Metric, series)
		}
	}
	total, days, hasData, err := fetchBlockedHours(ctx, client, start, end, "", nil, org)
	if err != nil {
		t.Fatalf("blocked hours: %v", err)
	}
	if total != 0 || len(days) != 0 || hasData {
		t.Errorf("blocked hours = %v over %d days (data %v), want no day and no data", total, len(days), hasData)
	}
	risk, err := fetchRiskSignals(ctx, client, DefaultFilters(), start, end, org)
	if err != nil {
		t.Fatalf("risk signals: %v", err)
	}
	if len(risk) != 0 {
		t.Errorf("risk signals list a retraction row: %+v", risk)
	}
}
