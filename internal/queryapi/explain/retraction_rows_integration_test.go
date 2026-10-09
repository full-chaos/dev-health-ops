//go:build integration

package explain

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// explainAnswers is what the explain readers give for one team-scoped metric.
type explainAnswers struct {
	Value        float64
	HasData      bool
	Contributors []metricRow
	Drivers      []metricRow
}

func sortedByID(rows []metricRow) []metricRow {
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows
}

// TestExplainReadersGiveRetractionRowsNoWeight reads every team-scoped explain
// metric of two organizations that hold the same measurements. One of them
// also holds the old rows of the retired team ids and the retraction row over
// each (package retractionseed). The value, the contributors and the drivers
// must be the same: a retraction row is not a sample of a mean and its team
// id is not a contributor or a driver.
func TestExplainReadersGiveRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	start, end := store.Days[1], store.Days[len(store.Days)-1].AddDate(0, 0, 1)
	compareStart, compareEnd := store.Days[0], store.Days[1]

	read := func(org string) map[string]explainAnswers {
		t.Helper()
		answers := map[string]explainAnswers{}
		for metric, config := range metricConfigs {
			if config.Scope != "team" {
				continue
			}
			filter := ""
			if config.StatusFilter != "" {
				filter = "AND status = '" + config.StatusFilter + "'"
			}
			var answer explainAnswers
			var err error
			answer.Value, answer.HasData, err = reader.fetchMetricValue(ctx, config.Table, config.Column, config.Aggregator, start, end, filter, nil, org)
			if err != nil {
				t.Fatalf("%s %s value: %v", org, metric, err)
			}
			contributors, err := reader.fetchMetricContributors(ctx, config.Table, config.Column, config.GroupBy, config.Aggregator, start, end, filter, nil, org)
			if err != nil {
				t.Fatalf("%s %s contributors: %v", org, metric, err)
			}
			answer.Contributors = sortedByID(contributors)
			drivers, err := reader.fetchMetricDriverDelta(ctx, config.Table, config.Column, config.GroupBy, config.Aggregator, start, end, compareStart, compareEnd, filter, nil, org)
			if err != nil {
				t.Fatalf("%s %s drivers: %v", org, metric, err)
			}
			answer.Drivers = sortedByID(drivers)
			answers[metric] = answer
		}
		return answers
	}

	control := read(retractionseed.ControlOrg)

	// The control values, from the seed: four teams, six days. cycle_time:
	// the mean of 8, 16, 24 and 32 hours. wip_saturation: the mean of 0.5,
	// 0.75, 1.0 and 1.25. throughput: 14 items on each of three days and 10 on
	// each of the other three. blocked_work: 6 + 8 + 10 + 12 hours a day.
	wantValues := map[string]float64{"cycle_time": 20, "wip_saturation": 0.875, "throughput": 72, "blocked_work": 216}
	if len(control) != len(wantValues) {
		t.Fatalf("control covers %d team metrics, want %d: %+v", len(control), len(wantValues), control)
	}
	for metric, want := range wantValues {
		answer, ok := control[metric]
		if !ok || answer.Value != want || !answer.HasData {
			t.Fatalf("control %s = %+v, want value %v with data", metric, answer, want)
		}
		// The four keyed ids are the contributors (the read keeps at most 6).
		var ids []string
		for _, row := range answer.Contributors {
			ids = append(ids, row.ID)
		}
		if want := []string{"github:platform", "gitlab:ops", "jira:ENG", "linear:core"}; !reflect.DeepEqual(ids, want) {
			t.Fatalf("control %s contributors = %v, want %v", metric, ids, want)
		}
	}

	retracted := read(retractionseed.RetractedOrg)
	for metric := range wantValues {
		got, want := retracted[metric], control[metric]
		if got.Value != want.Value || got.HasData != want.HasData || !reflect.DeepEqual(got.Contributors, want.Contributors) {
			t.Errorf("the retraction rows changed %s:\n control   %+v\n retracted %+v", metric, want, got)
		}
		// The driver read keeps 3 of the 4 teams and the compare window holds
		// no row of a keyed id, so every keyed id has the same (missing)
		// change and which 3 are kept is not decided. What is decided: each
		// driver is a measured team with the value its contributor row has.
		byID := map[string]float64{}
		for _, row := range want.Contributors {
			byID[row.ID] = row.Value
		}
		for _, answers := range []explainAnswers{want, got} {
			if len(answers.Drivers) != 3 {
				t.Errorf("%s drivers = %+v, want 3 rows", metric, answers.Drivers)
			}
			for _, driver := range answers.Drivers {
				if value, ok := byID[driver.ID]; !ok || value != driver.Value {
					t.Errorf("%s driver %+v is not a measured team with its measured value %v", metric, driver, byID)
				}
			}
		}
	}
}
