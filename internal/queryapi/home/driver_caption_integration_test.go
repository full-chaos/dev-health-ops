//go:build integration

package home

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"
)

// CHAOS-8573: the driver caption of a Home metric signal, on a real ClickHouse
// through the production-shaped query client.

type batchCountingClient struct {
	QueryClient
	batches int
}

func (c *batchCountingClient) Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if strings.Contains(statement, "LIMIT {limit_n:UInt32} BY metric") {
		c.batches++
	}
	return c.QueryClient.Query(ctx, statement, bindings)
}

func seedRepoDriver(ctx context.Context, t *testing.T, admin interface {
	Exec(context.Context, string, ...any) error
}, org, name string, repo uuid.UUID, prior, current time.Time, churnPrior, churnCurrent uint32, computedAt time.Time) {
	t.Helper()
	for day, churn := range map[time.Time]uint32{prior: churnPrior, current: churnCurrent} {
		if err := admin.Exec(ctx, `INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, pr_first_review_p50_hours, computed_at, org_id) VALUES (?, ?, ?, ?, ?, ?)`,
			repo, day, churn, float64(churn), computedAt, org); err != nil {
			t.Fatal(err)
		}
	}
	if name == "" {
		return
	}
	if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider) VALUES (?, ?, ?, ?, ?, 'github')`,
		repo, name, computedAt, computedAt, org); err != nil {
		t.Fatal(err)
	}
}

// The signal of a stated move names the three drivers with the largest percent
// move, by name, in the order of the move, once each, from ONE batched read;
// the caption is the one-hedge sentence of the label.
func TestHomeSignalDriverNamesTheTopThreeByNameFromOneRead(t *testing.T) {
	ctx := context.Background()
	admin, base := newHomeTestClickHouse(ctx, t)
	client := &batchCountingClient{QueryClient: base}
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	f := DefaultFilters()
	current, _, prior, _, err := TimeWindow(f, now)
	if err != nil {
		t.Fatal(err)
	}
	const org = "signal-driver-top3"
	computedAt := now.Add(-time.Hour)
	// percent moves: r1 +300, r2 +200, r0 +100, r3 +50; "nameless" +900 has no name.
	for i, step := range []uint32{200, 400, 300, 150} {
		seedRepoDriver(ctx, t, admin, org, fmt.Sprintf("acme/r%d", i), uuid.New(), prior, current, 100, step, computedAt)
	}
	nameless := uuid.New()
	seedRepoDriver(ctx, t, admin, org, "", nameless, prior, current, 100, 1000, computedAt)

	resp, err := BuildResponse(ctx, client, nil, org, f, now)
	if err != nil {
		t.Fatal(err)
	}
	if client.batches != 1 {
		t.Errorf("the driver read ran %d times for one request, want 1 batched statement", client.batches)
	}
	var churn *Signal
	for i := range resp.Signals {
		if resp.Signals[i].ID == "metric:churn" {
			churn = &resp.Signals[i]
		}
	}
	if churn == nil || churn.Driver == nil {
		t.Fatalf("no driver on the churn signal: %+v", resp.Signals)
	}
	t.Logf("driver: %s", *churn.Driver)
	// the nameless repository ranks first by its move and is dropped; the top three
	// of the NAMED ones are not backfilled past the limit of three rows.
	want := "Code Churn appears to have moved most in acme/r1 and acme/r2."
	if *churn.Driver != want {
		t.Errorf("driver = %q, want %q", *churn.Driver, want)
	}
	if loc := idShape.FindString(*churn.Driver); loc != "" || strings.Contains(*churn.Driver, nameless.String()) {
		t.Errorf("the caption prints an id: %q", *churn.Driver)
	}
	assertHedgedOncePerSentence(t, "caption", *churn.Driver)
	for _, s := range resp.Signals {
		if s.ID != "metric:churn" && s.ID != "metric:review_latency" && s.Driver != nil {
			t.Errorf("signal %s carries a driver %q", s.ID, *s.Driver)
		}
	}
}

// The batch returns, for each metric, the rows of the single-metric read that
// serves the summary sentence, for a repository metric and a team metric.
func TestDriverBatchMatchesTheSingleMetricRead(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	f := DefaultFilters()
	current, endDay, prior, priorEnd, err := TimeWindow(f, now)
	if err != nil {
		t.Fatal(err)
	}
	const org = "signal-driver-batch-equals-single"
	computedAt := now.Add(-time.Hour)
	for i, step := range []uint32{130, 400, 300, 150, 120} {
		seedRepoDriver(ctx, t, admin, org, "", uuid.New(), prior, current, 100, step+uint32(i), computedAt)
	}
	seedTeamDrivers(ctx, t, admin, org, []driverTeam{
		{"github:acme/ops", "Ops", true, false}, {"gitlab:grp/web", "Web", true, false}, {"linear:ENG", "Eng", true, false}}, prior, current)

	metricsUnderTest := []string{"churn", "review_latency", "cycle_time"}
	var batch []driverBatchMetric
	for _, m := range metricsUnderTest {
		batch = append(batch, driverBatchMetric{Metric: m})
	}
	got, err := fetchMetricDriversBatch(ctx, client, batch, current, endDay, prior, priorEnd, nil, org, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range metricsUnderTest {
		single, err := fetchMetricDriverDelta(ctx, client, metricTable(m), metricColumn(m), metricGroup(m), current, endDay, prior, priorEnd, "", nil, org, 3)
		if err != nil {
			t.Fatal(err)
		}
		var want, have []string
		for _, r := range single {
			want = append(want, r.ID)
		}
		for _, r := range got[m] {
			have = append(have, r.ID)
		}
		if len(want) == 0 {
			t.Errorf("%s: the single read returned nothing; the fixture proves nothing", m)
		}
		if m == "cycle_time" { // equal moves: the single read orders ties by nothing
			slices.Sort(want)
			slices.Sort(have)
		}
		if !slices.Equal(want, have) {
			t.Errorf("%s: batch %v, single read %v", m, have, want)
		}
	}
}

// The production-shaped client caps a result at 1,000 rows: the batch is cut at
// three rows per metric IN the SQL, so an organization with more repositories
// than the cap still gets its three drivers per metric.
func TestDriverBatchIsBoundedAboveTheClientRowCap(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	current, endDay, prior, priorEnd, err := TimeWindow(DefaultFilters(), now)
	if err != nil {
		t.Fatal(err)
	}
	const org = "signal-driver-batch-above-cap"
	computedAt := now.Add(-time.Hour)
	if err := admin.Exec(ctx, `INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, pr_first_review_p50_hours, computed_at, org_id)
SELECT generateUUIDv4(), arrayJoin([toDate(?), toDate(?)]), 100 + number % 50, 1.0, ?, ? FROM numbers(1100)`,
		prior, current, computedAt, org); err != nil {
		t.Fatal(err)
	}
	got, err := fetchMetricDriversBatch(ctx, client, []driverBatchMetric{{Metric: "churn"}, {Metric: "review_latency"}}, current, endDay, prior, priorEnd, nil, org, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{"churn", "review_latency"} {
		if len(got[m]) != 3 {
			t.Errorf("%s: %d driver rows, want 3 from 1,100 repositories", m, len(got[m]))
		}
	}
}

// A team metric (grouped by team) names teams, not repositories.
func TestHomeSignalDriverOfATeamMetricNamesTheTeams(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	f := DefaultFilters()
	current, _, prior, _, err := TimeWindow(f, now)
	if err != nil {
		t.Fatal(err)
	}
	const org = "signal-driver-team-metric"
	seedTeamDrivers(ctx, t, admin, org, []driverTeam{{"github:acme/ops", "Ops", true, false}}, prior, current)
	resp, err := BuildResponse(ctx, client, nil, org, f, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range resp.Signals {
		if s.ID == "metric:cycle_time" {
			if s.Driver == nil || *s.Driver != "Cycle Time appears to have moved most in Ops." {
				t.Errorf("cycle time driver = %v", s.Driver)
			}
			return
		}
	}
	t.Fatalf("no cycle time signal: %+v", resp.Signals)
}

// Every metric of the Home spec list, the rate metrics included, goes into ONE
// statement that ClickHouse accepts (the union of team and repository ids and of
// the percent expressions has one type).
func TestDriverBatchOfEveryMetricIsOneValidStatement(t *testing.T) {
	ctx := context.Background()
	_, client := newHomeTestClickHouse(ctx, t)
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	current, endDay, prior, priorEnd, err := TimeWindow(DefaultFilters(), now)
	if err != nil {
		t.Fatal(err)
	}
	var batch []driverBatchMetric
	for _, m := range metrics {
		batch = append(batch, driverBatchMetric{Metric: m.Metric})
	}
	if _, err := fetchMetricDriversBatch(ctx, client, batch, current, endDay, prior, priorEnd, nil, "signal-driver-every-metric", 3); err != nil {
		t.Fatal(err)
	}
}
