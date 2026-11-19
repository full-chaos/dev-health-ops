package benchmarking

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// lookupTables are the tables a series read joins or filters by that hold no
// metric value: they cannot add a scope or a day to a series, only label or
// drop one. A key not read by the compute fails the census.
var lookupTables = map[string]string{
	"teams":               "active and inactive team ids (teamactive, teamscope): drops a team series, never adds one",
	"team_repo_ownership": "owners of a repository at the run (teamscope): labels a repository series with its teams",
	"repos":               "catalog guard of the ownership join (teamscope): drops an owner row, never adds a series",
}

// statementConn records every statement and answers it with no row.
type statementConn struct {
	statements []string
	args       [][]any
	err        error
}

func (conn *statementConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	conn.statements = append(conn.statements, query)
	conn.args = append(conn.args, args)
	if conn.err != nil {
		return nil, conn.err
	}
	return noRows{}, nil
}

func (conn *statementConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, errors.New("unused")
}

var readTable = regexp.MustCompile(`(?i)\b(?:FROM|JOIN)\s+([a-z_][a-z0-9_]*)`)

func tablesOf(statements []string) map[string]bool {
	tables := map[string]bool{}
	for _, statement := range statements {
		for _, match := range readTable.FindAllStringSubmatch(statement, -1) {
			tables[match[1]] = true
		}
	}
	return tables
}

// fetchRecord is one fetch of the compute.
type fetchRecord struct {
	metric     string
	start, end time.Time
}

// richFetcher records every fetch and gives a metric whose table is in tables
// (every metric when tables is nil) two scopes with a point on each day of the
// window: a rising series with a spike on the last day, so each of the six
// outputs has rows when the series reaches it.
type richFetcher struct {
	tables  map[string]bool
	fetches []fetchRecord
}

func (fetcher *richFetcher) FetchMetricSeriesByScope(
	_ context.Context, metricName string, startDay, endDay time.Time, _ string,
) (map[string][]MetricPoint, error) {
	fetcher.fetches = append(fetcher.fetches, fetchRecord{metricName, startDay, endDay})
	if fetcher.tables != nil && !fetcher.tables[metricDefinitions[CanonicalMetricName(metricName)].Table] {
		return map[string][]MetricPoint{}, nil
	}
	series := map[string][]MetricPoint{}
	for index, scope := range []string{"scope-a", "scope-b"} {
		var points []MetricPoint
		for day, step := startDay, 0; !day.After(endDay); day, step = day.AddDate(0, 0, 1), step+1 {
			value := 10 + float64(step%7) + float64(step)*0.5 + float64(index)
			if day.Equal(endDay) {
				value = 1000
			}
			points = append(points, MetricPoint{Day: day, Value: value})
		}
		series[scope] = points
	}
	return series, nil
}

func outputCounts(outputs Outputs) map[string]int {
	return map[string]int{
		"testops_metric_baselines":    len(outputs.Baselines),
		"testops_maturity_bands":      len(outputs.MaturityBands),
		"testops_metric_anomalies":    len(outputs.Anomalies),
		"testops_period_comparisons":  len(outputs.PeriodComparisons),
		"testops_metric_correlations": len(outputs.Correlations),
		"testops_benchmark_insights":  len(outputs.Insights),
	}
}

func computeWith(t *testing.T, fetcher SeriesFetcher) Outputs {
	t.Helper()
	outputs, err := ComputeBenchmarkingForDay(context.Background(), fetcher, goldenAsOf(), goldenStamp(), goldenOrgID,
		DefaultBenchmarkMetrics, DefaultCorrelationPairs, nil)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	return outputs
}

// The probe counts exactly the tables of InputTables, each by the loader's own
// key and live-row rule. Every other table a read of the compute takes either
// cannot make a row alone (executed: a series only from it gives no row of the
// six outputs) or is a lookup table.
func TestEveryTableTheComputeReadsIsCountedByTheProbeOrCannotMakeARow(t *testing.T) {
	asOf := goldenAsOf()
	probeTables, err := InputTables(DefaultBenchmarkMetrics, DefaultCorrelationPairs)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("probe tables: %v", probeTables)

	probeConn := &statementConn{}
	probe, err := NewClickHouseLoader(probeConn, goldenOrgID)
	if err != nil {
		t.Fatal(err)
	}
	live, err := probe.LiveInputKeys(context.Background(), asOf, DefaultBenchmarkMetrics, DefaultCorrelationPairs)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if len(probeConn.statements) != 1 {
		t.Fatalf("probe sent %d statements, want one read", len(probeConn.statements))
	}
	probed := tablesOf(probeConn.statements)
	var probedList []string
	for table := range probed {
		probedList = append(probedList, table)
	}
	sort.Strings(probedList)
	if !reflect.DeepEqual(probedList, probeTables) {
		t.Errorf("the probe reads %v, want InputTables %v", probedList, probeTables)
	}
	for _, table := range probeTables {
		if _, ok := live[table]; !ok {
			t.Errorf("the probe result has no count of %s", table)
		}
		if liverow.Registered(table) && !strings.Contains(probeConn.statements[0], liverow.NewestPredicate(table, "")) {
			t.Errorf("the probe of %s does not apply the live-row rule of the loader", table)
		}
	}
	for name, definition := range metricDefinitions {
		if !probed[definition.Table] {
			continue
		}
		if !reflect.DeepEqual(definition.InnerGroupColumns, []string{"repo_id"}) || len(definition.ExtraFilters) != 0 {
			t.Errorf("%s on %s: key %v filters %v, want (org_id, day, repo_id) and no filter, the key of the probe",
				name, definition.Table, definition.InnerGroupColumns, definition.ExtraFilters)
		}
	}

	// The tables of the SQL of every read the compute sends.
	readConn := &statementConn{}
	loader, err := NewClickHouseLoader(readConn, goldenOrgID)
	if err != nil {
		t.Fatal(err)
	}
	loader.SetOwnersAsOf(goldenStamp())
	computeWith(t, loader)
	read := tablesOf(readConn.statements)
	t.Logf("tables the compute reads: %v", read)

	metricTables := map[string]bool{}
	for _, definition := range metricDefinitions {
		metricTables[definition.Table] = true
	}
	usedLookup := map[string]bool{}
	for table := range read {
		switch {
		case probed[table]:
		case metricTables[table]:
			outputs := computeWith(t, &richFetcher{tables: map[string]bool{table: true}})
			for output, count := range outputCounts(outputs) {
				if count != 0 {
					t.Errorf("a series only from %s, which the probe does not count, gives %d row(s) of %s", table, count, output)
				}
			}
			t.Logf("%s alone: %v", table, outputCounts(outputs))
		case lookupTables[table] != "":
			usedLookup[table] = true
		default:
			t.Errorf("the compute reads %s: neither counted by the probe nor a metric or lookup table", table)
		}
	}
	for table := range lookupTables {
		if !usedLookup[table] {
			t.Errorf("lookupTables key %s is stale: the compute does not read it", table)
		}
	}

	// The cell is not vacuous: the same series from every table gives rows of each output.
	for output, count := range outputCounts(computeWith(t, &richFetcher{})) {
		if count == 0 {
			t.Errorf("control: a series from every table gives no row of %s", output)
		}
	}
}

// The probe window is the union of the windows of the compute's fetches.
func TestInputWindowStartIsTheFirstDayTheComputeReads(t *testing.T) {
	asOf := goldenAsOf()
	fetcher := &richFetcher{}
	computeWith(t, fetcher)
	first, last := fetcher.fetches[0].start, fetcher.fetches[0].end
	for _, fetch := range fetcher.fetches {
		if fetch.start.Before(first) {
			first = fetch.start
		}
		if fetch.end.After(last) {
			last = fetch.end
		}
	}
	t.Logf("fetches=%d first=%s last=%s InputWindowStart=%s", len(fetcher.fetches),
		first.Format("2006-01-02"), last.Format("2006-01-02"), InputWindowStart(asOf).Format("2006-01-02"))
	if !first.Equal(InputWindowStart(asOf)) {
		t.Errorf("first day read = %s, InputWindowStart = %s", first.Format("2006-01-02"), InputWindowStart(asOf).Format("2006-01-02"))
	}
	if !last.Equal(asOf) {
		t.Errorf("last day read = %s, want the day of the run %s", last.Format("2006-01-02"), asOf.Format("2006-01-02"))
	}
	if got := asOf.Sub(InputWindowStart(asOf)) / (24 * time.Hour); got != 89 {
		t.Errorf("window = [D-%d, D], want [D-89, D]", got)
	}

	conn := &statementConn{}
	loader, err := NewClickHouseLoader(conn, goldenOrgID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loader.LiveInputKeys(context.Background(), asOf, DefaultBenchmarkMetrics, DefaultCorrelationPairs); err != nil {
		t.Fatal(err)
	}
	bound := map[string]any{}
	for _, arg := range conn.args[0] {
		named, ok := arg.(driver.NamedValue)
		if !ok {
			t.Fatalf("probe argument %T is not named", arg)
		}
		bound[named.Name] = named.Value
	}
	want := map[string]any{
		"start_day": InputWindowStart(asOf).Format(clickHouseDateLayout),
		"end_day":   asOf.Format(clickHouseDateLayout),
		"org_id":    goldenOrgID,
	}
	if !reflect.DeepEqual(bound, want) {
		t.Errorf("probe arguments %v, want %v", bound, want)
	}
}

// A failed probe read is an error, never a count of 0.
func TestLiveInputKeysFailsOnAFailedRead(t *testing.T) {
	loader, err := NewClickHouseLoader(&statementConn{err: errors.New("clickhouse down")}, goldenOrgID)
	if err != nil {
		t.Fatal(err)
	}
	live, err := loader.LiveInputKeys(context.Background(), goldenAsOf(), DefaultBenchmarkMetrics, DefaultCorrelationPairs)
	if err == nil {
		t.Fatalf("probe over a failed read = %v, nil; want an error", live)
	}
}
