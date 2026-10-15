//go:build integration

package scancensus

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

var destTypes = map[string]reflect.Type{
	"float64": reflect.TypeOf(float64(0)), "float32": reflect.TypeOf(float32(0)),
	"int64": reflect.TypeOf(int64(0)), "int32": reflect.TypeOf(int32(0)),
	"int16": reflect.TypeOf(int16(0)), "int8": reflect.TypeOf(int8(0)), "int": reflect.TypeOf(int(0)),
	"uint64": reflect.TypeOf(uint64(0)), "uint32": reflect.TypeOf(uint32(0)),
	"uint16": reflect.TypeOf(uint16(0)), "uint8": reflect.TypeOf(uint8(0)), "uint": reflect.TypeOf(uint(0)),
	"string": reflect.TypeOf(""), "bool": reflect.TypeOf(false),
	"time.Time": reflect.TypeOf(time.Time{}), "[]string": reflect.TypeOf([]string(nil)),
}

func destType(name string) (reflect.Type, bool) {
	stars := 0
	for strings.HasPrefix(name, "*") {
		stars++
		name = name[1:]
	}
	slice := strings.HasPrefix(name, "[]")
	t, ok := destTypes[strings.TrimPrefix(name, "[]")]
	if !ok || stars == 0 {
		return nil, false
	}
	if slice {
		t = reflect.SliceOf(t)
	}
	for i := 0; i < stars; i++ {
		t = reflect.PointerTo(t)
	}
	return t, true
}

func probeExpr(chType string) string {
	if inner, ok := strings.CutPrefix(chType, "Nullable("); ok {
		inner = strings.TrimSuffix(inner, ")")
		return fmt.Sprintf("CAST(defaultValueOfTypeName('%s') AS %s)", strings.ReplaceAll(inner, "'", `\'`), chType)
	}
	return fmt.Sprintf("defaultValueOfTypeName('%s')", strings.ReplaceAll(chType, "'", `\'`))
}

type pairKey struct{ ch, dest string }

// scannable asks the real driver whether a value of chType scans into dest.
func scannable(ctx context.Context, conn driver.Conn, chType, dest string) error {
	rt, ok := destType(dest)
	if !ok {
		return fmt.Errorf("destination type %s is not modelled by the census", dest)
	}
	rows, err := conn.Query(ctx, "SELECT "+probeExpr(chType))
	if err != nil {
		return fmt.Errorf("probe of %s: %w", chType, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return fmt.Errorf("probe of %s returned no row", chType)
	}
	return rows.Scan(reflect.New(rt.Elem()).Interface())
}

func describe(ctx context.Context, conn driver.Conn, statement string) ([]string, error) {
	var last error
	for _, variant := range Neutralize(statement) {
		cols, err := describeOne(ctx, conn, variant)
		if err == nil {
			return cols, nil
		}
		last = err
	}
	return nil, last
}

func describeOne(ctx context.Context, conn driver.Conn, statement string) ([]string, error) {
	rows, err := conn.Query(ctx, "DESCRIBE ("+statement+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var types []string
	for rows.Next() {
		var name, typ, a, b, c, d, e string
		if err := rows.Scan(&name, &typ, &a, &b, &c, &d, &e); err != nil {
			return nil, err
		}
		types = append(types, typ)
	}
	return types, rows.Err()
}

// Verdict of one site against the server.
type Verdict struct {
	Site       Site
	Unresolved string
	Mismatches []string
}

// Classify pairs every site's destinations with the column types the server
// infers for its statement.
func Classify(ctx context.Context, conn driver.Conn, sites []Site) []Verdict {
	probed := map[pairKey]error{}
	var out []Verdict
	for _, s := range sites {
		v := Verdict{Site: s}
		if len(s.Dests) == 1 && strings.HasPrefix(s.Dests[0], "[]") {
			v.Unresolved = "variadic destinations"
			out = append(out, v)
			continue
		}
		var matched bool
		var describeErr error
		var failing []string
		for _, st := range s.Statements {
			if SelectListSplices(st) {
				describeErr = fmt.Errorf("the select list holds a spliced fragment")
				continue
			}
			cols, err := describe(ctx, conn, st)
			if err != nil {
				describeErr = err
				continue
			}
			if len(cols) != len(s.Dests) {
				continue
			}
			matched = true
			for i, ct := range cols {
				k := pairKey{ct, s.Dests[i]}
				err, seen := probed[k]
				if !seen {
					err = scannable(ctx, conn, ct, s.Dests[i])
					probed[k] = err
				}
				if err != nil {
					failing = append(failing, fmt.Sprintf("column %d: %s into %s: %s", i+1, ct, s.Dests[i], firstLine(err.Error())))
				}
			}
		}
		v.Mismatches = failing
		if !matched {
			v.Unresolved = "no statement of the function resolves to the destination count"
			if describeErr != nil {
				v.Unresolved += ": " + firstLine(describeErr.Error())
			}
		}
		out = append(out, v)
	}
	return out
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// siteKey names a site without its line, so an unrelated edit above it does not
// disturb the pin.
func siteKey(s Site) string {
	file := s.File
	if i := strings.Index(file, "internal/queryapi/"); i >= 0 {
		file = file[i:]
	}
	return file + "|" + s.Func + "|" + strings.Join(s.Dests, ",")
}

func openCensusConn(t *testing.T) (context.Context, driver.Conn) {
	t.Helper()
	ctx := context.Background()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = inst.Close(context.Background()) })
	chschema.Apply(ctx, t, inst)
	opts, err := clickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return ctx, conn
}

// TestScanTypeCensus pairs every Scan of a query-API read with the column types
// the real migrated ClickHouse infers for its statement and has the real driver
// scan a value of each type into the destination. RED on the class CHAOS-5866
// named: sum()/count() are UInt64, which the driver refuses to scan into
// *float64 or *int64.
//
// A site the census cannot resolve to a statement (a statement built from a
// struct field, a template whose select list is spliced) is not checked, and the
// census says so: such a site must be in unverifiedSites, and a site that is
// neither verified nor listed fails, so a new reader cannot skip the census
// without a reviewed line in that list. A listed site the census can now verify
// fails too, so the list only shrinks.
func TestScanTypeCensus(t *testing.T) {
	ctx, conn := openCensusConn(t)
	sites, err := LoadSites("../../..", "./internal/queryapi/...")
	if err != nil {
		t.Fatal(err)
	}
	var unresolved, bad []string
	for _, v := range Classify(ctx, conn, sites) {
		if v.Unresolved != "" {
			unresolved = append(unresolved, siteKey(v.Site))
		}
		for _, m := range v.Mismatches {
			bad = append(bad, fmt.Sprintf("%s:%d %s -- %s", v.Site.File, v.Site.Line, v.Site.Func, m))
		}
	}
	for _, b := range bad {
		t.Errorf("MISMATCH %s", b)
	}
	sort.Strings(unresolved)
	listed := strings.Split(strings.TrimSpace(unverifiedSites), "\n")
	sort.Strings(listed)
	if got, want := strings.Join(unresolved, "\n"), strings.Join(listed, "\n"); got != want {
		t.Errorf("sites the census cannot verify changed (verified %d of %d). Put new sites in unverifiedSites only after reading their statement against the column types; remove sites that are now verified.\n--- got ---\n%s\n--- want ---\n%s",
			len(sites)-len(unresolved), len(sites), got, want)
	}
}

// TestScanTypeCensusRejectsAUInt64IntoFloat64 plants the CHAOS-5866 defect: a
// bare sum() over an unsigned column scanned into *float64, and its two fixes.
func TestScanTypeCensusRejectsAUInt64IntoFloat64(t *testing.T) {
	ctx, conn := openCensusConn(t)
	statement := func(expr string) []string {
		return []string{"SELECT status, " + expr + " AS n FROM work_item_state_durations_daily GROUP BY status"}
	}
	cases := []struct {
		name      string
		expr      string
		dest      string
		wantClean bool
	}{
		{"planted: sum() into *float64", "sum(items_touched)", "*float64", false},
		{"planted: count() into *int64", "count()", "*int64", false},
		{"fix: CAST in SQL", "CAST(sum(items_touched) AS Float64)", "*float64", true},
		{"fix: scan at the real type", "sum(items_touched)", "*uint64", true},
	}
	for _, c := range cases {
		verdict := Classify(ctx, conn, []Site{{File: "planted.go", Func: "planted", Dests: []string{"*string", c.dest}, Statements: statement(c.expr)}})[0]
		if verdict.Unresolved != "" {
			t.Fatalf("%s: unresolved: %s", c.name, verdict.Unresolved)
		}
		if got := len(verdict.Mismatches) == 0; got != c.wantClean {
			t.Errorf("%s: clean=%v, want %v (%v)", c.name, got, c.wantClean, verdict.Mismatches)
		}
	}
}

const unverifiedSites = `
internal/queryapi/aianalytics/ai_opportunities.go|loadClusters|*string,*string,*uint64,*[]string
internal/queryapi/aianalytics/ai_opportunities.go|loadClusters|*string,*uint64,*[]string
internal/queryapi/aianalytics/ai_opportunities.go|loadDocDrift|*string,*uint64,*uint64
internal/queryapi/aianalytics/ai_opportunities.go|loadFlaky|*string,*uint64,*float64
internal/queryapi/aianalytics/flow_opportunities.go|loadRepoFlowRows|*string,*uint64,**float64,**float64,**float64,**float64
internal/queryapi/aianalytics/flow_opportunities.go|loadTeamFlowRows|*string,*uint64,**float64,**float64,**float64
internal/queryapi/aianalytics/loaders.go|loadLabels|*string,*string
internal/queryapi/analytics/breakdown.go|executeBreakdownRaw|*string,**float64
internal/queryapi/analytics/catalog.go|executeCatalogValues|*string,*uint64
internal/queryapi/analytics/flowmatrix.go|queryEdges|*string,*string,*string,*string,**float64
internal/queryapi/analytics/flowmatrix.go|queryNodes|*string,*string,**float64
internal/queryapi/analytics/investmentquality.go|executeEvidenceQualityByGroup|*string,*float64,*uint64,*uint64
internal/queryapi/analytics/investmentquality.go|executeInvestmentQualityStats|*uint64,*uint64,*float64,*float64,*uint64,*uint64,*uint64,*uint64,*uint64
internal/queryapi/analytics/sankeycoverage.go|resolveSankeyCoverage|*float64,*float64,*float64,*float64,**float64,**float64,**float64
internal/queryapi/analytics/sankeygrouped.go|executeSankeyGrouped|[]any
internal/queryapi/analytics/timeseries.go|ExecuteOrgTimeseries|*time.Time,**float64,**float64
internal/queryapi/analytics/timeseries.go|ExecuteTimeseries|*time.Time,*string,**float64
internal/queryapi/complexitytimeseries/complexitytimeseries.go|fetchFileTimeseries|*time.Time,*string,*string,**uint32,**float64,**uint32,**uint32
internal/queryapi/complexitytimeseries/complexitytimeseries.go|fetchRepoTimeseries|*time.Time,*string,**uint64,**uint64,**float64,**uint64,**uint64
internal/queryapi/compoundingrisk/read.go|latestDay|**time.Time
internal/queryapi/compoundingrisk/read.go|latestRows|*string,**float64,*string,**float64,**float64,**float64,**float64,**float64,**float64,**float64,**float64,**float64,**float64,*float64,*float64,*float64,*float64,*float64,*float64,*time.Time
internal/queryapi/compoundingrisk/read.go|repoTrend|*time.Time,**float64
internal/queryapi/datahealth/coverage.go|MetricLineage|**time.Time,*uint64
internal/queryapi/drilldown/issues.go|BuildIssuesResponse|*string,*string,*string,**string,**string,**float64,**float64,**time.Time,**time.Time
internal/queryapi/drilldown/issues.go|buildBlockedIssuesResponse|*string,*string,**string,**string,*uint64
internal/queryapi/explain/metrics.go|fetchChangeFailureView|[]any
internal/queryapi/explain/metrics.go|fetchMetricContributors|*string,**float64
internal/queryapi/explain/metrics.go|fetchMetricDriverDelta|*string,**float64,*uint8,*uint8,**float64
internal/queryapi/explain/metrics.go|fetchMetricValue|**float64,*uint64
internal/queryapi/explain/repositories.go|fetchWorkItemProviders|*string
internal/queryapi/home/queries_metrics.go|fetchChangeFailureView|[]any
internal/queryapi/home/queries_metrics.go|fetchMetricDriverDelta|*string,**float64,**float64
internal/queryapi/home/queries_metrics.go|fetchMetricSeries|*time.Time,**float64
internal/queryapi/home/queries_metrics.go|fetchMetricValue|*int64,**float64
internal/queryapi/home/queries_metrics.go|fetchPRReworkView|[]any
internal/queryapi/home/queries_metrics.go|fetchRateDriverDelta|*string,**float64,**float64
internal/queryapi/home/queries_signals.go|fetchRiskSignals|*string,*string,**float64,*string,*time.Time,**float64
internal/queryapi/hotspots/hotspots.go|queryHotspotRows|*string,*string,*uint64,*uint32,*uint32,*float64,**float64,*float64
internal/queryapi/investment/sunburst.go|FetchInvestmentSunburst|*string,*string,*string,*float64
internal/queryapi/investmentexplain/reader.go|FetchInvestmentBreakdown|*string,*string,*float64
internal/queryapi/investmentexplain/reader.go|FetchInvestmentThemeEffort|*string,*float64,*float64,*float64
internal/queryapi/investmentexplain/workunitreader.go|FetchWorkUnitInvestments|*string,**string,**string,*time.Time,*time.Time,**string,**string,**string,**float64,*float64,*[]string,*[]float64,*[]string,*[]float64,**string,**float64,**string,**string,**string,**string,*time.Time
internal/queryapi/operatingreview/operatingreview.go|fetchChangeFailureAgg|[]any
internal/queryapi/people/metricqueries.go|fetchPersonBreakdown|*string,*float64
internal/queryapi/people/metricqueries.go|fetchPersonMetricSeries|*time.Time,*float64
internal/queryapi/people/metricqueries.go|fetchPersonMetricValue|*uint64,*float64
internal/queryapi/people/search.go|BuildSearchResponse|*string,*time.Time
internal/queryapi/people/summary.go|fetchPersonCollaboration|*string,*string,*float64
internal/queryapi/quadrant/quadrant.go|scanMetricRows|*time.Time,*string,*string,*float64
internal/queryapi/server/mcp_route.go|Scan|[]any
internal/queryapi/workgraph/edges.go|countMatchingEdges|*uint64
internal/queryapi/workgraph/edges.go|fetchDedupedEdgeRows|*string,*string,*string,*string,*string,*string,*string,*string,*string,*float64,*string
internal/queryapi/workgraph/edges.go|queryDependencyEdges|*string,*string,*string,*string,*string,*string,*string,*string,*string,*float64,*string
internal/queryapi/workgraph/membership.go|batchResolveMembership|*string,*string,*string,*string
internal/queryapi/workgraph/scope.go|detectMembershipDegradedReason|*uint64,*uint64
internal/queryapi/workgraph/teamattribution.go|resolveWorkUnitTeamAttributions|*string,*string,*string,*string,*string,*uint64
`
