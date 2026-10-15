//go:build integration

package scancensus

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
// disturb the pin, and carries a digest of the file and of the
// statements the census evaluated: any change to a pinned site fails the pin
// until it is read again.
func siteKey(s Site) string {
	file := s.File
	if i := strings.Index(file, "internal/queryapi/"); i >= 0 {
		file = file[i:]
	}
	digest := sha256.New()
	digest.Write([]byte(s.Source))
	for _, st := range s.Statements {
		digest.Write([]byte{0})
		digest.Write([]byte(st))
	}
	return file + "|" + s.Func + "|" + strings.Join(s.Dests, ",") + "|" + hex.EncodeToString(digest.Sum(nil))[:12]
}

// reasonClass names why a site is unverified: statement-not-static (the
// statement is built from a struct field or a value the census cannot
// evaluate), spliced-select-list (a runtime fragment stands in a select item),
// not-describable (the neutralized statement is refused by the server, so its
// columns are unknown) or variadic-destinations (Scan receives a spread slice).
func reasonClass(unresolved string) string {
	switch {
	case unresolved == "variadic destinations":
		return "variadic-destinations"
	case strings.Contains(unresolved, "spliced fragment"):
		return "spliced-select-list"
	case strings.Contains(unresolved, "code: "):
		return "not-describable"
	}
	return "statement-not-static"
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
			unresolved = append(unresolved, siteKey(v.Site)+"|"+reasonClass(v.Unresolved))
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

// TestScanTypeCensusPinsAnOuterSplicedSelectItem: a spliced fragment in the
// outer select list is never neutralized to a literal that scans cleanly, even
// when a sibling subquery holds a WHERE.
func TestScanTypeCensusPinsAnOuterSplicedSelectItem(t *testing.T) {
	ctx, conn := openCensusConn(t)
	statement := "SELECT (SELECT count() FROM repos WHERE id != toUUID('00000000-0000-0000-0000-000000000000')) AS c, %s AS n FROM work_item_state_durations_daily"
	verdict := Classify(ctx, conn, []Site{{File: "planted.go", Func: "planted", Dests: []string{"*uint64", "*uint8"}, Statements: []string{statement}}})[0]
	if verdict.Unresolved == "" || len(verdict.Mismatches) != 0 {
		t.Errorf("verdict = %+v, want unresolved without a verdict on the spliced column", verdict)
	}
}

// unverifiedSites is `file|func|destinations|digest|reason`; see reasonClass.
const unverifiedSites = `
internal/queryapi/aianalytics/ai_opportunities.go|loadClusters|*string,*string,*uint64,*[]string|0fdd93221b9d|not-describable
internal/queryapi/aianalytics/ai_opportunities.go|loadClusters|*string,*uint64,*[]string|0fdd93221b9d|not-describable
internal/queryapi/aianalytics/ai_opportunities.go|loadDocDrift|*string,*uint64,*uint64|8cb0a8451c96|not-describable
internal/queryapi/aianalytics/ai_opportunities.go|loadFlaky|*string,*uint64,*float64|588427328b4d|not-describable
internal/queryapi/aianalytics/flow_opportunities.go|loadRepoFlowRows|*string,*uint64,**float64,**float64,**float64,**float64|cbb5d1782658|not-describable
internal/queryapi/aianalytics/flow_opportunities.go|loadTeamFlowRows|*string,*uint64,**float64,**float64,**float64|b21758014bba|not-describable
internal/queryapi/aianalytics/loaders.go|loadLabels|*string,*string|86d27559c59d|spliced-select-list
internal/queryapi/analytics/breakdown.go|executeBreakdownRaw|*string,**float64|f7152f944e40|statement-not-static
internal/queryapi/analytics/catalog.go|executeCatalogValues|*string,*uint64|853aa66f46e6|statement-not-static
internal/queryapi/analytics/flowmatrix.go|queryEdges|*string,*string,*string,*string,**float64|eadd64e491c6|statement-not-static
internal/queryapi/analytics/flowmatrix.go|queryNodes|*string,*string,**float64|eadd64e491c6|statement-not-static
internal/queryapi/analytics/investmentquality.go|executeEvidenceQualityByGroup|*string,*float64,*uint64,*uint64|97a4440b2841|statement-not-static
internal/queryapi/analytics/investmentquality.go|executeInvestmentQualityStats|*uint64,*uint64,*float64,*float64,*uint64,*uint64,*uint64,*uint64,*uint64|97a4440b2841|statement-not-static
internal/queryapi/analytics/sankeycoverage.go|resolveSankeyCoverage|*float64,*float64,*float64,*float64,**float64,**float64,**float64|ef51d5932b6e|statement-not-static
internal/queryapi/analytics/sankeygrouped.go|executeSankeyGrouped|[]any|283bd284bf94|variadic-destinations
internal/queryapi/analytics/timeseries.go|ExecuteOrgTimeseries|*time.Time,**float64,**float64|a906f9724aa7|statement-not-static
internal/queryapi/analytics/timeseries.go|ExecuteTimeseries|*time.Time,*string,**float64|a906f9724aa7|statement-not-static
internal/queryapi/complexitytimeseries/complexitytimeseries.go|fetchFileTimeseries|*time.Time,*string,*string,**uint32,**float64,**uint32,**uint32|8d8e2f9a243f|spliced-select-list
internal/queryapi/complexitytimeseries/complexitytimeseries.go|fetchRepoTimeseries|*time.Time,*string,**uint64,**uint64,**float64,**uint64,**uint64|39443e2677ab|spliced-select-list
internal/queryapi/compoundingrisk/read.go|latestDay|**time.Time|69506448564d|not-describable
internal/queryapi/compoundingrisk/read.go|latestRows|*string,**float64,*string,**float64,**float64,**float64,**float64,**float64,**float64,**float64,**float64,**float64,**float64,*float64,*float64,*float64,*float64,*float64,*float64,*time.Time|e08e3d52a41d|not-describable
internal/queryapi/compoundingrisk/read.go|repoTrend|*time.Time,**float64|d4a197de2963|not-describable
internal/queryapi/datahealth/connectors.go|Connectors|*string,*string,*[]byte,**time.Time,**bool,**string,*[]byte,**time.Time,**int32,**time.Time,**time.Time,*[]byte,**string|50c815545839|not-describable
internal/queryapi/datahealth/coverage.go|MetricLineage|**time.Time,*uint64|f02f73fb1704|not-describable
internal/queryapi/datahealth/sourcehealth.go|SourceHealth|*string,**string,*bool,**time.Time,*bool,*string,*[]byte,**time.Time,**bool,*bool,**time.Time,**string,*bool,**time.Time,**time.Time,**string,**string|42ccd2fab40e|not-describable
internal/queryapi/drilldown/issues.go|BuildIssuesResponse|*string,*string,*string,**string,**string,**float64,**float64,**time.Time,**time.Time|a289a4a8d007|not-describable
internal/queryapi/drilldown/issues.go|buildBlockedIssuesResponse|*string,*string,**string,**string,*uint64|bfa16f7675a6|not-describable
internal/queryapi/explain/metrics.go|fetchChangeFailureView|[]any|2e45ca61a0e9|variadic-destinations
internal/queryapi/explain/metrics.go|fetchMetricContributors|*string,**float64|747f3b61b8ae|spliced-select-list
internal/queryapi/explain/metrics.go|fetchMetricDriverDelta|*string,**float64,*uint8,*uint8,**float64|4ed780db0d75|not-describable
internal/queryapi/explain/metrics.go|fetchMetricValue|**float64,*uint64|3e8805152bf7|spliced-select-list
internal/queryapi/explain/repositories.go|fetchWorkItemProviders|*string|9d21afc9744a|not-describable
internal/queryapi/home/queries_metrics.go|fetchChangeFailureView|[]any|0c3b377d3faa|variadic-destinations
internal/queryapi/home/queries_metrics.go|fetchMetricDriverDelta|*string,**float64,**float64|d0b9ed02fe41|not-describable
internal/queryapi/home/queries_metrics.go|fetchMetricSeries|*time.Time,**float64|65615416e08e|spliced-select-list
internal/queryapi/home/queries_metrics.go|fetchMetricValue|*int64,**float64|5dcf22b841ed|spliced-select-list
internal/queryapi/home/queries_metrics.go|fetchPRReworkView|[]any|06333026d8d8|variadic-destinations
internal/queryapi/home/queries_metrics.go|fetchRateDriverDelta|*string,**float64,**float64|f482f059844b|not-describable
internal/queryapi/home/queries_signals.go|fetchRiskSignals|*string,*string,**float64,*string,*time.Time,**float64|6bd1c30424af|not-describable
internal/queryapi/home/queries_sync.go|FetchLatestSuccessfulSyncAt|**time.Time|31cb9ec682dd|statement-not-static
internal/queryapi/hotspots/hotspots.go|queryHotspotRows|*string,*string,*uint64,*uint32,*uint32,*float64,**float64,*float64|eff6a22e140f|not-describable
internal/queryapi/investment/sunburst.go|FetchInvestmentSunburst|*string,*string,*string,*float64|cfc4e952be85|spliced-select-list
internal/queryapi/investmentexplain/reader.go|FetchInvestmentBreakdown|*string,*string,*float64|5b6cbfd46b13|spliced-select-list
internal/queryapi/investmentexplain/reader.go|FetchInvestmentThemeEffort|*string,*float64,*float64,*float64|b4b54a27e804|spliced-select-list
internal/queryapi/investmentexplain/workunitreader.go|FetchWorkUnitInvestments|*string,**string,**string,*time.Time,*time.Time,**string,**string,**string,**float64,*float64,*[]string,*[]float64,*[]string,*[]float64,**string,**float64,**string,**string,**string,**string,*time.Time|925574b78444|spliced-select-list
internal/queryapi/operatingreview/operatingreview.go|fetchChangeFailureAgg|[]any|6f03e9eca295|variadic-destinations
internal/queryapi/people/metricqueries.go|fetchPersonBreakdown|*string,*float64|f8f1b7a98931|spliced-select-list
internal/queryapi/people/metricqueries.go|fetchPersonMetricSeries|*time.Time,*float64|a5718ef11460|spliced-select-list
internal/queryapi/people/metricqueries.go|fetchPersonMetricValue|*uint64,*float64|c39ce0a6762b|spliced-select-list
internal/queryapi/people/search.go|BuildSearchResponse|*string,*time.Time|ad96f23cff3b|not-describable
internal/queryapi/people/summary.go|fetchPersonCollaboration|*string,*string,*float64|a9a269b4dc4b|not-describable
internal/queryapi/producttelemetry/platform.go|LoadOrgNames|*string,*string,*string|d6e2d8979e34|not-describable
internal/queryapi/quadrant/quadrant.go|scanMetricRows|*time.Time,*string,*string,*float64|77d3e2c1c404|statement-not-static
internal/queryapi/reports/reports.go|scanCount|*int64|7b556e4d2103|not-describable
internal/queryapi/reports/reports.go|scanReportRun|*string,*string,*string,**time.Time,**time.Time,**float64,**string,**string,**string,**string,*string,*time.Time|7deee3898a5f|not-describable
internal/queryapi/reports/reports.go|scanSavedReport|*string,*string,*string,**string,**string,*bool,**string,**string,**string,*bool,**time.Time,**string,*time.Time,*time.Time,**string|1fcac67ba0d5|not-describable
internal/queryapi/reports/writer.go|clone|*string,**string,**string,**string,**string|bc905c6f9a60|statement-not-static
internal/queryapi/reports/writer.go|trigger|*bool|bc905c6f9a60|statement-not-static
internal/queryapi/reports/writer.go|update|*string,*string,*string,**string,**time.Time,*time.Time|bc905c6f9a60|statement-not-static
internal/queryapi/routeswitch/class_decision_switch.go|Enabled|*string|e2b98404debe|not-describable
internal/queryapi/server/mcp_route.go|Scan|[]any|11cd497baabf|variadic-destinations
internal/queryapi/workgraph/edges.go|countMatchingEdges|*uint64|8c8878aaea77|not-describable
internal/queryapi/workgraph/edges.go|fetchDedupedEdgeRows|*string,*string,*string,*string,*string,*string,*string,*string,*string,*float64,*string|4b433bc49731|statement-not-static
internal/queryapi/workgraph/edges.go|queryDependencyEdges|*string,*string,*string,*string,*string,*string,*string,*string,*string,*float64,*string|4b433bc49731|statement-not-static
internal/queryapi/workgraph/membership.go|batchResolveMembership|*string,*string,*string,*string|59b7f11d11ce|not-describable
internal/queryapi/workgraph/scope.go|detectMembershipDegradedReason|*uint64,*uint64|8617af7ed909|spliced-select-list
internal/queryapi/workgraph/teamattribution.go|resolveWorkUnitTeamAttributions|*string,*string,*string,*string,*string,*uint64|57974528e1ab|not-describable
`
