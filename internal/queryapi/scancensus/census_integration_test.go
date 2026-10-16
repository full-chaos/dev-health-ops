//go:build integration

package scancensus

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
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
// disturb the pin, and carries a digest of the function, the SQL constants of its file and of the
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
	changed, added, stale := diffPins(listed, unresolved)
	if len(stale) > 0 && os.Getenv("SCANCENSUS_WRITE_PINS") == "1" {
		rewritePins(t, removePins(listed, stale))
		stale = nil
	}
	for _, c := range changed {
		t.Errorf("pinned site changed and is still unverifiable: %s\nA changed pin is never regenerated. Read the new statement against the column types, then edit the pin line by hand (digest and a reason) in unverifiedSites and say why in the PR.", c)
	}
	for _, a := range added {
		t.Errorf("site the census cannot verify is not pinned: %s\nResolve it so the census can check it, or add a pin line by hand with a reason after reading its statement against the column types.", a)
	}
	for _, st := range stale {
		t.Errorf("pin for a site that is now verified or gone: %s\nRemove it (SCANCENSUS_WRITE_PINS=1 removes such pins).", st)
	}
	t.Logf("verified %d of %d sites", len(sites)-len(unresolved), len(sites))
}

// rewritePins replaces unverifiedSites in this file with pins. It is only ever
// handed the current pins minus the stale ones: the census can remove a pin,
// never add one or refresh the digest of a site it cannot verify.
func rewritePins(t *testing.T, pins []string) {
	t.Helper()
	const path = "census_integration_test.go"
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	const marker = "const unverifiedSites = `\n"
	start := strings.Index(text, marker)
	if start < 0 {
		t.Fatal("unverifiedSites not found")
	}
	start += len(marker)
	end := start + strings.Index(text[start:], "\n`")
	if err := os.WriteFile(path, []byte(text[:start]+strings.Join(pins, "\n")+text[end:]), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("removed %d stale pin(s) from %s; review the diff", len(strings.Split(text[start:end], "\n"))-len(pins), path)
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
internal/queryapi/aianalytics/ai_opportunities.go|loadClusters|*string,*string,*uint64,*[]string|797f7a3b795e|not-describable
internal/queryapi/aianalytics/ai_opportunities.go|loadClusters|*string,*uint64,*[]string|797f7a3b795e|not-describable
internal/queryapi/aianalytics/ai_opportunities.go|loadDocDrift|*string,*uint64,*uint64|10f890afaa03|not-describable
internal/queryapi/aianalytics/ai_opportunities.go|loadFlaky|*string,*uint64,*float64|1e00a81bdcc4|not-describable
internal/queryapi/aianalytics/flow_opportunities.go|loadRepoFlowRows|*string,*uint64,**float64,**float64,**float64,**float64|877e3ede0140|not-describable
internal/queryapi/aianalytics/flow_opportunities.go|loadTeamFlowRows|*string,*uint64,**float64,**float64,**float64|13edc415554e|not-describable
internal/queryapi/aianalytics/loaders.go|loadLabels|*string,*string|b5e9cb05f9a6|spliced-select-list
internal/queryapi/analytics/breakdown.go|executeBreakdownRaw|*string,**float64|c64156aa3194|statement-not-static
internal/queryapi/analytics/catalog.go|executeCatalogValues|*string,*uint64|b97cd2e421d3|statement-not-static
internal/queryapi/analytics/flowmatrix.go|queryEdges|*string,*string,*string,*string,**float64|99296deaf547|statement-not-static
internal/queryapi/analytics/flowmatrix.go|queryNodes|*string,*string,**float64|a7d2ecaf3421|statement-not-static
internal/queryapi/analytics/investmentquality.go|executeEvidenceQualityByGroup|*string,*float64,*uint64,*uint64|7ac6ecae9f5a|statement-not-static
internal/queryapi/analytics/investmentquality.go|executeInvestmentQualityStats|*uint64,*uint64,*float64,*float64,*uint64,*uint64,*uint64,*uint64,*uint64|aa5afb2f46da|statement-not-static
internal/queryapi/analytics/sankeycoverage.go|resolveSankeyCoverage|*float64,*float64,*float64,*float64,**float64,**float64,**float64,**float64,**float64|a9227abafb37|statement-not-static
internal/queryapi/analytics/sankeygrouped.go|executeSankeyGrouped|[]any|02426fb74c03|variadic-destinations
internal/queryapi/analytics/timeseries.go|ExecuteOrgTimeseries|*time.Time,**float64,**float64|82d7f0efb34d|statement-not-static
internal/queryapi/analytics/timeseries.go|ExecuteTimeseries|*time.Time,*string,**float64|94c36d3a4476|statement-not-static
internal/queryapi/complexitytimeseries/complexitytimeseries.go|fetchFileTimeseries|*time.Time,*string,*string,**uint32,**float64,**uint32,**uint32|9b3cd67ee152|spliced-select-list
internal/queryapi/complexitytimeseries/complexitytimeseries.go|fetchRepoTimeseries|*time.Time,*string,**uint64,**uint64,**float64,**uint64,**uint64|8c9d8cb11e56|spliced-select-list
internal/queryapi/compoundingrisk/read.go|latestDay|**time.Time|aa76d186ba27|not-describable
internal/queryapi/compoundingrisk/read.go|latestRows|*string,**float64,*string,**float64,**float64,**float64,**float64,**float64,**float64,**float64,**float64,**float64,**float64,*float64,*float64,*float64,*float64,*float64,*float64,*time.Time|52ea628e3088|not-describable
internal/queryapi/compoundingrisk/read.go|repoTrend|*time.Time,**float64|89727969156c|not-describable
internal/queryapi/datahealth/connectors.go|Connectors|*string,*string,*[]byte,**time.Time,**bool,**string,*[]byte,**time.Time,**int32,**time.Time,**time.Time,*[]byte,**string|d9838116a756|not-describable
internal/queryapi/datahealth/coverage.go|MetricLineage|**time.Time,*uint64|61037e7ece4e|not-describable
internal/queryapi/datahealth/sourcehealth.go|SourceHealth|*string,**string,*bool,**time.Time,*bool,*string,*[]byte,**time.Time,**bool,*bool,**time.Time,**string,*bool,**time.Time,**time.Time,**string,**string|dabf0d087730|not-describable
internal/queryapi/drilldown/issues.go|BuildIssuesResponse|*string,*string,*string,**string,**string,**float64,**float64,**time.Time,**time.Time|f1275892f46b|not-describable
internal/queryapi/drilldown/issues.go|buildBlockedIssuesResponse|*string,*string,**string,**string,*uint64|b6ef87c612b0|not-describable
internal/queryapi/explain/metrics.go|fetchChangeFailureView|[]any|27c1bd601818|variadic-destinations
internal/queryapi/explain/metrics.go|fetchMetricContributors|*string,**float64|93fd97f3fe4c|spliced-select-list
internal/queryapi/explain/metrics.go|fetchMetricDriverDelta|*string,**float64,*uint8,*uint8,**float64|72bee1c1524c|not-describable
internal/queryapi/explain/metrics.go|fetchMetricValue|**float64,*uint64|473951d9d13d|spliced-select-list
internal/queryapi/explain/repositories.go|fetchWorkItemProviders|*string|42ca92597595|not-describable
internal/queryapi/home/queries_metrics.go|fetchChangeFailureView|[]any|55592e42870b|variadic-destinations
internal/queryapi/home/queries_metrics.go|fetchMetricDriverDelta|*string,**float64,**float64|4dc1a6e702d3|not-describable
internal/queryapi/home/queries_metrics.go|fetchMetricSeries|*time.Time,**float64|42beaf116a09|spliced-select-list
internal/queryapi/home/queries_metrics.go|fetchMetricValue|*int64,**float64|fdde55eaa07b|spliced-select-list
internal/queryapi/home/queries_metrics.go|fetchPRReworkView|[]any|6996634017b7|variadic-destinations
internal/queryapi/home/queries_metrics.go|fetchRateDriverDelta|*string,**float64,**float64|cbaa044e10d6|not-describable
internal/queryapi/home/queries_signals.go|fetchRiskSignals|*string,*string,**float64,*string,*time.Time,**float64|54de9ad8afd6|not-describable
internal/queryapi/home/queries_sync.go|FetchLatestSuccessfulSyncAt|**time.Time|9dfe899f0474|statement-not-static
internal/queryapi/hotspots/hotspots.go|queryHotspotRows|*string,*string,*uint64,*uint32,*uint32,*float64,**float64,*float64|448efde8f6df|not-describable
internal/queryapi/investment/sunburst.go|FetchInvestmentSunburst|*string,*string,*string,*float64|54b6486b78e7|spliced-select-list
internal/queryapi/investmentexplain/reader.go|FetchInvestmentBreakdown|*string,*string,*float64|8fa24d14b228|spliced-select-list
internal/queryapi/investmentexplain/reader.go|FetchInvestmentThemeEffort|*string,*float64,*float64,*float64|444d2e14edab|spliced-select-list
internal/queryapi/investmentexplain/reader.go|FetchSizingCoverage|*uint64,*uint64|13df7c74def4|not-describable
internal/queryapi/investmentexplain/workunitreader.go|FetchWorkUnitInvestments|*string,**string,**string,*time.Time,*time.Time,**string,**string,**string,**float64,*float64,*[]string,*[]float64,*[]string,*[]float64,**string,**float64,**string,**string,**string,**string,*time.Time|2fe6cf35c49b|spliced-select-list
internal/queryapi/operatingreview/operatingreview.go|fetchChangeFailureAgg|[]any|fa0c9e3b063a|variadic-destinations
internal/queryapi/people/metricqueries.go|fetchPersonBreakdown|*string,*float64|a120bde67c22|spliced-select-list
internal/queryapi/people/metricqueries.go|fetchPersonMetricSeries|*time.Time,*float64|5b284baef0b7|spliced-select-list
internal/queryapi/people/metricqueries.go|fetchPersonMetricValue|*uint64,*float64|ccfb038cf8cf|spliced-select-list
internal/queryapi/people/search.go|BuildSearchResponse|*string,*time.Time|d71e6f4a1e39|not-describable
internal/queryapi/people/summary.go|fetchPersonCollaboration|*string,*string,*float64|d3c2e6e50c4f|not-describable
internal/queryapi/producttelemetry/platform.go|LoadOrgNames|*string,*string,*string|64c4ca805d17|not-describable
internal/queryapi/quadrant/quadrant.go|scanMetricRows|*time.Time,*string,*string,*float64|433c0a3205e2|statement-not-static
internal/queryapi/reports/reports.go|scanCount|*int64|8d2d39db7d2b|not-describable
internal/queryapi/reports/reports.go|scanReportRun|*string,*string,*string,**time.Time,**time.Time,**float64,**string,**string,**string,**string,*string,*time.Time|0121a67bb651|not-describable
internal/queryapi/reports/reports.go|scanSavedReport|*string,*string,*string,**string,**string,*bool,**string,**string,**string,*bool,**time.Time,**string,*time.Time,*time.Time,**string|835998e4481b|not-describable
internal/queryapi/reports/writer.go|clone|*string,**string,**string,**string,**string|bd5216bf923e|statement-not-static
internal/queryapi/reports/writer.go|trigger|*bool|a8202561e27c|statement-not-static
internal/queryapi/reports/writer.go|update|*string,*string,*string,**string,**time.Time,*time.Time|6c5898b14827|statement-not-static
internal/queryapi/routeswitch/class_decision_switch.go|Enabled|*string|7a40513e2fab|not-describable
internal/queryapi/server/mcp_route.go|Scan|[]any|073743d6899c|variadic-destinations
internal/queryapi/workgraph/edges.go|countMatchingEdges|*uint64|8d8175642fa2|not-describable
internal/queryapi/workgraph/edges.go|fetchDedupedEdgeRows|*string,*string,*string,*string,*string,*string,*string,*string,*string,*float64,*string|98e2e21bd3c2|statement-not-static
internal/queryapi/workgraph/edges.go|queryDependencyEdges|*string,*string,*string,*string,*string,*string,*string,*string,*string,*float64,*string|4f3556505dd3|statement-not-static
internal/queryapi/workgraph/membership.go|batchResolveMembership|*string,*string,*string,*string|2e7589312710|not-describable
internal/queryapi/workgraph/scope.go|detectMembershipDegradedReason|*uint64,*uint64|e91eab4f4df6|spliced-select-list
internal/queryapi/workgraph/teamattribution.go|resolveWorkUnitTeamAttributions|*string,*string,*string,*string,*string,*uint64|81eed465819b|not-describable
`
