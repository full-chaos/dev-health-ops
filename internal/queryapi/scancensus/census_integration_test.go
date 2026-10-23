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

// nullableInner is the type inside a Nullable column type, looking through
// LowCardinality(...) wrappers: LowCardinality(Nullable(String)) is as Nullable
// as Nullable(String).
func nullableInner(chType string) (string, bool) {
	t := chType
	for {
		inner, ok := strings.CutPrefix(t, "LowCardinality(")
		if !ok {
			break
		}
		t = strings.TrimSuffix(inner, ")")
	}
	inner, ok := strings.CutPrefix(t, "Nullable(")
	if !ok {
		return "", false
	}
	return strings.TrimSuffix(inner, ")"), true
}

func probeExpr(chType string) string {
	if inner, ok := nullableInner(chType); ok {
		return fmt.Sprintf("CAST(defaultValueOfTypeName('%s') AS %s)", strings.ReplaceAll(inner, "'", `\'`), chType)
	}
	return fmt.Sprintf("defaultValueOfTypeName('%s')", strings.ReplaceAll(chType, "'", `\'`))
}

// nullProbeExpr is a NULL of a Nullable column type; ok is false for any other type.
func nullProbeExpr(chType string) (string, bool) {
	if _, ok := nullableInner(chType); !ok {
		return "", false
	}
	return fmt.Sprintf("CAST(NULL AS %s)", chType), true
}

type pairKey struct {
	ch, dest string
	null     bool
}

// scannable asks the real driver whether a value of chType scans into dest.
// With null set it probes a NULL of the (Nullable) type instead of a default
// non-NULL value: a plain *T destination passes the default probe of a
// Nullable column and still fails on a NULL row.
func scannable(ctx context.Context, conn driver.Conn, chType, dest string, null bool) error {
	rt, ok := destType(dest)
	if !ok {
		return fmt.Errorf("destination type %s is not modelled by the census", dest)
	}
	expr, kind := probeExpr(chType), "value"
	if null {
		nullExpr, nullable := nullProbeExpr(chType)
		if !nullable {
			return fmt.Errorf("a NULL probe needs a Nullable type, got %s", chType)
		}
		expr, kind = nullExpr, "NULL"
	}
	rows, err := conn.Query(ctx, "SELECT "+expr)
	if err != nil {
		return fmt.Errorf("%s probe of %s: %w", kind, chType, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return fmt.Errorf("%s probe of %s returned no row", kind, chType)
	}
	target := reflect.New(rt.Elem())
	if err := rows.Scan(target.Interface()); err != nil {
		return fmt.Errorf("%s probe of %s: %w", kind, chType, err)
	}
	// A pointer destination (**T) is NULL-safe only because the driver sets it to nil: check that
	// it did, per type, instead of assuming it from the shape of the destination.
	if null && target.Elem().Kind() == reflect.Pointer && !target.Elem().IsNil() {
		return fmt.Errorf("NULL probe of %s: the driver left the %s destination non-nil", chType, dest)
	}
	return nil
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
	// NullablePlain lists the columns the server types Nullable that the site
	// scans into a plain destination ("col<i>:<type>"). clickhouse-go scans a
	// NULL there without an error and leaves the destination untouched (the zero
	// value of a fresh variable, the previous row's value of a reused one), so no
	// probe fails and the read turns NULL into a value silently.
	NullablePlain []string
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
				_, nullable := nullProbeExpr(ct)
				if nullable && !strings.HasPrefix(s.Dests[i], "**") {
					v.NullablePlain = append(v.NullablePlain, fmt.Sprintf("col%d:%s", i+1, ct))
				}
				for _, null := range []bool{false, true} {
					if null && !nullable {
						continue
					}
					k := pairKey{ct, s.Dests[i], null}
					err, seen := probed[k]
					if !seen {
						err = scannable(ctx, conn, ct, s.Dests[i], null)
						probed[k] = err
					}
					if err != nil {
						failing = append(failing, fmt.Sprintf("column %d: %s into %s: %s", i+1, ct, s.Dests[i], firstLine(err.Error())))
					}
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
	var unresolved, bad, nullPlain []string
	for _, v := range Classify(ctx, conn, sites) {
		for _, np := range v.NullablePlain {
			nullPlain = append(nullPlain, siteKey(v.Site)+"|nullable-into-plain|"+np)
		}
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
		rewritePins(t, "unverifiedSites", removePins(listed, stale))
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
	sort.Strings(nullPlain)
	var listedNull []string
	if strings.TrimSpace(nullableIntoPlainSites) != "" {
		listedNull = strings.Split(strings.TrimSpace(nullableIntoPlainSites), "\n")
	}
	nChanged, nAdded, nStale := diffPins(listedNull, nullPlain)
	if len(nStale) > 0 && os.Getenv("SCANCENSUS_WRITE_PINS") == "1" {
		rewritePins(t, "nullableIntoPlainSites", removePins(listedNull, nStale))
		nStale = nil
	}
	for _, c := range nChanged {
		t.Errorf("nullable-into-plain pin changed and the column is still scanned into a plain destination: %s\nA changed pin is never regenerated. Decide per site whether a NULL left as the destination's value is right, then edit the pin by hand and say why in the PR.", c)
	}
	for _, a := range nAdded {
		t.Errorf("a Nullable column is scanned into a plain destination and is not pinned: %s\nclickhouse-go leaves the destination untouched on a NULL there, without an error. Scan into a pointer (**T) or CAST/coalesce in SQL; a pin is a reviewed exception, never added to make the census pass.", a)
	}
	for _, st := range nStale {
		t.Errorf("nullable-into-plain pin for a site that no longer scans a Nullable column into a plain destination (or is gone): %s\nRemove it (SCANCENSUS_WRITE_PINS=1 removes such pins).", st)
	}
	t.Logf("verified %d of %d sites; %d nullable-into-plain column(s) pinned", len(sites)-len(unresolved), len(sites), len(nullPlain))
}

// rewritePins replaces unverifiedSites in this file with pins. It is only ever
// handed the current pins minus the stale ones: the census can remove a pin,
// never add one or refresh the digest of a site it cannot verify.
func rewritePins(t *testing.T, constName string, pins []string) {
	t.Helper()
	const path = "census_integration_test.go"
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	marker := "const " + constName + " = `\n"
	start := strings.Index(text, marker)
	if start < 0 {
		t.Fatalf("%s not found", constName)
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

// TestScanTypeCensusProbesNullForANullableColumn pins what the real driver
// does with a NULL: it scans into a plain destination without an error and
// leaves the destination untouched. That is why the census reports a Nullable column
// scanned into a plain destination (Verdict.NullablePlain) instead of failing a
// probe, and why a driver that starts to refuse a NULL there must turn this red.
func TestScanTypeCensusProbesNullForANullableColumn(t *testing.T) {
	ctx, conn := openCensusConn(t)
	if err := scannable(ctx, conn, "Nullable(Float64)", "*float64", true); err != nil {
		t.Fatalf("the driver now refuses a NULL in a plain destination (%v): a plain-destination pin is no longer a silent NULL-to-zero, revisit nullableIntoPlainSites", err)
	}
	// A NULL leaves a plain destination untouched (the zero value for a fresh variable, the
	// PREVIOUS row's value for one a loop reuses) and sets a pointer destination to nil, for
	// every type a pinned or scanned column has.
	sentinel := map[string]any{
		"float64": 42.5, "string": "kept", "uint64": uint64(7), "int64": int64(7), "uint8": uint8(7),
		"bool": true, "time.Time": time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC),
	}
	for _, c := range []struct{ chType, goType string }{
		{"Nullable(Float64)", "float64"},
		{"Nullable(String)", "string"},
		{"LowCardinality(Nullable(String))", "string"},
		{"Nullable(UInt64)", "uint64"},
		{"Nullable(Int64)", "int64"},
		{"Nullable(UInt8)", "uint8"},
		{"Nullable(Bool)", "bool"},
		{"Nullable(Date)", "time.Time"},
		{"Nullable(DateTime64(3, 'UTC'))", "time.Time"},
	} {
		expr, _ := nullProbeExpr(c.chType)
		rt, _ := destType("*" + c.goType)
		plain := reflect.New(rt.Elem())
		plain.Elem().Set(reflect.ValueOf(sentinel[c.goType]))
		ptr := reflect.New(reflect.PointerTo(rt.Elem()))
		for _, dest := range []reflect.Value{plain, ptr} {
			rows, err := conn.Query(ctx, "SELECT "+expr)
			if err != nil || !rows.Next() {
				t.Fatalf("NULL probe of %s: %v", c.chType, err)
			}
			if err := rows.Scan(dest.Interface()); err != nil {
				t.Fatalf("scan NULL of %s into %s: %v", c.chType, dest.Type(), err)
			}
			rows.Close()
		}
		if got := plain.Elem().Interface(); got != sentinel[c.goType] {
			t.Errorf("NULL of %s over a plain %s changed it to %v, want it untouched", c.chType, c.goType, got)
		}
		if !ptr.Elem().IsNil() {
			t.Errorf("NULL of %s into **%s left the pointer non-nil", c.chType, c.goType)
		}
		if err := scannable(ctx, conn, c.chType, "**"+c.goType, true); err != nil {
			t.Errorf("the NULL probe of %s into **%s: %v", c.chType, c.goType, err)
		}
	}
	if err := scannable(ctx, conn, "Float64", "*float64", true); err == nil {
		t.Fatal("a NULL probe of a non-Nullable type must be refused, not pass")
	}
	for _, c := range []struct {
		dest, chType string
		want         []string
	}{
		{"*float64", "Nullable(Float64)", []string{"col2:Nullable(Float64)"}},
		{"**float64", "Nullable(Float64)", nil},
		{"*string", "LowCardinality(Nullable(String))", []string{"col2:LowCardinality(Nullable(String))"}},
		{"**string", "LowCardinality(Nullable(String))", nil},
	} {
		statement := []string{"SELECT status, CAST(NULL AS " + c.chType + ") AS n FROM work_item_state_durations_daily GROUP BY status"}
		verdict := Classify(ctx, conn, []Site{{File: "planted.go", Func: "planted", Dests: []string{"*string", c.dest}, Statements: statement}})[0]
		if verdict.Unresolved != "" || len(verdict.Mismatches) != 0 {
			t.Fatalf("%s: unexpected verdict %+v", c.dest, verdict)
		}
		if !reflect.DeepEqual(verdict.NullablePlain, c.want) {
			t.Errorf("%s: NullablePlain = %v, want %v", c.dest, verdict.NullablePlain, c.want)
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

// nullableIntoPlainSites is `file|func|destinations|digest|nullable-into-plain|col<i>:<type>`: a Nullable column
// scanned into a plain destination, which reads a NULL as whatever the destination held (the zero value of a fresh variable). It is empty and stays empty: scan a Nullable column into a **T.
const nullableIntoPlainSites = `
`

// unverifiedSites is `file|func|destinations|digest|reason`; see reasonClass.
const unverifiedSites = `
internal/queryapi/aianalytics/ai_opportunities.go|loadClusters|*string,*string,*uint64,*[]string|b6196bf6b5ec|not-describable
internal/queryapi/aianalytics/ai_opportunities.go|loadClusters|*string,*uint64,*[]string|b6196bf6b5ec|not-describable
internal/queryapi/aianalytics/ai_opportunities.go|loadDocDrift|*string,*uint64,*uint64|47f02ce33abc|not-describable
internal/queryapi/aianalytics/ai_opportunities.go|loadFlaky|*string,*uint64,*float64|189f28fa062d|not-describable
internal/queryapi/aianalytics/flow_opportunities.go|loadRepoFlowRows|*string,*uint64,**float64,**float64,**float64,**float64|877e3ede0140|not-describable
internal/queryapi/aianalytics/flow_opportunities.go|loadTeamFlowRows|*string,*uint64,**float64,**float64,**float64|13edc415554e|not-describable
internal/queryapi/aianalytics/loaders.go|loadLabels|*string,*string|8ab0086629ab|spliced-select-list
internal/queryapi/analytics/breakdown.go|executeBreakdownRaw|*string,**float64|c64156aa3194|statement-not-static
internal/queryapi/analytics/catalog.go|executeCatalogValues|*string,*uint64|b97cd2e421d3|statement-not-static
internal/queryapi/analytics/flowmatrix.go|queryEdges|*string,*string,*string,*string,**float64|a19dc243308f|statement-not-static
internal/queryapi/analytics/flowmatrix.go|queryNodes|*string,*string,**float64|86e5d5b721b7|statement-not-static
internal/queryapi/analytics/investmentquality.go|executeEvidenceQualityByGroup|*string,*float64,*uint64,*uint64|441cf34157b6|statement-not-static
internal/queryapi/analytics/investmentquality.go|executeInvestmentQualityStats|*uint64,*uint64,*float64,*float64,*uint64,*uint64,*uint64,*uint64,*uint64|3a00ba4c26b9|statement-not-static
internal/queryapi/analytics/sankeycoverage.go|runSankeyCoverageQuery|*float64,*float64,*float64,*float64,**float64,**float64,**float64,**float64,**float64|7474017a0a07|statement-not-static
internal/queryapi/analytics/sankeygrouped.go|executeSankeyGrouped|[]any|02426fb74c03|variadic-destinations
internal/queryapi/analytics/timeseries.go|ExecuteOrgTimeseries|*time.Time,**float64,**float64|30a7cdd2fa01|statement-not-static
internal/queryapi/analytics/timeseries.go|ExecuteTimeseries|*time.Time,*string,**float64|f2eec06b6811|statement-not-static
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
internal/queryapi/home/queries_metric_drivers_batch.go|fetchMetricDriversBatch|*string,*string|8e8c97ce9520|not-describable
internal/queryapi/home/queries_metrics.go|fetchChangeFailureView|[]any|55592e42870b|variadic-destinations
internal/queryapi/home/queries_metrics.go|fetchMetricDriverDelta|*string,**float64,**float64|dd0172c85c4d|not-describable
internal/queryapi/home/queries_metrics.go|fetchMetricSeries|*time.Time,**float64|42beaf116a09|spliced-select-list
internal/queryapi/home/queries_metrics.go|fetchMetricValue|*int64,**float64|fdde55eaa07b|spliced-select-list
internal/queryapi/home/queries_metrics.go|fetchPRReworkView|[]any|6996634017b7|variadic-destinations
internal/queryapi/home/queries_metrics.go|fetchRateDriverDelta|*string,**float64,**float64|cbaa044e10d6|not-describable
internal/queryapi/home/queries_signals.go|fetchRiskSignals|*string,*string,**float64,*string,*time.Time,**float64|54de9ad8afd6|not-describable
internal/queryapi/home/queries_sync.go|FetchLatestSuccessfulSyncAt|**time.Time|9dfe899f0474|statement-not-static
internal/queryapi/home/repolinked.go|readBasis|*string,*string,*uint8|3e248e77f109|spliced-select-list
internal/queryapi/hotspots/hotspots.go|queryHotspotRows|*string,*string,*uint64,*uint32,*uint32,*float64,**float64,*float64|448efde8f6df|not-describable
internal/queryapi/investment/sunburst.go|FetchInvestmentSunburst|*string,*string,*string,*float64|d4d0c8231707|spliced-select-list
internal/queryapi/investmentexplain/reader.go|FetchInvestmentBreakdown|*string,*string,*float64|2283428d08a0|spliced-select-list
internal/queryapi/investmentexplain/reader.go|FetchInvestmentThemeEffort|*string,*float64,*float64,*float64|66b93f6e2aee|spliced-select-list
internal/queryapi/investmentexplain/workunitreader.go|FetchWorkUnitInvestments|*string,**string,**string,*time.Time,*time.Time,**string,**string,**string,**float64,*float64,*[]string,*[]float64,*[]string,*[]float64,**string,**float64,**string,**string,**string,**string,*time.Time|cd7a97bbd38c|spliced-select-list
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
internal/queryapi/reports/writer.go|update|*string,*string,*string,**string,**time.Time,*time.Time|02fbaf29d245|statement-not-static
internal/queryapi/routeswitch/class_decision_switch.go|Enabled|*string|7a40513e2fab|not-describable
internal/queryapi/server/mcp_route.go|Scan|[]any|073743d6899c|variadic-destinations
internal/queryapi/workgraph/edges.go|countMatchingEdges|*uint64|8d8175642fa2|not-describable
internal/queryapi/workgraph/edges.go|fetchDedupedEdgeRows|*string,*string,*string,*string,*string,*string,*string,*string,*string,*float64,*string|98e2e21bd3c2|statement-not-static
internal/queryapi/workgraph/edges.go|queryDependencyEdges|*string,*string,*string,*string,*string,*string,*string,*string,*string,*float64,*string|4f3556505dd3|statement-not-static
internal/queryapi/workgraph/membership.go|batchResolveMembership|*string,*string,*string,*string|2e7589312710|not-describable
internal/queryapi/workgraph/scope.go|detectMembershipDegradedReason|*uint64,*uint64|e91eab4f4df6|spliced-select-list
internal/queryapi/workgraph/teamattribution.go|resolveWorkUnitTeamAttributions|*string,*string,*string,*string,*string,*uint64|81eed465819b|not-describable
`
