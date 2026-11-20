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
// does with a NULL: since clickhouse-go v2.50 it refuses a NULL in a plain
// destination with an error and leaves the destination untouched, and it sets a
// pointer destination to nil. That is why nullableIntoPlainSites stays empty (a
// Nullable column scanned into a plain destination fails the read), and why a
// driver that goes back to a silent NULL-to-zero must turn this red.
func TestScanTypeCensusProbesNullForANullableColumn(t *testing.T) {
	ctx, conn := openCensusConn(t)
	if err := scannable(ctx, conn, "Nullable(Float64)", "*float64", true); err == nil {
		t.Fatal("the driver scanned a NULL into a plain destination without an error: a Nullable column into a plain destination is a silent NULL-to-zero again, revisit nullableIntoPlainSites")
	}
	// A NULL into a plain destination is an error that leaves it untouched, and it sets a
	// pointer destination to nil, for every type a pinned or scanned column has.
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
			err = rows.Scan(dest.Interface())
			rows.Close()
			if wantErr := dest == plain; (err != nil) != wantErr {
				t.Fatalf("scan NULL of %s into %s: err = %v, want an error = %v", c.chType, dest.Type(), err, wantErr)
			}
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
	// A Nullable column into a plain destination is now also a NULL-probe mismatch: the read fails.
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
		wantMismatch := c.want != nil
		if verdict.Unresolved != "" || (len(verdict.Mismatches) != 0) != wantMismatch {
			t.Fatalf("%s: unexpected verdict %+v", c.dest, verdict)
		}
		if wantMismatch && (len(verdict.Mismatches) != 1 || !strings.Contains(verdict.Mismatches[0], "column 2:") || !strings.Contains(verdict.Mismatches[0], "converting NULL")) {
			t.Fatalf("%s: want one NULL-probe mismatch on column 2, got %v", c.dest, verdict.Mismatches)
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

// CHAOS-14148: eight pins re-pinned by hand, digests copied from the local TestScanTypeCensus failure output (not computed):
// flow_opportunities.go (loadRepoFlowRows, loadTeamFlowRows), operatingreview.go (fetchChangeFailureAgg, fetchStateDurations,
// fetchWorkItems), people/summary.go (fetchPersonCollaboration) and analytics/timeseries.go (ExecuteOrgTimeseries,
// ExecuteTimeseries). The readers of the four pooled daily tables (migration 164) add the live-row rule (a WHERE or HAVING
// predicate, or the live-row wrapper of the dedup source), which changes the file's SQL digest of every site; no select list,
// scan target or column type changed, and the reason class of each pin is kept.
// unverifiedSites is `file|func|destinations|digest|reason`; see reasonClass.
//
// CHAOS-14376 merge of main (the CHAOS-14355 slice 3 tie-break of the throughput forecast reads): four throughputforecast
// pins re-pinned by hand, digests copied from the local TestScanTypeCensus output (not computed): loadBacklog,
// loadStaleWIP, loadThroughputHistory, loadWorkItemOverlay. The newest version is now the argMax of the value tuple by
// (computed_at, cityHash64(values)) and the measures are its picked.N elements; the scan targets read the same element
// types as before (UInt32 sums are UInt64, the Date day/min day is *time.Time, avg() is Float64, the Nullable wip ages
// stay Nullable), so each pin keeps its reason class.
// CHAOS-14376: six forecast pins added by hand, digests copied from the local TestScanTypeCensus output (not computed):
// capacityforecast/clickhouse.go (loadBacklog, loadThroughput) and throughputforecast/clickhouse.go (loadBacklog,
// loadStaleWIP, loadThroughputHistory, loadWorkItemOverlay). Their FROM is now a source chosen at run time (the table, or
// the served team view of one team), which the census cannot neutralize. Read by hand against the column types: the
// select lists did not change. day is Date (*time.Time); sum() and countIf() over UInt32 are UInt64 (*uint64); avg() of
// a UInt32 sum is Float64 (*float64); avg() of the Nullable(Float64) wip ages is Nullable (**float64); minIf() of a
// Date is Date (*time.Time).
//
// aianalytics/scope.go ownerTeams (digest c0b3734eb92d) is not-describable because the census
// cannot resolve its coalesce() expression; read by hand: owner_repo_id (the coalesce) and
// owner_team_id are plain String, not Nullable, so the scan into *string is safe.
// CHAOS-13705: three explain pins re-pinned by hand, digests copied from the hosted TestScanTypeCensus failure output of job 132076907184
// (not computed): scope filter now from the unresolved branch, types unchanged (no column or type change):
//
//	09e3a4863c03 -> 06f0dd79850a
//	ed3ed30df2f7 -> 696073003934
//	42ca92597595 -> cc6f1e4e4f8c
//
// ai_opportunities.go re-pinned by hand, digests copied from the local TestScanTypeCensus failure output (not computed):
// flakyStatement now weighs only the days with a flake rate (sumIf/nullIf over the Nullable flake_rate), which changes
// the file's SQL digest of every site. loadClusters and loadDocDrift: SQL and scan unchanged, digest only. loadFlaky:
// weighted_flake_rate is Nullable(Float64) (NULL for a repository with no day that measured cases; the HAVING drops it),
// scanned into **float64 so a NULL is never read as a plain 0:
//
//	loadClusters 7ecb45f4962b -> e251e44419fd
//	loadDocDrift e303544dfe85 -> 1b7d64b044ee
//	loadFlaky 0232862afb43 -> a06340e42973
//
// ai_opportunities.go re-pinned by hand again (CHAOS-13843), digests copied from the local TestScanTypeCensus failure output
// (not computed): flakyStatement is now the share of distinct cases that flaked (SUM(flake_cases)/SUM(distinct_cases)
// with a count guard), which changes the file's SQL digest of every site. loadClusters and loadDocDrift: SQL and scan
// unchanged, digest only. loadFlaky: the scan targets *string,*uint64,**float64 are unchanged; flake_share is
// Nullable(Float64) (NULL for a repository with a day without counts or with no distinct case), scanned into **float64
// so a NULL is never read as a plain 0:
//
//	loadClusters e251e44419fd -> 8c4f62d6f207
//	loadDocDrift 1b7d64b044ee -> ed7df227e4af
//	loadFlaky a06340e42973 -> 28265e8c788f
//
// ai_opportunities.go re-pinned by hand again (CHAOS-14256), digests copied from the local TestScanTypeCensus failure output
// (not computed): loadDocDrift and loadFlaky now build their opportunity with the unclamped score (scoreDeltaRaw), and the
// impact read gains an ORDER BY over its group key. The SQL and the scan targets of both sites are unchanged
// (*string,*uint64,*uint64 and *string,*uint64,**float64; flake_share stays Nullable(Float64) into **float64):
//
//	loadDocDrift ce3024a41da9 -> f049a4598b46
//	loadFlaky 32950ea2e0be -> be2a35ea7bc8
//
// loadClusters re-pinned by hand (CHAOS-14256, r1), digest copied from the local TestScanTypeCensus failure output (not
// computed): the body now sorts each cluster's pull request refs (sortPRRefs) after the scan. The SQL and the scan
// targets (*string,*string,*uint64,*[]string and *string,*uint64,*[]string) are unchanged:
//
//	loadClusters f62124ab26af -> 0dcb9c7aceb8
//
// ai_opportunities.go re-pinned by hand again (CHAOS-14258), digests copied from the local TestScanTypeCensus failure output
// (not computed): the four workflow reads end with a total ORDER BY (each read's own GROUP BY key after its count) before
// LIMIT 100. Only ORDER BY terms are added: the SELECT columns, their types and the scan targets of loadClusters, loadDocDrift
// and loadFlaky are unchanged:
//
//	loadClusters 0dcb9c7aceb8 -> 9b80cc03e333
//	loadDocDrift f049a4598b46 -> 3a7b416f4596
//	loadFlaky be2a35ea7bc8 -> f848d53341ed
//
// ai_opportunities.go re-pinned by hand again (CHAOS-14258 round 1), digests copied from the local TestScanTypeCensus failure
// output (not computed): the repetitive read selects two more columns, cluster_author (the author email, else the author
// name, else empty; String) and cluster_work_type (the work item type, else pull_request; String), scanned into two more *string targets; its
// ORDER BY uses them. Both are non-Nullable String expressions, so *string is the right target. The other sites' SQL and scan
// targets are unchanged (the file's SQL digest moved):
//
//	loadClusters (with prefix) *string,*string,*uint64,*[]string 9b80cc03e333 -> *string,*string,*uint64,*[]string,*string,*string 6782179e5bab
//	loadClusters (no prefix) 9b80cc03e333 -> 6782179e5bab
//	loadDocDrift 3a7b416f4596 -> 9d9f4e10b7aa
//	loadFlaky f848d53341ed -> e2c82e8e8355
//
// timeseries.go ExecuteOrgTimeseries re-pinned by hand, digest copied from the local TestScanTypeCensus failure output
// (not computed): the org series of the pooled TestOps measures (PIPELINE_DURATION_P95, TEST_SUITE_DURATION_P95,
// PIPELINE_QUEUE_TIME) take the bucket value unscaled when the sample count is above 0. The statement and the scan
// targets are unchanged in type: numerator is toFloat64 of a Nullable(Float64) (the pooled percentile or the exact queue
// mean, NULL for a bucket with no sample) and denominator toFloat64(count(...)), both scanned into **float64:
//
//	ExecuteOrgTimeseries 30a7cdd2fa01 -> 1e3ee3af097b
//
// CHAOS-13928: five pins re-pinned by hand after reading each moved statement, digests copied from the local TestScanTypeCensus
// failure output (not computed). A testops_test_metrics_daily row that retires a day carries the new UInt8 column retracted
// (not Nullable); the readers add its liverow predicate to the WHERE of the statement and select no new column. flakyStatement
// (loadClusters, loadDocDrift: digest only; loadFlaky: same scan targets, flake_share still Nullable scanned into **float64) and
// dedupFromMeasureSource (ExecuteOrgTimeseries, ExecuteTimeseries: SELECT * wrapped in a WHERE; scan targets and types
// unchanged) change only a filter:
//
//	loadClusters 8c4f62d6f207 -> f62124ab26af
//	loadDocDrift ed7df227e4af -> ce3024a41da9
//	loadFlaky 28265e8c788f -> 37c05084ba13
//	ExecuteOrgTimeseries 1e3ee3af097b -> 6637e944ff79
//	ExecuteTimeseries f2eec06b6811 -> 8842c256cd1a
//
// liverow gained a third rule form for a table with a Retracted column in its own writer (cicd_metrics_daily,
// migration 159): Predicate and NewestPredicate have one more return statement, and the census evaluates every return
// of a helper a site calls, so six sites that call liverow got more evaluated statements. Re-pinned by hand,
// digests copied from the local TestScanTypeCensus failure output (not computed). Read (statements dumped at base and
// head): every base statement is still there; an added statement is the same query with the new rule text in its WHERE
// or HAVING, or a repeat of a statement already listed (no select item, column or scan target changes); the sites stay
// not-describable for their old reason:
//
//	clickhouse.go fetchCycleBreakdown a003456ad69e -> a0d30b3aefa3
//	flow_opportunities.go loadTeamFlowRows cef9073b1eae -> 9719c8e633d4
//	coverage.go MetricLineage 61037e7ece4e -> a3f56be4ccc9
//	operatingreview.go fetchStateDurations 2999640fe32f -> 11d3ca602c18
//	operatingreview.go fetchWorkItems a3d0e349ea87 -> 64839a153860
//	search.go BuildSearchResponse 168278cbea0a -> afd45e38cfbd
//
// Both together (the retired-day predicate of testops_test_metrics_daily and the third liverow rule form) move one more
// evaluated statement of flakyStatement, copied from the local TestScanTypeCensus failure output (not computed); scan
// targets unchanged:
//
//	loadFlaky 37c05084ba13 -> 32950ea2e0be
//
// CHAOS-14339: MetricLineage counts the org view of work_item_metrics_daily (countIf of rowview.OrgPredicate); its statement text
// changed and the site stays not-describable. Digest copied from the local TestScanTypeCensus failure output (not computed):
//
//	coverage.go MetricLineage a3f56be4ccc9 -> bb9897d55705
//
// testopsrisk.go readDaily is pinned by hand (CHAOS-14009), digest copied from the local TestScanTypeCensus failure output
// (not computed): dailyQuery now applies the live-row rule (liverow.NewestPredicate in the release and stability HAVING,
// liverow.Predicate in the FINAL drag WHERE), and the census evaluates every return of those helpers, so the variants that
// name another table's marker columns are refused by the server. Read by hand: the select list, the column types and the
// nine scan targets are unchanged (day Date into **time.Time, the Float64 means and Nullable(Float64) sums into **float64,
// partial_rows UInt64 into **uint64); only a WHERE line and two HAVING clauses were added.
// CHAOS-14008: two pins re-pinned by hand after reading the moved statement, digests copied from the local TestScanTypeCensus
// failure output (not computed). The testops_coverage_metrics_daily row that retires a day carries the new UInt8 column retracted
// (not Nullable); dedupFromMeasureSource wraps its SELECT * in a WHERE (liverow predicate); scan targets and types are unchanged:
//
//	ExecuteOrgTimeseries 1e3ee3af097b -> 6637e944ff79
//	ExecuteTimeseries f2eec06b6811 -> 8842c256cd1a
//
// CHAOS-14266: ai_opportunities.go re-pinned by hand, digests copied from the local TestScanTypeCensus failure output (not
// computed), on the merge with CHAOS-14256 (total served order). repetitiveStatement selects two more columns,
// cluster_author and cluster_work_type: the author and work type coalesce expressions it already grouped by, both String
// (coalesce with a non-null last argument), scanned into *string. The cluster read's scan targets gain *string,*string;
// the other sites of the file change digest only:
//
//	loadClusters (repetitive) 0dcb9c7aceb8 -> 4ae1cede92ca, scan +*string,*string
//	loadClusters (title toil) 0dcb9c7aceb8 -> 4ae1cede92ca
//	loadDocDrift f049a4598b46 -> ee855478db42
//	loadFlaky be2a35ea7bc8 -> 74566417b303
//
// CHAOS-14266 anchor (acp ruling: no author in the id input): repetitiveStatement selects one more column,
// cluster_anchor = argMin(pr.number, (pr.created_at, pr.number)), UInt32 (the number column, not Nullable), scanned into
// *uint32; cluster_author stays selected (a GROUP BY key and a sort value) and is scanned but never read. Re-pinned by
// hand, digests copied from the local TestScanTypeCensus failure output (not computed):
//
//	loadClusters (repetitive) 4ae1cede92ca -> b1659de79ad1, scan +*uint32
//	loadClusters (title toil) 4ae1cede92ca -> b1659de79ad1
//	loadDocDrift ee855478db42 -> 84c1bc669446
//	loadFlaky 74566417b303 -> 32baba4ff289
//
// CHAOS-14218: six pins re-pinned for the nillable scan destinations clickhouse-go v2.51 needs (breakdown, flow-matrix nodes and
// edges, both timeseries reads, quadrant metric rows); the statements are unchanged, digests copied from the hosted
// TestScanTypeCensus failure output (job 135986472250), not computed.
// CHAOS-14271: two pins re-pinned by hand, digests copied from the local TestScanTypeCensus failure output (not computed).
// executeSankeyGrouped's statement gains, on the health path, a post-aggregation condition per node set (a NULL node key is
// dropped, as the edge sets already did); its scan targets are unchanged. scanMetricRows leaves a row with a NULL value
// out after the scan (no point, never a 0); its destinations are the CHAOS-14218 ones:
//
//	executeSankeyGrouped 02426fb74c03 -> 06df22123891
//	scanMetricRows 261210b712cd -> 9b55a7ceff60
//
// CHAOS-14266 on the merge with CHAOS-14258 (total order at the workflow cut): repetitiveStatement keeps the CHAOS-14258 ORDER BY
// and our cluster_anchor column (scan +*uint32); re-pinned by hand, digests copied from the local TestScanTypeCensus failure
// output (not computed), the old values being the CHAOS-14258 pins:
//
//	loadClusters (repetitive) 6782179e5bab -> 02e42d9124bc
//	loadClusters (title toil) 6782179e5bab -> 02e42d9124bc
//	loadDocDrift 9d9f4e10b7aa -> 74e03d28cf3f
//	loadFlaky e2c82e8e8355 -> 7740a9c9e213
//
// CHAOS-14281 (health TEAM key projection): the SELECT names the empty team "unassigned" and GROUP BY / ORDER BY keep the
// stored column; scan targets are unchanged in type (a String key, a Float64 value). Re-pinned by hand, digests copied
// from the local TestScanTypeCensus failure output (not computed):
//
//	executeBreakdownRaw e6927e7d3bab -> 7d5055461009
//	ExecuteOrgTimeseries 7a0e4208d61e -> 06967e304c8f
//	ExecuteTimeseries 5ed46b8e987a -> d06318794cbb
//
// CHAOS-14294 (the author is no input to the served order): repetitiveStatement no longer selects cluster_author (a String
// coalesce, scanned into *string and never read) and orders by cluster_anchor as its last key (after cluster_work_type); the author expression stays in
// GROUP BY only. The scan loses one *string; cluster_work_type (String) and cluster_anchor (UInt32, not Nullable) keep
// their targets. Re-pinned by hand, digests copied from the local TestScanTypeCensus failure output (not computed):
//
//	loadClusters (repetitive) 02e42d9124bc -> 3306241167e7, scan -*string
//	loadClusters (title toil) 02e42d9124bc -> 3306241167e7
//	loadDocDrift 74e03d28cf3f -> 018c6639543c
//	loadFlaky 7740a9c9e213 -> c0315233e3ef
//
// CHAOS-14285: the repetitive and title-toil reads read git_pull_requests FINAL (and work_graph_issue_pr FINAL), and the
// repetitive count is uniqExact(pr.number) with groupUniqArray refs: UInt64 and Array(String) as before, so the scan
// targets are unchanged; the file's SQL digest moved for every site. Re-pinned by hand on the merge with CHAOS-14294,
// digests copied from the local TestScanTypeCensus failure output (not computed), the old values being the CHAOS-14294 pins:
//
//	loadClusters (repetitive) 3306241167e7 -> 70a278016bab
//	loadClusters (title toil) 3306241167e7 -> 70a278016bab
//	loadDocDrift 018c6639543c -> cb6682629840
//	loadFlaky c0315233e3ef -> 965f0e7bc997
//
// CHAOS-14286: flakyStatement orders the newest-row pick by computed_at DESC, cityHash64(tuple(*)) DESC (a total order for
// two versions of one day in one second). The selected columns, their types and the scan targets are unchanged; the file's
// SQL digest moved for every site. Re-pinned by hand, digests copied from the local TestScanTypeCensus failure output (not computed):
//
//	loadClusters (repetitive) 70a278016bab -> 87d2d1cf0eaf
//	loadClusters (title toil) 70a278016bab -> 87d2d1cf0eaf
//	loadDocDrift cb6682629840 -> 598c61985afe
//	loadFlaky 965f0e7bc997 -> 2673b9f034d0
//
// CHAOS-14351: the repo-flow and change-failure newest-row picks order by computed_at DESC, cityHash64(tuple(*)) DESC
// (a total order for two versions of one day in one second). The selected columns, their types and the scan targets are
// unchanged; the file's SQL digest moved. Re-pinned by hand, digests copied from the local TestScanTypeCensus failure output (not computed):
//
//	loadRepoFlowRows 39a7e63ddf0e -> d2aa188ecd2d
//	loadTeamFlowRows 2653b84e5b21 -> 7f2f10dcf329
//	fetchChangeFailureAgg 73d54a21b859 -> 78aa9a6d5455
//
// CHAOS-14355: the argMax(x, computed_at) reads of aianalytics pick ONE version per key, ordered by (computed_at,
// cityHash64(tuple(values))) (loadImpactRowsForOpportunities in ai_opportunities.go; dailyStatement and
// concentrationStatement in loaders.go). The statements of the four pinned sites below are not edited; the SQL digest of
// their file moved. Their scan targets and column types are unchanged. Re-pinned by hand, digests copied from the local
// TestScanTypeCensus failure output (not computed):
//
//	loadClusters (repetitive) 87d2d1cf0eaf -> e67003e26fc2
//	loadClusters (title toil) 87d2d1cf0eaf -> e67003e26fc2
//	loadDocDrift 598c61985afe -> 3c8b7092e723
//	loadFlaky 2673b9f034d0 -> 9c8219b2ccb8
//	loadLabels ffbc61d0410c -> 9df0068c57ab
//
// CHAOS-14355 (operating review): the argMax(x, computed_at) period reads pick ONE version per key, ordered by
// (computed_at, cityHash64(tuple(values))), and test liveness with liverow.Predicate on the picked row in an outer WHERE
// (no per-column HAVING). fetchWorkItems and fetchStateDurations: same select list, column types and scan targets; the
// statement text changed. fetchChangeFailureAgg: statement not edited, the file's SQL digest moved. Re-pinned by hand,
// digests copied from the local TestScanTypeCensus failure output (not computed):
//
//	fetchChangeFailureAgg 78aa9a6d5455 -> 33e78a54a14c
//	fetchStateDurations 4e40f48a6488 -> dd682a464928
//	fetchWorkItems 02d2157b4812 -> 6adb8910eb84
//
// CHAOS-14355 (Home signals): the compounding-risk read picks ONE version per key, ordered by (computed_at,
// cityHash64(tuple(values))) in both the day picker and the served rows, and tests liveness with liverow.Predicate on
// the picked row in an outer WHERE (no per-column HAVING). The select list, the column types and the scan targets of
// fetchRiskSignals are unchanged (the tuple keeps elements 1..11 at their indexes); the statement text changed and the
// site stays not-describable. Re-pinned by hand, digest copied from the local TestScanTypeCensus failure output (not
// computed):
//
//	fetchRiskSignals 54de9ad8afd6 -> 06cb489933a0
//
// CHAOS-14395: the TEAM and REPO flow-matrix templates filter blank team ids with match() instead of an empty-string test. Select lists,
// column types and scan targets are unchanged; only the statement text moved. Re-pinned by hand, digests copied from the
// local TestScanTypeCensus failure output (not computed):
//
//	queryEdges 5935396ef678 -> 7cdf790f21f3
//	queryNodes 3dacd49c8128 -> 3c13f441d675
//
// CHAOS-14393: the WORK_TYPE nodes and edges templates filter blank types with match() instead of an empty-string test. Select lists,
// column types and scan targets are unchanged; only the statement text moved. Re-pinned by hand, digests copied from the
// local TestScanTypeCensus failure output (not computed):
//
//	queryEdges 5935396ef678 -> d6369bb81568
//	queryNodes 3dacd49c8128 -> 92e198fc2e0f
//
// With both the CHAOS-14395 and the CHAOS-14393 filters on the same file the pinned digests are (copied from the failing
// TestScanTypeCensus run at the merged tree):
//
//	queryEdges 7cdf790f21f3 -> f32e4fde7205
//	queryNodes 3c13f441d675 -> 3d7e7d56784d
const unverifiedSites = `
internal/queryapi/aggflame/clickhouse.go|fetchCycleBreakdown|*string,*float64,*uint64|a0d30b3aefa3|not-describable
internal/queryapi/aianalytics/ai_opportunities.go|loadClusters|*string,*string,*uint64,*[]string,*string,*uint32|e67003e26fc2|not-describable
internal/queryapi/aianalytics/ai_opportunities.go|loadClusters|*string,*uint64,*[]string|e67003e26fc2|not-describable
internal/queryapi/aianalytics/ai_opportunities.go|loadDocDrift|*string,*uint64,*uint64|3c8b7092e723|not-describable
internal/queryapi/aianalytics/ai_opportunities.go|loadFlaky|*string,*uint64,**float64|9c8219b2ccb8|not-describable
internal/queryapi/aianalytics/flow_opportunities.go|loadRepoFlowRows|*string,*uint64,**float64,**float64,**float64,**float64|d2aa188ecd2d|not-describable
internal/queryapi/aianalytics/flow_opportunities.go|loadTeamFlowRows|*string,*uint64,**float64,**float64,**float64|7f2f10dcf329|not-describable
internal/queryapi/aianalytics/loaders.go|loadLabels|*string,*string|9df0068c57ab|spliced-select-list
internal/queryapi/aianalytics/scope.go|ownerTeams|*string,*string|c0b3734eb92d|not-describable
internal/queryapi/analytics/breakdown.go|executeBreakdownRaw|**string,**float64|7d5055461009|statement-not-static
internal/queryapi/analytics/catalog.go|executeCatalogValues|*string,*uint64|92f0953d0b7c|statement-not-static
internal/queryapi/analytics/flowmatrix.go|queryEdges|*string,*string,**string,**string,**float64|f32e4fde7205|statement-not-static
internal/queryapi/analytics/flowmatrix.go|queryNodes|*string,**string,**float64|3d7e7d56784d|statement-not-static
internal/queryapi/analytics/investmentquality.go|executeEvidenceQualityByGroup|*string,*float64,*uint64,*uint64|30049a8da29f|statement-not-static
internal/queryapi/analytics/investmentquality.go|executeInvestmentQualityStats|*uint64,*uint64,*float64,*float64,*uint64,*uint64,*uint64,*uint64,*uint64,*uint64,*uint64|2219e7f838c7|statement-not-static
internal/queryapi/analytics/sankeycoverage.go|runSankeyCoverageQuery|*float64,*float64,*float64,*float64,**float64,**float64,**float64,**float64,**float64|7474017a0a07|statement-not-static
internal/queryapi/analytics/sankeygrouped.go|executeSankeyGrouped|[]any|06df22123891|variadic-destinations
internal/queryapi/analytics/timeseries.go|ExecuteOrgTimeseries|**time.Time,**float64,**float64|06967e304c8f|statement-not-static
internal/queryapi/analytics/timeseries.go|ExecuteTimeseries|**time.Time,**string,**float64|d06318794cbb|statement-not-static
internal/queryapi/capacityforecast/clickhouse.go|loadBacklog|*uint64|26ee34e4be86|not-describable
internal/queryapi/capacityforecast/clickhouse.go|loadThroughput|*time.Time,*uint64|d73049462fd5|not-describable
internal/queryapi/cognitiveload/cognitiveload.go|fetchOwnershipCandidates|*string,*string,*uint8,*uint16|e598129679a3|spliced-select-list
internal/queryapi/complexitytimeseries/complexitytimeseries.go|fetchFileTimeseries|*time.Time,*string,*string,**uint32,**float64,**uint32,**uint32|69bcb13af3df|spliced-select-list
internal/queryapi/complexitytimeseries/complexitytimeseries.go|fetchRepoTimeseries|*time.Time,*string,**uint64,**uint64,**float64,**uint64,**uint64|2d500256c88b|spliced-select-list
internal/queryapi/compoundingrisk/read.go|latestDay|**time.Time|818fa20627a4|not-describable
internal/queryapi/compoundingrisk/read.go|latestRows|*string,**float64,*string,**float64,**float64,**float64,**float64,**float64,**float64,**float64,**float64,**float64,**float64,*float64,*float64,*float64,*float64,*float64,*float64,*time.Time|9af7066815e3|not-describable
internal/queryapi/compoundingrisk/read.go|repoTrend|*time.Time,**float64|e90c541be45a|not-describable
internal/queryapi/datahealth/connectors.go|Connectors|*string,*string,*[]byte,**time.Time,**bool,**string,*[]byte,**time.Time,**int32,**time.Time,**time.Time,*[]byte,**string|d9838116a756|not-describable
internal/queryapi/datahealth/coverage.go|MetricLineage|**time.Time,*uint64|bb9897d55705|not-describable
internal/queryapi/datahealth/sourcehealth.go|SourceHealth|*string,**string,*bool,**time.Time,*bool,*string,*[]byte,**time.Time,**bool,*bool,**time.Time,**string,*bool,**time.Time,**time.Time,**string,**string|dabf0d087730|not-describable
internal/queryapi/drilldown/issues.go|BuildIssuesResponse|*string,*string,*string,**string,**string,**float64,**float64,**time.Time,**time.Time|efc62e04415b|not-describable
internal/queryapi/drilldown/issues.go|buildBlockedIssuesResponse|*string,*string,**string,**string,*uint64|0df7b140bb0b|not-describable
internal/queryapi/explain/metrics.go|fetchChangeFailureView|[]any|74d73326912d|variadic-destinations
internal/queryapi/explain/metrics.go|fetchMetricContributors|*string,**float64|06f0dd79850a|spliced-select-list
internal/queryapi/explain/metrics.go|fetchMetricDriverDelta|*string,**float64,*uint8,*uint8,**float64|696073003934|not-describable
internal/queryapi/explain/metrics.go|fetchMetricValue|**float64,*uint64|67264c7b19cb|spliced-select-list
internal/queryapi/explain/repositories.go|fetchWorkItemProviders|*string|cc6f1e4e4f8c|not-describable
internal/queryapi/home/queries_metric_drivers_batch.go|fetchMetricDriversBatch|*string,*string|61d29f14b0f2|not-describable
internal/queryapi/home/queries_metrics.go|fetchBlockedHours|*time.Time,*float64|626f0d2551e1|not-describable
internal/queryapi/home/queries_metrics.go|fetchChangeFailureView|[]any|cf1d07948519|variadic-destinations
internal/queryapi/home/queries_metrics.go|fetchMetricDriverDelta|*string,**float64,**float64|66779bcf8e59|not-describable
internal/queryapi/home/queries_metrics.go|fetchMetricSeries|*time.Time,**float64|8fb98d9737e7|spliced-select-list
internal/queryapi/home/queries_metrics.go|fetchMetricValue|*int64,**float64|dc4e9ea6b024|spliced-select-list
internal/queryapi/home/queries_metrics.go|fetchPRReworkView|[]any|edd1ff81c902|variadic-destinations
internal/queryapi/home/queries_metrics.go|fetchRateDriverDelta|*string,**float64,**float64|b707a8dcf55e|not-describable
internal/queryapi/home/queries_signals.go|fetchRiskSignals|*string,*string,**float64,*string,*time.Time,**float64|06cb489933a0|not-describable
internal/queryapi/home/queries_sync.go|FetchLatestSuccessfulSyncAt|**time.Time|9dfe899f0474|statement-not-static
internal/queryapi/home/repolinked.go|readBasis|*string,*string,*uint8|ed45fd9e4470|spliced-select-list
internal/queryapi/hotspots/hotspots.go|queryHotspotRows|*string,*string,*uint64,*uint32,*uint32,*float64,**float64,*float64|448efde8f6df|not-describable
internal/queryapi/investment/qualitystats.go|FetchInvestmentQualityStats|*uint64,*uint64,*float64,*float64,*uint64,*uint64,*uint64,*uint64,*uint64,*uint64,*uint64|fa80d64b4a5b|not-describable
internal/queryapi/investment/sunburst.go|FetchInvestmentSunburst|*string,*string,*string,*float64|9c36b6f1ad4c|spliced-select-list
internal/queryapi/investmentexplain/reader.go|FetchInvestmentBreakdown|*string,*string,*float64|2283428d08a0|spliced-select-list
internal/queryapi/investmentexplain/reader.go|FetchInvestmentThemeEffort|*string,*float64,*float64,*float64|fd01dffa31fe|spliced-select-list
internal/queryapi/investmentexplain/workunitreader.go|FetchWorkUnitInvestments|*string,**string,**string,*time.Time,*time.Time,**string,**string,**string,**float64,*float64,*[]string,*[]float64,*[]string,*[]float64,**string,**float64,**string,**string,**string,**string,*time.Time|63286cc9b046|spliced-select-list
internal/queryapi/operatingreview/operatingreview.go|fetchChangeFailureAgg|[]any|33e78a54a14c|variadic-destinations
internal/queryapi/operatingreview/operatingreview.go|fetchStateDurations|*string,*uint64,*float64,*float64|dd682a464928|not-describable
internal/queryapi/operatingreview/operatingreview.go|fetchWorkItems|*time.Time,*uint64,*uint64,*uint32,**float64,**float64,**float64,**float64|6adb8910eb84|not-describable
internal/queryapi/people/metricqueries.go|fetchPersonBreakdown|*string,*float64|f158cf132a83|spliced-select-list
internal/queryapi/people/metricqueries.go|fetchPersonMetricSeries|*time.Time,*float64|aaa0e43ef5ee|spliced-select-list
internal/queryapi/people/metricqueries.go|fetchPersonMetricValue|*uint64,*float64|44f613075ca4|spliced-select-list
internal/queryapi/people/search.go|BuildSearchResponse|*string,*time.Time|afd45e38cfbd|not-describable
internal/queryapi/people/summary.go|fetchPersonCollaboration|*string,*string,*float64|be7c30414391|not-describable
internal/queryapi/pooledmttr/pooledmttr.go|scorecard|*string,**string,**float64|3a0c54cda9e6|spliced-select-list
internal/queryapi/producttelemetry/platform.go|LoadOrgNames|*string,*string,*string|64c4ca805d17|not-describable
internal/queryapi/quadrant/quadrant.go|scanMetricRows|**time.Time,**string,**string,**float64|9b55a7ceff60|statement-not-static
internal/queryapi/reports/reports.go|scanCount|*int64|8d2d39db7d2b|not-describable
internal/queryapi/reports/reports.go|scanReportRun|*string,*string,*string,**time.Time,**time.Time,**float64,**string,**string,**string,**string,*string,*time.Time|0121a67bb651|not-describable
internal/queryapi/reports/reports.go|scanSavedReport|*string,*string,*string,**string,**string,*bool,**string,**string,**string,*bool,**time.Time,**string,*time.Time,*time.Time,**string|835998e4481b|not-describable
internal/queryapi/reports/writer.go|clone|*string,**string,**string,**string,**string|bd5216bf923e|statement-not-static
internal/queryapi/reports/writer.go|trigger|*bool|a8202561e27c|statement-not-static
internal/queryapi/reports/writer.go|update|*string,*string,*string,**string,**time.Time,*time.Time|02fbaf29d245|statement-not-static
internal/queryapi/routeswitch/class_decision_switch.go|Enabled|*string|7a40513e2fab|not-describable
internal/queryapi/sankey/queries.go|fetchExpenseCounts|*float64,*float64,*float64|d97047e82483|not-describable
internal/queryapi/sankey/queries.go|fetchStateStatusCounts|*string,*float64|d810cfa8eeb7|not-describable
internal/queryapi/server/mcp_route.go|Scan|[]any|073743d6899c|variadic-destinations
internal/queryapi/testopsrisk/testopsrisk.go|readDaily|**time.Time,**float64,**float64,**float64,**float64,**float64,**float64,**uint64,**float64|2072b04aaa64|not-describable
internal/queryapi/throughputforecast/clickhouse.go|loadBacklog|*uint64,*uint64,*uint64,*time.Time|2d4d80074e2b|not-describable
internal/queryapi/throughputforecast/clickhouse.go|loadStaleWIP|**float64,**float64|3dd402e56d1e|not-describable
internal/queryapi/throughputforecast/clickhouse.go|loadThroughputHistory|*time.Time,*uint64|a930dc834fd5|not-describable
internal/queryapi/throughputforecast/clickhouse.go|loadWorkItemOverlay|*float64,*uint64|740708b6aca9|not-describable
internal/queryapi/workgraph/edges.go|countMatchingEdges|*uint64|8d8175642fa2|not-describable
internal/queryapi/workgraph/edges.go|fetchDedupedEdgeRows|*string,*string,*string,*string,*string,*string,*string,*string,*string,*float64,*string|9ec6525b760f|statement-not-static
internal/queryapi/workgraph/edges.go|queryDependencyEdges|*string,*string,*string,*string,*string,*string,*string,*string,*string,*float64,*string|c998ded583fc|statement-not-static
internal/queryapi/workgraph/membership.go|batchResolveMembership|*string,*string,*string,*string|2e7589312710|not-describable
internal/queryapi/workgraph/scope.go|detectMembershipDegradedReason|*uint64,*uint64|e91eab4f4df6|spliced-select-list
internal/queryapi/workgraph/teamattribution.go|resolveWorkUnitTeamAttributions|*string,*string,*string,*string,*string,*uint64|9cc5588be558|not-describable
`
