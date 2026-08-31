//go:build integration

// CHAOS-4506 / CHAOS-4534 NaN-class proof, GO side.
//
// STATUS: WRITTEN, NOT YET EXECUTED. Discretionary per orchestrator
// ruling 2026-08-29 -- the slot's mandatory items are the 3 flow-matrix
// dual-run tests (test_go_api_dual_run_flow_matrix.py) and the full
// `ci/local_validate.sh` gate; this proof runs only if the slot is still
// open after those are green, and is fine to leave written-but-unexecuted
// if it is not (deploy56 already read prod and found NO NaN reaching the
// wire across 538 executions, so its value is documentation of a latent
// class, not a blocker).
//
// COMPANION FILE: tests/api/graphql/test_nan_class_breakdown_python_live.py
// proves the PYTHON side against the SAME measure/shape. Together they
// are the split (populated proves the port / empty documents the
// divergence) the standing NaN-class ruling requires -- neither file
// alone is the proof.
//
// CHAOS-4643: this file used to be enrolled in ci/go_integration_shards.tsv
// even though .github/workflows/go.yml never sets CLICKHOUSE_URI for the
// integration-shard job -- CI ran the package, the guard below fired on
// every run, and the resulting skip reported as a pass, silently, forever.
// It is now denylisted out of that shard (see INTEGRATION_DENYLIST in
// ci/check_go.sh) instead of enrolled-but-perpetually-skipping: the STATUS
// note above already says this proof is discretionary and slot-only, so an
// enrolment CI could never satisfy was actively misleading, not merely
// unused. A missing CLICKHOUSE_URI now only skips when nanClassClickHouseURI
// is called without DEV_HEALTH_REQUIRE_LIVE=1; set that alongside
// CLICKHOUSE_URI when running this as an actual slot proof and a still-missing
// URI fails loudly instead. The rebuilt DSN also used to drop the URI's
// userinfo, so even a correct slot run with real credentials failed
// ClickHouse auth -- nanClassClickHouseURI now carries parsed.User through.
//
// WHY NOT ROUTED THROUGH HTTP, UNLIKE THE FLOW-MATRIX DUAL-RUN FILE:
// breakdown has NO registered document yet -- query_route.go
// (digestByOperation, ~L478) only registers flowMatrix this wave. Both
// real production documents that select breakdown
// (INVESTMENT_BREAKDOWN_QUERY / INVESTMENT_FULL_QUERY) send
// `useInvestment: true` unconditionally and are blocked on CHAOS-4538.
// There is no live entry point to dual-run against, so this exercises
// CompileBreakdown's real dbExpression() -> a live ClickHouse round trip
// -> the real gqlgen marshal call DIRECTLY, the same chain a routed
// request would use once one exists.
//
// TWO SEPARATE, REAL MECHANISMS PROVEN HERE, NOT ONE -- UPDATED BY
// CHAOS-4643/CHAOS-4650: the FIRST live run of this file (CHAOS-4643 was
// what made a live run possible at all) falsified mechanism 1 below as
// originally written. It is left here, corrected, as the record of what
// was assumed and what running it actually found -- see
// TestNaNClass_AllNullGroup_ReturnsSQLNullScannedAsZero_NotNaN for the
// current, executed mechanism and CHAOS-4650 (Urgent) for the resulting
// defect, which this file does not fix:
//  1. CORRECTED: ClickHouse's own engine (26.7.5.10, matching prod's
//     engine major version) does NOT return NaN for AVG() over a
//     Nullable(Float64) group whose rows are ALL NULL -- it returns SQL
//     NULL, which dev-health-go's client then silently scans into a
//     non-pointer float64 as 0.0. The class validate.go:184-201 still
//     enumerates as category 2 ("AT RISK") is real, but the actual risk
//     is a silent 0.0, not a marshal error -- tracked as CHAOS-4650.
//  2. gqlgen's own library: graphql.MarshalFloatContext (the exact
//     function generated.go:91369/94794 call for every non-nullable
//     Float! field, BreakdownItem.value included --
//     contracts/graphql/v1/schema.graphql:386) refuses to write a NaN,
//     returning "cannot marshal infinite no NaN float values"
//     (gqlgen's float.go:38-45, a stock library check, nothing this
//     port added) -- true, but per (1) above, this mechanism never fires
//     for the all-NULL coverage shape on this engine, because the value
//     it would refuse never arrives as NaN in the first place.
//
// WHY A SYNTHETIC SUBQUERY, NOT AN INSERT INTO THE REAL TABLE: the
// QueryClient this package uses (clickhouse.Client.Query, dev-health-go
// v0.4.0) enforces validateReadOnlyStatement -- first token must be
// SELECT, no ";" -- by design (see CLIENT STATEMENT GUARD in this lane's
// brief); it cannot issue an INSERT at all, on purpose. The claim under
// test here is ClickHouse's OWN aggregate semantics over a Nullable
// column shape, which is independent of which table the column lives in
// -- validate.go's own category-2 comment cites the DDL's nullability,
// not any table's content, as the property this depends on. So this
// test wraps the REAL dbExpression() output (the literal string the port
// emits, called live, not hand-copied) around a synthetic two-row
// UNION ALL standing in for "the pipeline ran, the column came back
// NULL both times" -- a materialised repro of the exact shape, executed
// read-only, without a producer-derived table insert. This is a
// narrower claim than the flow-matrix file's row-level cross-plane
// parity proofs (which DO require producer-derived fixtures per root
// AGENTS.md) -- this file compares no rows between planes; it verifies
// one SQL-engine fact and one marshal-library fact, each independently,
// each executed for real.
package analytics

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/99designs/gqlgen/graphql"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// requireLiveEnv is the single opt-in that turns a missing CLICKHOUSE_URI
// from a silent skip into a loud failure. Whoever runs this file as a slot
// proof sets it; CI's deterministic integration shard never does, because
// this package is not enrolled there -- see ci/go_integration_shards.tsv's
// INTEGRATION_DENYLIST entry and this file's own header. Deliberately the
// SAME variable both tests below funnel through nanClassClickHouseURI,
// rather than a second, easily-forgotten flag.
const requireLiveEnv = "DEV_HEALTH_REQUIRE_LIVE"

// nanClassClickHouseURI mirrors the Python dual-run harness's
// _go_clickhouse_uri (test_go_api_dual_run_flow_matrix.py:426-431):
// CLICKHOUSE_URI as set for the slot's Python-side stages is typically
// clickhouse-connect's HTTP-port form (:8123); this repo's Go side
// requires the native protocol port (:9000, TestBuildQueryRoute_FailsFastOnWrongClickHouseProtocol's
// regression). Doing the same translation here lets this test run off
// the SAME env var value the slot already exports, rather than requiring
// a second, easily-forgotten one.
func nanClassClickHouseURI(t testing.TB) string {
	t.Helper()
	raw := os.Getenv("CLICKHOUSE_URI")
	if raw == "" {
		if os.Getenv(requireLiveEnv) == "1" {
			t.Fatalf("CLICKHOUSE_URI not set but %s=1 -- this is a live-ClickHouse proof and the "+
				"caller required it to actually run; a slot run that silently skips is exactly the "+
				"false-pass this flag exists to prevent (CHAOS-4643)", requireLiveEnv)
		}
		t.Skip("CLICKHOUSE_URI not set -- this is a live-ClickHouse proof, run only in a container " +
			"slot; set " + requireLiveEnv + "=1 alongside CLICKHOUSE_URI to make a missing URI fail " +
			"instead of skip")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse CLICKHOUSE_URI: %v", err)
	}
	host := parsed.Hostname()
	port := parsed.Port()
	if port == "8123" || port == "8443" {
		port = "9000"
	} else if port == "" {
		port = "9000"
	}
	scheme := parsed.Scheme
	if scheme == "" || scheme == "http" || scheme == "https" || scheme == "clickhouse+http" {
		scheme = "clickhouse"
	}
	// Carry the userinfo through the rebuild (CHAOS-4643 defect 1): a
	// credentialed CLICKHOUSE_URI (clickhouse://user:pass@host:8123/db)
	// used to lose "user:pass" here, so even a correct slot run with a
	// real password failed ClickHouse auth. parsed.User.String() returns
	// the same percent-encoded "user[:pass]" form url.URL itself accepts
	// back in, matching how the Python harness's string-replace on netloc
	// (which never touches the userinfo segment) already preserves it.
	//
	// net.JoinHostPort, not a bare "%s:%s" -- codex review round 1 caught
	// that Hostname() strips IPv6 brackets ("::1", not "[::1]"), so a raw
	// Sprintf produced an unparseable "host:port" for any IPv6 CLICKHOUSE_URI
	// (clickhouse://u:p@[::1]:8123/db rebuilt as ...@::1:9000, ambiguous
	// with the port). JoinHostPort re-adds the brackets exactly when the
	// host needs them and is a no-op for ordinary hostnames/IPv4.
	hostPort := net.JoinHostPort(host, port)
	if parsed.User != nil {
		return fmt.Sprintf("%s://%s@%s", scheme, parsed.User.String(), hostPort)
	}
	return fmt.Sprintf("%s://%s", scheme, hostPort)
}

// nanClassAllNullGroupSQL and nanClassPopulatedGroupSQL wrap the REAL
// dbExpression() output for MeasureCoverageLinePct (breakdown.go's own
// production function, called live -- not a hand-copied string) around a
// synthetic group standing in for testops_coverage_metrics_daily's
// line_coverage_pct column (Nullable(Float64), migrations/clickhouse/029_testops_tables.sql:158).
func nanClassQuerySQL(t *testing.T, allNull bool) string {
	t.Helper()
	measureExpr, err := dbExpression(MeasureCoverageLinePct, false, false)
	if err != nil {
		t.Fatalf("dbExpression(MeasureCoverageLinePct): %v", err)
	}
	measureExpr = "toFloat64(" + measureExpr + ")"

	var rows string
	if allNull {
		// Two rows exist (the pipeline ran) -- rules out the ALREADY
		// CLEARED zero-source-row shape (BRIEF's "NaN UPDATE": a GROUP
		// BY over zero rows yields zero result rows, no NaN reaches
		// anywhere) -- but the averaged column is NULL in both.
		rows = "SELECT CAST(NULL AS Nullable(Float64)) AS line_coverage_pct " +
			"UNION ALL SELECT CAST(NULL AS Nullable(Float64)) AS line_coverage_pct"
	} else {
		// Pinned values, not arbitrary ones, so the populated assertion
		// below can check the MECHANISM (dbExpression actually reached
		// this column) rather than merely "not NaN".
		rows = "SELECT CAST(80.0 AS Nullable(Float64)) AS line_coverage_pct " +
			"UNION ALL SELECT CAST(90.0 AS Nullable(Float64)) AS line_coverage_pct"
	}

	return fmt.Sprintf(
		"SELECT %s AS value FROM (%s) AS testops_coverage_metrics_daily",
		measureExpr, rows,
	)
}

func nanClassRunScalarQuery(t *testing.T, ctx context.Context, client *dhclickhouse.Client, sql string) float64 {
	t.Helper()
	rows, err := client.Query(ctx, sql, nil)
	if err != nil {
		t.Fatalf("query failed (statement guard rejection or live ClickHouse error): %v\nsql=%s", err, sql)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("expected exactly one result row, got zero -- the synthetic group did not materialise; check ClickHouse accepted the UNION ALL shape.\nsql=%s", sql)
	}
	var value float64
	if err := rows.Scan(&value); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if rows.Next() {
		t.Fatalf("expected exactly one result row, got more than one -- the GROUP BY did not collapse to a single group as this test assumes.\nsql=%s", sql)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return value
}

// TestNaNClass_PopulatedGroup_ReturnsRealValue_MarshalsCleanly is the
// parity half of the split: a normal, fully-populated group proves the
// port's own dbExpression() reaches the right column and produces the
// pinned average, and that gqlgen's marshaler writes it with no error --
// establishes the mechanism works before using it to observe the
// divergence below (same "prove the harness first" discipline as the
// flow-matrix file's test 1).
func TestNaNClass_PopulatedGroup_ReturnsRealValue_MarshalsCleanly(t *testing.T) {
	dsn := nanClassClickHouseURI(t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("connect to ClickHouse at %s: %v", dsn, err)
	}
	ctx := context.Background()

	value := nanClassRunScalarQuery(t, ctx, client, nanClassQuerySQL(t, false))

	if math.IsNaN(value) {
		t.Fatalf("populated group unexpectedly produced NaN")
	}
	const wantAvg = 85.0 // AVG(80.0, 90.0)
	if value != wantAvg {
		t.Fatalf("expected the pinned average of the two seeded rows (80.0, 90.0) = %v, got %v -- "+
			"a wrong value here means dbExpression's output did not reach the column this test believes it is exercising",
			wantAvg, value)
	}

	var buf bytes.Buffer
	if err := graphql.MarshalFloatContext(value).MarshalGQLContext(ctx, &buf); err != nil {
		t.Fatalf("expected a populated value to marshal cleanly via the SAME gqlgen call the real response path uses, got error: %v", err)
	}
	if got := strings.TrimSpace(buf.String()); got != "85" {
		t.Fatalf("expected the marshaled body to be the literal number 85, got %q", got)
	}
}

// nanClassRawAllNullExpr returns the REAL dbExpression() output
// (unwrapped -- no toFloat64 cast) over the SAME all-NULL synthetic group
// nanClassQuerySQL(t, true) uses, so TestNaNClass_AllNullGroup below can
// observe what ClickHouse's own Nullable(Float64) result column carries
// BEFORE any Go-side cast or scan touches it.
func nanClassRawAllNullExpr(t *testing.T) string {
	t.Helper()
	measureExpr, err := dbExpression(MeasureCoverageLinePct, false, false)
	if err != nil {
		t.Fatalf("dbExpression(MeasureCoverageLinePct): %v", err)
	}
	rows := "SELECT CAST(NULL AS Nullable(Float64)) AS line_coverage_pct " +
		"UNION ALL SELECT CAST(NULL AS Nullable(Float64)) AS line_coverage_pct"
	return fmt.Sprintf(
		"SELECT %s AS value FROM (%s) AS testops_coverage_metrics_daily",
		measureExpr, rows,
	)
}

// TestNaNClass_AllNullGroup_ReturnsSQLNullScannedAsZero_NotNaN pins a
// MECHANISM, not the outcome CHAOS-4506/4534's design assumed. This file's
// original claim -- "AVG() over an all-NULL Nullable(Float64) group
// returns NaN, not NULL and not 0" -- was written but never executed
// (this file's own STATUS header said so) until CHAOS-4643 made a live
// run possible. Running it for the first time, against ClickHouse
// 26.7.5.10 (matching prod's engine major version), FALSIFIED that claim:
//
//  1. ClickHouse's own avg() over an all-NULL Nullable(Float64) group
//     returns SQL NULL, not NaN -- verified below by scanning the RAW
//     (unwrapped) dbExpression() output into a *float64, where a true SQL
//     NULL surfaces as nil.
//  2. dev-health-go's Client.Query then scans that SQL NULL into a
//     non-pointer float64 -- the exact shape ExecuteBreakdown's real call
//     site uses -- as the Go zero value, 0.0, silently, with no error.
//
// CHAOS-4650 (Urgent) TRACKS THIS AS A DEFECT, NOT THE INTENDED CONTRACT:
// an all-NULL coverage group ("no data" -- the pipeline ran, nothing was
// measured) reaches the wire today as a plain, indistinguishable 0.0
// ("real zero coverage"), silently -- worse than the NaN-marshal-error
// CHAOS-4506/4534 was built to catch, because NaN at least announces
// itself by breaking serialization. This test does not fix that; it pins
// today's mechanism so that if the engine starts returning NaN, or the
// client starts erroring on a NULL-into-non-pointer scan, THIS test fails
// and forces a re-read of this decision -- rather than the divergence
// staying silent a second time. Do not read a passing run as "the
// behavior is fine"; read it as "the mechanism is still what CHAOS-4650
// describes".
func TestNaNClass_AllNullGroup_ReturnsSQLNullScannedAsZero_NotNaN(t *testing.T) {
	dsn := nanClassClickHouseURI(t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("connect to ClickHouse at %s: %v", dsn, err)
	}
	ctx := context.Background()

	// Mechanism half 1: the engine's own result IS a SQL NULL, not NaN.
	rawSQL := nanClassRawAllNullExpr(t)
	rawRows, err := client.Query(ctx, rawSQL, nil)
	if err != nil {
		t.Fatalf("query failed (statement guard rejection or live ClickHouse error): %v\nsql=%s", err, rawSQL)
	}
	defer rawRows.Close()
	if !rawRows.Next() {
		t.Fatalf("expected exactly one result row, got zero.\nsql=%s", rawSQL)
	}
	var rawValue *float64
	if err := rawRows.Scan(&rawValue); err != nil {
		t.Fatalf("scan into *float64: %v", err)
	}
	if rawRows.Next() {
		t.Fatalf("expected exactly one result row, got more than one.\nsql=%s", rawSQL)
	}
	if err := rawRows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if rawValue != nil {
		t.Fatalf("expected the raw dbExpression() AVG() over an all-NULL group to be SQL NULL "+
			"(scanned as nil via *float64), got %v -- if ClickHouse now returns NaN here instead, "+
			"CHAOS-4650 may be resolved upstream, but do not just update this assertion: re-derive "+
			"the disposition on CHAOS-4650 first, since the 0.0-coercion mechanism below would also "+
			"need re-checking", *rawValue)
	}

	// Mechanism half 2: the SAME query, scanned into the non-pointer
	// float64 shape ExecuteBreakdown's real call site uses (via
	// nanClassRunScalarQuery / nanClassQuerySQL's toFloat64 wrap), becomes
	// the Go zero value, silently.
	scannedValue := nanClassRunScalarQuery(t, ctx, client, nanClassQuerySQL(t, true))
	if math.IsNaN(scannedValue) {
		t.Fatalf("got NaN from the non-pointer scan -- that was the ORIGINAL (falsified) claim; if the " +
			"engine now genuinely returns NaN, the rawValue assertion above should have failed FIRST -- " +
			"investigate that mismatch before treating this half alone as stale")
	}
	if scannedValue != 0 {
		t.Fatalf("expected the SQL-NULL-into-non-pointer-float64 scan to silently coerce to the Go "+
			"zero value 0, got %v -- the coercion mechanism CHAOS-4650 pins may have changed; "+
			"re-derive CHAOS-4650's disposition before updating this assertion", scannedValue)
	}

	// The SILENT half of CHAOS-4650, executed: this 0.0 marshals CLEANLY,
	// no error -- unlike the NaN case CHAOS-4506/4534 catches, nothing
	// here announces that "no data" just became a plausible zero on the
	// wire.
	var buf bytes.Buffer
	if err := graphql.MarshalFloatContext(scannedValue).MarshalGQLContext(ctx, &buf); err != nil {
		t.Fatalf("expected the coerced 0.0 to marshal cleanly -- that IS the silent half of CHAOS-4650, "+
			"nothing is supposed to error here today -- got an unexpected marshal error instead: %v", err)
	}
	if got := strings.TrimSpace(buf.String()); got != "0" {
		t.Fatalf("expected the marshaled body to be the literal number 0, got %q", got)
	}
}

// TestNanClassClickHouseURI_CarriesUserinfoThroughRebuild is CHAOS-4643
// defect 1's proof, and needs no container: it only exercises the pure
// string-rebuild in nanClassClickHouseURI, never dials ClickHouse. Before
// the fix, the rebuilt DSN dropped parsed.User entirely -- a credentialed
// CLICKHOUSE_URI became uncredentialed, so even a correct slot run with a
// real password failed ClickHouse auth.
func TestNanClassClickHouseURI_CarriesUserinfoThroughRebuild(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "user_and_password_preserved_and_port_translated",
			raw:  "clickhouse://chuser:chpass@example.internal:8123/default",
			want: "clickhouse://chuser:chpass@example.internal:9000",
		},
		{
			name: "user_only_preserved",
			raw:  "clickhouse://chuser@example.internal:8443/default",
			want: "clickhouse://chuser@example.internal:9000",
		},
		{
			name: "no_userinfo_still_works",
			raw:  "clickhouse://example.internal:8123/default",
			want: "clickhouse://example.internal:9000",
		},
		{
			name: "http_scheme_and_native_port_pass_through",
			raw:  "http://chuser:chpass@example.internal:9000/default",
			want: "clickhouse://chuser:chpass@example.internal:9000",
		},
		{
			// codex review round 1 (EXECUTED): Hostname() strips the brackets
			// off an IPv6 literal ("::1", not "[::1]"), and a bare "%s:%s"
			// Sprintf never put them back, so the rebuilt DSN was ambiguous
			// (host and port both after the last ":"). net.JoinHostPort fixes
			// this by re-adding brackets exactly when the host needs them.
			name: "ipv6_host_gets_bracketed",
			raw:  "clickhouse://chuser:chpass@[::1]:8123/default",
			want: "clickhouse://chuser:chpass@[::1]:9000",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CLICKHOUSE_URI", tc.raw)
			t.Setenv(requireLiveEnv, "")
			got := nanClassClickHouseURI(t)
			if got != tc.want {
				t.Fatalf("nanClassClickHouseURI(%q) = %q, want %q -- credentials or host/port were "+
					"dropped or mangled in the rebuild", tc.raw, got, tc.want)
			}
		})
	}

	// codex review round 1 (EXECUTED mutation survivor): every case above
	// runs with DEV_HEALTH_REQUIRE_LIVE unset, so a mutant that made
	// nanClassClickHouseURI fail whenever DEV_HEALTH_REQUIRE_LIVE=1 --
	// regardless of whether CLICKHOUSE_URI was even set -- passed the whole
	// suite. DEV_HEALTH_REQUIRE_LIVE=1 must be a no-op once CLICKHOUSE_URI is
	// present; it only changes behavior on the MISSING-URI path (see
	// TestNanClassClickHouseURI_MissingURI_RequireLiveFailsInsteadOfSkipping).
	t.Run("require_live_with_uri_present_is_a_no_op", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_URI", "clickhouse://chuser:chpass@example.internal:8123/default")
		t.Setenv(requireLiveEnv, "1")
		got := nanClassClickHouseURI(t)
		want := "clickhouse://chuser:chpass@example.internal:9000"
		if got != want {
			t.Fatalf("nanClassClickHouseURI with CLICKHOUSE_URI set and %s=1 = %q, want %q -- "+
				"the require-live flag must only affect the missing-URI path", requireLiveEnv, got, want)
		}
	})
}

// fakeSkipFailTB is a minimal testing.TB that records whether the code under
// test called Skip or Fatal(f), instead of letting either call propagate into
// a REAL subtest failure. A genuine t.Run subtest failure always marks its
// parent failed too (Go's testing package does this unconditionally,
// independent of what the parent does with t.Run's returned bool), which
// would turn this exact proof -- deliberately exercising the FAIL path -- into
// a permanently red test. Embedding a real testing.TB satisfies its
// unexported method and is never otherwise called; every method this file
// uses is overridden below.
type fakeSkipFailTB struct {
	testing.TB
	mu      sync.Mutex
	failed  bool
	skipped bool
	msg     string
}

func (f *fakeSkipFailTB) Helper() {}

func (f *fakeSkipFailTB) Fatalf(format string, args ...any) {
	f.mu.Lock()
	f.failed = true
	f.msg = fmt.Sprintf(format, args...)
	f.mu.Unlock()
	runtime.Goexit()
}

func (f *fakeSkipFailTB) Skip(args ...any) {
	f.mu.Lock()
	f.skipped = true
	f.msg = fmt.Sprint(args...)
	f.mu.Unlock()
	runtime.Goexit()
}

// runNanClassClickHouseURI calls nanClassClickHouseURI(fake) on its own
// goroutine and waits for it: Skip/Fatalf call runtime.Goexit, which only
// unwinds the calling goroutine, so the call must run on one dedicated to it
// (exactly how the real testing package runs each (sub)test).
func runNanClassClickHouseURI(fake *fakeSkipFailTB) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		nanClassClickHouseURI(fake)
	}()
	<-done
}

// TestNanClassClickHouseURI_MissingURI_RequireLiveFailsInsteadOfSkipping is
// CHAOS-4643 defect 3's proof, and needs no container: DEV_HEALTH_REQUIRE_LIVE=1
// with CLICKHOUSE_URI unset must FAIL, not skip -- otherwise a slot run that
// failed to receive its ClickHouse URI reports the identical false pass this
// ticket exists to close, one level down inside the runs meant to prove
// parity. Also proves the ORIGINAL behaviour (skip, not fail) still holds
// when the opt-in is not set, so a normal unconfigured run stays a skip, not a
// surprise failure -- the pair is what distinguishes "the guard fires
// correctly either way" from "it always fails now", which would be a
// different, equally wrong bug.
func TestNanClassClickHouseURI_MissingURI_RequireLiveFailsInsteadOfSkipping(t *testing.T) {
	t.Run("without_require_live_skips_not_fails", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_URI", "")
		t.Setenv(requireLiveEnv, "")
		fake := &fakeSkipFailTB{TB: t}
		runNanClassClickHouseURI(fake)
		if !fake.skipped || fake.failed {
			t.Fatalf("expected Skip (not Fatal) when %s is unset, got failed=%v skipped=%v msg=%q",
				requireLiveEnv, fake.failed, fake.skipped, fake.msg)
		}
	})

	t.Run("with_require_live_fails_not_skips", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_URI", "")
		t.Setenv(requireLiveEnv, "1")
		fake := &fakeSkipFailTB{TB: t}
		runNanClassClickHouseURI(fake)
		if !fake.failed || fake.skipped {
			t.Fatalf("expected Fatal (not Skip) when %s=1 and CLICKHOUSE_URI is unset, got failed=%v "+
				"skipped=%v msg=%q -- a slot run with a missing URI would silently report green",
				requireLiveEnv, fake.failed, fake.skipped, fake.msg)
		}
	})
}
