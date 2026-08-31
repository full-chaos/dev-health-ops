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
// CHAOS-4643 round 3 (EXECUTED, both P2): the hand-rolled url.Parse +
// Hostname()/Port() + net.JoinHostPort rewrite -- three rounds of
// string-surgery patches by this point -- had a FOURTH gap: a comma-joined
// multi-host DSN ("host1:8123,host2:8123") corrupted into one bogus bracketed
// host, because Hostname()/Port() only ever see the first host. That composed
// with a second gap in the redactor this rewrite required in the first place:
// redactDSNForLog never looked at ?username=/?password= query-param
// credentials, and returned the raw DSN verbatim on its own parse failure --
// which multi-host corruption could trigger. Ruling: three same-class bugs in
// one hand-rolled rewriter means the approach was the defect, not each gap.
// Two structural changes replace it: (1) nanClassClickHouseURI now sources
// the host list and credentials from clickhouse.ParseDSN (the driver's own
// parser, backed by lib/churl, which correctly splits multi-host and folds
// query-param credentials the same way it folds userinfo) instead of
// hand-rewriting; Path/RawQuery still come from a plain net/url parse, which
// is safe because Go 1.27's stdlib leaves those fields correct even on a
// multi-host authority -- only Hostname()/Port() are corrupted by the comma,
// and this file no longer calls either. (2) redactDSNForLog is deleted
// outright: the two connect-failure log sites now print a credential-free
// host:port string computed at DSN-build time, never the DSN itself, so there
// is no redaction step left to have a gap in.
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
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/99designs/gqlgen/graphql"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
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
// nanClassClickHouseURI, through round 4, PARSED and REBUILT CLICKHOUSE_URI
// (port translation, host-list rebuild, credential-shape normalisation).
// Round 5 (EXECUTED by codex, on tip d10061b49) found that "stop
// hand-rewriting, use the driver's parser" (round 3's ruling) was still
// under-scoped: this function kept the REWRITING responsibility, and every
// step of a rewrite was independently a new way to get malformed input
// wrong --
//   - a missing/empty host entry (CLICKHOUSE_URI="clickhouse://ch:ch@,")
//     silently became ":9000" and DIALED LOCALHOST -- the live proof
//     PASSED, against the wrong target, silently. This is the same
//     "reports success while covering something other than what it
//     claims" shape as every other CHAOS-4643 finding, just not a
//     credential this time.
//   - a malformed multi-colon host ("host:123:456") was rebuilt into an
//     invalid DSN ("[host:123:456]:9000") -- loud, but still wrong
//     normalisation of malformed input.
//   - a DSN's OWN ?http_proxy= query value can itself be a malformed URL;
//     clickhouse.ParseDSN's fromDSN formats THAT inner url.Parse error
//     with fmt.Errorf("...: %s", err) -- a plain formatted string, not a
//     wrapped *url.Error -- so round 4's errors.As-based unwrap in
//     dsnParseErrorForLog could not reach it, and the proxy URL's own
//     credentials (if any) rode straight through to test output. Likewise
//     a password of exactly "%ZZ" reached url.Error's own .Err.Error()
//     text. Type-based unwrapping cannot enumerate every shape an error
//     can take -- "if you cannot enumerate the shapes an error can take,
//     you cannot sanitise it, you can only decline to print it."
//
// Orchestrator ruling (2026-08-31): STOP REWRITING THE DSN ENTIRELY.
// CLICKHOUSE_URI must already be a usable native-protocol ClickHouse DSN;
// this function ONLY VALIDATES that shape (via the driver's own parser,
// never re-derived by hand) and hands the value to the driver COMPLETELY
// UNMODIFIED -- no url.Parse of raw, no host-list rebuild, no port
// substitution, no scheme normalisation. On ANY validation failure the
// message is FIXED and derives NOTHING from raw or from the underlying
// parse error's text, for the same reason: this file cannot enumerate
// every shape a future malformed input's error could take, so it never
// prints one. This dissolves all three round-5 findings at once: no
// rebuild means no silent ":9000,:9000" default-to-localhost and no
// "[host:123:456]:9000"; no interpolated parse error means no http_proxy
// or "%ZZ" leak, regardless of what shape a future malformed DSN's error
// takes.
//
// PRACTICAL CONSEQUENCE, reported to the orchestrator rather than decided
// here: this repo's STANDARD CLICKHOUSE_URI convention (e.g.
// docs/contribute/start/development-environment.md:89,
// "clickhouse://ch:ch@localhost:8123/default") and the value the slot
// environment exports for the Python-side stages are BOTH the HTTP-port
// form. The Python dual-run harness's own _go_clickhouse_uri
// (test_go_api_dual_run_flow_matrix.py:426-431) translates that HTTP-port
// value to native-protocol before handing it to the Go SERVER process --
// but this file reads CLICKHOUSE_URI directly from the OS environment
// (the same value the Python stages see), and no longer performs that
// translation itself. Whoever runs this file as a slot proof (Wave 5
// exit-run step C) must supply an ALREADY-native-protocol CLICKHOUSE_URI
// for this step specifically, not the repo's usual HTTP-port one.
//
// Returns (dsn, hostPort): dsn is raw, VERBATIM, unmodified, handed
// straight to the driver. hostPort is a comma-joined "host:port" list
// read from the driver's own validated Addr (no credentials, since
// Addr never carries them) -- for callers that must name the CONNECTION
// TARGET in a log line or in a test assertion without ever touching the
// credentialed DSN a second time. A step-C proof that cannot verify its
// own target must refuse to run rather than report (the measurement-
// precondition rule finding 2 above exists to enforce) -- callers MUST
// check hostPort is non-empty before treating a subsequent connection as
// proof of anything; see TestNaNClass_PopulatedGroup_ReturnsRealValue_MarshalsCleanly
// and TestNaNClass_AllNullGroup_ReturnsSQLNullScannedAsZero_NotNaN for the
// pattern.
func nanClassClickHouseURI(t testing.TB) (dsn string, hostPort string) {
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

	// Validate-only, via the driver's OWN parser (clickhouse.ParseDSN) --
	// raw is never touched by url.Parse or any hand-rolled parsing of our
	// own, and is returned VERBATIM below. On failure: a FIXED message,
	// nothing derived from err. CHAOS-4643 round 5 found a parse error's
	// own text can carry a credential fragment through a shape (a nested
	// ?http_proxy= value's own inner, differently-typed parse error) no
	// enumerated unwrap could catch -- so this file no longer tries to
	// print any part of a parse error, ever.
	opts, err := chdriver.ParseDSN(raw)
	if err != nil {
		t.Fatalf("CLICKHOUSE_URI failed to parse as a ClickHouse DSN -- set it to an already-valid, " +
			"native-protocol URI (clickhouse://host:9000/db); the parse error's own text is withheld " +
			"here because it can itself carry credential fragments in shapes this file cannot " +
			"enumerate ahead of time (CHAOS-4643 round 5)")
	}
	if opts.Protocol == chdriver.HTTP {
		t.Fatalf("CLICKHOUSE_URI uses an http:// or https:// scheme -- this file requires an " +
			"already-native-protocol DSN and no longer translates one for you (CHAOS-4643 round 5 " +
			"ruling: stop rewriting the DSN). If the environment only supplies this repo's usual " +
			"HTTP-port CLICKHOUSE_URI convention, the caller must supply a native-protocol value for " +
			"this step specifically")
	}
	if len(opts.Addr) == 0 {
		t.Fatalf("CLICKHOUSE_URI has no host")
	}
	for _, addr := range opts.Addr {
		host, port, splitErr := net.SplitHostPort(addr)
		if splitErr != nil || host == "" || port == "" {
			// CHAOS-4643 round 5: this used to be "no port present, so
			// default to 9000" -- which is exactly how an empty host
			// entry from a malformed multi-host DSN
			// (CLICKHOUSE_URI="clickhouse://ch:ch@,") silently became
			// ":9000" and dialed localhost, with the live proof then
			// PASSING against the wrong target. A malformed or empty
			// entry is now a hard failure, never a default.
			t.Fatalf("CLICKHOUSE_URI's host list contains a malformed or empty entry -- every host " +
				"must be an explicit, non-empty \"host:port\" pair; this file no longer fixes up a " +
				"malformed entry (CHAOS-4643 round 5: a malformed entry silently redirected a live " +
				"proof to localhost and still reported PASS)")
		}
		if port == "8123" || port == "8443" {
			t.Fatalf("CLICKHOUSE_URI host %q uses an HTTP port (8123 or 8443) -- this file requires "+
				"native protocol (port 9000, or your ClickHouse's configured native port) and no "+
				"longer translates the port for you (CHAOS-4643 round 5 ruling)", host)
		}
	}

	return raw, strings.Join(opts.Addr, ",")
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
	dsn, hostPort := nanClassClickHouseURI(t)
	// CHAOS-4643 round 5: a proof that cannot verify its own target must
	// refuse to run, not report -- finding 2 was a malformed host list
	// silently dialing localhost and the test PASSING anyway. hostPort is
	// guaranteed non-empty by nanClassClickHouseURI itself, but this
	// assertion makes "this test knows and checked what it connected to"
	// a fact visible IN THIS TEST, not something only the helper enforces.
	if hostPort == "" {
		t.Fatalf("refusing to connect: no target host resolved from CLICKHOUSE_URI")
	}
	t.Logf("connecting to ClickHouse at %s", hostPort)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("connect to ClickHouse at %s: %v", hostPort, err)
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
	dsn, hostPort := nanClassClickHouseURI(t)
	// CHAOS-4643 round 5: see the matching assertion in
	// TestNaNClass_PopulatedGroup_ReturnsRealValue_MarshalsCleanly -- a
	// proof that cannot verify its own target must refuse to run.
	if hostPort == "" {
		t.Fatalf("refusing to connect: no target host resolved from CLICKHOUSE_URI")
	}
	t.Logf("connecting to ClickHouse at %s", hostPort)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("connect to ClickHouse at %s: %v", hostPort, err)
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

// TestNanClassClickHouseURI_ValidatesAndPassesThroughUnmodified is CHAOS-4643
// round 5's structural proof: nanClassClickHouseURI no longer rewrites
// CLICKHOUSE_URI at all -- a well-formed native-protocol DSN passes through
// VERBATIM (dsn == raw, byte for byte), and a malformed or HTTP-port one is
// rejected outright rather than "fixed up". This replaces
// TestNanClassClickHouseURI_CarriesUserinfoThroughRebuild, whose whole
// premise (port translation, host-list rebuild) round 5 ruled out.
func TestNanClassClickHouseURI_ValidatesAndPassesThroughUnmodified(t *testing.T) {
	t.Run("well_formed_native_dsn_passed_through_verbatim", func(t *testing.T) {
		cases := []string{
			"clickhouse://chuser:chpass@example.internal:9000/default",
			"clickhouse://chuser@example.internal:9000/default",
			"clickhouse://example.internal:9000/default",
			"clickhouse://chuser:chpass@[::1]:9000/default",
			"clickhouse://u:p@host1:9000,host2:9000,host3:9000/db",
			"clickhouse://host:9000/db?username=u&password=p",
			"clickhouse://chuser:chpass@example.internal:9000/my_custom_db",
			"clickhouse://ci-user:ci-secret@[fe80::1%25en0]:9000/ci_scratch?secure=true",
		}
		for _, raw := range cases {
			t.Run(raw, func(t *testing.T) {
				t.Setenv("CLICKHOUSE_URI", raw)
				t.Setenv(requireLiveEnv, "")
				got, _ := nanClassClickHouseURI(t)
				if got != raw {
					t.Fatalf("nanClassClickHouseURI(%q) = %q, want the EXACT same string (no rewriting) -- "+
						"CHAOS-4643 round 5 ruling: this function only validates, never rebuilds", raw, got)
				}
			})
		}
	})

	// codex review round 1 (EXECUTED mutation survivor, still relevant): a
	// mutant that made nanClassClickHouseURI fail whenever
	// DEV_HEALTH_REQUIRE_LIVE=1 -- regardless of whether CLICKHOUSE_URI was
	// even set -- must not survive. The flag is a no-op once CLICKHOUSE_URI
	// is present; it only changes behavior on the MISSING-URI path (see
	// TestNanClassClickHouseURI_MissingURI_RequireLiveFailsInsteadOfSkipping).
	t.Run("require_live_with_uri_present_is_a_no_op", func(t *testing.T) {
		raw := "clickhouse://chuser:chpass@example.internal:9000/default"
		t.Setenv("CLICKHOUSE_URI", raw)
		t.Setenv(requireLiveEnv, "1")
		got, _ := nanClassClickHouseURI(t)
		if got != raw {
			t.Fatalf("nanClassClickHouseURI with CLICKHOUSE_URI set and %s=1 = %q, want %q -- "+
				"the require-live flag must only affect the missing-URI path", requireLiveEnv, got, raw)
		}
	})

	// CHAOS-4643 round 5 findings 2 and 3 (EXECUTED by codex): these two
	// shapes used to be silently "fixed up" by the deleted rewrite --
	// finding 2's empty host entry became ":9000" and dialed localhost
	// (the live proof PASSED against the wrong target); finding 3's
	// multi-colon host became an invalid bracketed DSN. Both are now hard
	// validation failures, never a default.
	t.Run("malformed_host_entries_are_rejected_not_fixed_up", func(t *testing.T) {
		cases := []struct {
			name string
			raw  string
		}{
			{
				name: "empty_host_entry_in_multi_host_list",
				raw:  "clickhouse://ch:ch@,",
			},
			{
				name: "multi_colon_host",
				raw:  "clickhouse://host:123:456/db?readonly=1",
			},
			{
				name: "empty_host_no_port_at_all",
				raw:  "clickhouse://ch:ch@/db",
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Setenv("CLICKHOUSE_URI", tc.raw)
				t.Setenv(requireLiveEnv, "")
				fake := &fakeSkipFailTB{TB: t}
				runNanClassClickHouseURI(fake)
				if !fake.failed || fake.skipped {
					t.Fatalf("expected a malformed host list to FAIL validation (not skip, not silently "+
						"default a port), got failed=%v skipped=%v msg=%q", fake.failed, fake.skipped, fake.msg)
				}
			})
		}
	})

	// CHAOS-4643 round 5 ruling: an HTTP-port DSN (this repo's usual
	// CLICKHOUSE_URI convention, e.g.
	// docs/contribute/start/development-environment.md:89) is no longer
	// translated -- it is rejected, since the caller must now supply an
	// already-native-protocol value.
	t.Run("http_port_dsn_is_rejected_not_translated", func(t *testing.T) {
		cases := []string{
			"clickhouse://chuser:chpass@example.internal:8123/default",
			"clickhouse://chuser:chpass@example.internal:8443/default",
			"http://chuser:chpass@example.internal:9000/default",
		}
		for _, raw := range cases {
			t.Run(raw, func(t *testing.T) {
				t.Setenv("CLICKHOUSE_URI", raw)
				t.Setenv(requireLiveEnv, "")
				fake := &fakeSkipFailTB{TB: t}
				runNanClassClickHouseURI(fake)
				if !fake.failed || fake.skipped {
					t.Fatalf("expected an HTTP-port/HTTP-scheme DSN to FAIL validation (not be translated "+
						"to native protocol), got failed=%v skipped=%v msg=%q", fake.failed, fake.skipped, fake.msg)
				}
			})
		}
	})
}

// TestNanClassClickHouseURI_HostPortForLogNeverCarriesCredentials proves
// hostPort -- the value the two connect-failure log sites and the live
// tests' target-assertion print -- never carries a credential, across
// userinfo, query-param, and multi-host shapes. Unaffected in principle by
// round 5 (hostPort was already read-only extraction of opts.Addr, never
// re-parsed from the finished DSN), but the input DSNs below are updated to
// native-protocol (port 9000) so they pass round 5's new validation and
// actually reach the point where hostPort is computed.
func TestNanClassClickHouseURI_HostPortForLogNeverCarriesCredentials(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{
			name: "userinfo_credentials",
			raw:  "clickhouse://ci-user:ci-secret@example.internal:9000/default",
		},
		{
			name: "query_param_credentials",
			raw:  "clickhouse://example.internal:9000/default?username=ci-user&password=ci-secret",
		},
		{
			name: "multi_host_with_userinfo_credentials",
			raw:  "clickhouse://ci-user:ci-secret@host1:9000,host2:9000/default",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CLICKHOUSE_URI", tc.raw)
			t.Setenv(requireLiveEnv, "")
			_, hostPort := nanClassClickHouseURI(t)
			if hostPort == "" {
				t.Fatalf("expected a well-formed DSN to validate and return a non-empty hostPort")
			}
			if strings.Contains(hostPort, "ci-secret") {
				t.Fatalf("hostPort %q leaked the password", hostPort)
			}
			if strings.Contains(hostPort, "ci-user") {
				t.Fatalf("hostPort %q leaked the username", hostPort)
			}
			if strings.Contains(hostPort, "@") {
				t.Fatalf("hostPort %q should never contain the userinfo delimiter '@'", hostPort)
			}
		})
	}
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

// TestNanClassClickHouseURI_FailureMessageNeverDerivesFromInput is CHAOS-4643
// round 5's closing property test, strengthened by round 7 (codex EXECUTED
// finding 3, ARGUED finding 2 -- both fixed here, per orchestrator ruling:
// neither needed chris, both are "a guard that cannot fail over ground it
// appears to cover", the same shape as the round-6 hostlist gap).
//
// Every case now carries its OWN username/password (never a shared literal
// like the old "ci-user"/"ci-secret" every case reused) for two reasons:
// (1) the cross-case equality check below is only meaningful if credentials
// actually DIFFER between cases in the same class -- a shared credential
// can never demonstrate the message ignores it; (2) leak checks assert
// against the BARE username too, not just the full password or the
// combined "user:pass" userinfo, closing round 7 finding 2 (codex could
// not execute this against a read-only review target; the lane can and
// did -- see the mutation proof this test's own history records).
//
// A THIRD class, "protocol" (round 7 finding 3), covers the HTTP-scheme/
// port rejection branch (~line 246), which the round-5/6 "parse"/"hostlist"
// classes never reached -- that branch could not fail this guard either,
// for the same reason the hostlist branch could not before round 6's fix.
// Its two members deliberately share the SAME host:port (chris's ruling:
// host:port is sanctioned content in this branch's message) and vary ONLY
// credentials, so the equality check isolates "did a credential leak" from
// "did the sanctioned host legitimately differ" -- conflating those would
// make the check meaningless.
func TestNanClassClickHouseURI_FailureMessageNeverDerivesFromInput(t *testing.T) {
	cases := []struct {
		name      string
		raw       string
		wantClass string
		username  string // for leak-checking; the case's own credential.
		password  string // for leak-checking; may itself be malformed input.
	}{
		{
			// codex round 1-2's shape: unbalanced IPv6 bracket.
			name:      "unbalanced_bracket",
			raw:       "clickhouse://unbalbrk-user:unbalbrk-secret@[::1:9000/db",
			wantClass: "parse",
			username:  "unbalbrk-user",
			password:  "unbalbrk-secret",
		},
		{
			name:      "bad_port",
			raw:       "clickhouse://badport-user:badport-secret@example.internal:notaport/db",
			wantClass: "parse",
			username:  "badport-user",
			password:  "badport-secret",
		},
		{
			name:      "empty_host",
			raw:       "clickhouse://emptyhost-user:emptyhost-secret@/db",
			wantClass: "parse",
			username:  "emptyhost-user",
			password:  "emptyhost-secret",
		},
		{
			// codex round 4's shape: malformed percent-escape INSIDE the
			// userinfo, the case most likely to echo a credential fragment.
			name:      "bad_percent_escape_in_userinfo",
			raw:       "clickhouse://pctesc-user:pctesc-sec%ZZret@example.internal:9000/db",
			wantClass: "parse",
			username:  "pctesc-user",
			password:  "pctesc-sec%ZZret",
		},
		{
			name:      "multi_host_malformed_with_query_param_credentials",
			raw:       "clickhouse://host1:9000,[::1:9443/db?username=qpcred-user&password=qpcred-secret",
			wantClass: "parse",
			username:  "qpcred-user",
			password:  "qpcred-secret",
		},
		{
			// codex round 5 finding 1a (EXECUTED): a malformed ?http_proxy=
			// value's own inner parse error is a FORMATTED string, not a
			// wrapped *url.Error -- the exact shape no errors.As-based
			// unwrap could reach.
			name:      "malformed_http_proxy_query_value",
			raw:       "clickhouse://httpproxy-user:httpproxy-secret@host:9000/db?http_proxy=http://proxyA-user:proxyA-secret@[",
			wantClass: "parse",
			username:  "httpproxy-user",
			password:  "httpproxy-secret",
		},
		{
			// codex round 5 finding 1b (EXECUTED): a password of exactly
			// "%ZZ" reaches url.Error's own .Err.Error() text directly (not
			// through userinfo -- through the escape-diagnostic itself).
			name:      "password_is_the_literal_bad_escape",
			raw:       "clickhouse://litesc-user:%ZZ@example.internal:9000/db",
			wantClass: "parse",
			username:  "litesc-user",
			password:  "%ZZ",
		},
		{
			// Round 5 finding 2's shape: ParseDSN succeeds (Host is "," --
			// not empty -- so ParseDSN's own guard does not fire), and
			// net.SplitHostPort rejects the resulting empty Addr entries.
			name:      "empty_host_entry_in_multi_host_list",
			raw:       "clickhouse://hostlist1-user:hostlist1-secret@,",
			wantClass: "hostlist",
			username:  "hostlist1-user",
			password:  "hostlist1-secret",
		},
		{
			// Round 5 finding 3's shape: ParseDSN succeeds; SplitHostPort
			// rejects the multi-colon entry.
			name:      "multi_colon_host",
			raw:       "clickhouse://hostlist2-user:hostlist2-secret@host:123:456/db",
			wantClass: "hostlist",
			username:  "hostlist2-user",
			password:  "hostlist2-secret",
		},
		{
			// THIRD hostlist member, distinct credentials again.
			name:      "empty_host_no_port_at_all",
			raw:       "clickhouse://hostlist3-user:hostlist3-secret@,",
			wantClass: "hostlist",
			username:  "hostlist3-user",
			password:  "hostlist3-secret",
		},
		{
			// Round 7 finding 3: the HTTP-scheme/port rejection branch,
			// never covered by any class before this fix. SAME host:port
			// as the next case (sanctioned content, held constant) so the
			// equality check below isolates credential leakage from
			// legitimately-varying host content.
			name:      "http_scheme_credentialed_1",
			raw:       "http://protoA-user:protoA-secret@example.internal:9000/default",
			wantClass: "protocol",
			username:  "protoA-user",
			password:  "protoA-secret",
		},
		{
			name:      "http_scheme_credentialed_2",
			raw:       "http://protoB-user:protoB-secret@example.internal:9000/default2",
			wantClass: "protocol",
			username:  "protoB-user",
			password:  "protoB-secret",
		},
	}

	baselineByClass := map[string]string{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CLICKHOUSE_URI", tc.raw)
			t.Setenv(requireLiveEnv, "")
			fake := &fakeSkipFailTB{TB: t}
			runNanClassClickHouseURI(fake)
			if !fake.failed || fake.skipped {
				t.Fatalf("expected this malformed/credentialed DSN to FAIL: failed=%v skipped=%v msg=%q",
					fake.failed, fake.skipped, fake.msg)
			}
			if strings.Contains(fake.msg, tc.password) {
				t.Fatalf("failure message leaked the PASSWORD: %q", fake.msg)
			}
			// Round 7 finding 2 (codex ARGUED, lane EXECUTED): the bare
			// username must never leak either, not just the full password
			// or the combined userinfo -- a per-case unique username is
			// what makes this check able to fail at all.
			if strings.Contains(fake.msg, tc.username) {
				t.Fatalf("failure message leaked the USERNAME: %q", fake.msg)
			}
			if strings.Contains(fake.msg, tc.username+":"+tc.password) {
				t.Fatalf("failure message leaked the USERINFO: %q", fake.msg)
			}
			if strings.Contains(fake.msg, "proxyA-secret") || strings.Contains(fake.msg, "proxyA-user:proxyA-secret") {
				t.Fatalf("failure message leaked the PROXY credential: %q", fake.msg)
			}
			if strings.Contains(fake.msg, "username="+tc.username) || strings.Contains(fake.msg, "password="+tc.password) {
				t.Fatalf("failure message leaked the RAW QUERY STRING credentials: %q", fake.msg)
			}
			if strings.Contains(fake.msg, tc.raw) {
				t.Fatalf("failure message embedded the entire raw DSN verbatim: %q", fake.msg)
			}
			if tc.password != "%ZZ" && strings.Contains(fake.msg, "%ZZ") {
				t.Fatalf("failure message echoed the malformed escape sequence: %q", fake.msg)
			}
			// The stronger property, now covering EVERY class INCLUDING
			// "protocol" (round 7 finding 3): every case that fails for
			// the SAME underlying reason (wantClass) produces the
			// byte-for-byte SAME message, proving nothing about the
			// specific input leaked through -- not even a fragment too
			// narrow for the substring checks above to name. This is now
			// a REAL check for "parse" and "protocol" too, not just
			// "hostlist": every case in those classes uses a DIFFERENT
			// credential (round 7 finding 2's fix), so equality can only
			// hold if the message genuinely ignores the credential.
			if want, ok := baselineByClass[tc.wantClass]; !ok {
				baselineByClass[tc.wantClass] = fake.msg
			} else if fake.msg != want {
				t.Fatalf("%s-class failure messages differ across inputs (%q vs baseline %q) -- "+
					"a message that varies with the input can only vary BECAUSE something about the "+
					"input leaked into it", tc.wantClass, fake.msg, want)
			}
		})
	}
}
