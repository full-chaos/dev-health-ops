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
	"errors"
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
// dsnParseErrorForLog converts a DSN-parsing error into a string safe to
// hand to t.Fatalf/log output: it never includes the raw input URL that
// produced the error, even though the error VALUE itself does. CHAOS-4643
// round 4 (EXECUTED by codex, immediately after redactDSNForLog was deleted
// and every direct dsn-logging call site was removed): Go's own
// net/url.Error type -- returned by both url.Parse and, via lib/churl,
// clickhouse.ParseDSN -- embeds the ENTIRE input URL in its own Error()
// string ("parse \"<url>\": <cause>"). Neither this file's redaction
// removal nor its stop-logging-the-dsn discipline touched that: the
// credential was never handed to a log call, it rode inside the %v
// formatting of the error VALUE at the two t.Fatalf sites below. Unwrapping
// to the inner cause (errors.As to *url.Error, then .Err) closes that path
// by construction -- .Err carries a fixed description ("missing ']' in
// host", "invalid port \":abc\" after host") or, for a malformed escape,
// a short fragment of the OFFENDING BYTES ("invalid URL escape \"%ZZ\""),
// never the surrounding userinfo/query. Errors clickhouse.ParseDSN returns
// that do NOT come from churl (e.g. its own "parse dsn address failed" for
// an empty host) are already static strings with no interpolated input --
// verified empirically, not assumed, and pinned by
// TestDSNParseErrorForLog_NeverLeaksCredentials below across both call
// sites and multiple malformed-input shapes. Every DSN-parse-error site in
// this file MUST route through this one helper, so there is a single place
// this can be gotten wrong rather than one t.Fatalf per site that each has
// to stay correct independently.
func dsnParseErrorForLog(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return urlErr.Err.Error()
	}
	return err.Error()
}

// nanClassClickHouseURI returns (dsn, hostPort): dsn is the full rebuilt
// connection string for the driver; hostPort is host:port only (no
// credentials, no path, no query) for callers that need to name the target
// in a log/error line without ever handling the credentialed DSN a second
// time -- see the CHAOS-4643 round 3 note above the package doc comment.
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

	// Path/RawQuery only, from a plain net/url parse. Safe even on a
	// multi-host DSN: Go 1.27's stdlib Parse leaves u.Path/u.RawQuery
	// correct on a comma-joined Host authority -- only its Hostname()/
	// Port() accessors are corrupted by the comma, and this function
	// never calls either of those.
	pathQuery, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse CLICKHOUSE_URI: %s", dsnParseErrorForLog(err))
	}

	// Host list and credentials come ONLY from the driver's OWN DSN
	// parser (clickhouse.ParseDSN, backed by lib/churl) -- never
	// re-derived by string surgery. churl.Parse is a copy of net/url's
	// parser specifically patched to keep a comma-joined multi-host
	// authority intact in dsn.Host (see its own header comment), and
	// ParseDSN splits that correctly into Addr -- unlike Hostname()/
	// Port(), which only ever see the FIRST host of such a string. It
	// also folds ?username=/?password= query-param credentials into Auth
	// the same way it folds userinfo, so this file has exactly one
	// source of truth for "what are the hosts and credentials", not two
	// half-implementations of the same job.
	opts, err := chdriver.ParseDSN(raw)
	if err != nil {
		t.Fatalf("clickhouse.ParseDSN(CLICKHOUSE_URI): %s", dsnParseErrorForLog(err))
	}
	if len(opts.Addr) == 0 {
		t.Fatalf("CLICKHOUSE_URI has no host: %q", raw)
	}

	translated := make([]string, len(opts.Addr))
	for i, addr := range opts.Addr {
		host, port, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			// No port present at all (a bare host, or a bracketed IPv6
			// literal with no port -- SplitHostPort requires ":port"
			// even then). Strip any brackets ourselves so JoinHostPort
			// below does not double them up.
			host = addr
			port = ""
			if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
				host = host[1 : len(host)-1]
			}
		}
		if port == "8123" || port == "8443" || port == "" {
			port = "9000"
		}
		translated[i] = net.JoinHostPort(host, port)
	}
	hostPort = strings.Join(translated, ",")

	scheme := pathQuery.Scheme
	switch scheme {
	case "", "http", "https", "clickhouse+http":
		scheme = "clickhouse"
	}

	var userinfo *url.Userinfo
	if opts.Auth.Password != "" {
		userinfo = url.UserPassword(opts.Auth.Username, opts.Auth.Password)
	} else if opts.Auth.Username != "" {
		userinfo = url.User(opts.Auth.Username)
	}

	// Path/RawQuery pass through unchanged, same invariant as before --
	// except the "username"/"password" query keys, which are now
	// expressed as userinfo above (whichever form the source DSN used)
	// and must not also survive in the query, or the credential would
	// appear twice in the same string.
	query := pathQuery.Query()
	query.Del("username")
	query.Del("password")

	rebuilt := url.URL{
		Scheme:   scheme,
		User:     userinfo,
		Host:     hostPort,
		Path:     pathQuery.Path,
		RawQuery: query.Encode(),
	}
	return rebuilt.String(), hostPort
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
			want: "clickhouse://chuser:chpass@example.internal:9000/default",
		},
		{
			name: "user_only_preserved",
			raw:  "clickhouse://chuser@example.internal:8443/default",
			want: "clickhouse://chuser@example.internal:9000/default",
		},
		{
			name: "no_userinfo_still_works",
			raw:  "clickhouse://example.internal:8123/default",
			want: "clickhouse://example.internal:9000/default",
		},
		{
			name: "http_scheme_and_native_port_pass_through",
			raw:  "http://chuser:chpass@example.internal:9000/default",
			want: "clickhouse://chuser:chpass@example.internal:9000/default",
		},
		{
			// codex review round 1 (EXECUTED): Hostname() strips the brackets
			// off an IPv6 literal ("::1", not "[::1]"), and a bare "%s:%s"
			// Sprintf never put them back, so the rebuilt DSN was ambiguous
			// (host and port both after the last ":"). net.JoinHostPort fixes
			// this by re-adding brackets exactly when the host needs them.
			name: "ipv6_host_gets_bracketed",
			raw:  "clickhouse://chuser:chpass@[::1]:8123/default",
			want: "clickhouse://chuser:chpass@[::1]:9000/default",
		},
		{
			// codex review round 3 (EXECUTED): a comma-joined multi-host DSN's
			// Hostname()/Port() only ever saw the FIRST host under the old
			// url.Parse + string-surgery approach, corrupting the rebuild into
			// one bogus bracketed host ("[host1:8123,host2]:9000"). Fixed by
			// sourcing the host list from clickhouse.ParseDSN's Addr (churl-
			// backed, comma-aware) and translating every entry.
			name: "multi_host_dsn_all_hosts_translated",
			raw:  "clickhouse://u:p@host1:8123,host2:8443,host3:9000/db",
			want: "clickhouse://u:p@host1:9000,host2:9000,host3:9000/db",
		},
		{
			// codex review round 3 (EXECUTED): credentials carried as
			// ?username=/?password= query params (clickhouse-connect's other
			// DSN form) were previously ignored entirely by the old
			// url.Parse-based rebuild, which only ever looked at u.User. Now
			// folded into userinfo via the same clickhouse.ParseDSN call that
			// reads the hosts, and dropped from the query so the credential
			// does not end up duplicated.
			name: "query_param_credentials_folded_into_userinfo",
			raw:  "clickhouse://host:8123/db?username=u&password=p",
			want: "clickhouse://u:p@host:9000/db",
		},
		{
			name: "non_default_database_path",
			raw:  "clickhouse://chuser:chpass@example.internal:8123/my_custom_db",
			want: "clickhouse://chuser:chpass@example.internal:9000/my_custom_db",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CLICKHOUSE_URI", tc.raw)
			t.Setenv(requireLiveEnv, "")
			got, _ := nanClassClickHouseURI(t)
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
		got, _ := nanClassClickHouseURI(t)
		want := "clickhouse://chuser:chpass@example.internal:9000/default"
		if got != want {
			t.Fatalf("nanClassClickHouseURI with CLICKHOUSE_URI set and %s=1 = %q, want %q -- "+
				"the require-live flag must only affect the missing-URI path", requireLiveEnv, got, want)
		}
	})

	// codex review round 2 (EXECUTED): the manual "%s://%s:%s" rebuild
	// dropped Path/RawQuery entirely and mis-escaped an IPv6 zone ID's
	// "%" -- both fixed by rebuilding from a copy of the parsed *url.URL
	// instead of Sprintf. Regression-tested together since both trace to
	// the same fix.
	t.Run("ipv6_zone_id_and_path_query_preserved", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_URI", "clickhouse://ci-user:ci-secret@[fe80::1%25en0]:8123/ci_scratch?secure=true")
		t.Setenv(requireLiveEnv, "")
		got, _ := nanClassClickHouseURI(t)
		want := "clickhouse://ci-user:ci-secret@[fe80::1%25en0]:9000/ci_scratch?secure=true"
		if got != want {
			t.Fatalf("nanClassClickHouseURI with an IPv6-zone-ID + non-default-db + query CLICKHOUSE_URI "+
				"= %q, want %q -- the zone ID's \"%%\" must round-trip as \"%%25\" through the URI host "+
				"component, and Path/RawQuery must survive the rebuild unchanged", got, want)
		}
	})
}

// TestNanClassClickHouseURI_HostPortForLogNeverCarriesCredentials is CHAOS-4643
// round 3 finding B's structural fix, proved rather than patched: there is no
// longer any redactDSNForLog function whose job is "take a credentialed DSN
// and hope to strip the password before it is logged" -- codex found that
// function never inspected ?username=/?password= query-param credentials,
// and its own comment admitted it fell back to returning the raw DSN on a
// parse failure it called "should not happen". Both gaps are gone because the
// thing they gated no longer exists: the two connect-failure log sites now
// print nanClassClickHouseURI's second return value, hostPort, which is
// built from the pre-credential host list inside the function -- never by
// re-parsing the finished (credentialed) DSN string -- so there is no
// parse-failure path that could fall back to leaking the secret. This test
// proves hostPort carries no credential for every shape codex's round 3
// findings and this ruling named: userinfo, ?username=/?password= query
// params, and a multi-host DSN combining userinfo with several hosts.
func TestNanClassClickHouseURI_HostPortForLogNeverCarriesCredentials(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{
			name: "userinfo_credentials",
			raw:  "clickhouse://ci-user:ci-secret@example.internal:8123/default",
		},
		{
			name: "query_param_credentials",
			raw:  "clickhouse://example.internal:8123/default?username=ci-user&password=ci-secret",
		},
		{
			name: "multi_host_with_userinfo_credentials",
			raw:  "clickhouse://ci-user:ci-secret@host1:8123,host2:8443/default",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CLICKHOUSE_URI", tc.raw)
			t.Setenv(requireLiveEnv, "")
			_, hostPort := nanClassClickHouseURI(t)
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

// TestDSNParseErrorForLog_NeverLeaksCredentials is CHAOS-4643 round 4's
// EXECUTED finding, and the property test the ruling required: rounds 1-4
// on this file are ALL one class -- credentials reaching an output stream
// -- and this is the brick that was still missing after round 4's
// structural fix (deleting redactDSNForLog, no longer logging the DSN
// directly) closed every site THIS FILE'S OWN CODE writes to a log/error
// string. What remained was the error VALUE itself: Go's net/url.Error
// embeds the entire input URL in its own Error() string, so
// t.Fatalf("...: %v", err) on either DSN-parse failure in
// nanClassClickHouseURI printed the credentialed DSN through the error's
// formatting, never through anything this file explicitly logged. A
// table-driven PROPERTY test, not a single example, because the point is
// that this must hold for every malformed-input SHAPE that can reach
// either parse call, not just the one codex happened to try -- a future
// edit that reintroduces a leak at a different malformed-input shape must
// fail this test too, not just the shape codex tried once.
func TestDSNParseErrorForLog_NeverLeaksCredentials(t *testing.T) {
	const password = "ci-secret"
	const userinfo = "ci-user:ci-secret"

	cases := []struct {
		name string
		raw  string
	}{
		{
			// Unbalanced IPv6 bracket -- fails inside url.Parse (the
			// FIRST call in nanClassClickHouseURI).
			name: "unbalanced_bracket",
			raw:  "clickhouse://ci-user:ci-secret@[::1:8123/db",
		},
		{
			// Non-numeric port -- fails inside url.Parse.
			name: "bad_port",
			raw:  "clickhouse://ci-user:ci-secret@example.internal:notaport/db",
		},
		{
			// Empty host (userinfo present, host component empty) --
			// url.Parse itself accepts this syntactically; the failure
			// comes from the SECOND call, clickhouse.ParseDSN, whose own
			// "parse dsn address failed" is a static string with no
			// interpolated input. Exercises the other call site.
			name: "empty_host",
			raw:  "clickhouse://ci-user:ci-secret@/db",
		},
		{
			// Malformed percent-escape INSIDE the userinfo itself --
			// the case most likely to echo a credential fragment back,
			// since the escape error's diagnostic can include the
			// offending bytes. Fails inside url.Parse.
			name: "bad_percent_escape_in_userinfo",
			raw:  "clickhouse://ci-user:ci-sec%ZZret@example.internal:8123/db",
		},
		{
			// Multi-host with a malformed piece in the SECOND host, and
			// credentials carried as ?username=/?password= query params
			// instead of userinfo -- covers the multi-host class finding
			// A fixed, the query-param-credential class finding B fixed,
			// AND the raw-query-string leak surface in one input. Fails
			// inside url.Parse ("invalid IP-literal").
			name: "multi_host_malformed_with_query_param_credentials",
			raw:  "clickhouse://host1:8123,[::1:8443/db?username=ci-user&password=ci-secret",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CLICKHOUSE_URI", tc.raw)
			t.Setenv(requireLiveEnv, "")
			fake := &fakeSkipFailTB{TB: t}
			runNanClassClickHouseURI(fake)
			if !fake.failed || fake.skipped {
				t.Fatalf("expected this malformed DSN to FAIL (not skip or succeed): failed=%v skipped=%v msg=%q",
					fake.failed, fake.skipped, fake.msg)
			}
			if strings.Contains(fake.msg, password) {
				t.Fatalf("failure message leaked the PASSWORD: %q", fake.msg)
			}
			if strings.Contains(fake.msg, userinfo) {
				t.Fatalf("failure message leaked the USERINFO: %q", fake.msg)
			}
			if strings.Contains(fake.msg, "username=ci-user") || strings.Contains(fake.msg, "password=ci-secret") {
				t.Fatalf("failure message leaked the RAW QUERY STRING credentials: %q", fake.msg)
			}
			if strings.Contains(fake.msg, tc.raw) {
				t.Fatalf("failure message embedded the entire raw DSN verbatim: %q", fake.msg)
			}
		})
	}
}
