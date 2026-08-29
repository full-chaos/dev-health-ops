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
// TWO SEPARATE, REAL MECHANISMS PROVEN HERE, NOT ONE:
//  1. ClickHouse's own engine: AVG() over a Nullable(Float64) group whose
//     rows are ALL NULL returns NaN, not NULL and not 0. This is the
//     class validate.go:184-201 enumerates as category 2 ("AT RISK"),
//     verified live rather than read off a doc comment.
//  2. gqlgen's own library: graphql.MarshalFloatContext (the exact
//     function generated.go:91369/94794 call for every non-nullable
//     Float! field, BreakdownItem.value included --
//     contracts/graphql/v1/schema.graphql:386) refuses to write a NaN,
//     returning "cannot marshal infinite no NaN float values"
//     (gqlgen's float.go:38-45, a stock library check, nothing this
//     port added).
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
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/99designs/gqlgen/graphql"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// nanClassClickHouseURI mirrors the Python dual-run harness's
// _go_clickhouse_uri (test_go_api_dual_run_flow_matrix.py:426-431):
// CLICKHOUSE_URI as set for the slot's Python-side stages is typically
// clickhouse-connect's HTTP-port form (:8123); this repo's Go side
// requires the native protocol port (:9000, TestBuildQueryRoute_FailsFastOnWrongClickHouseProtocol's
// regression). Doing the same translation here lets this test run off
// the SAME env var value the slot already exports, rather than requiring
// a second, easily-forgotten one.
func nanClassClickHouseURI(t *testing.T) string {
	t.Helper()
	raw := os.Getenv("CLICKHOUSE_URI")
	if raw == "" {
		t.Skip("CLICKHOUSE_URI not set -- this is a live-ClickHouse proof, run only in a container slot")
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
	return fmt.Sprintf("%s://%s:%s", scheme, host, port)
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

// TestNaNClass_AllNullGroup_ProducesNaN_GqlgenRefusesToMarshal is THE
// divergence this file exists to document: a group that HAS rows (the
// pipeline ran) where line_coverage_pct is NULL in every one of them.
// AVG() over an all-NULL Nullable(Float64) group is the UNCHARACTERISED
// shape BRIEF's NaN UPDATE explicitly left open (distinct from the
// CLEARED zero-source-row shape). Splits into two assertions matching
// the two mechanisms in the file-level doc comment: the raw ClickHouse
// value IS NaN (not 0.0, not some driver default -- a wrong fallback
// here would prove nothing), and the SAME gqlgen call the real response
// path would use on it refuses to write it.
//
// IMPORTANT SCOPE NOTE, not to be over-read: this does NOT exercise
// ExecuteBreakdown's own error return. ExecuteBreakdown would return NO
// error for this value -- the ClickHouse query itself succeeds and scans
// a legitimate (if NaN) float64; resolve.go's swallow-to-empty-on-execute-error
// path (the one flow-matrix test 2's docstring warns can silently
// masquerade as "the fix took effect") never fires here, because there
// is no execute error to swallow. The marshal failure this test proves
// happens at a SEPARATE, LATER step -- when gqlgen serializes the
// already-successful BreakdownResult into the HTTP response -- which is
// exactly why this test calls graphql.MarshalFloatContext directly
// rather than asserting on ExecuteBreakdown's return.
func TestNaNClass_AllNullGroup_ProducesNaN_GqlgenRefusesToMarshal(t *testing.T) {
	dsn := nanClassClickHouseURI(t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("connect to ClickHouse at %s: %v", dsn, err)
	}
	ctx := context.Background()

	value := nanClassRunScalarQuery(t, ctx, client, nanClassQuerySQL(t, true))

	if !math.IsNaN(value) {
		t.Fatalf("expected AVG(line_coverage_pct) over an all-NULL group to be NaN "+
			"(ClickHouse's avg() over Nullable(Float64) with every value NULL), got %v -- "+
			"if this is 0.0, a coercion swallowed the NaN before it reached this scan; "+
			"if this is a ClickHouse NULL surfacing some other way, this AT-RISK "+
			"classification needs re-checking against the live engine, not assuming "+
			"the divergence is safely absent", value)
	}

	var buf bytes.Buffer
	marshalErr := graphql.MarshalFloatContext(value).MarshalGQLContext(ctx, &buf)
	if marshalErr == nil {
		t.Fatalf("expected gqlgen's MarshalFloatContext to refuse a NaN value (its own float.go:38-45 "+
			"check), got a clean write of %q instead -- either gqlgen's behavior changed underneath "+
			"this pinned dependency version, or this value was not actually NaN by the time it reached "+
			"the marshaler", buf.String())
	}
	if got := marshalErr.Error(); got != "cannot marshal infinite no NaN float values" {
		t.Fatalf("expected gqlgen's exact stock error text, got %q -- if gqlgen's wording changed, "+
			"that's fine to update here, but confirm it's still the SAME NaN/Inf guard and not a "+
			"different failure standing in for it", got)
	}
}
