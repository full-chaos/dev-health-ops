//go:build integration

package daily

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/compoundingrisk"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// compoundingRiskSchema is the production shape of the three tables this family
// touches. ONE copy, deliberately: it was inlined per-test, and a schema
// duplicated per test is how #2231's stale-sorting-key bug happened -- one copy
// drifted from production while the others looked fine, and the test that
// depended on the drifted copy silently stopped testing what it claimed.
var compoundingRiskSchema = []string{
	// Production shape: 001_metrics_v2.sql's repo_metrics_daily plus the
	// columns 004/006 add, with migration 027's sorting key
	// (org_id, repo_id, day). The column TYPES are the point of this test
	// -- three plain Float64, one UInt32, one Nullable(Float64) -- so they
	// are reproduced exactly rather than normalized to nullable floats.
	`CREATE TABLE repo_metrics_daily (
    repo_id UUID, day Date,
    rework_churn_ratio_30d Float64,
    single_owner_file_ratio_30d Float64,
    code_ownership_gini Float64 DEFAULT 0.0,
    bus_factor UInt32 DEFAULT 0,
    pr_first_review_p90_hours Nullable(Float64),
    computed_at DateTime, org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day)`,
	// 007_complexity_investment_issues.sql + migration 027's sorting key.
	`CREATE TABLE repo_complexity_daily (
    repo_id UUID, day Date, loc_total UInt64, cyclomatic_total UInt64,
    cyclomatic_per_kloc Float64, high_complexity_functions UInt64,
    very_high_complexity_functions UInt64,
    computed_at DateTime, org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day)`,
	// 040_compounding_risk_daily.sql verbatim, including the two Enum8s --
	// writing an out-of-range severity or scope string is a server-side
	// error here, which is itself part of what this test proves.
	`CREATE TABLE compounding_risk_daily (
    org_id String, day Date,
    scope Enum8('repo' = 1, 'team' = 2), scope_id String,
    compounding_risk Nullable(Float64),
    severity Enum8('unknown' = 0, 'low' = 1, 'elevated' = 2, 'high' = 3),
    churn_norm Nullable(Float64), complexity_norm Nullable(Float64),
    ownership_norm Nullable(Float64), review_norm Nullable(Float64),
    rework_churn Nullable(Float64), complexity_delta Nullable(Float64),
    bus_factor Nullable(Float64), ownership_gini Nullable(Float64),
    single_owner_ratio Nullable(Float64), review_latency_p90h Nullable(Float64),
    w_churn Float64, w_complexity Float64, w_ownership Float64, w_review Float64,
    threshold_elevated Float64, threshold_high Float64,
    computed_at DateTime DEFAULT now()
) ENGINE = MergeTree PARTITION BY toYYYYMM(day)
  ORDER BY (org_id, scope, scope_id, day, computed_at)`,
}

// TestCompoundingRiskComputeFamilyAgainstRealClickHouse is
// CompoundingRiskExecutor's live-ClickHouse proof (CHAOS-4287), run through the
// real production entry point rather than exercising the writer in isolation.
// It is the only layer that can prove four things the compute-side golden test
// structurally cannot:
//
//  1. WIRE TYPES. repo_metrics_daily's five input columns are NOT uniformly
//     Nullable(Float64) -- three are plain Float64 and bus_factor is a UInt32.
//     A fake RowScanner would happily hand back *float64 for all five and
//     prove nothing (CHAOS-4977). Only a real driver round-trip does.
//  2. LOADER PREDICATES. The argMax(col, computed_at) GROUP BY repo_id dedup,
//     and the complexity window's midpoint split, run as real SQL.
//  3. PARTITION SCOPING. Org A has TWO repos but the partition names only one;
//     exactly one row must come back. This is the guard on the deliberate
//     divergence documented on repoMetricsQuery -- Python's degenerate path
//     broadens to an org-wide refetch, and this port does not.
//  4. CROSS-TENANT ISOLATION. Org B's rows must never satisfy org A's read,
//     and no row may carry a blank org_id.
//
// Values are asserted BIT-EXACT against the same pure kernel the frozen Python
// golden covers, so this test proves the plumbing without forking a second
// definition of the arithmetic.
func TestCompoundingRiskComputeFamilyAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clickhouseInstance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range compoundingRiskSchema {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		orgA = "00000000-0000-4000-8000-0000000000a0"
		orgB = "00000000-0000-4000-8000-0000000000b0"
		// repoA1 is in the partition; repoA2 belongs to the same org but is
		// NOT, and must not produce a row.
		repoA1 = "00000000-0000-4000-8000-0000000000a1"
		repoA2 = "00000000-0000-4000-8000-0000000000a2"
		repoB1 = "00000000-0000-4000-8000-0000000000b1"
	)

	// Two rows for repoA1 with different computed_at: argMax must keep the
	// LATER one (0.15/0.35/0.20/3/12.0), never the earlier decoy.
	if err := conn.Exec(ctx, `
INSERT INTO repo_metrics_daily
(repo_id, day, rework_churn_ratio_30d, single_owner_file_ratio_30d, code_ownership_gini, bus_factor, pr_first_review_p90_hours, computed_at, org_id) VALUES
(toUUID('`+repoA1+`'), '2026-08-24', 0.99, 0.99, 0.99, 99, 999.0, '2026-08-24 06:00:00', '`+orgA+`'),
(toUUID('`+repoA1+`'), '2026-08-24', 0.15, 0.35, 0.20,  3,  12.0, '2026-08-24 09:00:00', '`+orgA+`'),
(toUUID('`+repoA2+`'), '2026-08-24', 0.50, 0.50, 0.50,  1,  50.0, '2026-08-24 09:00:00', '`+orgA+`'),
(toUUID('`+repoB1+`'), '2026-08-24', 0.15, 0.35, 0.20,  3,  12.0, '2026-08-24 09:00:00', '`+orgB+`')
`); err != nil {
		t.Fatal(err)
	}

	// Complexity window for the 2026-08-24 target day: window_start =
	// 2026-07-26 (day - 29), midpoint = window_start + 15 = 2026-08-10.
	// repoA1 gets rows on BOTH sides, so the delta resolves:
	// first_half avg = (10+12)/2 = 11, second_half avg = (13+15)/2 = 14,
	// delta = (14 - 11) / max(11, 1) = 3/11.
	if err := conn.Exec(ctx, `
INSERT INTO repo_complexity_daily
(repo_id, day, loc_total, cyclomatic_total, cyclomatic_per_kloc, high_complexity_functions, very_high_complexity_functions, computed_at, org_id) VALUES
(toUUID('`+repoA1+`'), '2026-08-01', 1000, 10, 10.0, 0, 0, '2026-08-01 01:00:00', '`+orgA+`'),
(toUUID('`+repoA1+`'), '2026-08-05', 1000, 12, 12.0, 0, 0, '2026-08-05 01:00:00', '`+orgA+`'),
(toUUID('`+repoA1+`'), '2026-08-15', 1000, 13, 13.0, 0, 0, '2026-08-15 01:00:00', '`+orgA+`'),
(toUUID('`+repoA1+`'), '2026-08-20', 1000, 15, 15.0, 0, 0, '2026-08-20 01:00:00', '`+orgA+`')
`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewCompoundingRiskExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	targetDay := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)

	// TYPE PIN. Every typed {name:Type} query parameter must be bound as a Go
	// STRING. clickhouse-go passes a Go string through verbatim and quotes
	// everything else, and the SERVER parses a wire-level typed parameter --
	// so the quotes land inside the literal and the type parser rejects it.
	// Both instances of this class have now been caught here, on a real
	// server, and nowhere else:
	//
	//   Date  (#2231 CI): Cannot parse date here:
	//     toDateTime('2026-08-24 00:00:00') cannot be parsed as Date
	//   UUID  (#2230 bigboy integration): Cannot parse UUID from String:
	//     invalid format, expected 32 hexadecimal digits
	//
	// The UUID one survived the Date fix because the pin only covered Date --
	// so it is written as a CLASS pin now, not a per-type one. Note the trap
	// that hid it: {repo_ids:Array(UUID)} binds []uuid.UUID happily, because
	// quoted elements are valid inside array-literal syntax. A passing array
	// bind says nothing about the scalar bind beside it.
	//
	// No compute-side test can reach any of this; it is a driver/server
	// contract. These calls exist so the BINDING is the thing under test,
	// failing with the message above instead of a confusing row mismatch.
	if _, err := executor.loader.LoadRepoMetrics(
		ctx, orgA, []uuid.UUID{uuid.MustParse(repoA1)}, targetDay,
	); err != nil {
		t.Fatalf("typed-parameter binding regressed in LoadRepoMetrics: %v", err)
	}
	if _, err := executor.loader.LoadComplexityDelta(
		ctx, orgA, uuid.MustParse(repoA1), targetDay, compoundingrisk.ComplexityWindowDays,
	); err != nil {
		t.Fatalf("typed-parameter binding regressed in LoadComplexityDelta: %v", err)
	}
	run := Run{ID: "run-a", OrganizationID: orgA, TargetDay: targetDay}
	partition := Partition{ID: "partition-a", RunID: "run-a", RepoIDs: []RepositoryID{RepositoryID(repoA1)}}

	rowsWritten, err := executor.ComputeFamily(ctx, run, partition)
	if err != nil {
		t.Fatal(err)
	}
	if rowsWritten != 1 {
		t.Fatalf(
			"ComputeFamily wrote %d rows, want exactly 1 -- org A has two repos but the "+
				"partition names one, and this family must not broaden to an org-wide read",
			rowsWritten,
		)
	}

	// The expected values come from the SAME pure kernel the frozen Python
	// golden covers, so this asserts the plumbing rather than restating the
	// arithmetic a second time.
	churn, gini := 0.15, 0.20
	singleOwner, review := 0.35, 12.0
	busFactor := 3.0
	delta := compoundingrisk.ComplexityDeltaRatio(11.0, 14.0)
	want := compoundingrisk.Compute(
		targetDay, repoA1, orgA,
		compoundingrisk.Inputs{
			ReworkChurn:       &churn,
			ComplexityDelta:   &delta,
			ReviewLatencyP90H: &review,
			SingleOwnerRatio:  &singleOwner,
			OwnershipGini:     &gini,
			BusFactor:         &busFactor,
		},
		time.Now().UTC(),
		compoundingrisk.DefaultWeights,
		compoundingrisk.DefaultThresholds,
		compoundingrisk.DefaultReferences,
	)

	var (
		gotScope     string
		gotScopeID   string
		gotScore     *float64
		gotSeverity  string
		gotChurnNorm *float64
		gotComplex   *float64
		gotOwnership *float64
		gotReview    *float64
		gotDelta     *float64
		gotBusFactor *float64
	)
	readback := conn.QueryRow(ctx, `
SELECT scope, scope_id, compounding_risk, severity,
       churn_norm, complexity_norm, ownership_norm, review_norm,
       complexity_delta, bus_factor
FROM compounding_risk_daily
WHERE org_id = ? AND day = '2026-08-24'`, orgA)
	if err := readback.Scan(
		&gotScope, &gotScopeID, &gotScore, &gotSeverity,
		&gotChurnNorm, &gotComplex, &gotOwnership, &gotReview,
		&gotDelta, &gotBusFactor,
	); err != nil {
		t.Fatal(err)
	}

	if gotScope != "repo" || gotScopeID != repoA1 {
		t.Errorf("got scope %q/%q, want repo/%s", gotScope, gotScopeID, repoA1)
	}
	if gotSeverity != want.Severity {
		t.Errorf("severity = %q, want %q", gotSeverity, want.Severity)
	}
	for _, pair := range []struct {
		name      string
		got, want *float64
	}{
		{"compounding_risk", gotScore, want.CompoundingRisk},
		{"churn_norm", gotChurnNorm, want.ChurnNorm},
		{"complexity_norm", gotComplex, want.ComplexityNorm},
		{"ownership_norm", gotOwnership, want.OwnershipNorm},
		{"review_norm", gotReview, want.ReviewNorm},
		{"complexity_delta", gotDelta, want.ComplexityDelta},
		{"bus_factor", gotBusFactor, want.BusFactor},
	} {
		if pair.got == nil || pair.want == nil {
			if pair.got != pair.want {
				t.Errorf("%s: got %v, want %v", pair.name, pair.got, pair.want)
			}
			continue
		}
		// Bit-exact: the round trip through ClickHouse's Float64 must not
		// perturb a value the golden pinned to the last bit.
		if math.Float64bits(*pair.got) != math.Float64bits(*pair.want) {
			t.Errorf(
				"%s: got %v (%#016x), want %v (%#016x)",
				pair.name, *pair.got, math.Float64bits(*pair.got),
				*pair.want, math.Float64bits(*pair.want),
			)
		}
	}

	// Cross-tenant and blank-org guards.
	var otherOrgRows uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM compounding_risk_daily WHERE org_id != ?`, orgA,
	).Scan(&otherOrgRows); err != nil {
		t.Fatal(err)
	}
	if otherOrgRows != 0 {
		t.Errorf(
			"%d row(s) written outside org A -- org_id leads this table's ORDER BY, so a "+
				"mislabelled row is invisible to every org-filtered read",
			otherOrgRows,
		)
	}

	// repoA2 is org A's, and in the table, but NOT in the partition.
	var strayRepoRows uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM compounding_risk_daily WHERE scope_id = ?`, repoA2,
	).Scan(&strayRepoRows); err != nil {
		t.Fatal(err)
	}
	if strayRepoRows != 0 {
		t.Errorf("wrote %d row(s) for a repo outside the partition's scope", strayRepoRows)
	}
}

// TestCompoundingRiskComplexityWindowOneSidedYieldsUnknown pins the
// missing-input path end to end: a repo whose complexity history sits entirely
// on ONE side of the window midpoint gets a NULL half from ClickHouse's avg(),
// which Python turns into None and which must block the composite -- score
// NULL, severity "unknown", and the row still written so absence of signal
// stays inspectable.
func TestCompoundingRiskComplexityWindowOneSidedYieldsUnknown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clickhouseInstance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range compoundingRiskSchema {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		org  = "00000000-0000-4000-8000-0000000000c0"
		repo = "00000000-0000-4000-8000-0000000000c1"
	)
	if err := conn.Exec(ctx, `
INSERT INTO repo_metrics_daily
(repo_id, day, rework_churn_ratio_30d, single_owner_file_ratio_30d, code_ownership_gini, bus_factor, pr_first_review_p90_hours, computed_at, org_id) VALUES
(toUUID('`+repo+`'), '2026-08-24', 0.15, 0.35, 0.20, 3, 12.0, '2026-08-24 09:00:00', '`+org+`')
`); err != nil {
		t.Fatal(err)
	}
	// Both rows are BEFORE the 2026-08-10 midpoint, so second_half is NULL.
	if err := conn.Exec(ctx, `
INSERT INTO repo_complexity_daily
(repo_id, day, loc_total, cyclomatic_total, cyclomatic_per_kloc, high_complexity_functions, very_high_complexity_functions, computed_at, org_id) VALUES
(toUUID('`+repo+`'), '2026-08-01', 1000, 10, 10.0, 0, 0, '2026-08-01 01:00:00', '`+org+`'),
(toUUID('`+repo+`'), '2026-08-05', 1000, 12, 12.0, 0, 0, '2026-08-05 01:00:00', '`+org+`')
`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewCompoundingRiskExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	rowsWritten, err := executor.ComputeFamily(
		ctx,
		Run{ID: "run-c", OrganizationID: org, TargetDay: time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)},
		Partition{ID: "partition-c", RunID: "run-c", RepoIDs: []RepositoryID{RepositoryID(repo)}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if rowsWritten != 1 {
		t.Fatalf("wrote %d rows, want 1 -- a missing input must still persist the row", rowsWritten)
	}

	var (
		score    *float64
		severity string
		delta    *float64
		churn    *float64
	)
	if err := conn.QueryRow(ctx, `
SELECT compounding_risk, severity, complexity_delta, churn_norm
FROM compounding_risk_daily WHERE org_id = ?`, org,
	).Scan(&score, &severity, &delta, &churn); err != nil {
		t.Fatal(err)
	}
	if score != nil {
		t.Errorf("compounding_risk = %v, want NULL (complexity delta unresolvable)", *score)
	}
	if delta != nil {
		t.Errorf("complexity_delta = %v, want NULL (no data past the window midpoint)", *delta)
	}
	if severity != compoundingrisk.SeverityUnknown {
		t.Errorf("severity = %q, want %q", severity, compoundingrisk.SeverityUnknown)
	}
	// The components that DID resolve are still persisted -- absence of one
	// signal must not erase the others.
	if churn == nil || *churn != 0.5 {
		t.Errorf("churn_norm = %v, want 0.5 (0.15/0.30) even though the composite is unknown", churn)
	}
}

// TestCompoundingRiskTupleDedupKeepsTheWinningRowsNullP90 is the guard the
// LEDGER previously CLAIMED existed and did not.
//
// codex r1 on #2230 caught that: the existing fixture's two candidate rows both
// carry a non-NULL pr_first_review_p90_hours, so a regression from one
// argMax(tuple(...)) back to five INDEPENDENT argMax calls would have passed
// unnoticed. The claim was in the ledger, the proof was not in the tree.
//
// The discriminating shape is a NEWER row whose p90 is NULL. Independent
// per-column argMax skips NULLs, so it keeps the OLDER row's 12.0 and pairs it
// with the newer row's other four columns -- a row that never existed in the
// table, and one that reports a score where the real latest snapshot has no
// review data at all.
//
// This test does not merely assert the correct answer. It runs BOTH query
// shapes against the SAME fixture and requires them to DISAGREE, because an
// assertion that the tuple form returns NULL proves nothing unless the
// independent form would have returned something else on that very data.
func TestCompoundingRiskTupleDedupKeepsTheWinningRowsNullP90(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clickhouseInstance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range compoundingRiskSchema {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		orgID  = "00000000-0000-4000-8000-0000000000c0"
		repoID = "00000000-0000-4000-8000-0000000000c1"
	)

	// OLDER row: p90 present. NEWER row: p90 NULL, every other column different.
	if err := conn.Exec(ctx, `
INSERT INTO repo_metrics_daily
(repo_id, day, rework_churn_ratio_30d, single_owner_file_ratio_30d, code_ownership_gini, bus_factor, pr_first_review_p90_hours, computed_at, org_id) VALUES
(toUUID('`+repoID+`'), '2026-08-24', 0.90, 0.90, 0.90, 9, 12.0, '2026-08-24 06:00:00', '`+orgID+`'),
(toUUID('`+repoID+`'), '2026-08-24', 0.15, 0.35, 0.20, 3, NULL, '2026-08-24 09:00:00', '`+orgID+`')
`); err != nil {
		t.Fatal(err)
	}

	// POSITIVE CONTROL: the independent-argMax shape, run against this exact
	// fixture. If it does NOT return 12.0, the fixture cannot exhibit the defect
	// and every assertion below is vacuous -- so this failing is a failure of
	// the test, stated as such.
	var independentP90 *float64
	if err := conn.QueryRow(ctx, `
SELECT argMax(pr_first_review_p90_hours, computed_at)
FROM repo_metrics_daily
WHERE org_id = ? AND repo_id = ? AND day = '2026-08-24'
GROUP BY org_id, repo_id`, orgID, repoID).Scan(&independentP90); err != nil {
		t.Fatal(err)
	}
	if independentP90 == nil || *independentP90 != 12.0 {
		t.Fatalf(
			"positive control: independent argMax returned %v, want 12 -- this fixture "+
				"cannot exhibit the Frankenstein defect, so the assertion below proves nothing",
			independentP90,
		)
	}

	// The shipped loader uses argMax(tuple(...)): the NEWER row wins WHOLE,
	// NULL p90 included.
	executor, err := NewCompoundingRiskExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := executor.loader.LoadRepoMetrics(
		ctx, orgID, []uuid.UUID{uuid.MustParse(repoID)},
		time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d row(s), want 1", len(rows))
	}
	got := rows[0]
	if got.PRFirstReviewP90Hours != nil {
		t.Errorf(
			"p90 = %v, want NULL -- the winning row's NULL was replaced by the older "+
				"row's value, which is the independent-argMax Frankenstein this dedup exists to prevent",
			*got.PRFirstReviewP90Hours,
		)
	}
	// The other four columns must ALSO come from the newer row. Checking only
	// the NULL would pass on a row that took p90 from the winner and the rest
	// from the loser -- the same defect with the columns swapped.
	deref := func(name string, value *float64) float64 {
		t.Helper()
		if value == nil {
			t.Fatalf("%s is NULL, want a value from the winning row", name)
		}
		return *value
	}
	for _, column := range []struct {
		name string
		got  float64
		want float64
	}{
		{"rework_churn_ratio_30d", deref("rework_churn_ratio_30d", got.ReworkChurnRatio30D), 0.15},
		{"single_owner_file_ratio_30d", deref("single_owner_file_ratio_30d", got.SingleOwnerFileRatio30D), 0.35},
		{"code_ownership_gini", deref("code_ownership_gini", got.CodeOwnershipGini), 0.20},
		{"bus_factor", deref("bus_factor", got.BusFactor), 3},
	} {
		if column.got != column.want {
			t.Errorf(
				"%s = %v, want %v -- this column did not come from the winning row, so the "+
					"dedup assembled one row out of two snapshots",
				column.name, column.got, column.want,
			)
		}
	}
}
