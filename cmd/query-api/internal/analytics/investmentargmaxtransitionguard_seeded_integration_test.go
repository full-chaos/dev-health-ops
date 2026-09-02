//go:build integration

package analytics

// CHAOS-4759's durable, CI-enrolled real-engine regression test for the
// transition guard (investmentargmaxtransitionguard.go). Same harness as
// sankeycoverage_seeded_integration_test.go / investmentquality_seeded_integration_test.go
// (internal/testsupport/containers.StartClickHouse, the digest-pinned image
// every Go Testcontainers test in this repo uses) and the same base schema
// (investmentquality_seeded_integration_test.go's seededQualitySchemaDDL --
// this test only needs work_unit_investments out of its three tables).
//
// WHY A REAL ENGINE IS THE ONLY USEFUL TEST HERE: the whole defect class
// this guard exists to catch is an argMax NULL-SKIP semantic
// (investment.go's CHAOS-4547/CHAOS-4759 doc comments) that only a real
// ClickHouse engine evaluates -- a mocked row-scanner test can assert on
// generated SQL TEXT, never on what argMax(col, computed_at) actually
// picks when col is NULL on the newest row for a work unit.
//
// TWO ORGS, RED THEN GREEN:
//   - "argmax-guard-baseline" reproduces CHAOS-4759's own measured
//     shape (multi-generation work units where NO column transitions --
//     the "0 of 203" snapshot the ticket cites): FetchArgMaxNullTransitionState
//     must report all-zero divergence and RecordArgMaxNullTransitionGuard
//     must NOT fire. This is the guard's "nothing to see here" baseline --
//     the guard absent OR present, ordinary multi-generation data alone
//     must never trip it, which is what a naive "any multi-generation
//     unit" detector (the guard's absence) would get wrong in the other
//     direction: it would need to know which columns transitioned, not
//     merely that a unit has more than one generation.
//   - "argmax-guard-transition" constructs the exact mechanism CHAOS-4759
//     describes: one work unit's newest generation clears all four
//     Nullable columns (repo_id, provider, work_unit_type, work_unit_name)
//     that an earlier generation had set. FetchArgMaxNullTransitionState
//     must report a divergence on ALL FOUR columns and
//     RecordArgMaxNullTransitionGuard must fire through the real
//     ClickHouse-executed query.

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// argMaxGuardRow is one seeded work_unit_investments generation. A nil
// pointer field seeds SQL NULL for that Nullable column; a non-nil one
// seeds its value.
type argMaxGuardRow struct {
	workUnitID   string
	computedAt   string
	repoID       *string // UUID string, or nil for NULL
	provider     *string
	workUnitType *string
	workUnitName *string
}

func strPtr(s string) *string { return &s }

// seedArgMaxGuardRows inserts each row with its OWN INSERT statement --
// load-bearing, not stylistic. ClickHouse's `optimize_on_insert` setting
// (default ON) pre-merges a ReplacingMergeTree's rows that share the same
// ORDER BY sort key WITHIN a single inserted block, before they ever
// reach storage -- so a single batched INSERT carrying two generations of
// the SAME work_unit_id (this table's whole sort key; computed_at is the
// ReplacingMergeTree version column, not part of the key) collapses them
// down to one row at insert time, long before any SELECT, FINAL, or
// background merge is involved. Found by this test failing with
// MultiGenerationUnits=0 despite seeding two generations per unit; a raw
// `SELECT work_unit_id, computed_at FROM work_unit_investments` dump
// showed only the newest generation had survived. Production never hits
// this: each categorization run is its own job execution and its own
// INSERT, so a work unit's two generations are never in the same block.
// One INSERT per row reproduces that faithfully.
func seedArgMaxGuardRows(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, orgID string, rows []argMaxGuardRow) {
	t.Helper()
	nullableSQL := func(v *string) string {
		if v == nil {
			return "NULL"
		}
		return fmt.Sprintf("'%s'", *v)
	}
	nullableUUIDSQL := func(v *string) string {
		if v == nil {
			return "NULL"
		}
		return fmt.Sprintf("toUUID('%s')", *v)
	}

	for _, r := range rows {
		insert := fmt.Sprintf(
			`INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, work_unit_type, work_unit_name, org_id) VALUES ('%s', toDateTime64('2026-01-01 00:00:00',3), toDateTime64('2026-01-02 00:00:00',3), %s, %s, 'fte_days', 1.0, map(), map(), '', 0.5, 'moderate', 'ok', '', 'v1', 'hash', 'run-1', toDateTime64('%s',3), %s, %s, '%s')`,
			r.workUnitID, nullableUUIDSQL(r.repoID), nullableSQL(r.provider),
			r.computedAt, nullableSQL(r.workUnitType), nullableSQL(r.workUnitName), orgID,
		)
		if err := conn.Exec(ctx, insert); err != nil {
			t.Fatalf("seed work_unit_investments (work_unit_id=%s computed_at=%s): %v", r.workUnitID, r.computedAt, err)
		}
	}
}

const (
	argMaxGuardRepo1 = "11111111-1111-1111-1111-111111111111"
)

// TestArgMaxNullTransitionGuard_SeededRealClickHouse_BaselineThenTransition
// is the red-then-green proof: an ordinary multi-generation org (no
// transition) must not fire the guard; a constructed transition must.
func TestArgMaxNullTransitionGuard_SeededRealClickHouse_BaselineThenTransition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	for _, stmt := range splitSQLStatements(seededQualitySchemaDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const baselineOrg = "argmax-guard-baseline"
	const transitionOrg = "argmax-guard-transition"

	// --- RED: baseline org, ordinary multi-generation data, NO column
	// ever clears -- reproduces CHAOS-4759's own "0 of 203" measurement
	// shape. Two multi-generation units (both stable), one single-
	// generation unit (excluded by HAVING uniqExact(computed_at) > 1).
	seedArgMaxGuardRows(t, ctx, conn, baselineOrg, []argMaxGuardRow{
		{workUnitID: "wu-stable-1", computedAt: "2026-01-05 00:00:00", repoID: strPtr(argMaxGuardRepo1), provider: strPtr("github"), workUnitType: strPtr("feature"), workUnitName: strPtr("Stable One v1")},
		{workUnitID: "wu-stable-1", computedAt: "2026-01-06 00:00:00", repoID: strPtr(argMaxGuardRepo1), provider: strPtr("github"), workUnitType: strPtr("feature"), workUnitName: strPtr("Stable One v2")},
		{workUnitID: "wu-stable-2", computedAt: "2026-01-05 00:00:00", repoID: strPtr(argMaxGuardRepo1), provider: strPtr("gitlab"), workUnitType: strPtr("bug"), workUnitName: strPtr("Stable Two v1")},
		{workUnitID: "wu-stable-2", computedAt: "2026-01-06 00:00:00", repoID: strPtr(argMaxGuardRepo1), provider: strPtr("gitlab"), workUnitType: strPtr("bug"), workUnitName: strPtr("Stable Two v2")},
		{workUnitID: "wu-single", computedAt: "2026-01-05 00:00:00", repoID: strPtr(argMaxGuardRepo1), provider: strPtr("github"), workUnitType: strPtr("chore"), workUnitName: strPtr("Single Gen")},
	})

	baselineState, err := FetchArgMaxNullTransitionState(ctx, client, baselineOrg, 60)
	if err != nil {
		t.Fatalf("FetchArgMaxNullTransitionState(baseline) error = %v", err)
	}
	wantBaseline := ArgMaxNullTransitionState{MultiGenerationUnits: 2}
	if baselineState != wantBaseline {
		t.Fatalf("baseline state = %+v, want %+v (CHAOS-4759's own measured shape: zero divergence)", baselineState, wantBaseline)
	}
	if baselineState.Diverged() {
		t.Fatalf("baseline state reports Diverged() = true on ordinary multi-generation data with no transition")
	}

	resetArgMaxNullTransitionGate(t)
	var baselineCaptured *ArgMaxNullTransitionState
	origRecord := recordArgMaxNullTransition
	recordArgMaxNullTransition = func(_ context.Context, _ string, state ArgMaxNullTransitionState) {
		c := state
		baselineCaptured = &c
	}
	RecordArgMaxNullTransitionGuard(ctx, client, baselineOrg, 60)
	recordArgMaxNullTransition = origRecord
	if baselineCaptured != nil {
		t.Fatalf("RecordArgMaxNullTransitionGuard fired on baseline (no-transition) data: %+v", *baselineCaptured)
	}

	// --- GREEN: transition org. wu-transition's newest generation clears
	// ALL FOUR Nullable columns an earlier generation had set -- the
	// exact CHAOS-4759 mechanism. wu-stable is a second, non-transitioning
	// multi-generation unit seeded alongside it, so the guard must count
	// the divergence PER COLUMN rather than merely noticing "this org has
	// a transition somewhere".
	seedArgMaxGuardRows(t, ctx, conn, transitionOrg, []argMaxGuardRow{
		{workUnitID: "wu-transition", computedAt: "2026-01-05 00:00:00", repoID: strPtr(argMaxGuardRepo1), provider: strPtr("github"), workUnitType: strPtr("feature"), workUnitName: strPtr("Feature A")},
		{workUnitID: "wu-transition", computedAt: "2026-01-06 00:00:00", repoID: nil, provider: nil, workUnitType: nil, workUnitName: nil},
		{workUnitID: "wu-stable", computedAt: "2026-01-05 00:00:00", repoID: strPtr(argMaxGuardRepo1), provider: strPtr("gitlab"), workUnitType: strPtr("bug"), workUnitName: strPtr("Stable v1")},
		{workUnitID: "wu-stable", computedAt: "2026-01-06 00:00:00", repoID: strPtr(argMaxGuardRepo1), provider: strPtr("gitlab"), workUnitType: strPtr("bug"), workUnitName: strPtr("Stable v2")},
	})

	transitionState, err := FetchArgMaxNullTransitionState(ctx, client, transitionOrg, 60)
	if err != nil {
		t.Fatalf("FetchArgMaxNullTransitionState(transition) error = %v", err)
	}
	wantTransition := ArgMaxNullTransitionState{RepoID: 1, Provider: 1, WorkUnitType: 1, WorkUnitName: 1, MultiGenerationUnits: 2}
	if transitionState != wantTransition {
		t.Fatalf("transition state = %+v, want %+v", transitionState, wantTransition)
	}
	if !transitionState.Diverged() {
		t.Fatalf("transition state reports Diverged() = false on a constructed CHAOS-4759 transition")
	}

	resetArgMaxNullTransitionGate(t)
	var transitionCaptured *ArgMaxNullTransitionState
	recordArgMaxNullTransition = func(_ context.Context, gotOrgID string, state ArgMaxNullTransitionState) {
		if gotOrgID != transitionOrg {
			t.Errorf("recorder org_id = %q, want %q", gotOrgID, transitionOrg)
		}
		c := state
		transitionCaptured = &c
	}
	RecordArgMaxNullTransitionGuard(ctx, client, transitionOrg, 60)
	recordArgMaxNullTransition = origRecord
	if transitionCaptured == nil {
		t.Fatal("RecordArgMaxNullTransitionGuard did not fire on a constructed CHAOS-4759 transition")
	}
	if *transitionCaptured != wantTransition {
		t.Fatalf("recorder captured %+v, want %+v", *transitionCaptured, wantTransition)
	}
}

// TestArgMaxNullTransitionGuard_SeededRealClickHouse_SameTimestampTieDiverges
// is codex round-1's P2 finding, re-verified and closed: a work unit with
// exactly ONE distinct computed_at across its rows (a tie) is not
// "multi-generation" under `uniqExact(computed_at) > 1` -- the exact
// criterion CHAOS-4759's own cited baseline query uses -- yet
// argMax(col, ver) and (argMax(tuple(col), ver)).1 CAN pick DIFFERENT
// rows on a tied version. The guard's `HAVING count() > 1` (rows, not
// distinct timestamps) is the fix that stops such a unit being silently
// excluded; this test is its regression lock.
//
// THREADING NOTE (found running this test against a real engine, not
// guessed): whether the tie actually PRODUCES a divergence is itself
// execution-plan dependent. ClickHouse merges each thread's partial
// argMax/argMaxState(tuple(...)) state, and the two aggregate forms do
// not always resolve an equal-version tie to the SAME winner across that
// merge -- reproduced 6/6 with `SETTINGS max_threads = 1`, reproduced
// 0/6 with this engine's default thread count on a 2-row table. That
// makes the DEFAULT-settings production query path for this exact
// two-row fixture non-deterministic across environments (CPU count,
// data volume, ClickHouse version) -- exactly the shape of test this
// package must never ship (root AGENTS.md; CHAOS-3875's mutation-harness
// post-mortem: "never assert on a mechanism you cannot force
// deterministically"). So this test forces `max_threads=1` on its OWN
// dedicated client only, via a DSN query parameter -- NOT on the guard's
// production query path (investmentargmaxtransitionguard.go is
// unchanged) -- to pin the execution plan and make the regression
// reproducible. What this proves: IF ClickHouse's tie-break ever favors
// the stale non-null value (which it demonstrably can), `HAVING
// count() > 1` ensures the guard's own aggregate still SEES that unit and
// counts the resulting divergence, rather than filtering it out before
// the countIf ever runs -- which is the actual defect class this ledger
// entry closes. Whether any given production request's own thread count
// happens to trigger the tie is an orthogonal, inherent-to-ClickHouse
// property this guard does not control and is not responsible for.
func TestArgMaxNullTransitionGuard_SeededRealClickHouse_SameTimestampTieDiverges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	for _, stmt := range splitSQLStatements(seededQualitySchemaDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}

	const tieOrg = "argmax-guard-tie"
	const tieTimestamp = "2026-01-05 00:00:00"

	// Insertion ORDER matters for the tie -- verified live (5/5 runs
	// deterministic in this direction): the NULL row must land before the
	// non-NULL row for the plain/tuple argMax split to appear. Each row is
	// its own INSERT (see seedArgMaxGuardRows's doc comment on
	// optimize_on_insert) so the tie is a genuine two-row read, not an
	// artifact of in-block pre-merging.
	seedArgMaxGuardRows(t, ctx, conn, tieOrg, []argMaxGuardRow{
		{workUnitID: "wu-tie", computedAt: tieTimestamp, repoID: nil, provider: nil, workUnitType: nil, workUnitName: nil},
		{workUnitID: "wu-tie", computedAt: tieTimestamp, repoID: strPtr(argMaxGuardRepo1), provider: strPtr("github"), workUnitType: strPtr("feature"), workUnitName: strPtr("Tie Case")},
	})

	// max_threads=1 pins the execution plan -- see this test's doc comment.
	// inst.URI carries no query string of its own (containers.StartClickHouse),
	// so this is a plain append, not a merge.
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI + "?max_threads=1"})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	state, err := FetchArgMaxNullTransitionState(ctx, client, tieOrg, 60)
	if err != nil {
		t.Fatalf("FetchArgMaxNullTransitionState(tie) error = %v", err)
	}
	want := ArgMaxNullTransitionState{RepoID: 1, Provider: 1, WorkUnitType: 1, WorkUnitName: 1, MultiGenerationUnits: 1}
	if state != want {
		t.Fatalf("tie state = %+v, want %+v -- HAVING count() > 1 must catch a same-timestamp tie that HAVING uniqExact(computed_at) > 1 (CHAOS-4759's own cited baseline criterion) would miss", state, want)
	}
	if !state.Diverged() {
		t.Fatalf("tie state reports Diverged() = false on a constructed same-timestamp argMax tie")
	}
}
