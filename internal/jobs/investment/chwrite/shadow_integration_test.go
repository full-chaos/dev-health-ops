//go:build integration

package chwrite

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

var shadowColumns = []string{
	"org_id String", "work_unit_id String", "categorization_input_hash String", "shadow_config String",
	"rubric_sha256 String", "model_returned String", "state LowCardinality(String)",
	"categorization_status LowCardinality(String)", "complete_strict UInt8",
	"subcategory_distribution_json Map(String, Float64)", "theme_distribution_json Map(String, Float64)",
	"levels Map(String, UInt8)", "level_probabilities Map(String, Array(Float32))",
	"sufficiency_level Int8", "evidence_span_id String", "evidence_handle String",
	"evidence_source_type LowCardinality(String)", "evidence_source_id String",
	"warnings Array(String)", "error_codes Array(String)", "served_run_id String", "computed_at DateTime64(3)",
}

var attemptColumns = []string{
	"org_id String", "run_id String", "work_unit_id String", "role LowCardinality(String)", "config String",
	"rubric_sha256 String", "provider LowCardinality(String)", "api_mode LowCardinality(String)",
	"model_requested String", "model_returned String", "attempt UInt8", "kind LowCardinality(String)",
	"http_status UInt16", "error_class LowCardinality(String)", "state LowCardinality(String)",
	"request_id String", "input_tokens UInt32", "output_tokens UInt32", "cached_input_tokens UInt32",
	"billed_cost_usd Float64", "rates_version LowCardinality(String)", "latency_ms UInt32",
	"retry_wait_ms UInt32", "computed_at DateTime64(3)",
}

func tableColumns(t *testing.T, ctx context.Context, conn driver.Conn, table string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = ? ORDER BY position", table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, name+" "+typ)
	}
	return columns
}

// The reached state, from the real migration chain: both tables exist with the
// designed columns, engine, version column, sorting key (org_id first) and
// retention, and running the two migration files again is not an error and
// loses no row.
func TestShadowTablesHaveTheDesignedShapeAndMigrationsAreRerunnable(t *testing.T) {
	writer, conn, ctx := newTestWriter(t)

	for _, want := range []struct {
		table, sortingKey, ttlDays string
		columns                    []string
	}{
		{"work_unit_investment_shadow", "org_id, work_unit_id, categorization_input_hash, shadow_config", "toIntervalDay(90)", shadowColumns},
		{"llm_categorization_attempts", "org_id, run_id, work_unit_id, role, config, kind, attempt", "toIntervalDay(400)", attemptColumns},
	} {
		var engine, sortingKey, engineFull string
		if err := conn.QueryRow(ctx,
			"SELECT engine, sorting_key, engine_full FROM system.tables WHERE database = currentDatabase() AND name = ?", want.table,
		).Scan(&engine, &sortingKey, &engineFull); err != nil {
			t.Fatalf("%s is not in the migrated schema: %v", want.table, err)
		}
		if engine != "ReplacingMergeTree" || !strings.Contains(engineFull, "ReplacingMergeTree(computed_at)") {
			t.Errorf("%s engine = %q (%s), want ReplacingMergeTree(computed_at)", want.table, engine, engineFull)
		}
		if sortingKey != want.sortingKey {
			t.Errorf("%s sorting key = %q, want %q", want.table, sortingKey, want.sortingKey)
		}
		if !strings.Contains(engineFull, "TTL toDateTime(computed_at) + "+want.ttlDays) {
			t.Errorf("%s has no %s retention: %s", want.table, want.ttlDays, engineFull)
		}
		if got := tableColumns(t, ctx, conn, want.table); !reflect.DeepEqual(got, want.columns) {
			t.Errorf("%s columns =\n%v\nwant\n%v", want.table, got, want.columns)
		}
	}

	// A row written before the second run of the migrations must survive it.
	if _, err := writer.WriteShadowInvestments(ctx, testOrgID, []ShadowRecord{{WorkUnitID: "wu-1", InputHash: "h", ShadowConfig: "c", ComputedAt: time.Now()}}); err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	rerun := 0
	for _, file := range chain {
		if file.Version != "109_work_unit_investment_shadow.sql" && file.Version != "110_llm_categorization_attempts.sql" {
			continue
		}
		statements := chmigrate.SplitStatements(file.SQL)
		if len(statements) != 1 || !strings.Contains(statements[0], "CREATE TABLE IF NOT EXISTS") {
			t.Fatalf("%s must be exactly one CREATE TABLE IF NOT EXISTS statement, got %d", file.Version, len(statements))
		}
		for pass := 0; pass < 2; pass++ {
			if err := conn.Exec(ctx, statements[0]); err != nil {
				t.Fatalf("re-running %s (pass %d): %v", file.Version, pass+1, err)
			}
		}
		rerun++
	}
	if rerun != 2 {
		t.Fatalf("re-ran %d of the 2 migrations: the chain does not hold them", rerun)
	}
	if got := scanUint64(t, ctx, conn, `SELECT count() FROM work_unit_investment_shadow WHERE org_id = ?`, testOrgID); got != 1 {
		t.Fatalf("a re-run of the migrations changed the row count to %d", got)
	}
}

// A written row is read back through the dedup reader (argMax over the sorting
// key), a repeat of the same key reads as the newest row, and the theme map is
// the roll-up of the mix.
func TestShadowRowsRoundTripThroughTheDedupReader(t *testing.T) {
	writer, conn, ctx := newTestWriter(t)
	first := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	keys := units.SortedSubcategories
	mix := map[string]float64{keys[0]: 0.75, keys[1]: 0.25}

	ok := ShadowRecord{
		WorkUnitID: "wu-ok", InputHash: "h1", ShadowConfig: "cfg-a", RubricSHA256: "r", ModelReturned: "jev-1.13.0",
		State: "ok", CategorizationStatus: "ok", CompleteStrict: true,
		SubcategoryDistribution: mix,
		Levels:                  map[string]uint8{keys[0]: 4, keys[1]: 2},
		LevelProbabilities:      map[string][]float32{keys[0]: {0.1, 0.2, 0.3, 0.4}},
		SufficiencyLevel:        2, EvidenceSpanID: "s1", EvidenceHandle: "H1", EvidenceSourceType: "pr_body", EvidenceSourceID: "pr-1",
		Warnings: []string{"presence_floor_applied"}, ErrorCodes: nil, ServedRunID: "run-1", ComputedAt: first,
	}
	failure := ShadowRecord{
		WorkUnitID: "wu-fail", InputHash: "h2", ShadowConfig: "cfg-a", State: "evidence_none", CategorizationStatus: "invalid_llm_output",
		SufficiencyLevel: -1, ErrorCodes: []string{"evidence_none"}, ServedRunID: "run-1", ComputedAt: first,
	}
	if n, err := writer.WriteShadowInvestments(ctx, testOrgID, []ShadowRecord{ok, failure}); err != nil || n != 2 {
		t.Fatalf("write: %d, %v", n, err)
	}
	// The same key again, later, with another state: the reader must see it.
	newer := ok
	newer.State, newer.ComputedAt = "zero_support", first.Add(time.Minute)
	if _, err := writer.WriteShadowInvestments(ctx, testOrgID, []ShadowRecord{newer}); err != nil {
		t.Fatal(err)
	}

	const reader = `SELECT work_unit_id,
		argMax(state, computed_at), argMax(complete_strict, computed_at),
		argMax(subcategory_distribution_json, computed_at), argMax(theme_distribution_json, computed_at),
		argMax(levels, computed_at), argMax(level_probabilities, computed_at),
		argMax(sufficiency_level, computed_at), argMax(evidence_span_id, computed_at),
		argMax(warnings, computed_at), argMax(error_codes, computed_at)
		FROM work_unit_investment_shadow WHERE org_id = ?
		GROUP BY org_id, work_unit_id, categorization_input_hash, shadow_config ORDER BY work_unit_id`
	rows, err := conn.Query(ctx, reader, testOrgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type read struct {
		unit, state          string
		strict               uint8
		sub, theme           map[string]float64
		levels               map[string]uint8
		probabilities        map[string][]float32
		sufficiency          int8
		span                 string
		warnings, errorCodes []string
	}
	var got []read
	for rows.Next() {
		var r read
		if err := rows.Scan(&r.unit, &r.state, &r.strict, &r.sub, &r.theme, &r.levels, &r.probabilities, &r.sufficiency, &r.span, &r.warnings, &r.errorCodes); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("the dedup reader returned %d keys, want 2", len(got))
	}
	fail, okRead := got[0], got[1] // wu-fail < wu-ok
	if okRead.unit != "wu-ok" || okRead.state != "zero_support" {
		t.Errorf("the reader must return the newest row of a key: %+v", okRead)
	}
	if !reflect.DeepEqual(okRead.sub, mix) || !reflect.DeepEqual(okRead.theme, units.RollupSubcategoriesToThemes(mix)) {
		t.Errorf("mix %v / themes %v: the theme map must be the roll-up of the mix", okRead.sub, okRead.theme)
	}
	if okRead.strict != 1 || okRead.sufficiency != 2 || okRead.span != "s1" ||
		!reflect.DeepEqual(okRead.levels, ok.Levels) || !reflect.DeepEqual(okRead.probabilities, ok.LevelProbabilities) ||
		!reflect.DeepEqual(okRead.warnings, ok.Warnings) || len(okRead.errorCodes) != 0 {
		t.Errorf("typed columns did not round-trip: %+v", okRead)
	}
	if fail.unit != "wu-fail" || fail.state != "evidence_none" || len(fail.sub) != 0 || len(fail.theme) != 0 ||
		fail.sufficiency != -1 || !reflect.DeepEqual(fail.errorCodes, []string{"evidence_none"}) {
		t.Errorf("a failure row must have empty maps and sufficiency -1: %+v", fail)
	}
	// Org scoping.
	if got := scanUint64(t, ctx, conn, `SELECT count() FROM work_unit_investment_shadow WHERE org_id = ?`, "99999999-9999-4999-8999-999999999999"); got != 0 {
		t.Fatalf("another org sees %d rows", got)
	}
}

// One flush is one insert; a repeat of the same batch does not double the cost
// the dedup reader sums; the cap and the dropped count hold on the real table.
func TestAttemptRowsFlushAndTheReaderDedupsBeforeItSums(t *testing.T) {
	writer, conn, ctx := newTestWriter(t)
	at := time.Date(2026, 10, 7, 12, 0, 0, 123_000_000, time.UTC)
	fill := func(buffer *AttemptBuffer, n int) {
		for i := 0; i < n; i++ {
			row := attemptRow(fmt.Sprintf("wu-%d", i))
			row.ComputedAt, row.BilledCostUSD, row.LatencyMS, row.RequestID = at, 0.5, 100+uint32(i), "req-1"
			buffer.Add(row)
		}
	}

	buffer := NewAttemptBuffer(5)
	fill(buffer, 8)
	result := writer.FlushAttempts(ctx, testOrgID, buffer)
	if result != (AttemptFlushResult{Written: 5, Dropped: 3}) {
		t.Fatalf("result = %+v", result)
	}
	// The same rows again: an unmerged duplicate batch.
	buffer = NewAttemptBuffer(5)
	fill(buffer, 5)
	if result := writer.FlushAttempts(ctx, testOrgID, buffer); result.Written != 5 {
		t.Fatalf("second flush = %+v", result)
	}

	var dedupRows, dedupLatencyTotal, dedupCostMilli uint64
	if err := conn.QueryRow(ctx, `SELECT count(), sum(latency), toUInt64(sum(cost) * 1000) FROM (
		SELECT org_id, run_id, work_unit_id, role, attempt,
			argMax(latency_ms, computed_at) AS latency, argMax(billed_cost_usd, computed_at) AS cost
		FROM llm_categorization_attempts WHERE org_id = ? GROUP BY org_id, run_id, work_unit_id, role, attempt)`, testOrgID,
	).Scan(&dedupRows, &dedupLatencyTotal, &dedupCostMilli); err != nil {
		t.Fatal(err)
	}
	if dedupRows != 5 || dedupCostMilli != 2500 || dedupLatencyTotal != 100+101+102+103+104 {
		t.Fatalf("dedup rows %d, cost*1000 %d, latency %d: a repeat batch must not double the sum", dedupRows, dedupCostMilli, dedupLatencyTotal)
	}
	var role, kind, provider string
	var tokens uint32
	if err := conn.QueryRow(ctx, `SELECT role, kind, provider, input_tokens FROM llm_categorization_attempts WHERE org_id = ? AND work_unit_id = 'wu-0' LIMIT 1`, testOrgID).
		Scan(&role, &kind, &provider, &tokens); err != nil || role != RoleShadow || kind != "first" || provider != "typesafe" || tokens != 10 {
		t.Fatalf("row did not round-trip: %q %q %q %d %v", role, kind, provider, tokens, err)
	}
}

// A missing table is a stop signal for the shadow phase, never a returned error.
func TestFlushAttemptsOnAMissingTableIsSwallowedAndFlagged(t *testing.T) {
	writer, conn, ctx := newTestWriter(t)
	if err := conn.Exec(ctx, "DROP TABLE llm_categorization_attempts"); err != nil { // the scratch database of this test only
		t.Fatal(err)
	}
	buffer := NewAttemptBuffer(5)
	buffer.Add(attemptRow("wu-1"))
	result := writer.FlushAttempts(ctx, testOrgID, buffer)
	if !result.Failed || !result.TableMissing || result.Written != 0 {
		t.Fatalf("result = %+v", result)
	}
	if _, err := writer.WriteAttempts(ctx, testOrgID, []AttemptRecord{attemptRow("wu-1")}); !errors.Is(err, ErrShadowTableMissing) {
		t.Fatalf("WriteAttempts on a missing table: %v, want ErrShadowTableMissing", err)
	}
	if err := conn.Exec(ctx, "DROP TABLE work_unit_investment_shadow"); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.WriteShadowInvestments(ctx, testOrgID, []ShadowRecord{{WorkUnitID: "a", ComputedAt: time.Now()}}); !errors.Is(err, ErrShadowTableMissing) {
		t.Fatalf("WriteShadowInvestments on a missing table: %v, want ErrShadowTableMissing", err)
	}
}

// Two attempts that differ in config or in kind are two attempts. After a forced
// merge only an exact repeat of one row (the same batch inserted two times) may
// collapse into one. The sorting key is the identity the merge dedups on, so a
// key that lacks config or kind would silently delete one of the two rows.
func TestAttemptsThatDifferByConfigOrKindDoNotCollapseAfterAMerge(t *testing.T) {
	writer, conn, ctx := newTestWriter(t)
	at := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	base := attemptRow("wu-1")
	base.ComputedAt, base.Config, base.Kind = at, "cfg-a", "first"
	otherConfig := base
	otherConfig.Config = "cfg-b"
	otherKind := base
	otherKind.Kind = "repair"
	otherRole := base
	otherRole.Role = RoleFallback
	nextAttempt := base
	nextAttempt.Attempt = 2

	if n, err := writer.WriteAttempts(ctx, testOrgID, []AttemptRecord{base, otherConfig, otherKind, otherRole, nextAttempt}); err != nil || n != 5 {
		t.Fatalf("write: %d, %v", n, err)
	}
	// The same batch again: an exact repeat that the merge MAY collapse.
	if _, err := writer.WriteAttempts(ctx, testOrgID, []AttemptRecord{base, otherConfig, otherKind, otherRole, nextAttempt}); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, "OPTIMIZE TABLE llm_categorization_attempts FINAL"); err != nil {
		t.Fatal(err)
	}
	if got := scanUint64(t, ctx, conn, `SELECT count() FROM llm_categorization_attempts WHERE org_id = ?`, testOrgID); got != 5 {
		t.Fatalf("%d rows after a forced merge, want 5: five distinct attempts (config, kind, role and attempt each differ once) written twice must leave exactly five", got)
	}
	for _, config := range []string{"cfg-a", "cfg-b"} {
		if got := scanUint64(t, ctx, conn, `SELECT count() FROM llm_categorization_attempts WHERE org_id = ? AND config = ? AND kind = 'first' AND role = 'shadow' AND attempt = 1`, testOrgID, config); got != 1 {
			t.Fatalf("config %s: %d rows for the same run, unit, role, kind and attempt, want 1", config, got)
		}
	}
}

// Rows of two organizations that share every key value stay apart through a
// forced merge, in both tables, and each organization reads its own values.
func TestShadowAndAttemptRowsOfTwoOrganizationsSurviveAForcedMerge(t *testing.T) {
	writer, conn, ctx := newTestWriter(t)
	const otherOrg = "44444444-4444-4444-8444-444444444444"
	at := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	for i, org := range []string{testOrgID, otherOrg} {
		attempt := attemptRow("wu-1")
		attempt.ComputedAt, attempt.Config, attempt.BilledCostUSD = at, "cfg", float64(i+1)
		if _, err := writer.WriteAttempts(ctx, org, []AttemptRecord{attempt}); err != nil {
			t.Fatal(err)
		}
		shadow := ShadowRecord{WorkUnitID: "wu-1", InputHash: "h", ShadowConfig: "cfg", State: fmt.Sprintf("state-%d", i), ComputedAt: at}
		if _, err := writer.WriteShadowInvestments(ctx, org, []ShadowRecord{shadow}); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"llm_categorization_attempts", "work_unit_investment_shadow"} {
		if err := conn.Exec(ctx, "OPTIMIZE TABLE "+table+" FINAL"); err != nil {
			t.Fatal(err)
		}
	}
	for i, org := range []string{testOrgID, otherOrg} {
		if got := scanUint64(t, ctx, conn, `SELECT count() FROM llm_categorization_attempts WHERE org_id = ?`, org); got != 1 {
			t.Fatalf("org %d: %d attempt rows, want 1", i, got)
		}
		if got := scanString(t, ctx, conn, `SELECT toString(billed_cost_usd) FROM llm_categorization_attempts WHERE org_id = ?`, org); got != fmt.Sprint(i+1) {
			t.Fatalf("org %d reads cost %q, want its own %d", i, got, i+1)
		}
		if got := scanString(t, ctx, conn, `SELECT state FROM work_unit_investment_shadow WHERE org_id = ?`, org); got != fmt.Sprintf("state-%d", i) {
			t.Fatalf("org %d reads shadow state %q, want its own", i, got)
		}
	}
}
