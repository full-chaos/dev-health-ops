//go:build integration

package daily

// CHAOS-13926 (F3): a run re-run on the next day must not leave its first day with a stored row of 0 counted cases and pass
// rate 0. The suite read counts the newest attempt of each report slot; the case read's "run is in today" test must follow the
// same kept suites (form F-B), while a case's own-suite day test keeps every suite, so the attempt-1 failure still counts as a
// flake on the newest attempt's day. Every cell syncs through the production chunked route and sink into ClickHouse (the real
// migration chain) and computes through the production daily executors.
//
// Legacy rows (written before migration 151) are made from route-written rows by re-keying them the way the writer before 151
// keyed them (suite_id = hash(run, artifact id, suite name), case_id = hash(suite_id, case name), no slot, attempt 0); the
// formula is checked against a key that writer stored (TestF3LegacyKeyFormulaMatchesTheRecordedPreSlotKey).

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/benchmarking"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const f3Run, f3Run2 = "43600000301", "43600000302"

// f3Key is the pre-151 writer's key: hashTestIdentifier's sha256 over the "::"-joined parts.
func f3Key(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "::")))
	return hex.EncodeToString(sum[:])
}

func TestF3LegacyKeyFormulaMatchesTheRecordedPreSlotKey(t *testing.T) {
	// providersync/github_tests_legacy_ids_test.go records this suite_id from the writer before 151 (run 43600000001, artifact 102, suite pkg/b).
	if got := f3Key("43600000001", "102", "pkg/b", ""); got != "e1fc7a762df81aecc2381b023958c48493810dc5c46cf192ea05549cb777049f" {
		t.Fatalf("legacy suite key formula %s differs from the recorded pre-slot key", got)
	}
}

type f3Env struct {
	t     *testing.T
	ctx   context.Context
	stack attemptsStack
}

func f3Start(t *testing.T) *f3Env {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	chschema.Apply(ctx, t, instance)
	return &f3Env{t: t, ctx: ctx, stack: attemptsStack{ctx: ctx, conn: conn}}
}

func (e *f3Env) exec(q string, args ...any) {
	e.t.Helper()
	if err := e.stack.conn.Exec(e.ctx, q, args...); err != nil {
		e.t.Fatalf("exec: %v\n%s", err, q)
	}
}

// f3Junit is one report member with the given raw testcase XML.
func f3Junit(t *testing.T, suite, timestamp, cases string) []byte {
	t.Helper()
	return legacyF3Zip(t, `<testsuites><testsuite name="`+suite+`" time="1" timestamp="`+timestamp+`">`+cases+`</testsuite></testsuites>`)
}

func legacyF3Zip(t *testing.T, xml string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	member, err := writer.Create("report.xml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := member.Write([]byte(xml)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func f3Case(name, status string) string {
	switch status {
	case "failed":
		return `<testcase classname="pkg" name="` + name + `" time="0.1"><failure message="Failed" type="">boom</failure></testcase>`
	case "skipped":
		return `<testcase classname="pkg" name="` + name + `" time="0.1"><skipped/></testcase>`
	}
	return `<testcase classname="pkg" name="` + name + `" time="0.1"/>`
}

// The cross-day re-run of one slot: attempt 1 on D (TestXX fails, TestXY passes), attempt 2 on D+1 (both pass). The attempt-2
// listing still holds attempt 1's artifact of the same name (it is superseded, not read again).
func f3Attempts(t *testing.T) (attempt1, attempt2 attemptsRunState) {
	a1 := attemptsArtifact{9301, "go-test-x", "2026-07-22T10:20:00Z", false,
		f3Junit(t, "pkg/x", "2026-07-22T10:02:00Z", f3Case("TestXX", "failed")+f3Case("TestXY", "passed"))}
	a2 := attemptsArtifact{9402, "go-test-x", "2026-07-23T00:30:00Z", false,
		f3Junit(t, "pkg/x", "2026-07-23T00:12:00Z", f3Case("TestXX", "passed")+f3Case("TestXY", "passed"))}
	attempt1 = attemptsRunState{attempt: 1, started: "2026-07-22T10:01:00Z", updated: "2026-07-22T10:30:00Z", artifacts: []attemptsArtifact{a1}}
	attempt2 = attemptsRunState{created: "2026-07-22T10:01:00Z", attempt: 2, started: "2026-07-23T00:10:00Z", updated: "2026-07-23T00:40:00Z",
		artifacts: []attemptsArtifact{a1, a2}}
	return attempt1, attempt2
}

// toLegacy re-keys the slotted rows of one attempt of run as the writer before 151 stored them for the artifact.
func (e *f3Env) toLegacy(org, run string, attempt int, artifactID string) {
	e.t.Helper()
	rows, err := e.stack.conn.Query(e.ctx, `SELECT suite_id, suite_name FROM test_suite_results FINAL
WHERE org_id = ? AND run_id = ? AND report_slot != '' AND run_attempt = ?`, org, run, uint32(attempt))
	if err != nil {
		e.t.Fatal(err)
	}
	var olds, news []string
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			e.t.Fatal(err)
		}
		olds, news = append(olds, id), append(news, f3Key(run, artifactID, name, ""))
	}
	rows.Close()
	if len(olds) == 0 {
		e.t.Fatalf("toLegacy %s/%s attempt %d: no slotted suite rows", org, run, attempt)
	}
	for _, table := range []string{"test_suite_results", "test_case_results"} {
		columns := e.columns(table)
		exprs := make([]string, 0, len(columns))
		var args []any
		for _, column := range columns {
			switch column {
			case "suite_id":
				exprs = append(exprs, "transform(suite_id, ?, ?, suite_id)")
				args = append(args, olds, news)
			case "case_id":
				exprs = append(exprs, "lower(hex(SHA256(concat(transform(suite_id, ?, ?, suite_id), '::', case_name))))")
				args = append(args, olds, news)
			case "report_slot":
				exprs = append(exprs, "''")
			case "run_attempt":
				exprs = append(exprs, "toUInt32(0)")
			case "legacy_suite_ids":
				exprs = append(exprs, "CAST([] AS Array(String))")
			default:
				exprs = append(exprs, "`"+column+"`")
			}
		}
		quoted := make([]string, len(columns))
		for i, column := range columns {
			quoted[i] = "`" + column + "`"
		}
		query := "INSERT INTO " + table + " (" + strings.Join(quoted, ", ") + ") SELECT " + strings.Join(exprs, ", ") +
			" FROM " + table + " FINAL WHERE org_id = ? AND run_id = ? AND has(?, suite_id)"
		args = append(args, org, run, olds)
		if err := e.stack.conn.Exec(e.ctx, query, args...); err != nil {
			e.t.Fatalf("toLegacy %s: %v", table, err)
		}
		e.exec("ALTER TABLE "+table+" DELETE WHERE org_id = ? AND run_id = ? AND has(?, suite_id) SETTINGS mutations_sync = 2", org, run, olds)
	}
}

func (e *f3Env) columns(table string) []string {
	e.t.Helper()
	rows, err := e.stack.conn.Query(e.ctx, `SELECT name FROM system.columns WHERE database = currentDatabase() AND table = ?
  AND default_kind NOT IN ('MATERIALIZED', 'ALIAS', 'EPHEMERAL') ORDER BY position`, table)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, name)
	}
	return out
}

// compute runs the production pipeline and test executors for the org's repository and the day.
func (e *f3Env) compute(org string, day time.Time) {
	e.t.Helper()
	repo := e.stack.repoID(e.t, org)
	run := Run{ID: fmt.Sprintf("run-%s-%s-%d", org, day.Format(time.DateOnly), time.Now().UnixNano()), OrganizationID: org, TargetDay: day}
	part := Partition{ID: "part-" + run.ID, RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo.String())}}
	pipeline, err := NewTestopsPipelineExecutor(e.stack.conn)
	if err != nil {
		e.t.Fatal(err)
	}
	tests, err := NewTestopsTestExecutor(e.stack.conn)
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := pipeline.ComputeFamily(e.ctx, run, part); err != nil {
		e.t.Fatal(err)
	}
	if _, err := tests.ComputeFamily(e.ctx, run, part); err != nil {
		e.t.Fatal(err)
	}
	// computed_at has millisecond resolution: keep two computes of one day apart.
	time.Sleep(5 * time.Millisecond)
}

// row is the stored day row (the newest computed_at), or nil. A newest row that retires the day (retracted = 1) is no row.
func (e *f3Env) row(org string, day time.Time) map[string]any {
	e.t.Helper()
	rows, err := e.stack.conn.Query(e.ctx, `SELECT formatRow('JSONEachRow', total_cases, passed_count, failed_count, skipped_count, pass_rate,
  failure_rate, total_suites, flake_cases, distinct_cases, flake_rate, failure_recurrence_score)
FROM (SELECT * FROM testops_test_metrics_daily FINAL WHERE org_id = ? AND day = ?) WHERE retracted = 0`, org, day)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out map[string]any
	n := 0
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			e.t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(text), &out); err != nil {
			e.t.Fatal(err)
		}
		n++
	}
	if n > 1 {
		e.t.Fatalf("%s %s: %d stored rows after FINAL", org, day.Format(time.DateOnly), n)
	}
	e.t.Logf("F3CELL ROW org=%s day=%s row=%v", org, day.Format(time.DateOnly), out)
	return out
}

func f3Num(row map[string]any, key string) float64 {
	switch value := row[key].(type) {
	case float64:
		return value
	case string:
		var f float64
		_, _ = fmt.Sscan(value, &f)
		return f
	}
	return -1
}

func (e *f3Env) wantNoRow(label, org string, day time.Time) {
	e.t.Helper()
	if row := e.row(org, day); row != nil {
		e.t.Errorf("%s: day %s stores %v; want no row (the run's counted suite moved to a later day)", label, day.Format(time.DateOnly), row)
	}
}

func (e *f3Env) wantRow(label, org string, day time.Time, want map[string]float64) map[string]any {
	e.t.Helper()
	row := e.row(org, day)
	if row == nil {
		e.t.Errorf("%s: day %s has no row; want %v", label, day.Format(time.DateOnly), want)
		return nil
	}
	for key, value := range want {
		if got := f3Num(row, key); got != value {
			e.t.Errorf("%s: day %s %s = %v, want %v (row %v)", label, day.Format(time.DateOnly), key, got, value, row)
		}
	}
	return row
}

func TestF3CrossDayRerun(t *testing.T) {
	f3Day := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	f3Next, f3Two := f3Day.Add(24*time.Hour), f3Day.Add(48*time.Hour)
	e := f3Start(t)
	at := func(day time.Time, h int) time.Time { return day.Add(time.Duration(h) * time.Hour) }
	attempt1, attempt2 := f3Attempts(t)
	moved := map[string]float64{"total_cases": 2, "passed_count": 2, "pass_rate": 1, "flake_cases": 1}

	t.Run("F3_1_a_cross_day_rerun_leaves_no_row_on_the_first_day", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: e.stack}
		org := "f3-1-" + uuid.NewString()[:8]
		e.stack.pass(t, org, f3Run, at(f3Day, 10), attempt1)
		e.stack.pass(t, org, f3Run, f3Next, attempt2)
		e.compute(org, f3Day)
		e.compute(org, f3Next)
		e.wantNoRow("F3-1", org, f3Day)
		e.wantRow("F3-1", org, f3Next, moved)

		// The stored pass_rate readers: benchmarking (testMetric("pass_rate")) sees no point on D.
		loader, err := benchmarking.NewClickHouseLoader(e.stack.conn, org)
		if err != nil {
			t.Fatal(err)
		}
		for _, scope := range []string{benchmarking.ScopeGlobal, benchmarking.ScopeRepo} {
			series, err := loader.FetchMetricSeriesByScope(e.ctx, "pass_rate", f3Day, f3Next, scope)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("F3CELL BENCH scope=%s series=%v", scope, series)
			for key, points := range series {
				for _, point := range points {
					if point.Day.Equal(f3Day) {
						t.Errorf("F3-1 benchmarking %s %s: a point on D (value %v); want none", scope, key, point.Value)
					}
				}
			}
		}
	})

	t.Run("F3_2_legacy_attempt_1_then_a_slotted_rerun", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: e.stack}
		org := "f3-2-" + uuid.NewString()[:8]
		e.stack.pass(t, org, f3Run, at(f3Day, 10), attempt1)
		e.toLegacy(org, f3Run, 1, "9301")
		e.stack.pass(t, org, f3Run, f3Next, attempt2)
		e.compute(org, f3Day)
		e.compute(org, f3Next)
		e.wantNoRow("F3-2", org, f3Day)
		e.wantRow("F3-2", org, f3Next, moved)
	})

	t.Run("F3_3_both_attempts_legacy_then_the_slotted_reread_keeps_the_flake", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: e.stack}
		org := "f3-3-" + uuid.NewString()[:8]
		e.stack.pass(t, org, f3Run, at(f3Day, 10), attempt1)
		e.toLegacy(org, f3Run, 1, "9301")
		e.stack.pass(t, org, f3Run, f3Next, attempt2)
		e.toLegacy(org, f3Run, 2, "9402")
		e.stack.pass(t, org, f3Run, at(f3Next, 1), attempt2)
		e.compute(org, f3Day)
		e.compute(org, f3Next)
		e.wantNoRow("F3-3", org, f3Day)
		e.wantRow("F3-3", org, f3Next, moved)
	})

	t.Run("F3_4_G2_a_carried_slot_on_D_and_a_rerun_slot_on_D_plus_1", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: e.stack}
		org := "f3-4-" + uuid.NewString()[:8]
		a := attemptsArtifact{9501, "go-test-a", "2026-07-22T10:20:00Z", false, f3Junit(t, "pkg/a", "2026-07-22T10:02:00Z", f3Case("TestA", "passed"))}
		b1 := attemptsArtifact{9502, "go-test-b", "2026-07-22T10:20:00Z", false,
			f3Junit(t, "pkg/b", "2026-07-22T10:03:00Z", f3Case("TestB1", "failed")+f3Case("TestB2", "passed"))}
		b2 := attemptsArtifact{9602, "go-test-b", "2026-07-23T00:30:00Z", false,
			f3Junit(t, "pkg/b", "2026-07-23T00:12:00Z", f3Case("TestB1", "passed")+f3Case("TestB2", "passed"))}
		e.stack.pass(t, org, f3Run, at(f3Day, 10), attemptsRunState{attempt: 1, started: "2026-07-22T10:01:00Z", updated: "2026-07-22T10:30:00Z", artifacts: []attemptsArtifact{a, b1}})
		e.stack.pass(t, org, f3Run, f3Next, attemptsRunState{created: "2026-07-22T10:01:00Z", attempt: 2, started: "2026-07-23T00:10:00Z", updated: "2026-07-23T00:40:00Z", artifacts: []attemptsArtifact{a, b1, b2}})
		e.compute(org, f3Day)
		e.compute(org, f3Next)
		// G2 (acp e74e5f7a): a named residual. The same two-day counts must hold at the base and at the head.
		d, n := e.row(org, f3Day), e.row(org, f3Next)
		t.Logf("F3CELL G2 D total=%v distinct=%v flake=%v | D+1 total=%v distinct=%v flake=%v",
			f3Num(d, "total_cases"), f3Num(d, "distinct_cases"), f3Num(d, "flake_cases"), f3Num(n, "total_cases"), f3Num(n, "distinct_cases"), f3Num(n, "flake_cases"))
		// Recorded at ops c1f537f3 (base): D total 1, distinct 3 (b@1's cases count on D too), flake 0; D+1 total 2, distinct 3, flake 1.
		e.wantRow("F3-4", org, f3Day, map[string]float64{"total_cases": 1, "total_suites": 1, "distinct_cases": 3, "flake_cases": 0})
		e.wantRow("F3-4", org, f3Next, map[string]float64{"total_cases": 2, "total_suites": 1, "distinct_cases": 3, "flake_cases": 1})
	})

	t.Run("F3_5_a_later_day_still_sees_the_attempt_1_failure_as_history", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: e.stack}
		org := "f3-5-" + uuid.NewString()[:8]
		e.stack.pass(t, org, f3Run, at(f3Day, 10), attempt1)
		e.stack.pass(t, org, f3Run, f3Next, attempt2)
		later := attemptsArtifact{9701, "go-test-x", "2026-07-24T10:20:00Z", false,
			f3Junit(t, "pkg/x", "2026-07-24T10:02:00Z", f3Case("TestXX", "failed")+f3Case("TestXY", "passed"))}
		e.stack.pass(t, org, f3Run2, at(f3Two, 10), attemptsRunState{attempt: 1, started: "2026-07-24T10:01:00Z", updated: "2026-07-24T10:30:00Z", artifacts: []attemptsArtifact{later}})
		e.compute(org, f3Two)
		e.wantRow("F3-5", org, f3Two, map[string]float64{"total_cases": 2, "failed_count": 1, "failure_recurrence_score": 1})
	})

	t.Run("F3_6_off_default_branch_cross_day_rerun", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: e.stack}
		org := "f3-6-" + uuid.NewString()[:8]
		e.stack.pass(t, org, f3Run, at(f3Day, 10), attempt1)
		e.stack.pass(t, org, f3Run, f3Next, attempt2)
		// The fake lists every run on main; a default branch of trunk makes the run off the default branch.
		e.exec(`INSERT INTO repos (id, repo, provider, org_id, settings, created_at, last_synced) VALUES (?, 'acme/api', 'github', ?, ?, now(), now())`,
			e.stack.repoID(t, org).String(), org, `{"default_branch":"trunk"}`)
		e.compute(org, f3Day)
		e.compute(org, f3Next)
		// Through the writers the helper's run set is not visible: day D has no counted suite either way, so no row; D+1 keeps the
		// off-default-only marking (flake unknown).
		e.wantNoRow("F3-6", org, f3Day)
		if row := e.wantRow("F3-6", org, f3Next, map[string]float64{"total_cases": 2, "distinct_cases": 0}); row != nil && row["flake_rate"] != nil {
			t.Errorf("F3-6: D+1 flake_rate %v; want NULL (cases off the default branch only)", row["flake_rate"])
		}
		// The helper itself: on day D the run has no kept suite, so it holds no case row "today" by the suite rule.
		repo := e.stack.repoID(t, org)
		for _, c := range []struct {
			day  time.Time
			want bool
		}{{f3Day, false}, {f3Next, true}} {
			got, err := hasNativeTestopsOffDefaultCaseToday(e.ctx, e.stack.conn, org, repo, "trunk", c.day.AddDate(0, 0, -29), c.day, c.day.Add(24*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("F3CELL OFFDEFAULT day=%s got=%t", c.day.Format(time.DateOnly), got)
			if got != c.want {
				t.Errorf("F3-6 helper on %s = %t, want %t", c.day.Format(time.DateOnly), got, c.want)
			}
		}
	})

	t.Run("F3_7_an_all_skipped_day_is_unchanged", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: e.stack}
		org := "f3-7-" + uuid.NewString()[:8]
		skipped := attemptsArtifact{9801, "go-test-s", "2026-07-22T10:20:00Z", false,
			f3Junit(t, "pkg/s", "2026-07-22T10:02:00Z", f3Case("TestS1", "skipped")+f3Case("TestS2", "skipped"))}
		e.stack.pass(t, org, f3Run, at(f3Day, 10), attemptsRunState{attempt: 1, started: "2026-07-22T10:01:00Z", updated: "2026-07-22T10:30:00Z", artifacts: []attemptsArtifact{skipped}})
		e.compute(org, f3Day)
		// Pre-existing, not F3 (acp Q7, own ticket): pass_rate counts skipped cases in its denominator.
		e.wantRow("F3-7", org, f3Day, map[string]float64{"total_cases": 2, "skipped_count": 2, "pass_rate": 0})
	})

	t.Run("F3_8_G1_a_day_row_written_before_the_rerun_is_retired", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: e.stack}
		org := "f3-8-" + uuid.NewString()[:8]
		e.stack.pass(t, org, f3Run, at(f3Day, 10), attempt1)
		e.compute(org, f3Day)
		e.wantRow("F3-8 before", org, f3Day, map[string]float64{"total_cases": 2, "pass_rate": 0.5})
		e.stack.pass(t, org, f3Run, f3Next, attempt2)
		e.compute(org, f3Day)
		e.compute(org, f3Next)
		// G1: the recompute of D counts no kept suite, so a newer row retires the earlier D row (no zero row: the marker reads
		// as absent). The window D..D+1 counts the run once.
		e.wantNoRow("F3-8 after", org, f3Day)
		e.wantRow("F3-8 after", org, f3Next, moved)
	})
}
