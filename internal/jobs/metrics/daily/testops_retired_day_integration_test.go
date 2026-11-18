//go:build integration

package daily

// A testops_test_metrics_daily row written before a cross-day re-run is retired by the recompute of its day: the recompute
// counts no kept suite (the run's newest attempt moved to the next day), so the family writes a row over the key with the
// retracted marker, the team and service of the row it retires and computed_at strictly newer than it. Readers take the newest
// row of a key first and leave the key out when it holds the marker: the day reads as absent, never as 0. Every cell syncs
// through the production route and sink and computes through the production executors (the harness of
// testops_crossday_rerun_integration_test.go). Merges are stopped so a count of stored rows is the rows the family wrote.

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/benchmarking"
)

// retiredStored is every stored row of one (org, repo, day) key, read without FINAL, and its newest row.
type retiredStored struct {
	rows       uint64
	retracted  uint8
	totalCases uint32
	team       string
	service    string
	newest     time.Time
	previous   time.Time
}

func (e *f3Env) stored(org string, repo uuid.UUID, day time.Time) retiredStored {
	e.t.Helper()
	var got retiredStored
	if err := e.stack.conn.QueryRow(e.ctx, `SELECT count(), argMax(retracted, computed_at), argMax(total_cases, computed_at),
  ifNull(tupleElement(argMax(tuple(team_id), computed_at), 1), ''), ifNull(tupleElement(argMax(tuple(service_id), computed_at), 1), ''),
  max(computed_at), if(count() > 1, arrayElement(arrayReverseSort(groupArray(computed_at)), 2), toDateTime(0, 'UTC'))
FROM testops_test_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ?`, org, repo, day).Scan(
		&got.rows, &got.retracted, &got.totalCases, &got.team, &got.service, &got.newest, &got.previous); err != nil {
		e.t.Fatal(err)
	}
	e.t.Logf("RETIRED STORED org=%s repo=%s day=%s %+v", org, repo, day.Format(time.DateOnly), got)
	return got
}

func (e *f3Env) noBenchmarkPointOn(label, org string, day, end time.Time) {
	e.t.Helper()
	loader, err := benchmarking.NewClickHouseLoader(e.stack.conn, org)
	if err != nil {
		e.t.Fatal(err)
	}
	for _, metric := range []string{"pass_rate", "flake_rate"} {
		for _, scope := range []string{benchmarking.ScopeGlobal, benchmarking.ScopeRepo} {
			series, err := loader.FetchMetricSeriesByScope(e.ctx, metric, day, end, scope)
			if err != nil {
				e.t.Fatalf("%s %s %s: %v", label, metric, scope, err)
			}
			for key, points := range series {
				for _, point := range points {
					if point.Day.Equal(day) {
						e.t.Errorf("%s: benchmarking %s %s %s has a point on the retired day (value %v); want none, never 0",
							label, metric, scope, key, point.Value)
					}
				}
			}
		}
	}
}

func TestTestopsRetiredDay(t *testing.T) {
	f3Day := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	f3Next := f3Day.Add(24 * time.Hour)
	e := f3Start(t)
	e.exec("SYSTEM STOP MERGES testops_test_metrics_daily")
	at := func(day time.Time, h int) time.Time { return day.Add(time.Duration(h) * time.Hour) }
	attempt1, attempt2 := f3Attempts(t)
	moved := map[string]float64{"total_cases": 2, "passed_count": 2, "pass_rate": 1, "flake_cases": 1}

	t.Run("R1_R2_R3_retired_then_idempotent_then_live_again", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: e.stack}
		org := "ret-1-" + uuid.NewString()[:8]
		e.stack.pass(t, org, f3Run, at(f3Day, 10), attempt1)
		e.compute(org, f3Day)
		repo := e.stack.repoID(t, org)
		if got := e.stored(org, repo, f3Day); got.rows != 1 || got.retracted != 0 || got.totalCases != 2 {
			t.Fatalf("R1 before: D stores %+v, want one live row of 2 cases", got)
		}
		e.stack.pass(t, org, f3Run, f3Next, attempt2)
		e.compute(org, f3Day)
		e.compute(org, f3Next)

		// R1: D is retired by a marker row; D+1 holds the moved counts.
		got := e.stored(org, repo, f3Day)
		if got.rows != 2 || got.retracted != 1 || got.totalCases != 0 {
			t.Errorf("R1: D stores %+v, want 2 rows, the newest the marker (retracted 1, the column default 0 in total_cases)", got)
		}
		if !got.newest.After(got.previous) {
			t.Errorf("R1: the marker computed_at %v is not after the row it retires (%v)", got.newest, got.previous)
		}
		e.wantNoRow("R1", org, f3Day)
		e.wantRow("R1", org, f3Next, moved)
		e.noBenchmarkPointOn("R1", org, f3Day, f3Next)

		// R2: a second recompute writes nothing (the key is no longer live).
		e.compute(org, f3Day)
		if again := e.stored(org, repo, f3Day); again.rows != 2 || again.retracted != 1 {
			t.Errorf("R2: D stores %+v after a second recompute, want the same 2 rows (no second marker)", again)
		}

		// R3: a new run with a suite on D makes the day live again: its record is newer than the marker.
		run2 := attemptsRunState{attempt: 1, started: "2026-07-22T15:01:00Z", updated: "2026-07-22T15:30:00Z", artifacts: []attemptsArtifact{
			{9503, "go-test-y", "2026-07-22T15:20:00Z", false,
				f3Junit(t, "pkg/y", "2026-07-22T15:02:00Z", f3Case("TestYA", "passed")+f3Case("TestYB", "passed")+f3Case("TestYC", "failed"))},
		}}
		e.stack.pass(t, org, f3Run2, at(f3Day, 15), run2)
		// computed_at is whole seconds and the marker can sit one second ahead of the clock (one unit after the row it retired).
		time.Sleep(1100 * time.Millisecond)
		e.compute(org, f3Day)
		if live := e.stored(org, repo, f3Day); live.retracted != 0 || live.totalCases != 3 {
			t.Errorf("R3: D stores %+v after a new run on D, want a live newest row of the new run's 3 cases", live)
		}
		e.wantRow("R3", org, f3Day, map[string]float64{"total_cases": 3, "passed_count": 2, "failed_count": 1})
	})

	t.Run("R4_a_day_with_no_stored_row_gets_no_marker", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: e.stack}
		org := "ret-4-" + uuid.NewString()[:8]
		e.stack.pass(t, org, f3Run, at(f3Day, 10), attempt1)
		e.stack.pass(t, org, f3Run, f3Next, attempt2)
		e.compute(org, f3Day)
		if got := e.stored(org, e.stack.repoID(t, org), f3Day); got.rows != 0 {
			t.Errorf("R4: D stores %+v, want no row at all (missing stays missing, never a marker or a zero row)", got)
		}
	})

	t.Run("R5_R6_R7_scope_carried_values_and_a_strictly_newer_version", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: e.stack}
		org := "ret-5-" + uuid.NewString()[:8]
		e.stack.pass(t, org, f3Run, at(f3Day, 10), attempt1)
		e.compute(org, f3Day)
		repo := e.stack.repoID(t, org)
		// R6 + R7: the newest stored row of D names an owner and a service, and is stamped 30 s ahead of the clock.
		ahead := time.Now().UTC().Add(30 * time.Second).Truncate(time.Second)
		e.exec(`INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, total_cases, passed_count, failed_count, pass_rate,
  failure_rate, total_suites, team_id, service_id, computed_at) VALUES (?, ?, ?, 2, 1, 1, 0.5, 0.5, 1, 'team-x', 'svc-y', ?)`,
			org, repo, f3Day, ahead)
		// R5: another repository of the org with a live D row is in no partition of the compute below.
		other := uuid.New()
		e.exec(`INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, total_cases, passed_count, pass_rate, total_suites, computed_at)
  VALUES (?, ?, ?, 5, 5, 1, 1, ?)`, org, other, f3Day, time.Now().UTC().Add(-time.Hour))
		e.stack.pass(t, org, f3Run, f3Next, attempt2)
		e.compute(org, f3Day)

		got := e.stored(org, repo, f3Day)
		if got.retracted != 1 || got.team != "team-x" || got.service != "svc-y" {
			t.Errorf("R6: the marker is %+v, want retracted 1 carrying team-x and svc-y of the row it retires", got)
		}
		if want := ahead.Add(time.Second); !got.newest.Equal(want) {
			t.Errorf("R7: the marker computed_at is %v, want %v (one unit after the stored row stamped ahead of the clock)", got.newest, want)
		}
		if kept := e.stored(org, other, f3Day); kept.rows != 1 || kept.retracted != 0 {
			t.Errorf("R5: the repository in no partition stores %+v, want its one live row untouched", kept)
		}
	})

	t.Run("R8_a_stored_zero_row_of_an_older_build_is_retired_by_the_recompute", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: e.stack}
		org := "ret-8-" + uuid.NewString()[:8]
		e.stack.pass(t, org, f3Run, at(f3Day, 10), attempt1)
		e.stack.pass(t, org, f3Run, f3Next, attempt2)
		repo := e.stack.repoID(t, org)
		// The row the build before the kept-suite case read stored for D: 0 counted cases, pass rate 0, 2 distinct cases.
		e.exec(`INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, total_cases, pass_rate, total_suites, flake_cases,
  distinct_cases, computed_at) VALUES (?, ?, ?, 0, 0, 0, 1, 2, ?)`, org, repo, f3Day, time.Now().UTC().Add(-time.Hour))
		e.compute(org, f3Day)
		if got := e.stored(org, repo, f3Day); got.rows != 2 || got.retracted != 1 {
			t.Errorf("R8: D stores %+v, want the stored zero row and a newer marker", got)
		}
		e.wantNoRow("R8", org, f3Day)
		e.noBenchmarkPointOn("R8", org, f3Day, f3Next)
	})
}
