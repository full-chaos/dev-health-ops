//go:build integration

// Report completeness of the testops daily row and of the testopsTestWindow answer. The rule: the counted attempts of a run
// are the run_attempt values of its kept suite rows of the day; the newest attempt is the max of those and of its completeness
// rows (FINAL); the considered attempts are the counted ones and the newest. Partial when any considered attempt has a row with
// complete = 0; else unmarked when any considered attempt has no row; else complete. Partial and unmarked are disjoint.
package daily

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopswindow"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// C1: run A is synced through the production GitHub chunked route with one unreadable artifact (its
// completeness row is partial); run B has suite rows of the same repo-day and no completeness row (stored before the writer,
// or a provider that writes none). The production daily compute and writer then write the repo-day, and query-api answers
// testopsTestWindow over it. Want: the daily row counts report_runs=2, partial_report_runs=1, unmarked_report_runs=1, and the
// answer names the partial and the unmarked run.
func TestTestopsDailyAndWindowNameAPartialAndAnUnmarkedRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	chschema.Apply(ctx, t, instance)
	stack := attemptsStack{ctx: ctx, conn: conn}

	day := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	const made = "2026-07-22T10:20:00Z"
	passing := attemptsJUnit(t, "pkg/a", "2026-07-22T10:02:00", 0.2, map[string]string{"TestA1": "passed", "TestA2": "passed"})
	org := "org-14098-" + uuid.NewString()[:8]
	stack.partialPass(t, org, partialRunID, day.Add(10*time.Hour), attemptsRunState{
		attempt: 1, started: "2026-07-22T10:01:00Z", updated: "2026-07-22T10:30:00Z",
		artifacts: []attemptsArtifact{
			{101, "go-test-a", made, false, passing},
			{202, "go-test-b", made, false, []byte("not a zip archive")},
		},
	})
	repoID := stack.repoID(t, org)

	var complete uint8
	var reasons []string
	if err := conn.QueryRow(ctx, `SELECT complete, reasons FROM testops_run_report_completeness FINAL
WHERE org_id = ? AND repo_id = ? AND run_id = ?`, org, repoID, partialRunID).Scan(&complete, &reasons); err != nil {
		t.Fatalf("setup: no completeness row for run A: %v", err)
	}
	t.Logf("run A completeness row: complete=%d reasons=%v", complete, reasons)

	const unmarkedRunID = "43600000099"
	synced := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	if err := conn.Exec(ctx, `INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, suite_name, total_count, passed_count,
failed_count, started_at, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		org, repoID, unmarkedRunID, "suite-b", "pkg/c", uint32(3), uint32(2), uint32(1), day.Add(11*time.Hour), synced); err != nil {
		t.Fatalf("setup: run B suite row: %v", err)
	}
	var runBRows uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM testops_run_report_completeness FINAL WHERE org_id = ? AND run_id = ?`,
		org, unmarkedRunID).Scan(&runBRows); err != nil || runBRows != 0 {
		t.Fatalf("setup: run B has %d completeness rows (err %v), want 0", runBRows, err)
	}

	metrics, err := computeTestopsTestMetricsWithoutTeams(ctx, conn, org, repoID, day.AddDate(0, 0, -29), day, day.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writeTestopsTestMetrics(ctx, conn, org, day, time.Now().UTC(), metrics); err != nil {
		t.Fatal(err)
	}

	var total, passed, failed, suites uint32
	var reportRuns, partialRuns, unmarkedRuns *uint32
	if err := conn.QueryRow(ctx, `SELECT total_cases, passed_count, failed_count, total_suites, report_runs, partial_report_runs,
unmarked_report_runs FROM testops_test_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ?
ORDER BY computed_at DESC LIMIT 1`, org, repoID, day).Scan(&total, &passed, &failed, &suites, &reportRuns, &partialRuns, &unmarkedRuns); err != nil {
		t.Fatalf("daily row: %v", err)
	}
	show := func(p *uint32) string {
		if p == nil {
			return "NULL"
		}
		return strconv.FormatUint(uint64(*p), 10)
	}
	t.Logf("daily row: total=%d passed=%d failed=%d total_suites=%d report_runs=%s partial_report_runs=%s unmarked_report_runs=%s",
		total, passed, failed, suites, show(reportRuns), show(partialRuns), show(unmarkedRuns))
	if reportRuns == nil || *reportRuns != 2 || partialRuns == nil || *partialRuns != 1 || unmarkedRuns == nil || *unmarkedRuns != 1 {
		t.Errorf("RED daily row: report_runs=%s partial_report_runs=%s unmarked_report_runs=%s, want 2/1/1",
			show(reportRuns), show(partialRuns), show(unmarkedRuns))
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	since, until := graphqldate.New(day), graphqldate.New(day)
	answer, err := testopswindow.Resolve(ctx, client, org, since, until, testopswindow.Scope{})
	if err != nil {
		t.Fatalf("testopsTestWindow: %v", err)
	}
	body, err := json.Marshal(answer)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("testopsTestWindow answer: %s", body)
	if got := answer.ReportCompleteness; got == nil || got.ReportRuns != 2 || got.PartialRuns != 1 || got.UnmarkedRuns != 1 || got.UnmeasuredDays != 0 {
		t.Errorf("RED query-api: reportCompleteness = %+v, want reportRuns=2 partialRuns=1 unmarkedRuns=1 unmeasuredDays=0; its cases %+v",
			got, *answer.Cases)
	}
}

type completenessCounts struct{ reportRuns, partial, unmarked *uint32 }

type completenessSeed struct {
	t    *testing.T
	ctx  context.Context
	conn interface {
		Exec(context.Context, string, ...any) error
	}
	org    string
	repoID uuid.UUID
}

func (s completenessSeed) suite(runID, slot string, attempt uint32, started time.Time) {
	s.t.Helper()
	if err := s.conn.Exec(s.ctx, `INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, suite_name, report_slot, run_attempt,
total_count, passed_count, failed_count, started_at, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?, 2, 1, 1, ?, ?)`,
		s.org, s.repoID, runID, slot+"#"+itoa32(attempt), slot, slot, attempt, started, started.Add(time.Hour)); err != nil {
		s.t.Fatalf("seed suite: %v", err)
	}
}

func (s completenessSeed) mark(runID string, attempt uint32, complete bool, synced time.Time) {
	s.t.Helper()
	flag, reasons := uint8(0), []string{"unreadable_archive"}
	if complete {
		flag, reasons = 1, []string{}
	}
	version := uint64(flag)<<42 + uint64(synced.UnixMilli())
	if err := s.conn.Exec(s.ctx, `INSERT INTO testops_run_report_completeness (org_id, repo_id, run_id, run_attempt, provider, complete,
reasons, reports_expected, reports_admitted, last_synced, verdict_version) VALUES (?, ?, ?, ?, 'github', ?, ?, 2, ?, ?, ?)`,
		s.org, s.repoID, runID, attempt, flag, reasons, uint32(1+flag), synced, version); err != nil {
		s.t.Fatalf("seed completeness: %v", err)
	}
}

func itoa32(n uint32) string { return strconv.FormatUint(uint64(n), 10) }

func TestTestopsDailyRowCountsPartialAndUnmarkedRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	chschema.Apply(ctx, t, instance)
	// Unmerged parts: a read that is not FINAL sees both rows of an attempt (final_keeps_complete_over_later_partial).
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES testops_run_report_completeness"); err != nil {
		t.Fatal(err)
	}

	day := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	at := func(h int) time.Time { return day.Add(time.Duration(h) * time.Hour) }
	write := func(t *testing.T, org string, repoID uuid.UUID, d time.Time) completenessCounts {
		t.Helper()
		metrics, err := computeTestopsTestMetricsWithoutTeams(ctx, conn, org, repoID, d.AddDate(0, 0, -29), d, d.Add(24*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writeTestopsTestMetrics(ctx, conn, org, d, time.Now().UTC(), metrics); err != nil {
			t.Fatal(err)
		}
		var got completenessCounts
		if err := conn.QueryRow(ctx, `SELECT report_runs, partial_report_runs, unmarked_report_runs FROM testops_test_metrics_daily
WHERE org_id = ? AND repo_id = ? AND day = ? ORDER BY computed_at DESC LIMIT 1`, org, repoID, d).Scan(&got.reportRuns, &got.partial, &got.unmarked); err != nil {
			t.Fatalf("daily row: %v", err)
		}
		return got
	}
	want := func(t *testing.T, got completenessCounts, runs, partial, unmarked uint32) {
		t.Helper()
		if got.reportRuns == nil || got.partial == nil || got.unmarked == nil ||
			*got.reportRuns != runs || *got.partial != partial || *got.unmarked != unmarked {
			t.Fatalf("daily row report_runs/partial/unmarked = %v/%v/%v, want %d/%d/%d", got.reportRuns, got.partial, got.unmarked, runs, partial, unmarked)
		}
	}

	cells := []struct {
		name                    string
		seed                    func(s completenessSeed)
		runs, partial, unmarked uint32
	}{
		// C2a: the newest attempt of the slot is counted and is partial.
		{"C2a_attempt1_complete_attempt2_partial", func(s completenessSeed) {
			s.suite("r", "s", 1, at(10))
			s.suite("r", "s", 2, at(11))
			s.mark("r", 1, true, at(12))
			s.mark("r", 2, false, at(13))
		}, 1, 1, 0},
		// C2b: attempt 1 supplies no counted slot (attempt 2 replaced it): its partial row does not count.
		{"C2b_attempt1_partial_attempt2_complete_same_slot", func(s completenessSeed) {
			s.suite("r", "s", 1, at(10))
			s.suite("r", "s", 2, at(11))
			s.mark("r", 1, false, at(12))
			s.mark("r", 2, true, at(13))
		}, 1, 0, 0},
		// C2c (Q1): attempt 2 is counted and has no row of its own; the older attempt's complete row never vouches for it.
		{"C2c_counted_attempt_without_row_is_unmarked", func(s completenessSeed) {
			s.suite("r", "s", 1, at(10))
			s.suite("r", "s", 2, at(11))
			s.mark("r", 1, true, at(12))
		}, 1, 0, 1},
		// C2d: the newest attempt lost every report (no suite row) and is partial; attempt 1's slot is still counted.
		{"C2d_newest_attempt_without_suites_is_partial", func(s completenessSeed) {
			s.suite("r", "s", 1, at(10))
			s.mark("r", 1, true, at(12))
			s.mark("r", 2, false, at(13))
		}, 1, 1, 0},
		// Q2: re-run failed jobs. Slot a stays attempt 1 (partial), slot b is attempt 2 (complete): the counted data holds a partial attempt.
		{"Q2_rerun_failed_jobs_counted_partial_attempt", func(s completenessSeed) {
			s.suite("r", "a", 1, at(10))
			s.suite("r", "b", 1, at(10))
			s.suite("r", "b", 2, at(11))
			s.mark("r", 1, false, at(12))
			s.mark("r", 2, true, at(13))
		}, 1, 1, 0},
		// Q2 control: both attempts complete.
		{"Q2_control_both_attempts_complete", func(s completenessSeed) {
			s.suite("r", "a", 1, at(10))
			s.suite("r", "b", 1, at(10))
			s.suite("r", "b", 2, at(11))
			s.mark("r", 1, true, at(12))
			s.mark("r", 2, true, at(13))
		}, 1, 0, 0},
		// Partial wins over unmarked: attempt 1 counted and partial, attempt 2 counted with no row.
		{"partial_and_missing_reads_partial", func(s completenessSeed) {
			s.suite("r", "a", 1, at(10))
			s.suite("r", "b", 2, at(11))
			s.mark("r", 1, false, at(12))
		}, 1, 1, 0},
		// A legacy suite row (no slot, attempt 0) of a run with no completeness row: unmarked.
		{"legacy_row_without_mark_is_unmarked", func(s completenessSeed) {
			s.suite("r", "", 0, at(10))
		}, 1, 0, 1},
		// Three runs: complete, partial, unmarked.
		{"three_runs", func(s completenessSeed) {
			s.suite("c", "s", 1, at(10))
			s.mark("c", 1, true, at(12))
			s.suite("p", "s", 1, at(10))
			s.mark("p", 1, false, at(12))
			s.suite("u", "s", 1, at(10))
		}, 3, 1, 1},
		// A FINAL guard: a later partial walk of a complete attempt stays complete (verdict_version), so the read must use FINAL.
		{"final_keeps_complete_over_later_partial", func(s completenessSeed) {
			s.suite("r", "s", 1, at(10))
			s.mark("r", 1, true, at(12))
			s.mark("r", 1, false, at(14))
		}, 1, 0, 0},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			s := completenessSeed{t: t, ctx: ctx, conn: conn, org: "org-14098-" + uuid.NewString()[:8], repoID: uuid.New()}
			cell.seed(s)
			want(t, write(t, s.org, s.repoID, day), cell.runs, cell.partial, cell.unmarked)
		})
	}

	// C3: a run whose kept suites fall on two days counts on both days.
	t.Run("C3_run_on_two_days", func(t *testing.T) {
		s := completenessSeed{t: t, ctx: ctx, conn: conn, org: "org-14098-" + uuid.NewString()[:8], repoID: uuid.New()}
		s.suite("r", "a", 1, day.Add(23*time.Hour))
		s.suite("r", "b", 1, day.Add(25*time.Hour))
		s.mark("r", 1, false, day.Add(26*time.Hour))
		want(t, write(t, s.org, s.repoID, day), 1, 1, 0)
		want(t, write(t, s.org, s.repoID, day.AddDate(0, 0, 1)), 1, 1, 0)
	})

	// C4: the window answer sums measured days only and names the unmeasured ones (Q3); a pre-writer row adds nothing.
	t.Run("C4_window_sums_and_unmeasured_days", func(t *testing.T) {
		s := completenessSeed{t: t, ctx: ctx, conn: conn, org: "org-14098-" + uuid.NewString()[:8], repoID: uuid.New()}
		s.suite("p", "s", 1, at(10))
		s.mark("p", 1, false, at(12))
		s.suite("u", "s", 1, at(10))
		write(t, s.org, s.repoID, day)
		prior := day.AddDate(0, 0, -1)
		if err := conn.Exec(ctx, `INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, total_cases, passed_count, failed_count,
pass_rate, failure_rate, flake_rate, retry_dependency_rate, total_suites, failure_recurrence_score, computed_at)
VALUES (?, ?, ?, 4, 4, 0, 1, 0, 0, 0, 1, 0, ?)`, s.org, s.repoID, prior, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		// A retired day (its newest row is a retraction marker, NULL counts) is absent: neither measured nor unmeasured.
		retired := prior.AddDate(0, 0, -1)
		nine := uint32(9)
		for index, row := range []struct {
			counts *uint32
			flag   uint8
		}{{&nine, 0}, {nil, 1}} {
			if err := conn.Exec(ctx, `INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, total_cases, passed_count, failed_count,
pass_rate, failure_rate, flake_rate, retry_dependency_rate, total_suites, failure_recurrence_score, computed_at, report_runs,
partial_report_runs, unmarked_report_runs, retracted) VALUES (?, ?, ?, 2, 2, 0, 1, 0, 0, 0, 1, 0, ?, ?, ?, ?, ?)`,
				s.org, s.repoID, retired, time.Now().UTC().Add(time.Duration(index+1)*time.Second), row.counts, row.counts, row.counts, row.flag); err != nil {
				t.Fatal(err)
			}
		}
		client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: instance.URI})
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		answer, err := testopswindow.Resolve(ctx, client, s.org, graphqldate.New(retired), graphqldate.New(day), testopswindow.Scope{})
		if err != nil {
			t.Fatal(err)
		}
		got := answer.ReportCompleteness
		if got == nil || got.ReportRuns != 2 || got.PartialRuns != 1 || got.UnmarkedRuns != 1 || got.UnmeasuredDays != 1 {
			t.Fatalf("reportCompleteness = %+v, want reportRuns=2 partialRuns=1 unmarkedRuns=1 unmeasuredDays=1", got)
		}
	})
}

// failingCompletenessConn fails every read of testops_run_report_completeness and passes the rest through.
type failingCompletenessConn struct{ driver.Conn }

type refusedCompletenessRead struct{}

func (refusedCompletenessRead) Error() string { return "completeness read refused by the test" }

func (conn failingCompletenessConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if strings.Contains(query, "testops_run_report_completeness") {
		return nil, refusedCompletenessRead{}
	}
	return conn.Conn.Query(ctx, query, args...)
}

// C5: a failed completeness read fails the family: no row is written, never a row with 0 or NULL counts.
func TestTestopsDailyFailedCompletenessReadWritesNoRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	chschema.Apply(ctx, t, instance)

	day := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	s := completenessSeed{t: t, ctx: ctx, conn: conn, org: "org-14098-" + uuid.NewString()[:8], repoID: uuid.New()}
	s.suite("r", "s", 1, day.Add(10*time.Hour))
	s.mark("r", 1, false, day.Add(12*time.Hour))

	tests, err := NewTestopsTestExecutor(failingCompletenessConn{conn})
	if err != nil {
		t.Fatal(err)
	}
	run := Run{ID: "run-" + s.org, OrganizationID: s.org, TargetDay: day}
	part := Partition{ID: "part-" + s.org, RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(s.repoID.String())}}
	if _, err := tests.ComputeFamily(ctx, run, part); !errors.Is(err, refusedCompletenessRead{}) {
		t.Fatalf("ComputeFamily error = %v, want the completeness read error", err)
	}
	var rows uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM testops_test_metrics_daily WHERE org_id = ?`, s.org).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("testops_test_metrics_daily holds %d rows after a failed completeness read, want 0", rows)
	}

	// Control: the same seed through the real connection writes the row with its counts.
	tests, err = NewTestopsTestExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tests.ComputeFamily(ctx, run, part); err != nil {
		t.Fatal(err)
	}
	var runs, partial, unmarked *uint32
	if err := conn.QueryRow(ctx, `SELECT report_runs, partial_report_runs, unmarked_report_runs FROM testops_test_metrics_daily
WHERE org_id = ? ORDER BY computed_at DESC LIMIT 1`, s.org).Scan(&runs, &partial, &unmarked); err != nil {
		t.Fatal(err)
	}
	if runs == nil || partial == nil || unmarked == nil || *runs != 1 || *partial != 1 || *unmarked != 0 {
		t.Fatalf("control row report_runs/partial/unmarked = %v/%v/%v, want 1/1/0", runs, partial, unmarked)
	}
}
