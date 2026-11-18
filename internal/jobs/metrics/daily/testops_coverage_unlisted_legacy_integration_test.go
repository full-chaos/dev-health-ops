//go:build integration

package daily

// CHAOS-14146: a coverage row stored before migration 151 (report_slot '') that a later re-sync did not read again is named
// by no slotted row's legacy_snapshot_ids. The daily coverage read serves ONE snapshot per repo-day, the largest
// (run_id, snapshot_id), so such a row of an older attempt is served whenever its key sorts above the slotted row's: the day
// shows the old attempt's coverage. Cells sync through the production GitHub and GitLab routes and sink into ClickHouse (the
// real migration chain), re-key the pre-151 rows the way that writer keyed them (f3Key, checked in the cross-day cells), and
// compute through the production coverage and risk executors. Each shape must equal the same run synced after 151 only.

import (
	"archive/zip"
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func covLegacyZip(t *testing.T, name, body string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	member, err := writer.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := member.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

// covLegacyCobertura is a two-line report with `covered` lines hit.
func covLegacyCobertura(t *testing.T, covered int) []byte {
	hits := []string{"0", "0"}
	for i := 0; i < covered; i++ {
		hits[i] = "1"
	}
	return covLegacyZip(t, "coverage.xml", `<?xml version="1.0"?><coverage line-rate="0" branch-rate="0"><packages><package name="p"><classes>`+
		`<class name="c" filename="a.go"><lines><line number="1" hits="`+hits[0]+`"/><line number="2" hits="`+hits[1]+`"/></lines></class>`+
		`</classes></package></packages></coverage>`)
}

// rekeyCoverage writes the coverage rows of org/run that match where as pre-151 rows under snapshotID (no slot, attempt 0, no
// legacy ids), with linesCovered and the matching percentage when linesCovered >= 0, and deletes the originals when move.
func (e *f3Env) rekeyCoverage(org, run, where, snapshotID string, linesCovered int, move bool, args ...any) {
	e.t.Helper()
	columns := e.columns("coverage_snapshots")
	exprs := make([]string, 0, len(columns))
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted = append(quoted, "`"+column+"`")
		switch {
		case column == "snapshot_id":
			exprs = append(exprs, "'"+snapshotID+"'")
		case column == "report_slot":
			exprs = append(exprs, "''")
		case column == "run_attempt":
			exprs = append(exprs, "toUInt32(0)")
		case column == "legacy_snapshot_ids":
			exprs = append(exprs, "CAST([] AS Array(String))")
		case column == "lines_covered" && linesCovered >= 0:
			exprs = append(exprs, fmt.Sprintf("toNullable(toUInt32(%d))", linesCovered))
		case column == "line_coverage_pct" && linesCovered >= 0:
			exprs = append(exprs, fmt.Sprintf("toNullable(toFloat64(%d) * 100 / lines_total)", linesCovered))
		default:
			exprs = append(exprs, "`"+column+"`")
		}
	}
	all := append([]any{org, run}, args...)
	e.exec("INSERT INTO coverage_snapshots ("+strings.Join(quoted, ", ")+") SELECT "+strings.Join(exprs, ", ")+
		" FROM coverage_snapshots FINAL WHERE org_id = ? AND run_id = ? AND "+where, all...)
	if move {
		e.exec("ALTER TABLE coverage_snapshots DELETE WHERE org_id = ? AND run_id = ? AND "+where+" SETTINGS mutations_sync = 2", all...)
	}
}

// coverageServed runs the production coverage and risk executors for the day and returns the stored coverage row and the
// release-confidence coverage factor.
func (e *f3Env) coverageServed(org string, day time.Time) string {
	e.t.Helper()
	repo := e.stack.repoID(e.t, org)
	run := Run{ID: fmt.Sprintf("run-%s-%d", org, time.Now().UnixNano()), OrganizationID: org, TargetDay: day}
	part := Partition{ID: "part-" + run.ID, RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo.String())}}
	coverage, err := NewTestopsCoverageExecutor(e.stack.conn)
	if err != nil {
		e.t.Fatal(err)
	}
	risk, err := NewTestopsRiskExecutor(e.stack.conn)
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := coverage.ComputeFamily(e.ctx, run, part); err != nil {
		e.t.Fatal(err)
	}
	if _, err := risk.ComputeFamily(e.ctx, run, part); err != nil {
		e.t.Fatal(err)
	}
	var served string
	if err := e.stack.conn.QueryRow(e.ctx, `SELECT concat(
  'line_coverage_pct=', toString((SELECT any(line_coverage_pct) FROM testops_coverage_metrics_daily FINAL WHERE org_id = ? AND day = toDate(?))),
  ' lines_covered=', toString((SELECT any(lines_covered) FROM testops_coverage_metrics_daily FINAL WHERE org_id = ? AND day = toDate(?))),
  ' coverage_factor=', toString((SELECT any(coverage_factor) FROM testops_release_confidence FINAL WHERE org_id = ? AND day = toDate(?))))`,
		org, day.Format(time.DateOnly), org, day.Format(time.DateOnly), org, day.Format(time.DateOnly)).Scan(&served); err != nil {
		e.t.Fatal(err)
	}
	e.t.Logf("COVCELL %s served %s", org, served)
	return served
}

func (e *f3Env) slottedCoverageID(org, run string, attempt int) string {
	e.t.Helper()
	var id string
	if err := e.stack.conn.QueryRow(e.ctx, "SELECT snapshot_id FROM coverage_snapshots FINAL WHERE org_id = ? AND run_id = ? AND report_slot != '' AND run_attempt = ?",
		org, run, uint32(attempt)).Scan(&id); err != nil {
		e.t.Fatalf("slotted coverage %s/%s@%d: %v", org, run, attempt, err)
	}
	return id
}

// legacyIDAround is the first artifact or job id from 9900 whose pre-151 snapshot key sorts above (or below) slotted.
func legacyIDAround(run, format, path, slotted string, above bool) string {
	for id := 9900; id < 99999; id++ {
		key := f3Key(run, strconv.Itoa(id), format, path)
		if (key > slotted) == above && key != slotted {
			return strconv.Itoa(id)
		}
	}
	panic("no id")
}

func TestTestopsCoverageUnlistedLegacySnapshot(t *testing.T) {
	e := f3Start(t)
	stack := e.stack
	day := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	w1, w2 := day.Add(10*time.Hour), day.Add(11*time.Hour)
	const run = "43600001146"
	junit := covLegacyZip(t, "report.xml", `<testsuites><testsuite name="pkg/x" time="1" timestamp="2026-07-22T10:02:00">`+
		`<testcase classname="pkg" name="TestA" time="0.1"/></testsuite></testsuites>`)
	j := attemptsArtifact{9801, "go-test-x", "2026-07-22T10:20:00Z", false, junit}
	attempt1 := func(artifacts ...attemptsArtifact) attemptsRunState {
		return attemptsRunState{attempt: 1, started: "2026-07-22T10:01:00Z", updated: "2026-07-22T10:30:00Z", artifacts: artifacts}
	}
	attempt2 := func(artifacts ...attemptsArtifact) attemptsRunState {
		return attemptsRunState{created: "2026-07-22T10:01:00Z", attempt: 2, started: "2026-07-22T11:10:00Z", updated: "2026-07-22T11:40:00Z", artifacts: artifacts}
	}
	// Attempt 2's coverage-only artifact (100%). Attempt 1's (50%) is gone from the attempt-2 listing (a same-name re-upload).
	c2 := attemptsArtifact{9990, "coverage", "2026-07-22T11:30:00Z", false, covLegacyCobertura(t, 2)}
	slotted2 := f3Key(run, f3Key("github-report-slot", "coverage", "2"), "cobertura", "coverage.xml")

	control := "cov-ctl-" + uuid.NewString()[:8]
	stack.pass(t, control, run, w1, attempt1(j, attemptsArtifact{9901, "coverage", "2026-07-22T10:20:00Z", false, covLegacyCobertura(t, 1)}))
	stack.pass(t, control, run, w2, attempt2(j, c2))
	want := e.coverageServed(control, day)
	if !strings.Contains(want, "line_coverage_pct=100 ") {
		t.Fatalf("control serves %s; want attempt 2's 100%%", want)
	}

	// staleShape stores attempt 1 before 151 (artifact id), then re-syncs the run at attempt 2.
	staleShape := func(t *testing.T, org, artifactID string) {
		e := &f3Env{t: t, ctx: e.ctx, stack: e.stack}
		id, _ := strconv.Atoi(artifactID)
		stack.pass(t, org, run, w1, attempt1(j, attemptsArtifact{id, "coverage", "2026-07-22T10:20:00Z", false, covLegacyCobertura(t, 1)}))
		e.toLegacy(org, run, 1, "9801")
		e.rekeyCoverage(org, run, "report_slot != '' AND run_attempt = ?", f3Key(run, artifactID, "cobertura", "coverage.xml"), -1, true, uint32(1))
		stack.pass(t, org, run, w2, attempt2(j, c2))
		if got := e.slottedCoverageID(org, run, 2); got != slotted2 {
			t.Fatalf("slotted attempt-2 key %s, computed %s", got, slotted2)
		}
	}

	t.Run("github_stale_attempt_key_sorts_above_the_slotted_row", func(t *testing.T) {
		org := "cov-gh-above-" + uuid.NewString()[:8]
		staleShape(t, org, legacyIDAround(run, "cobertura", "coverage.xml", slotted2, true))
		if got := (&f3Env{t: t, ctx: e.ctx, stack: stack}).coverageServed(org, day); got != want {
			t.Fatalf("served %s; want the control's %s (attempt 1's unlisted pre-151 row must not be served)", got, want)
		}
	})

	t.Run("github_stale_attempt_key_sorts_below_the_slotted_row", func(t *testing.T) {
		org := "cov-gh-below-" + uuid.NewString()[:8]
		staleShape(t, org, legacyIDAround(run, "cobertura", "coverage.xml", slotted2, false))
		if got := (&f3Env{t: t, ctx: e.ctx, stack: stack}).coverageServed(org, day); got != want {
			t.Fatalf("served %s; want the control's %s", got, want)
		}
	})

	t.Run("github_unlisted_copy_with_the_same_values", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: stack}
		single := attempt1(j, attemptsArtifact{9991, "coverage", "2026-07-22T10:20:00Z", false, covLegacyCobertura(t, 2)})
		ctl, org := "cov-gh-dupc-"+uuid.NewString()[:8], "cov-gh-dup-"+uuid.NewString()[:8]
		stack.pass(t, ctl, run, w1, single)
		stack.pass(t, org, run, w1, single)
		slotted := e.slottedCoverageID(org, run, 1)
		e.rekeyCoverage(org, run, "report_slot != ''", f3Key(run, legacyIDAround(run, "cobertura", "coverage.xml", slotted, true), "cobertura", "coverage.xml"), -1, false)
		if got, w := e.coverageServed(org, day), e.coverageServed(ctl, day); got != w {
			t.Fatalf("served %s; want %s", got, w)
		}
	})

	t.Run("github_pre_151_run_never_re_synced_keeps_its_coverage", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: stack}
		single := attempt1(j, attemptsArtifact{9992, "coverage", "2026-07-22T10:20:00Z", false, covLegacyCobertura(t, 1)})
		ctl, org := "cov-gh-oldc-"+uuid.NewString()[:8], "cov-gh-old-"+uuid.NewString()[:8]
		stack.pass(t, ctl, run, w1, single)
		stack.pass(t, org, run, w1, single)
		e.toLegacy(org, run, 1, "9801")
		e.rekeyCoverage(org, run, "report_slot != ''", f3Key(run, "9992", "cobertura", "coverage.xml"), -1, true)
		got, w := e.coverageServed(org, day), e.coverageServed(ctl, day)
		if got != w || !strings.Contains(got, "line_coverage_pct=50 ") {
			t.Fatalf("served %s; want %s (a legacy row of a run with no slotted coverage row stays)", got, w)
		}
	})

	t.Run("gitlab_unlisted_pre_151_job_row_beside_slotted_jobs", func(t *testing.T) {
		e := &f3Env{t: t, ctx: e.ctx, stack: stack}
		const u1 = "2026-07-22T10:30:00Z"
		state := glState{updated: u1, report: glReport("unit", map[string]string{"TestX": "success"}),
			jobs: []glJob{{id: 11, name: "unit", artifact: glLcov(t, 2, 2)}, {id: 12, name: "integ", artifact: glLcov(t, 2, 2)}}}
		ctl, org := "cov-gl-ctl-"+uuid.NewString()[:8], "cov-gl-"+uuid.NewString()[:8]
		stack.glPass(t, ctl, w1, state)
		stack.glPass(t, org, w1, state)
		// Every current GitLab coverage row is slotted, attempt 1 included (gitlab_tests_attempts.go:191-193).
		if got := stack.glLabels(t, org, "coverage_snapshots"); got != "['integ@1','unit@1']" {
			t.Fatalf("GitLab coverage labels %s; want both jobs slotted at attempt 1", got)
		}
		var glRun, top string
		if err := stack.conn.QueryRow(e.ctx, "SELECT any(run_id), max(snapshot_id) FROM coverage_snapshots FINAL WHERE org_id = ?", org).Scan(&glRun, &top); err != nil {
			t.Fatal(err)
		}
		// A job of the pipeline that is no longer listed, stored before 151 with half the lines: its key sorts above both.
		e.rekeyCoverage(org, glRun, "snapshot_id = ?", f3Key(glRun, legacyIDAround(glRun, "lcov", "coverage.info", top, true), "lcov", "coverage.info"), 1, false, top)
		got, w := e.coverageServed(org, day), e.coverageServed(ctl, day)
		if got != w {
			t.Fatalf("served %s; want %s (the unlisted pre-151 job row must not be served)", got, w)
		}
	})
}
