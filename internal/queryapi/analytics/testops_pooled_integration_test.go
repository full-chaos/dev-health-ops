//go:build integration

// The TestOps P95 durations and the queue time are pooled over the raw runs (ci_pipeline_runs) and suites
// (test_suite_results) of the repository-days the daily writer computed: one 95th percentile over every run of the
// series, never a mean of stored per-repository-day values. The TEAM series take the owners at the request (a co-owned
// repository counts whole for each owner); the ORG series counts each run once under the request's filters. The daily
// rows of every cell are written by the real writer (the native testops executors) from the same raw rows, so a
// one-repository-day value is compared with what the writer stored. Every cell runs on a real ClickHouse.
package analytics

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

type poolRun struct {
	id       string
	started  time.Time
	duration *float64
	finished *time.Time
	queue    *float64
	queued   *time.Time
	synced   time.Time
	team     string // the team_id stored on the raw row ("" = NULL)
	status   string // the stored status ("" = success)
}

type poolSuite struct {
	run, id  string
	started  *time.Time
	finished *time.Time
	duration *float64
	synced   time.Time
}

func pf(v float64) *float64               { return &v }
func pt(v time.Time) *time.Time           { return &v }
func atHour(d time.Time, h int) time.Time { return d.Add(time.Duration(h) * time.Hour) }

var poolSynced = time.Date(2026, 1, 9, 0, 0, 0, 0, time.UTC)

// ms is a DateTime64(3) value as text: a time.Time argument of a statement is sent at whole seconds, which would drop the
// millisecond the duration and queue samples are taken at.
func ms(v *time.Time) any {
	if v == nil {
		return nil
	}
	return v.UTC().Format("2006-01-02 15:04:05.000")
}

func (e *ownersEnv) runs(org, repo string, runs ...poolRun) {
	e.t.Helper()
	for _, r := range runs {
		synced := r.synced
		if synced.IsZero() {
			synced = poolSynced
		}
		var team any
		if r.team != "" {
			team = r.team
		}
		status := r.status
		if status == "" {
			status = "success"
		}
		e.exec(`INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, org_id, duration_seconds, queue_seconds, team_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			repo, r.id, status, ms(r.queued), ms(&r.started), ms(r.finished), ms(&synced), org, r.duration, r.queue, team)
	}
}

func (e *ownersEnv) suites(org, repo string, suites ...poolSuite) {
	e.t.Helper()
	for _, s := range suites {
		synced := s.synced
		if synced.IsZero() {
			synced = poolSynced
		}
		e.exec(`INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, duration_seconds, started_at, finished_at, org_id, last_synced) VALUES (?, ?, ?, ?, 1, 1, 0, 0, ?, ?, ?, ?, ?)`,
			repo, s.run, s.id, s.id, s.duration, ms(s.started), ms(s.finished), org, ms(&synced))
	}
}

// durations seeds one run and one suite per duration, all started on day: the pipeline and the suite P95 see the same
// samples.
func (e *ownersEnv) durations(org, repo string, day time.Time, values ...float64) {
	e.t.Helper()
	for i, v := range values {
		id := fmt.Sprintf("%s-%s-%d", repo[len(repo)-4:], day.Format("0102"), i)
		started := day.Add(time.Duration(i+1) * time.Minute)
		e.runs(org, repo, poolRun{id: id, started: started, duration: pf(v)})
		e.suites(org, repo, poolSuite{run: id, id: "s-" + id, started: pt(started), duration: pf(v)})
	}
}

// write runs the real daily writer (pipeline and test families) for one day over the named repositories.
func (e *ownersEnv) write(org string, day time.Time, repos ...string) {
	e.t.Helper()
	ids := make([]daily.RepositoryID, len(repos))
	for i, r := range repos {
		ids[i] = daily.RepositoryID(r)
	}
	run := daily.Run{ID: "run-" + org, OrganizationID: org, TargetDay: day}
	partition := daily.Partition{ID: "part-" + org, RunID: run.ID, RepoIDs: ids}
	pipeline, err := daily.NewTestopsPipelineExecutor(e.admin)
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := pipeline.ComputeFamily(e.ctx, run, partition); err != nil {
		e.t.Fatalf("pipeline writer: %v", err)
	}
	tests, err := daily.NewTestopsTestExecutor(e.admin)
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := tests.ComputeFamily(e.ctx, run, partition); err != nil {
		e.t.Fatalf("test writer: %v", err)
	}
}

// stored reads the newest writer row of one repository-day: p95 duration, mean queue, suite p95.
func (e *ownersEnv) stored(org, repo string, day time.Time) (p95, queue, suite *float64) {
	e.t.Helper()
	row := e.admin.QueryRow(e.ctx, `SELECT p95_duration_seconds, avg_queue_seconds FROM testops_pipeline_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ? ORDER BY computed_at DESC LIMIT 1`, org, repo, day)
	if err := row.Scan(&p95, &queue); err != nil {
		e.t.Fatalf("stored pipeline row: %v", err)
	}
	row = e.admin.QueryRow(e.ctx, `SELECT suite_duration_p95_seconds FROM testops_test_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ? ORDER BY computed_at DESC LIMIT 1`, org, repo, day)
	if err := row.Scan(&suite); err != nil {
		e.t.Fatalf("stored test row: %v", err)
	}
	return p95, queue, suite
}

func (e *ownersEnv) dimSeries(org string, dim model.DimensionInput, measure model.MeasureInput, interval model.BucketIntervalInput, filters *model.FilterInput) (map[string][]*float64, []*float64) {
	e.t.Helper()
	input := tsInput(dim, measure)
	input.Interval = interval
	got, err := resolveOneTimeseries(e.ctx, e.client, org, input, false, filters, true)
	if err != nil {
		e.t.Fatalf("%s %s: %v", dim, measure, err)
	}
	dims := map[string][]*float64{}
	var orgValues []*float64
	for _, s := range got {
		var values []*float64
		for _, b := range s.Buckets {
			values = append(values, b.Value)
		}
		if s.Scope == model.TimeseriesScopeOrg {
			orgValues = values
		} else {
			dims[s.DimensionValue] = values
		}
	}
	return dims, orgValues
}

var pooledDurations = []model.MeasureInput{model.MeasureInputPipelineDurationP95, model.MeasureInputTestSuiteDurationP95}

func teamScope(ids ...string) *model.FilterInput {
	return &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: ids}}
}

func wantOne(t *testing.T, what string, got []*float64, want float64) {
	t.Helper()
	if len(got) != 1 || got[0] == nil || math.Abs(*got[0]-want) > 1e-9 {
		t.Errorf("%s = %v, want one bucket of %v", what, values(got), want)
	}
}

func values(got []*float64) []string {
	out := make([]string, len(got))
	for i, v := range got {
		out[i] = str(v)
	}
	return out
}

// C1, C14, C13, C16, C19, C5, C11: the P95 of a team or of the org is one percentile over its pooled runs.
func TestTestopsPooledP95IsOnePercentileOverThePooledRuns(t *testing.T) {
	e := startOwnersEnv(t)

	// C1: team-a 3 runs [10,20,30] (p95 29), team-b 17 runs [10..170] (p95 162). Mean of the teams 95.5; ORG pooled 160.5.
	const org1 = "org-13858-two-teams"
	const a1, b1 = "13858000-0000-4000-8000-0000000001a1", "13858000-0000-4000-8000-0000000001b1"
	e.repo(org1, a1, "acme/a1")
	e.repo(org1, b1, "acme/b1")
	e.owner(org1, "team-a", a1, "acme/a1", 1, openFrom(), nil)
	e.owner(org1, "team-b", b1, "acme/b1", 1, openFrom(), nil)
	e.durations(org1, a1, day1, 10, 20, 30)
	e.durations(org1, b1, day1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170)
	e.write(org1, day1, a1, b1)
	for _, m := range pooledDurations {
		teams, orgValues := e.dimSeries(org1, model.DimensionInputTeam, m, model.BucketIntervalInputDay, nil)
		wantOne(t, string(m)+" C1 team-a", teams["team-a"], 29)
		wantOne(t, string(m)+" C1 team-b", teams["team-b"], 162)
		wantOne(t, string(m)+" C1 ORG (pooled 20 runs, not the team mean 95.5)", orgValues, 160.5)
	}

	// C14: ONE team, two repositories of different run counts: the team p95 is the pooled 160.5, not the mean of the
	// repository p95s (29+162)/2 = 95.5 that the stored rows give. The REPO series stay the per-repository p95s.
	const org14 = "org-13858-one-team"
	const a14, d14 = "13858000-0000-4000-8000-0000000014a1", "13858000-0000-4000-8000-0000000014d1"
	e.repo(org14, a14, "acme/a14")
	e.repo(org14, d14, "acme/d14")
	e.owner(org14, "team-a", a14, "acme/a14", 1, openFrom(), nil)
	e.owner(org14, "team-a", d14, "acme/d14", 1, openFrom(), nil)
	e.durations(org14, a14, day1, 10, 20, 30)
	e.durations(org14, d14, day1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170)
	e.write(org14, day1, a14, d14)
	for _, m := range pooledDurations {
		teams, orgValues := e.dimSeries(org14, model.DimensionInputTeam, m, model.BucketIntervalInputDay, nil)
		wantOne(t, string(m)+" C14 team-a (pooled, not the repo mean 95.5)", teams["team-a"], 160.5)
		wantOne(t, string(m)+" C14 ORG", orgValues, 160.5)
		repos, _ := e.dimSeries(org14, model.DimensionInputRepo, m, model.BucketIntervalInputDay, nil)
		wantOne(t, string(m)+" C14 repo a14", repos[a14], 29)
		wantOne(t, string(m)+" C14 repo d14", repos[d14], 162)
		// C16: a one-team scope reads one definition: the team series and the ORG series under that scope agree.
		scopedTeams, scopedOrg := e.dimSeries(org14, model.DimensionInputTeam, m, model.BucketIntervalInputDay, teamScope("team-a"))
		wantOne(t, string(m)+" C16 scoped team-a", scopedTeams["team-a"], 160.5)
		wantOne(t, string(m)+" C16 scoped ORG", scopedOrg, 160.5)
	}

	// C13 + C19: S is co-owned by team-a and team-b; A team-a only; B team-b only; C team-c only.
	// team-a pools A+S (8 runs) = 465; team-b pools S+B (6 runs) = 875: S counts WHOLE for each owner.
	// ORG filter [team-a, team-b] pools A+S+B with S once (9 runs) = 800 (S twice would give 675);
	// ORG with no filter adds C = 3200; ORG [team-c] = 5000.
	const org13 = "org-13858-co-owned"
	const a13, s13, b13, c13 = "13858000-0000-4000-8000-0000000013a1", "13858000-0000-4000-8000-0000000013c5", "13858000-0000-4000-8000-0000000013b1", "13858000-0000-4000-8000-0000000013cc"
	for id, name := range map[string]string{a13: "acme/a13", s13: "acme/s13", b13: "acme/b13", c13: "acme/c13"} {
		e.repo(org13, id, name)
	}
	e.owner(org13, "team-a", a13, "acme/a13", 1, openFrom(), nil)
	e.owner(org13, "team-a", s13, "acme/s13", 1, openFrom(), nil)
	e.owner(org13, "team-b", s13, "acme/s13", 0, openFrom(), nil)
	e.owner(org13, "team-b", b13, "acme/b13", 1, openFrom(), nil)
	e.owner(org13, "team-c", c13, "acme/c13", 1, openFrom(), nil)
	e.durations(org13, a13, day1, 10, 20, 30)
	e.durations(org13, s13, day1, 100, 200, 300, 400, 500)
	e.durations(org13, b13, day1, 1000)
	e.durations(org13, c13, day1, 5000)
	e.write(org13, day1, a13, s13, b13, c13)
	for _, m := range pooledDurations {
		teams, orgAll := e.dimSeries(org13, model.DimensionInputTeam, m, model.BucketIntervalInputDay, nil)
		wantOne(t, string(m)+" C13 team-a (A+S whole)", teams["team-a"], 465)
		wantOne(t, string(m)+" C13 team-b (S whole+B)", teams["team-b"], 875)
		wantOne(t, string(m)+" C13 team-c", teams["team-c"], 5000)
		wantOne(t, string(m)+" C19 ORG no filter", orgAll, 3200)
		_, orgAB := e.dimSeries(org13, model.DimensionInputTeam, m, model.BucketIntervalInputDay, teamScope("team-a", "team-b"))
		wantOne(t, string(m)+" C19 ORG [team-a, team-b] (S once)", orgAB, 800)
		_, orgC := e.dimSeries(org13, model.DimensionInputTeam, m, model.BucketIntervalInputDay, teamScope("team-c"))
		wantOne(t, string(m)+" C19 ORG [team-c]", orgC, 5000)
		// C5: a repository filter narrows the ORG the same way.
		_, orgRepo := e.dimSeries(org13, model.DimensionInputTeam, m, model.BucketIntervalInputDay, &model.FilterInput{What: &model.WhatFilterInput{Repos: []string{a13}}})
		wantOne(t, string(m)+" C5 ORG repo filter [a13]", orgRepo, 29)
	}

	// C11: another org with the same repository ids and days changes nothing of org13.
	const orgX = "org-13858-other"
	e.repo(orgX, a13, "acme/a13")
	e.owner(orgX, "team-a", a13, "acme/a13", 1, openFrom(), nil)
	e.durations(orgX, a13, day1, 99999)
	e.write(orgX, day1, a13)
	for _, m := range pooledDurations {
		teams, orgAll := e.dimSeries(org13, model.DimensionInputTeam, m, model.BucketIntervalInputDay, nil)
		wantOne(t, string(m)+" C11 team-a after another org's rows", teams["team-a"], 465)
		wantOne(t, string(m)+" C11 ORG after another org's rows", orgAll, 3200)
	}
}

// C3 / C15: the queue time is the summed queue seconds over the runs that have a queue sample, pooled.
// A: 1 run of 600 s; D: 9 runs of 60 s and 1 run with no queue sample. Pooled 1140/10 = 114; the mean of the stored
// repo-day means is (600+60)/2 = 330; over every run 1140/11 = 103.6.
func TestTestopsPooledQueueTimeIsTheSummedSecondsOverTheRunsWithASample(t *testing.T) {
	e := startOwnersEnv(t)
	const org = "org-13858-queue"
	const a, d = "13858000-0000-4000-8000-0000000015a1", "13858000-0000-4000-8000-0000000015d1"
	e.repo(org, a, "acme/qa")
	e.repo(org, d, "acme/qd")
	e.owner(org, "team-a", a, "acme/qa", 1, openFrom(), nil)
	e.owner(org, "team-a", d, "acme/qd", 1, openFrom(), nil)
	e.runs(org, a, poolRun{id: "qa-0", started: atHour(day1, 1), duration: pf(10), queue: pf(600)})
	for i := 0; i < 9; i++ {
		e.runs(org, d, poolRun{id: fmt.Sprintf("qd-%d", i), started: atHour(day1, 2+i), duration: pf(10), queue: pf(60)})
	}
	e.runs(org, d, poolRun{id: "qd-none", started: atHour(day1, 12), duration: pf(10)})
	e.write(org, day1, a, d)
	teams, orgValues := e.dimSeries(org, model.DimensionInputTeam, model.MeasureInputPipelineQueueTime, model.BucketIntervalInputDay, nil)
	wantOne(t, "C15 team-a queue (pooled, not 330, not 103.6)", teams["team-a"], 114)
	wantOne(t, "C3 ORG queue", orgValues, 114)
	repos, _ := e.dimSeries(org, model.DimensionInputRepo, model.MeasureInputPipelineQueueTime, model.BucketIntervalInputDay, nil)
	wantOne(t, "queue repo a", repos[a], 600)
	wantOne(t, "queue repo d", repos[d], 60)
}

// P1 (+C7, C8, C12): one repository, one day, raw rows that walk every branch of the writer's sample filter. The pooled
// REPO and ORG values equal what the real writer stored for that repository-day, within 1e-9.
func TestTestopsPooledSeriesEqualTheWriterOnOneRepositoryDay(t *testing.T) {
	e := startOwnersEnv(t)
	const org = "org-13858-parity"
	const repo = "13858000-0000-4000-8000-0000000016a1"
	e.repo(org, repo, "acme/parity")
	e.owner(org, "team-a", repo, "acme/parity", 1, openFrom(), nil)
	h := func(hh int) time.Time { return atHour(day1, hh) }
	e.runs(org, repo,
		poolRun{id: "explicit", started: h(1), duration: pf(42), queue: pf(7)},
		poolRun{id: "negative-derived", started: h(2), duration: pf(-5), finished: pt(h(2).Add(90 * time.Second)), queue: pf(-1), queued: pt(h(2).Add(-30 * time.Second))},
		poolRun{id: "null-derived-ms", started: h(3), finished: pt(h(3).Add(30500 * time.Millisecond)), queued: pt(h(3).Add(-1500 * time.Millisecond))},
		poolRun{id: "no-finish", started: h(4)},
		poolRun{id: "finish-before-start", started: h(5), finished: pt(h(5).Add(-time.Second)), queued: pt(h(5).Add(time.Second))},
		poolRun{id: "zero", started: h(6), duration: pf(0), queue: pf(0)},
		poolRun{id: "big", started: h(7), duration: pf(100)},
		// FINAL: an older version of "versioned" (1000 s) and the newest (12 s); only the newest counts.
		poolRun{id: "versioned", started: h(8), duration: pf(1000), queue: pf(900), synced: poolSynced.Add(-time.Hour)},
		poolRun{id: "versioned", started: h(8), duration: pf(12), queue: pf(3), synced: poolSynced},
		// Started a millisecond before the day: the day before, in neither.
		poolRun{id: "day-before", started: day1.Add(-time.Millisecond), duration: pf(77777), queue: pf(77777)},
		// A run the provider no longer has (stored status deleted, any case and padding): no sample, as the writer.
		poolRun{id: "deleted", started: h(9), duration: pf(5000), queue: pf(5000), status: "deleted"},
		poolRun{id: "deleted-padded", started: h(10), duration: pf(6000), queue: pf(6000), status: " Deleted "},
	)
	e.suites(org, repo,
		poolSuite{run: "explicit", id: "s1", started: pt(h(1)), duration: pf(12.5)},
		poolSuite{run: "explicit", id: "s2", started: pt(h(1)), duration: pf(0)},
		poolSuite{run: "explicit", id: "s3", started: pt(h(1)), duration: pf(-1)},
		poolSuite{run: "explicit", id: "s4", started: pt(h(1)), finished: pt(h(1).Add(time.Minute))},
		poolSuite{run: "explicit", id: "s5", finished: pt(h(2)), duration: pf(33)},
		poolSuite{run: "explicit", id: "s6", started: pt(day1.Add(-time.Minute)), finished: pt(h(1)), duration: pf(88888)},
		poolSuite{run: "explicit", id: "s7", started: pt(h(3)), duration: pf(400), synced: poolSynced.Add(-time.Hour)},
		poolSuite{run: "explicit", id: "s7", started: pt(h(3)), duration: pf(4), synced: poolSynced},
	)
	e.write(org, day1, repo)
	var derived float64
	if err := e.admin.QueryRow(e.ctx, `SELECT toFloat64(dateDiff('millisecond', queued_at, started_at)) FROM ci_pipeline_runs FINAL WHERE org_id = ? AND run_id = 'null-derived-ms'`, org).Scan(&derived); err != nil || derived != 1500 {
		t.Fatalf("seeded queue of null-derived-ms = %v ms (err %v), want 1500: the millisecond did not reach the store", derived, err)
	}
	p95, queue, suite := e.stored(org, repo, day1)
	if p95 == nil || queue == nil || suite == nil {
		t.Fatalf("writer stored p95 %s queue %s suite %s, want three values", str(p95), str(queue), str(suite))
	}
	t.Logf("P1 writer row: p95 %.17g, avg queue %.17g, suite p95 %.17g", *p95, *queue, *suite)
	for _, c := range []struct {
		measure model.MeasureInput
		want    float64
	}{{model.MeasureInputPipelineDurationP95, *p95}, {model.MeasureInputPipelineQueueTime, *queue}, {model.MeasureInputTestSuiteDurationP95, *suite}} {
		repos, orgValues := e.dimSeries(org, model.DimensionInputRepo, c.measure, model.BucketIntervalInputDay, nil)
		wantOne(t, string(c.measure)+" P1 REPO == writer", repos[repo], c.want)
		wantOne(t, string(c.measure)+" P1 ORG == writer", orgValues, c.want)
		teams, _ := e.dimSeries(org, model.DimensionInputTeam, c.measure, model.BucketIntervalInputDay, nil)
		wantOne(t, string(c.measure)+" P1 TEAM == writer", teams["team-a"], c.want)
	}
}

// C6, C9: only the repository-days the writer computed are in a series (a day the writer has not computed yet and a
// repository outside the daily scope are not), and a computed day with no sample is a null bucket, never 0.
func TestTestopsPooledSeriesCoverOnlyTheComputedRepositoryDays(t *testing.T) {
	e := startOwnersEnv(t)
	const org = "org-13858-coverage"
	const a, x, n = "13858000-0000-4000-8000-0000000017a1", "13858000-0000-4000-8000-0000000017e1", "13858000-0000-4000-8000-0000000017f1"
	e.repo(org, a, "acme/ca")
	e.repo(org, n, "acme/cn")
	e.owner(org, "team-a", a, "acme/ca", 1, openFrom(), nil)
	e.owner(org, "team-a", x, "acme/cx", 1, openFrom(), nil)
	e.owner(org, "team-n", n, "acme/cn", 1, openFrom(), nil)
	e.durations(org, a, day1, 10, 20, 30)
	e.durations(org, a, day2, 5000) // raw runs of day 2: the writer has not computed day 2
	e.durations(org, x, day1, 7000) // raw runs of a repository the writer does not compute
	// n: a run and a suite with no sample (no duration, no finish, no queue)
	e.runs(org, n, poolRun{id: "n-0", started: atHour(day1, 1)})
	e.suites(org, n, poolSuite{run: "n-0", id: "s-n-0", started: pt(atHour(day1, 1))})
	e.write(org, day1, a, n)
	for _, m := range []model.MeasureInput{model.MeasureInputPipelineDurationP95, model.MeasureInputTestSuiteDurationP95, model.MeasureInputPipelineQueueTime} {
		teams, orgValues := e.dimSeries(org, model.DimensionInputTeam, m, model.BucketIntervalInputDay, nil)
		if m != model.MeasureInputPipelineQueueTime {
			wantOne(t, string(m)+" C6 team-a (day 2 and repo x left out)", teams["team-a"], 29)
			wantOne(t, string(m)+" C6 ORG", orgValues, 29)
		}
		if got := teams["team-n"]; len(got) != 1 || got[0] != nil {
			t.Fatalf("%s C9 team-n = %v, want one null bucket (a computed day with no sample)", m, values(got))
		}
	}
}

// C10, C18: a WEEK bucket and a breakdown over the window pool every run of the bucket, never a mean of daily values.
// A: day 1 [10,20,30] (p95 29), day 2 [100..1700 step 100] (p95 1620): the week pools 20 runs = 1605;
// the mean of the two daily values would be 824.5.
func TestTestopsPooledWeekAndBreakdownPoolTheWholeBucket(t *testing.T) {
	e := startOwnersEnv(t)
	const org = "org-13858-week"
	const a = "13858000-0000-4000-8000-0000000018a1"
	e.repo(org, a, "acme/wa")
	e.owner(org, "team-a", a, "acme/wa", 1, openFrom(), nil)
	e.durations(org, a, day1, 10, 20, 30)
	e.durations(org, a, day2, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700)
	e.write(org, day1, a)
	e.write(org, day2, a)
	for _, m := range pooledDurations {
		teams, orgValues := e.dimSeries(org, model.DimensionInputTeam, m, model.BucketIntervalInputWeek, nil)
		wantOne(t, string(m)+" C10 team-a week", teams["team-a"], 1605)
		wantOne(t, string(m)+" C10 ORG week", orgValues, 1605)
		got, err := resolveOneBreakdown(e.ctx, e.client, org, bdInput(model.DimensionInputTeam, m), false, nil, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Items) != 1 || got.Items[0].Key != "team-a" || got.Items[0].Value == nil || math.Abs(*got.Items[0].Value-1605) > 1e-9 {
			t.Fatalf("%s C18 breakdown = %+v, want team-a 1605 pooled over the window", m, got.Items)
		}
	}
}

// C17: the team of a run is its repository's owner at the request, never the stored team. D moved from team-a to
// team-b before the request; the writer stored team-a on D's row. team-b pools B [1000] and D [10,20,30] = 854.5;
// team-a holds A only.
func TestTestopsPooledTeamReadsTheOwnersAtTheRequest(t *testing.T) {
	e := startOwnersEnv(t)
	const org = "org-13858-owners"
	const a, b, d = "13858000-0000-4000-8000-0000000019a1", "13858000-0000-4000-8000-0000000019b1", "13858000-0000-4000-8000-0000000019d1"
	e.repo(org, a, "acme/oa")
	e.repo(org, b, "acme/ob")
	e.repo(org, d, "acme/od")
	e.owner(org, "team-a", a, "acme/oa", 1, openFrom(), nil)
	e.owner(org, "team-b", b, "acme/ob", 1, openFrom(), nil)
	moved := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	e.owner(org, "team-a", d, "acme/od", 1, openFrom(), &moved)
	e.owner(org, "team-b", d, "acme/od", 1, moved, nil)
	e.durations(org, a, day1, 7)
	e.durations(org, b, day1, 1000)
	// D's raw runs carry the team the provider stored (team-a, the owner before the move).
	for i, v := range []float64{10, 20, 30} {
		e.runs(org, d, poolRun{id: fmt.Sprintf("od-%d", i), started: atHour(day1, 1+i), duration: pf(v), team: "team-a"})
		e.suites(org, d, poolSuite{run: fmt.Sprintf("od-%d", i), id: fmt.Sprintf("s-od-%d", i), started: pt(atHour(day1, 1+i)), duration: pf(v)})
	}
	e.write(org, day1, a, b, d)
	for _, m := range pooledDurations {
		teams, _ := e.dimSeries(org, model.DimensionInputTeam, m, model.BucketIntervalInputDay, nil)
		wantOne(t, string(m)+" C17 team-a (A only)", teams["team-a"], 7)
		wantOne(t, string(m)+" C17 team-b (B + D, its owner now)", teams["team-b"], 854.5)
	}
}
