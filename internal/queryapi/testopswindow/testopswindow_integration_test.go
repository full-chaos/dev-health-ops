//go:build integration

package testopswindow

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-13773 on a migrated ClickHouse: GitHub and GitLab runs of one org, each
// with its own status spellings; failed runs with no report; a suite row with no
// cases; cancelled, skipped, running and queued runs; a run seen first as
// a failure and re-synced as a success; a run outside the window; another org; two
// stored versions of one repository day; a day stored without the flake counts.
func TestRealClickHouse_CoverageAndWindowRates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	// A merge would collapse the stored versions of a row before the read and hide a missing FINAL.
	for _, table := range []string{"ci_pipeline_runs", "test_suite_results"} {
		if err := admin.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			t.Fatalf("stop merges on %s: %v", table, err)
		}
	}

	const (
		org, other = "org-13773", "org-other"
		hubRepo    = "13773000-0000-4000-8000-000000000001"
		labRepo    = "13773000-0000-4000-8000-000000000002"
	)
	inWindow := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	synced := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := admin.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	for _, r := range []struct{ id, name, provider string }{{hubRepo, "acme/hub", "github"}, {labRepo, "acme/lab", "gitlab"}} {
		exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`, r.id, r.name, r.provider, org, synced, synced)
	}
	run := func(o, repo, id, status string, started, lastSynced time.Time) {
		exec(`INSERT INTO ci_pipeline_runs (org_id, repo_id, run_id, provider, status, started_at, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			o, repo, id, "p", status, started, lastSynced)
	}
	suite := func(o, repo, id, suiteID string, total uint32) {
		exec(`INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, suite_name, total_count, passed_count, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			o, repo, id, suiteID, suiteID, total, total, synced)
	}

	// GitHub: two reporting successes, one failure with a report, two failures with none, one failure whose suite row holds no case.
	run(org, hubRepo, "h-ok-1", "success", inWindow, synced)
	suite(org, hubRepo, "h-ok-1", "s1", 10)
	run(org, hubRepo, "h-ok-2", "success", inWindow, synced)
	suite(org, hubRepo, "h-ok-2", "s1", 10)
	suite(org, hubRepo, "h-ok-2", "s2", 5) // a second suite of one run counts the run once
	run(org, hubRepo, "h-fail-report", "failure", inWindow, synced)
	suite(org, hubRepo, "h-fail-report", "s1", 4)
	run(org, hubRepo, "h-fail-none-1", "failure", inWindow, synced)
	run(org, hubRepo, "h-fail-none-2", "timed_out", inWindow, synced)
	run(org, hubRepo, "h-fail-empty-suite", "failure", inWindow, synced)
	suite(org, hubRepo, "h-fail-empty-suite", "s1", 0)
	// A suite re-synced with no case: the newest version is the stored one, so the run reports no tests.
	run(org, hubRepo, "h-fail-resuite", "failure", inWindow, synced)
	exec(`INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, suite_name, total_count, passed_count, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		org, hubRepo, "h-fail-resuite", "s1", "s1", uint32(5), uint32(5), synced.Add(-time.Hour))
	exec(`INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, suite_name, total_count, passed_count, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		org, hubRepo, "h-fail-resuite", "s1", "s1", uint32(0), uint32(0), synced)
	// A run first seen as a failure and re-synced as a success counts once, as a success.
	run(org, hubRepo, "h-resync", "failure", inWindow, synced.Add(-time.Hour))
	run(org, hubRepo, "h-resync", "success", inWindow, synced)
	// Not completed: they never count, even with a stored suite.
	for _, status := range []string{"cancelled", "canceled", "skipped", "running", "queued"} {
		run(org, hubRepo, "h-"+status, status, inWindow, synced)
		suite(org, hubRepo, "h-"+status, "s1", 3)
	}
	// GitLab spellings: "failed" with a report, "success" with none.
	run(org, labRepo, "g-failed", "failed", inWindow, synced)
	suite(org, labRepo, "g-failed", "s1", 7)
	run(org, labRepo, "g-ok", "success", inWindow, synced)
	// Outside the window (before and after it), and another org with the same run ids.
	run(org, hubRepo, "h-future", "success", inWindow.AddDate(0, 2, 0), synced)
	suite(org, hubRepo, "h-future", "s1", 3)
	run(org, hubRepo, "h-old", "success", inWindow.AddDate(0, -2, 0), synced)
	suite(org, hubRepo, "h-old", "s1", 3)
	run(other, hubRepo, "h-ok-1", "success", inWindow, synced)
	run(other, hubRepo, "other-only", "failure", inWindow, synced)
	suite(other, hubRepo, "h-fail-none-1", "s1", 9) // another org's report does not cover this org's run

	validFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		org, "gitlab", "team-lab", labRepo, "acme/lab", "exact", "inferred", uint8(0), uint16(0), int32(0), validFrom, nil, validFrom)

	daily := func(o, repo string, day time.Time, passed, failed, skipped, quarantined uint32, flake, distinct any, computed time.Time) {
		exec(`INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, total_cases, passed_count, failed_count, skipped_count, quarantined_count, flake_cases, distinct_cases, computed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			o, repo, day, passed+failed+skipped+quarantined, passed, failed, skipped, quarantined, flake, distinct, computed)
	}
	d1 := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	d2 := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	// hub d1: an older version (wrong numbers) and the newest version; only the newest counts.
	daily(org, hubRepo, d1, 1, 1, 1, 1, uint32(1), uint32(1), synced.Add(-time.Hour))
	daily(org, hubRepo, d1, 900, 100, 40, 2, uint32(3), uint32(60), synced)
	daily(org, hubRepo, d2, 100, 0, 8, 0, uint32(1), uint32(40), synced)
	daily(org, labRepo, d1, 0, 10, 0, 0, uint32(0), uint32(5), synced) // a repository with no passed case: 0 % passed
	daily(org, hubRepo, d1.AddDate(0, -2, 0), 5000, 5000, 0, 0, uint32(9), uint32(9), synced)
	daily(other, hubRepo, d1, 7, 7, 7, 7, uint32(7), uint32(7), synced)
	daily(org, hubRepo, d1.AddDate(0, 1, 0), 3, 3, 3, 3, uint32(3), uint32(3), synced) // after the window

	near := func(got *float64, want float64) bool {
		return got != nil && *got > want-1e-9 && *got < want+1e-9
	}
	val := func(p *float64) any {
		if p == nil {
			return nil
		}
		return *p
	}

	since := graphqldate.New(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	until := graphqldate.New(time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC))

	got, err := Resolve(ctx, client, org, since, until, Scope{})
	if err != nil {
		t.Fatal(err)
	}
	// Completed: h-ok-1, h-ok-2, h-fail-report, h-fail-none-1, h-fail-none-2, h-fail-empty-suite, h-fail-resuite, h-resync, g-failed, g-ok = 10.
	// With tests: h-ok-1, h-ok-2, h-fail-report, h-resync? no (no suite), g-failed = 4.
	if got.Coverage.CompletedRuns != 10 || got.Coverage.RunsWithTests != 4 {
		t.Fatalf("coverage %+v, want 10 completed and 4 with tests", got.Coverage)
	}
	if !near(got.Coverage.SharePct, 4.0/10.0*100) {
		t.Fatalf("sharePct %v, want %v", val(got.Coverage.SharePct), 4.0/10.0*100)
	}
	// Passed 900+100+0 = 1000, failed 100+0+10 = 110, skipped 40+8 = 48, quarantined 2; total 1160.
	c := got.Cases
	if c.Passed != 1000 || c.Failed != 110 || c.Skipped != 48 || c.Quarantined != 2 || c.Total != 1160 {
		t.Fatalf("cases %+v, want passed 1000 failed 110 skipped 48 quarantined 2 total 1160 (newest version of a day, in-window days of this org only)", c)
	}
	if !near(got.PassRatePct, 1000.0/1110.0*100) {
		t.Fatalf("passRatePct %v, want %v (skipped and quarantined are not in the denominator)", val(got.PassRatePct), 1000.0/1110.0*100)
	}
	if !near(got.FailureRatePct, 110.0/1110.0*100) {
		t.Fatalf("failureRatePct %v, want %v", val(got.FailureRatePct), 110.0/1110.0*100)
	}
	// Flake: (3 + 1 + 0) / (60 + 40 + 5) = 4/105.
	if !near(got.FlakeRatePct, 4.0/105.0*100) {
		t.Fatalf("flakeRatePct %v, want %v", val(got.FlakeRatePct), 4.0/105.0*100)
	}

	// Team scope = the repositories the team OWNS: only the GitLab repository.
	owned, err := Resolve(ctx, client, org, since, until, Scope{TeamIDs: []string{"team-lab"}})
	if err != nil {
		t.Fatal(err)
	}
	if owned.Coverage.CompletedRuns != 2 || owned.Coverage.RunsWithTests != 1 || owned.Cases.Total != 10 || owned.Cases.Failed != 10 {
		t.Fatalf("team scope: coverage %+v cases %+v, want lab's 2 completed runs (1 with tests) and its 10 cases", owned.Coverage, owned.Cases)
	}
	if owned.PassRatePct == nil || *owned.PassRatePct != 0 {
		t.Fatalf("a repository with failures and no passed case reads %v, want a served 0", val(owned.PassRatePct))
	}

	byRepo, err := Resolve(ctx, client, org, since, until, Scope{RepoIDs: []string{"acme/hub"}})
	if err != nil {
		t.Fatal(err)
	}
	if byRepo.Coverage.CompletedRuns != 8 || byRepo.Coverage.RunsWithTests != 3 || byRepo.Cases.Total != 1150 {
		t.Fatalf("repository scope: coverage %+v cases %+v, want hub's 8 completed runs (3 with tests) and 1150 cases", byRepo.Coverage, byRepo.Cases)
	}

	// A day stored without the flake counts makes the whole window's flake rate unavailable, never a smaller window's.
	exec(`INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, total_cases, passed_count, failed_count, computed_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		org, hubRepo, time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC), 10, 10, 0, synced)
	partial, err := Resolve(ctx, client, org, since, until, Scope{})
	if err != nil {
		t.Fatal(err)
	}
	if partial.FlakeRatePct != nil {
		t.Fatalf("flakeRatePct %v with a day stored without counts, want null", val(partial.FlakeRatePct))
	}
	if partial.PassRatePct == nil || partial.Cases.Passed != 1010 {
		t.Fatalf("the pass rate must still be served: %+v", partial)
	}

	none, err := Resolve(ctx, client, "org-with-no-rows", since, until, Scope{})
	if err != nil {
		t.Fatal(err)
	}
	if none.Coverage.CompletedRuns != 0 || none.Coverage.SharePct != nil || none.PassRatePct != nil || none.FailureRatePct != nil || none.FlakeRatePct != nil || none.Cases.Total != 0 {
		t.Fatalf("an org with no rows: %+v %+v", none.Coverage, none.Cases)
	}
}
