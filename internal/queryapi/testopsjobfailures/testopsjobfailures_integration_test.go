//go:build integration

package testopsjobfailures

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-8513 on a migrated ClickHouse: GitHub Actions rows and GitLab CI rows in
// one org, each with its own status spellings; a re-synced job run; a job run
// with no pipeline row; a job that only succeeds; another org's rows; a team
// that owns one repository.
func TestRealClickHouse_JobFailuresByWorkflowAndJobName(t *testing.T) {
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

	const (
		org, other = "org-8513", "org-other"
		hubRepo    = "85130000-0000-4000-8000-000000000001"
		labRepo    = "85130000-0000-4000-8000-000000000002"
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
	pipeline := func(o, repo, run, name, provider string) {
		started := inWindow
		if run == "h1" && o == org {
			started = inWindow.AddDate(0, -2, 0)
		}
		exec(`INSERT INTO ci_pipeline_runs (org_id, repo_id, run_id, pipeline_name, provider, status, started_at, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			o, repo, run, name, provider, "completed", started, synced)
	}
	job := func(o, repo, run, id, name, status string, started, lastSynced time.Time) {
		exec(`INSERT INTO ci_job_runs (org_id, repo_id, run_id, job_id, job_name, status, started_at, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			o, repo, run, id, name, status, started, lastSynced)
	}
	pipeline(org, hubRepo, "h1", "CI", "github")
	pipeline(org, hubRepo, "h2", "CI", "github")
	pipeline(org, labRepo, "g1", "pipeline", "gitlab")
	pipeline(other, hubRepo, "h1", "Other CI", "github")
	// CHAOS-8593: pipelines whose jobs all started outside the window must not
	// change any group. Pipeline h1 started long before the window but has jobs
	// inside it, and keeps its name.
	for i := 0; i < 20; i++ {
		run := fmt.Sprintf("old-%d", i)
		exec(`INSERT INTO ci_pipeline_runs (org_id, repo_id, run_id, pipeline_name, provider, status, started_at, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			org, hubRepo, run, "Old CI", "github", "completed", inWindow.AddDate(0, -6, 0), synced)
		exec(`INSERT INTO ci_job_runs (org_id, repo_id, run_id, job_id, job_name, status, started_at, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			org, hubRepo, run, "oj", "old-build", "failure", inWindow.AddDate(0, -6, 0), synced)
	}

	// GitHub Actions: build fails once and succeeds once; lint only succeeds; one job is skipped.
	job(org, hubRepo, "h1", "j1", "build", "success", inWindow, synced.Add(-time.Hour)) // the older version of j1
	job(org, hubRepo, "h1", "j1", "build", "failure", inWindow, synced)                 // the re-synced version: counts once, as a failure
	job(org, hubRepo, "h2", "j2", "build", "success", inWindow, synced)
	job(org, hubRepo, "h1", "j3", "lint", "success", inWindow, synced)
	job(org, hubRepo, "h1", "j4", "build", "skipped", inWindow, synced)
	// GitLab CI spellings: "failed" and "canceled".
	job(org, labRepo, "g1", "j5", "build", "failed", inWindow, synced)
	job(org, labRepo, "g1", "j6", "deploy", "canceled", inWindow, synced)
	// A job run whose pipeline row is not stored; a timeout is a failure.
	job(org, hubRepo, "orphan", "j7", "e2e", "timed_out", inWindow, synced)
	// Outside the window, and another org with the same keys.
	job(org, hubRepo, "h2", "j8", "build", "failure", inWindow.AddDate(0, 0, -30), synced)
	job(other, hubRepo, "h1", "j1", "build", "failure", inWindow, synced)
	job(other, hubRepo, "h1", "j9", "other-only", "failure", inWindow, synced)

	validFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		org, "gitlab", "team-lab", labRepo, "acme/lab", "exact", "inferred", uint8(0), uint16(0), int32(0), validFrom, nil, validFrom)

	since := graphqldate.New(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	until := graphqldate.New(time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC))
	type row struct {
		workflow, job, provider string
		runs, failed            int
		rate                    float64
	}
	flat := func(groups []model.TestOpsJobFailureGroup) []row {
		out := make([]row, 0, len(groups))
		for _, g := range groups {
			r := row{job: g.JobName, runs: g.Runs, failed: g.FailedRuns, workflow: "<nil>", provider: "<nil>"}
			if g.WorkflowName != nil {
				r.workflow = *g.WorkflowName
			}
			if g.Provider != nil {
				r.provider = *g.Provider
			}
			if g.FailureRate != nil {
				r.rate = *g.FailureRate
			}
			out = append(out, r)
		}
		return out
	}
	equal := func(a, b []row) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	hub := row{"CI", "build", "github", 2, 1, 0.5}
	lab := row{"pipeline", "build", "gitlab", 1, 1, 1}
	orphan := row{"<nil>", "e2e", "<nil>", 1, 1, 1}

	all, err := Resolve(ctx, client, org, since, until, Scope{}, 20)
	if err != nil {
		t.Fatal(err)
	}
	if got := flat(all.Groups); !equal(got, []row{hub, lab, orphan}) || all.TotalCount != 3 || all.Truncated {
		t.Fatalf("org-wide: %+v total %d truncated %v\nwant %+v total 3", got, all.TotalCount, all.Truncated, []row{hub, lab, orphan})
	}

	cut, err := Resolve(ctx, client, org, since, until, Scope{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := flat(cut.Groups); !equal(got, []row{hub}) || cut.TotalCount != 3 || !cut.Truncated {
		t.Fatalf("limit 1: %+v total %d truncated %v, want the first group, total 3, truncated", got, cut.TotalCount, cut.Truncated)
	}

	owned, err := Resolve(ctx, client, org, since, until, Scope{TeamIDs: []string{"team-lab"}}, 20)
	if err != nil {
		t.Fatal(err)
	}
	if got := flat(owned.Groups); !equal(got, []row{lab}) || owned.TotalCount != 1 {
		t.Fatalf("team scope by ownership: %+v total %d, want only the GitLab repository's group", got, owned.TotalCount)
	}

	byRepo, err := Resolve(ctx, client, org, since, until, Scope{RepoIDs: []string{"acme/hub"}}, 20)
	if err != nil {
		t.Fatal(err)
	}
	if got := flat(byRepo.Groups); !equal(got, []row{hub, orphan}) || byRepo.TotalCount != 2 {
		t.Fatalf("repository scope: %+v total %d, want the two GitHub repository groups", got, byRepo.TotalCount)
	}

	none, err := Resolve(ctx, client, "org-with-no-rows", since, until, Scope{}, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(none.Groups) != 0 || none.TotalCount != 0 || none.Truncated {
		t.Fatalf("an org with no rows: %+v", none)
	}
}

// countingClient counts the statements Resolve issues. The ClickHouse client
// refuses a statement that starts with a comment, so the statements cannot be
// tagged; the test finds them in system.query_log by their table text instead
// (the container is this test's own, so no other statement reads that table).
type countingClient struct {
	inner QueryClient
	calls int
}

func (c *countingClient) Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.calls++
	return c.inner.Query(ctx, statement, bindings)
}

// CHAOS-10056: a result-only check cannot see the CHAOS-8593 bound on the
// pipeline side of the join (the answer is the same unbounded). The bound is
// pinned by what ClickHouse reads: with a large pipeline table and one run that
// has job rows in the window, the statements must read a small share of the
// table. Without the bound the groups statement reads the whole org's pipeline
// history (every seeded row).
func TestRealClickHouse_JobFailuresPipelineSideReadsOnlyTheWindowsRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
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

	const (
		org    = "org-10056"
		repo   = "10056000-0000-4000-8000-000000000001"
		seeded = 1_000_000
	)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := admin.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	exec(fmt.Sprintf(`INSERT INTO ci_pipeline_runs (org_id, repo_id, run_id, pipeline_name, provider, status, started_at, last_synced)
SELECT '%s', toUUID('%s'), toString(number), 'CI', 'github', 'completed', toDateTime64('2026-08-10 12:00:00', 3, 'UTC'), toDateTime64('2026-08-11 00:00:00', 3, 'UTC')
FROM numbers(%d)`, org, repo, seeded))
	inWindow := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	synced := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	for i, status := range []string{"failure", "success", "success"} {
		exec(`INSERT INTO ci_job_runs (org_id, repo_id, run_id, job_id, job_name, status, started_at, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			org, repo, "0", fmt.Sprintf("j%d", i), "build", status, inWindow, synced)
	}
	var stored uint64
	if err := admin.QueryRow(ctx, `SELECT count() FROM ci_pipeline_runs WHERE org_id = ?`, org).Scan(&stored); err != nil || stored != seeded {
		t.Fatalf("seeded pipeline rows = %d (err %v), want %d", stored, err, seeded)
	}

	tagged := &countingClient{inner: client}
	since := graphqldate.New(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	until := graphqldate.New(time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC))
	got, err := Resolve(ctx, tagged, org, since, until, Scope{}, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Groups) != 1 || got.TotalCount != 1 || got.Groups[0].JobName != "build" || got.Groups[0].Runs != 3 || got.Groups[0].FailedRuns != 1 ||
		got.Groups[0].WorkflowName == nil || *got.Groups[0].WorkflowName != "CI" {
		t.Fatalf("the answer is wrong: %+v total %d", got.Groups, got.TotalCount)
	}

	if err := admin.Exec(ctx, `SYSTEM FLUSH LOGS`); err != nil {
		t.Fatalf("flush logs: %v", err)
	}
	rows, err := admin.Query(ctx, `SELECT read_rows FROM system.query_log
WHERE type = 'QueryFinish' AND query_kind = 'Select' AND position(query, 'FROM ci_pipeline_runs FINAL') > 0 AND position(query, 'system.query_log') = 0`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var reads []uint64
	for rows.Next() {
		var n uint64
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		reads = append(reads, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if tagged.calls == 0 || len(reads) != tagged.calls {
		t.Fatalf("query_log holds %d finished statement(s) for the %d issued; the read was not measured", len(reads), tagged.calls)
	}
	for _, n := range reads {
		if limit := uint64(seeded / 20); n >= limit {
			t.Errorf("a statement read %d rows of %d seeded pipeline rows (limit %d): the pipeline side is not bounded to the window's runs; reads %v", n, seeded, limit, reads)
		}
	}
}
