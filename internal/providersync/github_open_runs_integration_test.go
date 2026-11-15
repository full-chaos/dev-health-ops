//go:build integration

package providersync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// ciPipelineRunsDDL is the table as the migrations build it: the head of the ClickHouse baseline (no later chain file
// alters ci_pipeline_runs).
func ciPipelineRunsDDL(t *testing.T) string {
	t.Helper()
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range baseline.Objects {
		if object.Name == "ci_pipeline_runs" {
			return object.Create
		}
	}
	t.Fatal("ci_pipeline_runs is not in the baseline")
	return ""
}

// The ClickHouse reader of the open phase on the real table shape: its key prefix, FINAL, the status and provider
// filters, the keyset order and the started-before bound (an epoch start time is older than any bound).
func TestGitHubOpenRunsClickHouseReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, ciPipelineRunsDDL(t)); err != nil {
		t.Fatal(err)
	}
	const repoA, repoB = "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	insert := func(org, repo, run, provider, status string, started, synced time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO ci_pipeline_runs (org_id, repo_id, run_id, provider, status, started_at, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			org, repo, run, provider, status, started, synced); err != nil {
			t.Fatal(err)
		}
	}
	insert("org-acme", repoA, "epoch", "github_actions", "running", time.Unix(0, 0).UTC(), base)
	insert("org-acme", repoA, "r1", "github_actions", "running", base.Add(1*time.Hour), base)
	insert("org-acme", repoA, "r2", "", "queued", base.Add(2*time.Hour), base)
	insert("org-acme", repoA, "r3", "github_actions", " Running ", base.Add(3*time.Hour), base)
	insert("org-acme", repoA, "r4", "github_actions", "success", base.Add(4*time.Hour), base)
	insert("org-acme", repoA, "r5", "gitlab_ci", "running", base.Add(5*time.Hour), base)
	insert("org-acme", repoA, "r6", "github_actions", "running", base.Add(6*time.Hour), base)
	insert("org-acme", repoA, "late", "github_actions", "running", base.Add(100*24*time.Hour), base)
	// r6 is later finalized: the newer row replaces it under FINAL.
	insert("org-acme", repoA, "r6", "github_actions", "success", base.Add(6*time.Hour), base.Add(time.Minute))
	insert("org-acme", repoB, "b1", "github_actions", "running", base.Add(1*time.Hour), base)
	insert("org-other", repoA, "o1", "github_actions", "running", base.Add(1*time.Hour), base)

	reader := GitHubOpenRunsClickHouseReader{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	claim := nativeTestClaim("github", "tests")
	before := base.Add(30 * 24 * time.Hour)
	ids := func(rows []githubTestsPipelineRow) string {
		out := ""
		for _, row := range rows {
			out += row.RunID + ","
		}
		return out
	}
	rows, err := reader.OpenRuns(ctx, claim, repoA, before, time.Time{}, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(rows); got != "epoch,r1,r2,r3," {
		t.Fatalf("open rows = %s, want epoch,r1,r2,r3, (oldest first; not r4 success, r5 gitlab, r6 finalized, late after the bound, no other repo or org)", got)
	}
	page, err := reader.OpenRuns(ctx, claim, repoA, before, rows[1].StartedAt, rows[1].RunID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(page); got != "r2,r3," {
		t.Fatalf("keyset page after r1 = %s, want r2,r3,", got)
	}
	limited, err := reader.OpenRuns(ctx, claim, repoA, before, time.Time{}, "", 2)
	if err != nil || len(limited) != 2 {
		t.Fatalf("limit 2 = %d rows, err %v", len(limited), err)
	}
	for _, row := range rows {
		if row.RepoID != repoA || row.OrgID != "org-acme" {
			t.Fatalf("row of another repo or org: %#v", row)
		}
	}
	backlog, err := reader.OpenRunBacklog(ctx, claim, repoA, before, 1001)
	if err != nil || backlog != 4 {
		t.Fatalf("backlog = %d err %v, want 4", backlog, err)
	}
	capped, err := reader.OpenRunBacklog(ctx, claim, repoA, before, 3)
	if err != nil || capped != 3 {
		t.Fatalf("backlog with limit 3 = %d err %v, want 3", capped, err)
	}
	other, err := reader.OpenRunBacklog(ctx, claim, repoB, before, 1001)
	if err != nil || other != 1 {
		t.Fatalf("backlog of the other repo = %d err %v, want 1", other, err)
	}
	// A bigger set pages in order across keyset steps.
	for index := 0; index < 250; index++ {
		insert("org-acme", repoB, fmt.Sprintf("p%04d", index), "github_actions", "queued", base.Add(time.Duration(index)*time.Minute+time.Hour), base)
	}
	var seen []string
	afterStarted, afterRun := time.Time{}, ""
	for {
		batch, err := reader.OpenRuns(ctx, claim, repoB, before, afterStarted, afterRun, 50)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) == 0 {
			break
		}
		for _, row := range batch {
			seen = append(seen, row.RunID)
		}
		last := batch[len(batch)-1]
		afterStarted, afterRun = last.StartedAt, last.RunID
	}
	if len(seen) != 251 {
		t.Fatalf("paged %d rows, want 251", len(seen))
	}
}
