//go:build integration

package investmentnormalize

import (
	"context"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9227 round 141: a work item is keyed by its provider AND its id. Two providers that use the same id
// string keep their own repository; the join to work_items never crosses providers.
func TestWorkItemJoinDoesNotCrossProviders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
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
	conn, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const org = "org-9227-providers"
	const repoA, repoB = "92270011-9227-4227-8227-922792279227", "92270012-9227-4227-8227-922792279227"
	day := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	completed := day.Add(14 * time.Hour)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	item := func(provider, repo string, synced time.Time) {
		exec(`INSERT INTO work_items (repo_id, work_item_id, provider, title, description, type, status, status_raw, project_key, project_id,
 assignees, reporter, created_at, updated_at, started_at, completed_at, closed_at, labels, story_points, sprint_id, sprint_name,
 parent_id, epic_id, url, last_synced, org_id)
VALUES (?, 'collision', ?, 'fix(api): crash', NULL, 'pr', 'done', 'Done', 'P', 'p', [], 'r', ?, ?, NULL, ?, NULL, [], NULL, '', '', '', '', '', ?, ?)`,
			repo, provider, day.AddDate(0, 0, -2), completed, completed, synced, org)
		exec(`INSERT INTO investment_classifications_daily (repo_id, day, artifact_type, artifact_id, provider, investment_area, project_stream, confidence, rule_id, computed_at, org_id)
VALUES (?, ?, 'work_item', 'collision', ?, 'product', 'general', 1.0, 'r', ?, ?)`, repo, day, provider, completed, org)
	}
	item("jira", repoA, completed)
	item("github", repoB, completed.Add(time.Hour)) // the NEWER version: the old join took its repository for both

	window := Options{Org: org, From: day.AddDate(0, 0, -1), To: day.AddDate(0, 0, 1), RunID: "normalized_legacy:providers"}
	records, efforts, report, err := Plan(ctx, conn, window)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, e := range efforts {
		got[strings.SplitN(e.WorkUnitID, ":", 3)[2]] = e.RepoID.String()
	}
	if report.Rows != 2 || len(records) != 2 || got["jira:collision"] != repoA || got["github:collision"] != repoB {
		t.Fatalf("the same id under two providers: %+v efforts by item = %v, want jira -> %s and github -> %s", report, got, repoA, repoB)
	}
}
