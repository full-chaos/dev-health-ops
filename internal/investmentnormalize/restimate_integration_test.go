//go:build integration

package investmentnormalize

import (
	"context"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9227 round 137 P1: a second apply skips only a row that is IDENTICAL. The estimate reads the item's
// description and labels, so a change of either that changes the estimate must rewrite the row (a new run
// id), and one that does not change the row must not.
func TestSecondApplyRewritesARowWhoseEstimateChanged(t *testing.T) {
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

	const org, repo = "org-9227-text", "92270009-9227-4227-8227-922792279227"
	day := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	completed := day.Add(14 * time.Hour)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	version := func(description string, labels []string, synced time.Time) {
		exec(`INSERT INTO work_items (repo_id, work_item_id, provider, title, description, type, status, status_raw, project_key, project_id,
 assignees, reporter, created_at, updated_at, started_at, completed_at, closed_at, labels, story_points, sprint_id, sprint_name,
 parent_id, epic_id, url, last_synced, org_id)
VALUES (?, 'ghpr:acme/api#5', 'github', 'Update the login flow', ?, 'pr', 'done', 'Done', 'P', 'p', [], 'r', ?, ?, NULL, ?, NULL, ?, NULL, '', '', '', '', '', ?, ?)`,
			repo, description, day.AddDate(0, 0, -3), completed, completed, labels, synced, org)
	}
	version("this fixes the crash on login", nil, completed)
	exec(`INSERT INTO investment_classifications_daily (repo_id, day, artifact_type, artifact_id, provider, investment_area, project_stream, confidence, rule_id, computed_at, org_id)
VALUES (?, ?, 'work_item', 'ghpr:acme/api#5', 'github', 'product', 'general', 1.0, 'r', ?, ?)`, repo, day, completed, org)

	runCLI := func(args ...string) string {
		t.Helper()
		var stdout, stderr strings.Builder
		code := run(ctx, cli.Env{
			Args:   args,
			Lookup: func(key string) (string, bool) { return ch.URI, key == "CLICKHOUSE_URI" },
			Stdout: &stdout, Stderr: &stderr,
		})
		if code != cli.ExitOK {
			t.Fatalf("dho backfill normalize-investment %v = exit %d: %s", args, code, stderr.String())
		}
		return stdout.String()
	}
	str := func(query string) string {
		t.Helper()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				t.Fatal(err)
			}
			out = append(out, v)
		}
		return strings.Join(out, ";")
	}
	window := []string{"--org", org, "--from", day.AddDate(0, 0, -1).Format("2006-01-02"), "--to", day.AddDate(0, 0, 1).Format("2006-01-02"), "--apply"}
	sub := func() string {
		return str(`SELECT arrayStringConcat(mapKeys(subcategory_distribution_json), ',') || '/' || categorization_run_id FROM work_unit_investments FINAL WHERE org_id = '` + org + `' AND categorization_status = '` + Status + `'`)
	}

	first := runCLI(window...)
	if !strings.Contains(first, "written: 1 rows") || !strings.HasPrefix(sub(), "quality.bugfix/") {
		t.Fatalf("the first apply: %q, row %q", first, sub())
	}
	before := sub()
	// Identical input: nothing is rewritten.
	if again := runCLI(window...); !strings.Contains(again, "written: 0 rows") || sub() != before {
		t.Fatalf("an identical second apply: %q, row %q (was %q)", again, sub(), before)
	}
	// The description changes the estimate: the row is rewritten under the new run id.
	version("implement the new search", nil, completed.Add(time.Hour))
	if changed := runCLI(window...); !strings.Contains(changed, "written: 1 rows") || !strings.HasPrefix(sub(), "feature_delivery.roadmap/") || sub() == before {
		t.Fatalf("after the description changed: %q, row %q (was %q)", changed, sub(), before)
	}
	// A change that leaves the estimate and the row as they were rewrites nothing.
	stable := sub()
	version("implement the new search, with a longer explanation", nil, completed.Add(2*time.Hour))
	if same := runCLI(window...); !strings.Contains(same, "written: 0 rows") || sub() != stable {
		t.Fatalf("a description change that keeps the estimate: %q, row %q (was %q)", same, sub(), stable)
	}
}
