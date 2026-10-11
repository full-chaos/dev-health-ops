//go:build integration

package investmentnormalize

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9227 fixes 1, 2 and 4 through the command as a caller runs it (dry run, apply, apply again, rollback by
// the FIRST run id) on a real ClickHouse, with the state read back by SQL: the repository of an item comes
// through the entity tree (a pull request item keeps its own repository; an issue reaches repositories only
// through work_graph_issue_pr, the effort split by pull request churn, the sum equal to the item's effort), an
// item a canonical unit already names is left out, and a second apply writes nothing.
func TestNormalizeEntityTree(t *testing.T) {
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
	conn, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const org = "org-9227"
	const repoA, repoB = "92270001-9227-4227-8227-922792279227", "92270002-9227-4227-8227-922792279227"
	day := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	completed := day.Add(14 * time.Hour)
	created := day.AddDate(0, 0, -5)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	// item seeds a work item and its legacy classification. ownRepo is the item's OWN repository column (nil = none).
	item := func(provider, id, kind string, ownRepo *string, title string, points *float64, area, stream string) {
		exec(`INSERT INTO work_items (repo_id, work_item_id, provider, title, description, type, status, status_raw, project_key, project_id,
 assignees, reporter, created_at, updated_at, started_at, completed_at, closed_at, labels, story_points, sprint_id, sprint_name,
 parent_id, epic_id, url, last_synced, org_id)
VALUES (?, ?, ?, ?, NULL, ?, 'done', 'Done', 'P', 'p', [], 'r', ?, ?, NULL, ?, NULL, [], ?, '', '', '', '', '', ?, ?)`,
			ownRepo, id, provider, title, kind, created, completed, completed, points, completed, org)
		exec(`INSERT INTO investment_classifications_daily (repo_id, day, artifact_type, artifact_id, provider, investment_area, project_stream, confidence, rule_id, computed_at, org_id)
VALUES (?, ?, 'work_item', ?, ?, ?, ?, 1.0, 'r', ?, ?)`, ownRepo, day, id, provider, area, stream, completed, org)
	}
	link := func(workItem, repo string, number uint32, provenance string, churn uint32) {
		exec(`INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
VALUES (?, ?, ?, 1.0, ?, 'x', ?, ?)`, repo, workItem, number, provenance, completed, org)
		exec(`INSERT INTO git_pull_requests (repo_id, number, title, state, author_name, created_at, additions, deletions, last_synced, org_id)
VALUES (?, ?, 'pr', 'merged', 'a', ?, ?, 0, ?, ?)`, repo, number, created, churn, completed, org)
	}
	repoA2, repoB2 := repoA, repoB
	four := 4.0
	// A pull request item keeps its own repository.
	item("github", "ghpr:acme/api#7", "pr", &repoA2, "fix(api): crash on login", nil, "product", "general") // quality.bugfix, 1 unit, own repository A
	// An issue reaches repositories only through its links: churn 30 in A, 10 in B -> weights 0.75 / 0.25 of 4 units.
	item("linear", "linear:SPLIT-1", "task", nil, "chore: upgrade the build image", &four, "product", "general")
	link("linear:SPLIT-1", repoA, 101, "native", 30)
	link("linear:SPLIT-1", repoB, 202, "heuristic", 10)
	// An issue that has an own repository column but NO link: the column is not the relation, it is skipped.
	item("jira", "jira:OWN-1", "task", &repoB2, "chore: bump the linter", nil, "product", "general")
	// An issue with no link at all, and one whose linked pull requests carry no churn.
	item("linear", "linear:NONE-1", "task", nil, "chore: tidy the scripts", nil, "product", "general")
	item("linear", "linear:ZERO-1", "task", nil, "chore: tidy the docs", nil, "product", "general")
	link("linear:ZERO-1", repoA, 303, "native", 0)
	// Named in the evidence of a canonical unit: already described, left out (an issue and a pull request).
	item("linear", "linear:COVERED-1", "task", nil, "chore: rename the module", nil, "product", "general")
	link("linear:COVERED-1", repoA, 404, "native", 20)
	item("github", "ghpr:acme/api#9", "pr", &repoA2, "chore: drop the flag", nil, "product", "general")
	exec(`INSERT INTO repos (id, repo, provider, last_synced, org_id) VALUES (?, 'acme/api', 'github', ?, ?)`, repoA, completed, org)
	exec(`INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_model_version, categorization_run_id, computed_at, org_id)
VALUES ('wu-jev', ?, ?, ?, 'churn_loc', 10, ?, ?, ?, 0.8, 'high', 'ok', 'jev-test', 'run-1', ?, ?)`,
		day, completed, repoA, map[string]float64{"quality": 1.0}, map[string]float64{"quality.bugfix": 1.0},
		fmt.Sprintf(`{"issues":["linear:COVERED-1"],"prs":["%s#pr9"]}`, repoA), completed, org)

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
	window := []string{"--org", org, "--from", day.AddDate(0, 0, -1).Format("2006-01-02"), "--to", day.AddDate(0, 0, 1).Format("2006-01-02")}
	str := func(query string) string {
		t.Helper()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			t.Fatalf("%v\n%s", err, query)
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
	normalizedIDs := func() string {
		return str(`SELECT work_unit_id FROM work_unit_investments FINAL WHERE org_id = '` + org + `' AND categorization_status = '` + Status + `' ORDER BY work_unit_id`)
	}

	runID := func(out string) string {
		match := regexp.MustCompile(`written: \d+ rows, run (normalized_legacy:[0-9a-f-]+)`).FindStringSubmatch(out)
		if match == nil {
			t.Fatalf("the apply printed no run id: %q", out)
		}
		return match[1]
	}

	// DRY RUN writes nothing and counts every item in one bucket.
	dry := runCLI(window...)
	for _, want := range []string{
		"items read: 7", "skipped: covered by a canonical unit: 2", "skipped, no repository reachable: 3",
		"no repository reachable: no link: 2", "no repository reachable: all links churn 0: 1", "rows (normalized): 2", "by tier: native = 1", "by tier: own_repository = 1",
	} {
		if !strings.Contains(dry, want) {
			t.Errorf("the dry run lacks %q:\n%s", want, dry)
		}
	}
	if ids := normalizedIDs(); ids != "" {
		t.Fatalf("a dry run wrote %q", ids)
	}

	// APPLY: the pull request item keeps its repository; the issue is split by the churn of its links.
	first := runCLI(append(append([]string{}, window...), "--apply")...)
	firstRun := runID(first)
	want := "normalized_legacy:org-9227:github:ghpr:acme/api#7;normalized_legacy:org-9227:linear:linear:SPLIT-1"
	if ids := normalizedIDs(); ids != want {
		t.Fatalf("normalized rows = %q, want %q (an item with no reachable repository or already covered is not a row)", ids, want)
	}
	if got := str(`SELECT toString(repo_id) FROM work_unit_investments FINAL WHERE org_id = '` + org + `' AND work_unit_id = 'normalized_legacy:org-9227:github:ghpr:acme/api#7'`); got != repoA {
		t.Errorf("the pull request item's repository = %q, want its own %q", got, repoA)
	}
	if got := str(`SELECT ifNull(toString(repo_id), 'none') FROM work_unit_investments FINAL WHERE org_id = '` + org + `' AND work_unit_id = 'normalized_legacy:org-9227:linear:linear:SPLIT-1'`); got != "none" {
		t.Errorf("the split issue's unit names repository %q, want none (it spans two)", got)
	}
	split := str(`SELECT concat(toString(repo_id), '=', toString(effort_value), '/', toString(allocation_weight)) FROM work_unit_repo_effort FINAL WHERE org_id = '` + org + `' AND work_unit_id = 'normalized_legacy:org-9227:linear:linear:SPLIT-1' ORDER BY repo_id`)
	if split != repoA+"=3/0.75;"+repoB+"=1/0.25" {
		t.Errorf("the split of the issue's effort = %q, want 3 and 1 of 4 units by churn 30 and 10", split)
	}
	var sum float64
	rows, err := conn.Query(ctx, `SELECT effort_value FROM work_unit_repo_effort FINAL WHERE org_id = '`+org+`' AND work_unit_id = 'normalized_legacy:org-9227:linear:linear:SPLIT-1'`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var v float64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		sum += v
	}
	rows.Close()
	if math.Abs(sum-4) > 1e-9 {
		t.Errorf("the repository efforts sum to %v, want the item's effort 4", sum)
	}
	if got := str(`SELECT JSONExtractString(structural_evidence_json, 'normalized_legacy', 'repository_tier') FROM work_unit_investments FINAL WHERE org_id = '` + org + `' AND work_unit_id = 'normalized_legacy:org-9227:linear:linear:SPLIT-1'`); got != "native" {
		t.Errorf("the tier named in the evidence = %q, want native (the strongest link)", got)
	}

	// A SECOND APPLY writes nothing: the same ids, the same run id, no new version of any row.
	rawBefore := str(`SELECT toString(count()) FROM work_unit_investments WHERE org_id = '` + org + `' AND categorization_status = '` + Status + `'`)
	effortBefore := str(`SELECT toString(count()) FROM work_unit_repo_effort WHERE org_id = '` + org + `' AND startsWith(work_unit_id, '` + Status + `')`)
	second := runCLI(append(append([]string{}, window...), "--apply")...)
	if secondRun := runID(second); secondRun != firstRun {
		t.Errorf("the second apply of the same window ran as %q, want the SAME run id %q (a retry or a later apply must be owned by the id the first printed)", secondRun, firstRun)
	}
	if !strings.Contains(second, "written: 0 rows") || !strings.Contains(second, "already stored, not rewritten: 2") {
		t.Errorf("the second apply = %q, want 0 rows written and 2 already stored", second)
	}
	if got := str(`SELECT toString(count()) FROM work_unit_investments WHERE org_id = '` + org + `' AND categorization_status = '` + Status + `'`); got != rawBefore {
		t.Errorf("raw normalized rows %s -> %s after a second apply", rawBefore, got)
	}
	if got := str(`SELECT toString(count()) FROM work_unit_repo_effort WHERE org_id = '` + org + `' AND startsWith(work_unit_id, '` + Status + `')`); got != effortBefore {
		t.Errorf("repository effort rows %s -> %s after a second apply", effortBefore, got)
	}
	if got := str(`SELECT arrayStringConcat(arrayDistinct(groupArray(categorization_run_id)), ',') FROM work_unit_investments WHERE org_id = '` + org + `' AND categorization_status = '` + Status + `'`); got != firstRun {
		t.Errorf("run ids after a second apply = %q, want only the first %q", got, firstRun)
	}

	// ROLLBACK by the FIRST run id restores the before-state after any number of applies.
	if out := runCLI("--org", org, "--rollback", "--run", firstRun, "--apply"); !strings.Contains(out, "normalized rows: 2") {
		t.Fatalf("rollback by the first run = %q", out)
	}
	if ids := normalizedIDs(); ids != "" {
		t.Fatalf("normalized rows left after the rollback: %q", ids)
	}
	if got := str(`SELECT toString(count()) FROM work_unit_repo_effort WHERE org_id = '` + org + `' AND startsWith(work_unit_id, '` + Status + `')`); got != "0" {
		t.Errorf("repository effort rows left after the rollback: %s", got)
	}
	if got := str(`SELECT toString(count()) FROM work_unit_investments FINAL WHERE org_id = '` + org + `' AND work_unit_id = 'wu-jev'`); got != "1" {
		t.Errorf("the rollback touched a categorized unit (%s)", got)
	}
	var _ driver.Conn = conn
}
