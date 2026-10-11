//go:build integration

package investmentnormalize

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The whole path on a real ClickHouse: legacy item classifications + work items in, normalized rows out
// (dry run counts, apply, a re-run writes the same rows, rollback by provenance), an unmapped key refused,
// a repository-day that already holds a categorized unit left alone.
func TestNormalizeLegacyHistoryEndToEnd(t *testing.T) {
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
	conn, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const org = "org-9206"
	const repoA, repoB = "92060001-9206-4206-8206-920692069206", "92060002-9206-4206-8206-920692069206"
	day := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	completed := day.Add(14 * time.Hour)
	created := day.AddDate(0, 0, -5)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	item := func(repo, id, title string, points *float64, area, stream string) {
		exec(`INSERT INTO work_items (repo_id, work_item_id, provider, title, description, type, status, status_raw, project_key, project_id,
 assignees, reporter, created_at, updated_at, started_at, completed_at, closed_at, labels, story_points, sprint_id, sprint_name,
 parent_id, epic_id, url, last_synced, org_id)
VALUES (?, ?, 'jira', ?, NULL, 'pr', 'done', 'Done', 'P', 'p', [], 'r', ?, ?, NULL, ?, NULL, [], ?, '', '', '', '', '', ?, ?)`,
			repo, id, title, created, completed, completed, points, completed, org)
		exec(`INSERT INTO investment_classifications_daily (repo_id, day, artifact_type, artifact_id, provider, investment_area, project_stream, confidence, rule_id, computed_at, org_id)
VALUES (?, ?, 'work_item', ?, 'jira', ?, ?, 1.0, 'r', ?, ?)`, repo, day, id, area, stream, completed, org)
	}
	three := 3.0
	item(repoA, "A-1", "Rotate the keys", &three, "security", "vulnerability") // mapped: risk.vulnerability, 3 units
	item(repoA, "A-2", "fix(api): crash on login", nil, "product", "general")  // estimated: cc:fix -> quality.bugfix, 1 unit
	item(repoA, "A-3", "Quarterly planning", nil, "product", "general")        // unmatched: stays out
	item(repoB, "B-1", "Upgrade the cluster", nil, "infra", "k8s")             // a canonical unit names it in its evidence: already covered
	// A classification with no repository: counted as skipped, never dropped silently.
	exec(`INSERT INTO investment_classifications_daily (repo_id, day, artifact_type, artifact_id, provider, investment_area, project_stream, confidence, rule_id, computed_at, org_id)
VALUES (NULL, ?, 'work_item', 'N-1', 'jira', 'security', 'general', 1.0, 'r', ?, ?)`, day, completed, org)
	exec(`INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_model_version, categorization_run_id, computed_at, org_id)
VALUES ('wu-jev', ?, ?, ?, 'churn_loc', 10, ?, ?, ?, 0.8, 'high', 'ok', 'jev-test', 'run-1', ?, ?)`,
		day, completed, repoB, map[string]float64{"quality": 1.0}, map[string]float64{"quality.bugfix": 1.0}, `{"issues":["B-1"],"prs":[]}`, completed, org)

	window := Options{Org: org, From: day.AddDate(0, 0, -1), To: day.AddDate(0, 0, 1), RunID: "normalized_legacy:test-run"}
	records, efforts, report, err := Plan(ctx, conn, window)
	if err != nil {
		t.Fatal(err)
	}
	if report.Items != 5 || report.Rows != 2 || report.Unmatched != 1 || report.AlreadyCovered != 1 || report.NotCompleted != 1 || report.UnmappedTotal() != 0 || report.Accounted() != report.Items {
		t.Fatalf("dry run = %+v, want items 5, rows 2, unmatched 1, already covered 1, not completed 1 (no work item row), unmapped 0, every item counted once\n%v", report, report.Lines())
	}
	if report.ByTheme["risk"] != 1 || report.ByTheme["quality"] != 1 || report.ByRule["cc:fix"] != 1 || report.ByRule["map:security/vulnerability"] != 1 {
		t.Fatalf("dry run by theme/rule = %v / %v", report.ByTheme, report.ByRule)
	}
	count := func(where string) uint64 {
		t.Helper()
		n, err := countRows(ctx, conn, `SELECT count() FROM work_unit_investments FINAL WHERE org_id = '`+org+`' AND `+where)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := count("categorization_status = '" + Status + "'"); n != 0 {
		t.Fatalf("a dry run wrote %d rows", n)
	}

	if err := Apply(ctx, conn, window, records, efforts, report); err != nil {
		t.Fatal(err)
	}
	if n := count("categorization_status = '" + Status + "'"); n != 2 {
		t.Fatalf("normalized rows = %d, want 2", n)
	}
	// The row says what it is: provenance, low quality, the matched rule in the evidence, effort = legacy delivery units.
	if n := count("work_unit_id = 'normalized_legacy:org-9206:jira:A-1' AND evidence_quality_band = 'low' AND evidence_quality = 0.2 AND effort_value = 3 " +
		"AND effort_metric = '" + EffortMetric + "' AND categorization_model_version LIKE '%mapping=" + MappingVersion + "%' " +
		"AND structural_evidence_json LIKE '%\"rule\":\"map:security/vulnerability\"%' AND theme_distribution_json['risk'] = 1.0 AND subcategory_distribution_json['risk.vulnerability'] = 1.0"); n != 1 {
		t.Fatalf("the mapped row is not as designed (matches %d)", n)
	}
	if n := count("work_unit_id = 'normalized_legacy:org-9206:jira:A-2' AND effort_value = 1 AND structural_evidence_json LIKE '%\"rule\":\"cc:fix\"%'"); n != 1 {
		t.Fatalf("the estimated row is not as designed (matches %d)", n)
	}
	// The categorized unit is untouched.
	if n := count("work_unit_id = 'wu-jev' AND categorization_status = 'ok'"); n != 1 {
		t.Fatalf("the categorized unit changed (%d)", n)
	}

	// Idempotent: a second run (another run id) finds the identical rows stored and writes nothing.
	again := window
	again.RunID = "normalized_legacy:second-run"
	records2, efforts2, report2, err := Plan(ctx, conn, again)
	if err != nil {
		t.Fatal(err)
	}
	if report2.Rows != 0 || report2.Unchanged != 2 || report2.AlreadyCovered != 1 {
		t.Fatalf("second plan = %+v", report2)
	}
	if err := Apply(ctx, conn, again, records2, efforts2, report2); err != nil {
		t.Fatal(err)
	}
	if n := count("categorization_status = '" + Status + "'"); n != 2 {
		t.Fatalf("after a re-run the normalized rows = %d, want still 2", n)
	}

	// Rollback: the first run owns the rows (the second wrote none).
	if n, err := RollbackCount(ctx, conn, org, ""); err != nil || n != 2 {
		t.Fatalf("rollback count = %d, %v, want 2", n, err)
	}
	if err := Rollback(ctx, conn, org, ""); err != nil {
		t.Fatal(err)
	}
	if n := count("categorization_status = '" + Status + "'"); n != 0 {
		t.Fatalf("after rollback normalized rows = %d, want 0", n)
	}
	if n := count("work_unit_id = 'wu-jev'"); n != 1 {
		t.Fatalf("the rollback touched a categorized unit (%d)", n)
	}

	// The command itself (round 129 r1 P3): dry run, apply, rollback of another run, rollback of this run.
	runCLI := func(args ...string) (string, int) {
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
		return stdout.String(), code
	}
	window2 := []string{"--org", org, "--from", day.AddDate(0, 0, -1).Format("2006-01-02"), "--to", day.AddDate(0, 0, 1).Format("2006-01-02")}
	if out, _ := runCLI(window2...); !strings.Contains(out, "dry run: nothing written") || !strings.Contains(out, "rows (normalized): 2") || count("categorization_status = '"+Status+"'") != 0 {
		t.Fatalf("the CLI dry run = %q (rows written %d)", out, count("categorization_status = '"+Status+"'"))
	}
	out, _ := runCLI(append(append([]string{}, window2...), "--apply")...)
	match := regexp.MustCompile(`written: 2 rows, run (normalized_legacy:[0-9a-f-]+)`).FindStringSubmatch(out)
	if match == nil || count("categorization_status = '"+Status+"'") != 2 {
		t.Fatalf("the CLI apply = %q", out)
	}
	if out, _ := runCLI("--org", org, "--rollback", "--run", "normalized_legacy:another-run", "--apply"); !strings.Contains(out, "normalized rows: 0") || count("categorization_status = '"+Status+"'") != 2 {
		t.Fatalf("a rollback of ANOTHER run touched rows: %q", out)
	}
	if out, _ := runCLI("--org", org, "--rollback", "--run", match[1], "--apply"); !strings.Contains(out, "normalized rows: 2") || count("categorization_status = '"+Status+"'") != 0 {
		t.Fatalf("the rollback of this run = %q", out)
	}
	if n := count("work_unit_id = 'wu-jev'"); n != 1 {
		t.Fatalf("the CLI rollback touched a categorized unit (%d)", n)
	}

	// An unmapped key: the plan counts it and apply refuses, nothing is written.
	item(repoA, "A-9", "Anything", nil, "weird", "key")
	bad, badEfforts, badReport, err := Plan(ctx, conn, window)
	if err != nil {
		t.Fatal(err)
	}
	if badReport.Unmapped["weird/key"] != 1 {
		t.Fatalf("unmapped = %v, want weird/key = 1", badReport.Unmapped)
	}
	if err := Apply(ctx, conn, window, bad, badEfforts, badReport); !errors.Is(err, ErrUnmappedKeys) {
		t.Fatalf("apply over an unmapped key = %v, want ErrUnmappedKeys", err)
	}
	if n := count("categorization_status = '" + Status + "'"); n != 0 {
		t.Fatalf("a refused apply wrote %d rows", n)
	}
}
