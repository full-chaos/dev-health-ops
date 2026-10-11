//go:build integration

package investmentnormalize

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// failingConn fails the SECOND batch an apply prepares, whichever table it names (the injected failure
// between the two writes of an apply).
type failingConn struct {
	Conn
	batches int
	failOn  int // the batch (1-based) that fails; 0 = the second
	failed  bool
}

func (c *failingConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	if strings.Contains(query, "INSERT INTO") {
		c.batches++
		if c.batches == max(c.failOn, 2) && !c.failed {
			c.failed = true
			return nil, errors.New("injected failure")
		}
	}
	return c.Conn.PrepareBatch(ctx, query, opts...)
}

// CHAOS-9227 round 139: (1) the classifier writes one row per active item per DAY; the report counts ITEMS, so
// an item classified on several days is one item, not one item plus "not completed" days. (2) A failure
// between the two writes of an apply leaves no unit row without its repository efforts: the efforts go first,
// the unit row last, and the retry writes what is missing.
func TestReportCountsItemsNotDaysAndAFailedApplyIsRetryable(t *testing.T) {
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

	const org, repo = "org-9227-days", "92270010-9227-4227-8227-922792279227"
	last := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	completed := last.Add(14 * time.Hour)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	exec(`INSERT INTO work_items (repo_id, work_item_id, provider, title, description, type, status, status_raw, project_key, project_id,
 assignees, reporter, created_at, updated_at, started_at, completed_at, closed_at, labels, story_points, sprint_id, sprint_name,
 parent_id, epic_id, url, last_synced, org_id)
VALUES (?, 'ghpr:acme/api#6', 'github', 'fix(api): crash', NULL, 'pr', 'done', 'Done', 'P', 'p', [], 'r', ?, ?, NULL, ?, NULL, [], NULL, '', '', '', '', '', ?, ?)`,
		repo, last.AddDate(0, 0, -4), completed, completed, completed, org)
	for back := 0; back < 3; back++ { // classified on three days; completed on the last
		exec(`INSERT INTO investment_classifications_daily (repo_id, day, artifact_type, artifact_id, provider, investment_area, project_stream, confidence, rule_id, computed_at, org_id)
VALUES (?, ?, 'work_item', 'ghpr:acme/api#6', 'github', 'product', 'general', 1.0, 'r', ?, ?)`, repo, last.AddDate(0, 0, -back), completed, org)
	}
	window := Options{Org: org, From: last.AddDate(0, 0, -5), To: last.AddDate(0, 0, 1), RunID: "normalized_legacy:partial"}

	records, efforts, report, err := Plan(ctx, conn, window)
	if err != nil {
		t.Fatal(err)
	}
	if report.Items != 1 || report.NotCompleted != 0 || report.Rows != 1 || report.Accounted() != report.Items {
		t.Fatalf("an item classified on three days: %+v, want 1 item, 0 not completed, 1 row\n%v", report, report.Lines())
	}

	// The second write fails: the repository efforts were written FIRST, so the unit row (the row the plan judges
	// "already stored" by) does not exist, and the retry writes both.
	failing := &failingConn{Conn: conn}
	if err := Apply(ctx, failing, window, records, efforts, report); err == nil || !failing.failed {
		t.Fatalf("the injected failure did not reach the apply: %v", err)
	}
	count := func(table string) string {
		t.Helper()
		rows, err := conn.Query(ctx, `SELECT toString(count()) FROM `+table+` FINAL WHERE org_id = '`+org+`' AND startsWith(work_unit_id, '`+Status+`')`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out string
		for rows.Next() {
			if err := rows.Scan(&out); err != nil {
				t.Fatal(err)
			}
		}
		return out
	}
	rawCount := func(table string) string {
		t.Helper()
		rows, err := conn.Query(ctx, `SELECT toString(count()) FROM `+table+` WHERE org_id = '`+org+`' AND startsWith(work_unit_id, '`+Status+`')`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out string
		for rows.Next() {
			if err := rows.Scan(&out); err != nil {
				t.Fatal(err)
			}
		}
		return out
	}
	if units := count("work_unit_investments"); units != "0" {
		t.Fatalf("a failed unit write left %s unit rows", units)
	}
	retry, retryEfforts, retryReport, err := Plan(ctx, conn, window)
	if err != nil {
		t.Fatal(err)
	}
	if retryReport.Rows != 1 || retryReport.Unchanged != 0 || len(retryEfforts) != len(efforts) {
		t.Fatalf("the retry plan after a failed apply: %+v, want the row planned again", retryReport)
	}
	// The retry is the SAME run (the id names the window): its effort rows replace the failed attempt's versions.
	retryWindow := window
	retry, retryEfforts, retryReport, err = Plan(ctx, conn, retryWindow)
	if err != nil {
		t.Fatal(err)
	}
	if err := Apply(ctx, conn, retryWindow, retry, retryEfforts, retryReport); err != nil {
		t.Fatal(err)
	}
	if units, eff := count("work_unit_investments"), count("work_unit_repo_effort"); units != "1" || eff != "1" {
		t.Fatalf("after the retry: unit rows %s, repository effort rows %s, want 1 and 1", units, eff)
	}
	// ROLLBACK of the retry's run removes EVERY version of the unit's efforts, not only the newest: no orphan
	// of the failed run comes back to light.
	if err := Rollback(ctx, conn, org, retryWindow.RunID); err != nil {
		t.Fatal(err)
	}
	if units, eff := rawCount("work_unit_investments"), rawCount("work_unit_repo_effort"); units != "0" || eff != "0" {
		t.Fatalf("after rolling back the retry: raw unit rows %s, raw repository effort rows %s, want 0 and 0", units, eff)
	}

	// A failed apply that is NOT retried is rolled back by its own run id (efforts only, no unit row).
	failing = &failingConn{Conn: conn}
	failWindow := window
	failWindow.RunID = "normalized_legacy:failed-only"
	records, efforts, report, err = Plan(ctx, conn, failWindow)
	if err != nil {
		t.Fatal(err)
	}
	if err := Apply(ctx, failing, failWindow, records, efforts, report); err == nil {
		t.Fatal("the injected failure did not reach the second apply")
	}
	if eff := rawCount("work_unit_repo_effort"); eff != "1" {
		t.Fatalf("the failed apply left %s repository effort rows, want 1", eff)
	}
	if err := Rollback(ctx, conn, org, failWindow.RunID); err != nil {
		t.Fatal(err)
	}
	if eff := rawCount("work_unit_repo_effort"); eff != "0" {
		t.Fatalf("a rollback by the failed run's id left %s repository effort rows", eff)
	}
}

// CHAOS-9227 round 142: an apply that writes several batches and fails in a LATER one leaves rows under the
// run; the retry skips what is stored and writes the rest; ONE run id owns all of it, so a rollback by that
// id (the one the failed apply printed) removes every row, units and efforts, whichever attempt wrote them.
func TestMultiBatchFailedApplyIsOwnedByOneRunAndRollsBackWhole(t *testing.T) {
	old := writeBatch
	writeBatch = 1
	t.Cleanup(func() { writeBatch = old })

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

	const org, repo = "org-9227-batches", "92270020-9227-4227-8227-922792279227"
	day := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	completed := day.Add(14 * time.Hour)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	for _, id := range []string{"ghpr:acme/api#1", "ghpr:acme/api#2", "ghpr:acme/api#3"} {
		exec(`INSERT INTO work_items (repo_id, work_item_id, provider, title, description, type, status, status_raw, project_key, project_id,
 assignees, reporter, created_at, updated_at, started_at, completed_at, closed_at, labels, story_points, sprint_id, sprint_name,
 parent_id, epic_id, url, last_synced, org_id)
VALUES (?, ?, 'github', 'fix(api): crash', NULL, 'pr', 'done', 'Done', 'P', 'p', [], 'r', ?, ?, NULL, ?, NULL, [], NULL, '', '', '', '', '', ?, ?)`,
			repo, id, day.AddDate(0, 0, -2), completed, completed, completed, org)
		exec(`INSERT INTO investment_classifications_daily (repo_id, day, artifact_type, artifact_id, provider, investment_area, project_stream, confidence, rule_id, computed_at, org_id)
VALUES (?, ?, 'work_item', ?, 'github', 'product', 'general', 1.0, 'r', ?, ?)`, repo, day, id, completed, org)
	}
	window := Options{Org: org, From: day.AddDate(0, 0, -1), To: day.AddDate(0, 0, 1)}
	window.RunID = RunIDFor(org, window.From, window.To)
	raw := func(table string) string {
		t.Helper()
		rows, err := conn.Query(ctx, `SELECT toString(count()) FROM `+table+` WHERE org_id = '`+org+`' AND startsWith(work_unit_id, '`+Status+`')`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out string
		for rows.Next() {
			if err := rows.Scan(&out); err != nil {
				t.Fatal(err)
			}
		}
		return out
	}

	records, efforts, report, err := Plan(ctx, conn, window)
	if err != nil || report.Rows != 3 {
		t.Fatalf("plan = %+v, %v", report, err)
	}
	// Batches: 3 effort batches, then unit batches; the 5th write (the second unit batch) fails.
	if err := Apply(ctx, &failingConn{Conn: conn, failOn: 5}, window, records, efforts, report); err == nil {
		t.Fatal("the injected failure did not reach the apply")
	}
	if units := raw("work_unit_investments"); units != "1" {
		t.Fatalf("the half-written apply left %s unit rows, want 1 (one batch of one row was written)", units)
	}
	// The retry is a new plan: the stored unit is skipped, the other two are written, under the SAME run id.
	records, efforts, report, err = Plan(ctx, conn, window)
	if err != nil || report.Rows != 2 || report.Unchanged != 1 {
		t.Fatalf("retry plan = %+v, %v; want 2 rows to write and 1 already stored", report, err)
	}
	if err := Apply(ctx, conn, window, records, efforts, report); err != nil {
		t.Fatal(err)
	}
	if units := raw("work_unit_investments"); units != "3" {
		t.Fatalf("after the retry: %s unit rows, want 3", units)
	}
	// The run id the failed apply printed owns EVERY row: one rollback removes all of them.
	if err := Rollback(ctx, conn, org, window.RunID); err != nil {
		t.Fatal(err)
	}
	if units, eff := raw("work_unit_investments"), raw("work_unit_repo_effort"); units != "0" || eff != "0" {
		t.Fatalf("after the rollback by the run id: unit rows %s, effort rows %s, want 0 and 0", units, eff)
	}
}
