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
	failed  bool
}

func (c *failingConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	if strings.Contains(query, "INSERT INTO") {
		c.batches++
		if c.batches == 2 && !c.failed {
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
	if err := Apply(ctx, conn, window, retry, retryEfforts, retryReport); err != nil {
		t.Fatal(err)
	}
	if units, eff := count("work_unit_investments"), count("work_unit_repo_effort"); units != "1" || eff != "1" {
		t.Fatalf("after the retry: unit rows %s, repository effort rows %s, want 1 and 1", units, eff)
	}
}
