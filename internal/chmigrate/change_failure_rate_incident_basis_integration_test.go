//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// INVARIANT (CHAOS-8981, migration 112): the migration only adds, and it
// changes no stored value: it holds no mutation. The deprecated
// repo_metrics_daily.change_failure_rate keeps its type (Float64, not
// Nullable) and its stored values, so a reader older than the migration and a
// rollback read what they read before. revert_rate is NOT filled from it: the
// values of the deprecated column were never a measured revert rate, so every
// row keeps NULL (unknown), and a revert rate a later writer stores is left
// alone. dora_metrics_daily is not touched: a row under the old metric name
// stays. A second run changes nothing.
func TestChangeFailureRateMigrationOnlyAddsAndIsSafeToRunAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance) // the whole chain, 112 included
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	for column, want := range map[string]string{
		"change_failure_rate": "Float64", "revert_rate": "Nullable(Float64)",
		"change_failure_rate_incident": "Nullable(Float64)",
	} {
		var columnType string
		if err := conn.QueryRow(ctx, `SELECT type FROM system.columns
			WHERE database = currentDatabase() AND table = 'repo_metrics_daily' AND name = ?`, column).Scan(&columnType); err != nil {
			t.Fatalf("repo_metrics_daily.%s: %v", column, err)
		}
		if columnType != want {
			t.Fatalf("repo_metrics_daily.%s is %s, want %s", column, columnType, want)
		}
	}

	const (
		legacyMerged   = "11111111-1111-4111-8111-111111111111"
		legacyNoMerges = "22222222-2222-4222-8222-222222222222"
		currentWriter  = "33333333-3333-4333-8333-333333333333"
		rollWindow     = "44444444-4444-4444-8444-444444444444"
		afterContract  = "55555555-5555-4555-8555-555555555555"
	)
	// A row as a writer older than the migration stores it: it does not name
	// the new columns, and change_failure_rate is the revert ratio (0.0 with
	// the forced denominator when nothing merged).
	oldWriter := func(repo string, prsMerged int, revertRatio string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO repo_metrics_daily
			(repo_id, day, commits_count, prs_merged, change_failure_rate, computed_at, org_id)
			VALUES (?, '2026-07-01', 3, ?, `+revertRatio+`, now(), 'org')`, repo, prsMerged); err != nil {
			t.Fatal(err)
		}
	}
	oldWriter(legacyMerged, 4, "0.25")
	oldWriter(legacyNoMerges, 0, "0.0")
	// A row with a stored revert rate, as a revert detector will write it, and
	// an incident-based value of its own.
	if err := conn.Exec(ctx, `INSERT INTO repo_metrics_daily
		(repo_id, day, commits_count, prs_merged, change_failure_rate, revert_rate, change_failure_rate_incident, computed_at, org_id)
		VALUES (?, '2026-07-01', 3, 10, 0.1, 0.1, 0.5, now(), 'org')`, currentWriter); err != nil {
		t.Fatal(err)
	}
	// A row as a writer stores it once it no longer writes the deprecated
	// column (the contract step): a revert rate, and the column's default 0.
	// The migration must never replace a stored revert rate.
	if err := conn.Exec(ctx, `INSERT INTO repo_metrics_daily
		(repo_id, day, commits_count, prs_merged, revert_rate, computed_at, org_id)
		VALUES (?, '2026-07-01', 3, 8, 0.25, now(), 'org')`, afterContract); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `INSERT INTO dora_metrics_daily (repo_id, day, metric_name, value, computed_at, org_id) VALUES
		(?, '2026-07-01', 'change_failure_rate', 0.3, now(), 'org'),
		(?, '2026-07-01', 'deployment_frequency', 4, now(), 'org')`, legacyMerged, legacyMerged); err != nil {
		t.Fatal(err)
	}

	sql, err := os.ReadFile("sql/112_change_failure_rate_incident_basis.sql")
	if err != nil {
		t.Fatal(err)
	}
	run := func(pass string) {
		t.Helper()
		for _, statement := range chmigrate.SplitStatements(string(sql)) {
			if err := conn.Exec(ctx, statement); err != nil {
				t.Fatalf("%s run of migration 112: %v", pass, err)
			}
		}
	}
	show := func(v *float64) string {
		if v == nil {
			return "NULL"
		}
		return strconv.FormatFloat(*v, 'g', -1, 64)
	}
	// row is the deprecated ratio, the revert rate and the incident value.
	row := func(repo string) string {
		t.Helper()
		var deprecated float64
		var revert, incident *float64
		if err := conn.QueryRow(ctx, `SELECT change_failure_rate, revert_rate, change_failure_rate_incident
			FROM repo_metrics_daily FINAL WHERE org_id = 'org' AND repo_id = ?`, repo).Scan(&deprecated, &revert, &incident); err != nil {
			t.Fatal(err)
		}
		return show(&deprecated) + " " + show(revert) + " " + show(incident)
	}
	check := func(pass string, want map[string]string) {
		t.Helper()
		for repo, wanted := range want {
			if got := row(repo); got != wanted {
				t.Errorf("%s: row %s = (deprecated, revert_rate, incident) %s, want %s", pass, repo[:8], got, wanted)
			}
		}
		names := map[string]float64{}
		var physical uint64
		rows, err := conn.Query(ctx, `SELECT metric_name, value, count() FROM dora_metrics_daily WHERE org_id = 'org' GROUP BY metric_name, value`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			var value float64
			var count uint64
			if err := rows.Scan(&name, &value, &count); err != nil {
				t.Fatal(err)
			}
			names[name] = value
			physical += count
		}
		if len(names) != 2 || physical != 2 || names["change_failure_rate"] != 0.3 || names["deployment_frequency"] != 4 {
			t.Errorf("%s: dora_metrics_daily holds %v in %d row(s), want its two rows untouched", pass, names, physical)
		}
	}

	run("first")
	check("first run", map[string]string{
		legacyMerged:   "0.25 NULL NULL", // the deprecated ratio stays, and is not copied into revert_rate
		legacyNoMerges: "0 NULL NULL",
		currentWriter:  "0.1 0.1 0.5", // untouched
		afterContract:  "0 0.25 NULL", // a stored revert rate is never replaced
	})
	// A pod older than the migration writes after it.
	oldWriter(rollWindow, 5, "0.2")
	check("after a roll-window write", map[string]string{rollWindow: "0.2 NULL NULL"})
	run("second")
	check("second run", map[string]string{
		legacyMerged:   "0.25 NULL NULL",
		legacyNoMerges: "0 NULL NULL",
		currentWriter:  "0.1 0.1 0.5",
		afterContract:  "0 0.25 NULL",
		rollWindow:     "0.2 NULL NULL", // an older pod's row also gets no revert rate
	})

	// The migration is DDL only: no statement rewrites a row, and no mutation
	// was started on either table by the two runs above or by the chain.
	for _, statement := range chmigrate.SplitStatements(string(sql)) {
		upper := strings.ToUpper(statement)
		if strings.Contains(upper, " UPDATE ") || strings.Contains(upper, " DELETE ") || strings.Contains(upper, "MODIFY COLUMN") || strings.Contains(upper, "INSERT INTO") {
			t.Errorf("migration 112 holds a statement that changes stored rows or a column type:\n%s", statement)
		}
	}
	var mutations uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.mutations
		WHERE database = currentDatabase() AND table IN ('repo_metrics_daily', 'dora_metrics_daily', 'repo_change_failure_daily')`).Scan(&mutations); err != nil {
		t.Fatal(err)
	}
	if mutations != 0 {
		t.Errorf("%d mutation(s) ran on the tables of migration 112, want none", mutations)
	}
}
