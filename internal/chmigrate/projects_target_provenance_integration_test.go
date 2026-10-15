//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9613: migration 127 adds the seven target provenance columns of
// projects and rollback/127_*.down.sql drops them. The test rehearses both on
// the pre-127 shape: a row written before the upgrade reads unknown (never
// "none dated"), except a Linear project that carries a date, which reads
// stated by Linear's own date; a row an older writer inserts without the new
// columns reads the same way; values written after the upgrade survive; the
// downgrade drops the columns and keeps target_date; the upgrade runs again
// (and twice).
func TestProjectsTargetProvenanceMigrationUpgradesAndDowngrades(t *testing.T) {
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
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	run := func(path string) {
		t.Helper()
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, statement := range chmigrate.SplitStatements(string(body)) {
			exec(statement)
		}
	}
	columnType := func(column string) string {
		t.Helper()
		var out string
		if err := conn.QueryRow(ctx, "SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'projects' AND name = ?", column).Scan(&out); err != nil {
			return ""
		}
		return out
	}
	readString := func(query string) string {
		t.Helper()
		var out string
		if err := conn.QueryRow(ctx, query).Scan(&out); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return out
	}

	exec(`CREATE TABLE projects (
    id String, org_id String, provider String, project_key Nullable(String), name String, is_active UInt8 DEFAULT 1,
    state LowCardinality(String) DEFAULT '', target_date Nullable(Date), url String DEFAULT '',
    updated_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (org_id, provider, id)`)
	exec(`INSERT INTO projects (id, org_id, provider, name, target_date, updated_at) VALUES
		('old-linear-dated', 'o', 'linear', 'Old', '2026-09-30', now64(3)),
		('old-linear-undated', 'o', 'linear', 'Old', NULL, now64(3)),
		('old-jira', 'o', 'jira', 'Old', NULL, now64(3)),
		('old-gitlab', 'o', 'gitlab', 'Old', NULL, now64(3))`)

	const up = "sql/127_projects_target_provenance.sql"
	const down = "rollback/127_projects_target_provenance.down.sql"
	versionRows := useSchemaMigrations(t, ctx, conn, "127_projects_target_provenance.sql")
	columns := map[string]string{
		"target_state": "LowCardinality(String)", "target_kind": "LowCardinality(String)", "target_source": "LowCardinality(String)", "target_ref": "String",
		"target_ref_name": "String", "target_other_open": "Nullable(UInt32)", "target_fetched_at": "Nullable(DateTime64(3, 'UTC'))",
	}
	run(up)
	for column, want := range columns {
		if got := columnType(column); got != want {
			t.Fatalf("after the upgrade %s is %q, want %s", column, got, want)
		}
	}
	const readTarget = `SELECT concat(ifNull(toString(target_date), 'NULL'), '|', target_state, '|', target_kind, '|', target_source, '|', target_ref, '|', target_ref_name, '|',
		ifNull(toString(target_other_open), 'NULL'), '|', ifNull(toString(target_fetched_at), 'NULL')) FROM projects WHERE id = `
	// A row written before the upgrade: a Linear project that carries a date reads
	// as stated by Linear's own date, every other row reads unknown, never none_dated.
	for id, want := range map[string]string{
		"old-linear-dated":   "2026-09-30|stated|project|linear.project.targetDate|||NULL|NULL",
		"old-linear-undated": "NULL|unknown|||||NULL|NULL",
		"old-jira":           "NULL|unknown|||||NULL|NULL",
		"old-gitlab":         "NULL|unknown|||||NULL|NULL",
	} {
		if got := readString(readTarget + "'" + id + "'"); got != want {
			t.Errorf("%s written before the upgrade reads %q, want %q", id, got, want)
		}
	}
	// An older writer that does not name the new columns during a roll: the
	// defaults fill them at insert, with the same reading and never a wrong claim.
	exec(`INSERT INTO projects (id, org_id, provider, name, target_date, updated_at) VALUES
		('rolled-linear-dated', 'o', 'linear', 'Rolled', '2026-10-31', now64(3)),
		('rolled-linear-undated', 'o', 'linear', 'Rolled', NULL, now64(3)),
		('rolled-jira', 'o', 'jira', 'Rolled', NULL, now64(3))`)
	for id, want := range map[string]string{
		"rolled-linear-dated":   "2026-10-31|stated|project|linear.project.targetDate|||NULL|NULL",
		"rolled-linear-undated": "NULL|unknown|||||NULL|NULL",
		"rolled-jira":           "NULL|unknown|||||NULL|NULL",
	} {
		if got := readString(readTarget + "'" + id + "'"); got != want {
			t.Errorf("%s inserted without the new columns reads %q, want %q", id, got, want)
		}
	}
	exec(`INSERT INTO projects (id, org_id, provider, name, target_date, target_state, target_kind, target_source, target_ref, target_ref_name, target_other_open, target_fetched_at, updated_at)
		VALUES ('new', 'o', 'jira', 'New', '2026-12-15', 'stated', 'version', 'jira.version.releaseDate', '101', '2.0', 2, '2026-10-14 12:00:00.000', now64(3))`)
	if got := readString(`SELECT concat(target_state, '|', target_ref, '|', toString(target_other_open)) FROM projects WHERE id = 'new'`); got != "stated|101|2" {
		t.Errorf("values written after the upgrade read %q, want stated|101|2", got)
	}
	run(up)

	run(down)
	if n := versionRows(); n != 0 {
		t.Errorf("after the rollback the version row is still recorded (%d rows)", n)
	}
	for column := range columns {
		if got := columnType(column); got != "" {
			t.Errorf("after the downgrade %s is still %q", column, got)
		}
	}
	if got := readString(`SELECT toString(target_date) FROM projects WHERE id = 'new'`); got != "2026-12-15" {
		t.Errorf("target_date after the downgrade = %q, want 2026-12-15 unchanged", got)
	}
	run(down)
	if n := versionRows(); n != 0 {
		t.Errorf("after the rollback the version row is still recorded (%d rows)", n)
	}

	run(up)
	for column, want := range columns {
		if got := columnType(column); got != want {
			t.Errorf("after a second upgrade %s is %q, want %s", column, got, want)
		}
	}
}
