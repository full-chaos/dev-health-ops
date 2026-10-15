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

// CHAOS-9432: migration 133 adds provider_team_id, provider_name and gone_at to
// teams and rollback/133_*.down.sql drops them with the version row. The test
// rehearses both on the pre-133 shape: a row written before the upgrade (and a
// row an older writer inserts without the columns) reads ” / ” / NULL; values
// written after the upgrade survive; the downgrade drops the columns and keeps
// the rest of the row; the upgrade runs again (and twice).
func TestTeamsProviderIdentityMigrationUpgradesAndDowngrades(t *testing.T) {
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
		if err := conn.QueryRow(ctx, "SELECT ifNull(max(type), '') FROM system.columns WHERE database = currentDatabase() AND table = 'teams' AND name = ?", column).Scan(&out); err != nil {
			t.Fatal(err)
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

	exec(`CREATE TABLE teams (
    id String, name String, org_id String DEFAULT 'default', provider String DEFAULT '', native_team_key Nullable(String),
    is_active UInt8 DEFAULT 1, updated_at DateTime64(6), last_synced DateTime64(6) DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (org_id, id)`)
	exec(`INSERT INTO teams (id, name, org_id, provider, native_team_key, updated_at) VALUES ('gh:old', 'Old', 'o', 'github', 'old', now64(6))`)

	const up = "sql/133_teams_provider_identity.sql"
	const down = "rollback/133_teams_provider_identity.down.sql"
	versionRows := useSchemaMigrations(t, ctx, conn, "133_teams_provider_identity.sql")
	columns := map[string]string{"provider_team_id": "String", "provider_name": "String", "gone_at": "Nullable(DateTime64(6))"}
	run(up)
	for column, want := range columns {
		if got := columnType(column); got != want {
			t.Fatalf("after the upgrade %s is %q, want %s", column, got, want)
		}
	}
	const read = `SELECT concat(provider_team_id, '|', provider_name, '|', ifNull(toString(gone_at), 'NULL')) FROM teams WHERE id = `
	if got := readString(read + "'gh:old'"); got != "||NULL" {
		t.Errorf("a row written before the upgrade reads %q, want ||NULL", got)
	}
	exec(`INSERT INTO teams (id, name, org_id, provider, native_team_key, updated_at) VALUES ('gh:rolled', 'Rolled', 'o', 'github', 'rolled', now64(6))`)
	if got := readString(read + "'gh:rolled'"); got != "||NULL" {
		t.Errorf("a row inserted without the new columns reads %q, want ||NULL", got)
	}
	exec(`INSERT INTO teams (id, name, org_id, provider, native_team_key, provider_team_id, provider_name, gone_at, updated_at)
		VALUES ('gh:new', 'New', 'o', 'github', 'new', '4711', 'New Team', '2026-10-15 12:00:00.000000', now64(6))`)
	if got := readString(read + "'gh:new'"); got != "4711|New Team|2026-10-15 12:00:00.000000" {
		t.Errorf("values written after the upgrade read %q", got)
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
	if got := readString(`SELECT name FROM teams WHERE id = 'gh:new'`); got != "New" {
		t.Errorf("name after the downgrade = %q, want New unchanged", got)
	}
	run(down)
	if n := versionRows(); n != 0 {
		t.Errorf("after the second rollback the version row is recorded (%d rows)", n)
	}

	run(up)
	for column, want := range columns {
		if got := columnType(column); got != want {
			t.Errorf("after a second upgrade %s is %q, want %s", column, got, want)
		}
	}
}
