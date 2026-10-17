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

// Migration 138 adds identity_path to repos and rollback/138_*.down.sql drops
// it with the version row. Rehearsed on the post-134 shape: a row written
// before the upgrade (and a row an older writer inserts without the column)
// reads empty; a value written after the upgrade survives; the downgrade drops
// the column and keeps the rest of the row, provider_repo_id included; the
// upgrade runs again (and twice).
func TestReposIdentityPathMigrationUpgradesAndDowngrades(t *testing.T) {
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
	columnType := func() string {
		t.Helper()
		var out string
		if err := conn.QueryRow(ctx, "SELECT ifNull(max(type), '') FROM system.columns WHERE database = currentDatabase() AND table = 'repos' AND name = 'identity_path'").Scan(&out); err != nil {
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

	exec(`CREATE TABLE repos (
    id UUID, repo String, ref Nullable(String), created_at DateTime64(3, 'UTC'), settings Nullable(String),
    tags Nullable(String), last_synced DateTime64(3, 'UTC'), org_id String DEFAULT 'default', provider String DEFAULT '',
    provider_repo_id String DEFAULT ''
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, id)`)
	exec(`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider, provider_repo_id) VALUES ('00000000-0000-0000-0000-000000000001', 'acme/old', now64(3), now64(3), 'o', 'gitlab', '41')`)

	const up = "sql/138_repos_identity_path.sql"
	const down = "rollback/138_repos_identity_path.down.sql"
	versionRows := useSchemaMigrations(t, ctx, conn, "138_repos_identity_path.sql")
	run(up)
	if got := columnType(); got != "String" {
		t.Fatalf("after the upgrade identity_path is %q, want String", got)
	}
	const read = `SELECT identity_path FROM repos FINAL WHERE id = `
	if got := readString(read + "'00000000-0000-0000-0000-000000000001'"); got != "" {
		t.Errorf("a row written before the upgrade reads %q, want empty", got)
	}
	exec(`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider) VALUES ('00000000-0000-0000-0000-000000000002', 'acme/rolled', now64(3), now64(3), 'o', 'gitlab')`)
	if got := readString(read + "'00000000-0000-0000-0000-000000000002'"); got != "" {
		t.Errorf("a row inserted without the new column reads %q, want empty", got)
	}
	exec(`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider, provider_repo_id, identity_path) VALUES ('00000000-0000-0000-0000-000000000003', 'acme/renamed', now64(3), now64(3), 'o', 'gitlab', '42', 'acme/first')`)
	if got := readString(read + "'00000000-0000-0000-0000-000000000003'"); got != "acme/first" {
		t.Errorf("a value written after the upgrade reads %q, want acme/first", got)
	}
	run(up)

	run(down)
	if n := versionRows(); n != 0 {
		t.Errorf("after the rollback the version row is still recorded (%d rows)", n)
	}
	if got := columnType(); got != "" {
		t.Errorf("after the downgrade identity_path is still %q", got)
	}
	if got := readString(`SELECT concat(repo, '|', provider_repo_id) FROM repos FINAL WHERE id = '00000000-0000-0000-0000-000000000003'`); got != "acme/renamed|42" {
		t.Errorf("repo and provider_repo_id after the downgrade = %q, want acme/renamed|42 unchanged", got)
	}
	run(down)
	if n := versionRows(); n != 0 {
		t.Errorf("after the second rollback the version row is recorded (%d rows)", n)
	}

	run(up)
	if got := columnType(); got != "String" {
		t.Errorf("after a second upgrade identity_path is %q, want String", got)
	}
}
