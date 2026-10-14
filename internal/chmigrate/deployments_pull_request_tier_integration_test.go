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

// CHAOS-9610: migration 126 adds deployments.pull_request_tier and
// rollback/126_*.down.sql drops it. The test rehearses both on the pre-126
// shape: a row written before the upgrade reads ” with its number intact, a
// tier written after it survives, the downgrade drops the column and keeps the
// number, and the upgrade runs again (and twice).
func TestDeploymentsPullRequestTierMigrationUpgradesAndDowngrades(t *testing.T) {
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
		if err := conn.QueryRow(ctx, "SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'deployments' AND name = ?", column).Scan(&out); err != nil {
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

	exec(`CREATE TABLE deployments (
    repo_id UUID, deployment_id String, status Nullable(String), environment Nullable(String),
    merged_at Nullable(DateTime64(3, 'UTC')), pull_request_number Nullable(UInt32),
    release_ref String DEFAULT '', last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, deployment_id)`)
	exec(`INSERT INTO deployments (repo_id, deployment_id, pull_request_number, last_synced, org_id) VALUES ('c7198fbc-1945-3717-05d8-eb78866b4e79', 'old', 7, now64(3), 'o')`)

	const up = "sql/126_deployments_pull_request_tier.sql"
	const down = "rollback/126_deployments_pull_request_tier.down.sql"
	run(up)
	if got := columnType("pull_request_tier"); got != "LowCardinality(String)" {
		t.Fatalf("after the upgrade pull_request_tier is %q, want LowCardinality(String)", got)
	}
	if got := readString(`SELECT concat(toString(pull_request_number), '|', pull_request_tier) FROM deployments WHERE deployment_id = 'old'`); got != "7|" {
		t.Errorf("a row written before the upgrade reads %q, want number 7 and an empty tier", got)
	}
	exec(`INSERT INTO deployments (repo_id, deployment_id, pull_request_number, pull_request_tier, last_synced, org_id) VALUES ('c7198fbc-1945-3717-05d8-eb78866b4e79', 'new', 8, 'exact_merge_sha', now64(3), 'o')`)
	if got := readString(`SELECT pull_request_tier FROM deployments WHERE deployment_id = 'new'`); got != "exact_merge_sha" {
		t.Errorf("a tier written after the upgrade reads %q, want exact_merge_sha", got)
	}
	run(up)

	run(down)
	if got := columnType("pull_request_tier"); got != "" {
		t.Errorf("after the downgrade pull_request_tier is still %q", got)
	}
	if got := readString(`SELECT toString(pull_request_number) FROM deployments WHERE deployment_id = 'new'`); got != "8" {
		t.Errorf("a number after the downgrade = %q, want 8 unchanged", got)
	}
	run(down)

	run(up)
	if got := columnType("pull_request_tier"); got != "LowCardinality(String)" {
		t.Errorf("after a second upgrade pull_request_tier is %q, want LowCardinality(String)", got)
	}
}
