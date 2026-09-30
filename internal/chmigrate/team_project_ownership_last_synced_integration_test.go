//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const ownershipBeforeLastSynced = `CREATE TABLE team_project_ownership (
    org_id String, provider String, team_id String, project_id String, project_key Nullable(String),
    source Enum8('native' = 1, 'jira_legacy' = 2, 'provider_access' = 3, 'manual' = 4, 'inferred' = 5),
    is_primary UInt8 DEFAULT 0, specificity UInt16 DEFAULT 0, priority Int32 DEFAULT 0,
    valid_from DateTime64(3, 'UTC'), valid_to Nullable(DateTime64(3, 'UTC')), updated_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (org_id, provider, project_id, team_id, source, valid_from)`

// A row that existed before the column must keep ONE last_synced value: a
// DEFAULT evaluated at query time would hand a consumer cursor a new "ingest
// time" on every read of every legacy row.
func TestLastSyncedMigrationGivesLegacyOwnershipRowsOneStableValue(t *testing.T) {
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
	read := func(query string) string {
		t.Helper()
		var out string
		if err := conn.QueryRow(ctx, query).Scan(&out); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return out
	}
	const insert = `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, valid_from, valid_to, updated_at) VALUES `
	exec(ownershipBeforeLastSynced)
	exec(insert + `('org-1', 'jira', 'T1', 'P1', 'P1', 'native', '2020-01-01 00:00:00', NULL, '2020-01-01 00:00:00')`)
	exec(`INSERT INTO team_project_ownership SELECT * FROM team_project_ownership`) // a second part, so a mutation spans more than one

	sql, err := os.ReadFile("sql/099_team_project_ownership_last_synced.sql")
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now().UTC().Add(-time.Second)
	for _, statement := range chmigrate.SplitStatements(string(sql)) {
		exec(statement)
	}
	after := time.Now().UTC().Add(time.Second)

	const stamp = `SELECT toString(toUnixTimestamp64Milli(last_synced)) FROM team_project_ownership WHERE project_id = 'P1' ORDER BY last_synced LIMIT 1`
	first := read(stamp)
	time.Sleep(50 * time.Millisecond)
	second := read(stamp)
	if first != second {
		t.Fatalf("a legacy row's last_synced changed between two reads: %s then %s", first, second)
	}
	var millis int64
	if err := conn.QueryRow(ctx, `SELECT toUnixTimestamp64Milli(max(last_synced)) FROM team_project_ownership`).Scan(&millis); err != nil {
		t.Fatal(err)
	}
	if got := time.UnixMilli(millis).UTC(); got.Before(before) || got.After(after) {
		t.Fatalf("a legacy row's last_synced = %s, want the migration time between %s and %s", got, before, after)
	}
	if got := read(`SELECT toString(countIf(last_synced = updated_at)) FROM team_project_ownership`); got != "0" {
		t.Fatalf("%s legacy row(s) took updated_at as last_synced; it must be an ingest time", got)
	}

	exec(insert + `('org-1', 'jira', 'T2', 'P2', 'P2', 'native', '2020-01-01 00:00:00', NULL, '2020-01-01 00:00:00')`)
	if got := read(`SELECT toString(last_synced > now64(3) - INTERVAL 1 MINUTE AND last_synced != updated_at) FROM team_project_ownership WHERE project_id = 'P2'`); strings.TrimSpace(got) != "true" && got != "1" {
		t.Fatalf("a writer that omits last_synced got %s, want the DEFAULT ingest time", got)
	}

	for i := 0; i < 2; i++ { // re-running the file is allowed to move a legacy stamp forward, never to drop a row
		for _, statement := range chmigrate.SplitStatements(string(sql)) {
			exec(statement)
		}
	}
	if got := read(`SELECT toString(count()) FROM team_project_ownership`); got != "3" {
		t.Fatalf("rows after re-running the migration = %s, want 3", got)
	}
}
