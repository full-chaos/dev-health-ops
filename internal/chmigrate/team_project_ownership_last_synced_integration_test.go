//go:build integration

package chmigrate_test

import (
	"context"
	"strings"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A writer that omits last_synced gets the server's ingest time (migration 099's DEFAULT), never its own
// updated_at, and the value is stored: two reads of one row give one value, so a consumer cursor never sees a
// row move. (The legacy-row cells are archived with 099, CHAOS-11751.)
func TestOwnershipLastSyncedIsTheStoredIngestTime(t *testing.T) {
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
	chschema.Apply(ctx, t, instance)
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
	exec(insert + `('org-1', 'jira', 'T2', 'P2', 'P2', 'native', '2020-01-01 00:00:00', NULL, '2020-01-01 00:00:00')`)
	if got := read(`SELECT toString(last_synced > now64(3) - INTERVAL 1 MINUTE AND last_synced != updated_at) FROM team_project_ownership WHERE project_id = 'P2'`); strings.TrimSpace(got) != "true" && got != "1" {
		t.Fatalf("a writer that omits last_synced got %s, want the DEFAULT ingest time", got)
	}
	const stamp = `SELECT toString(toUnixTimestamp64Milli(last_synced)) FROM team_project_ownership WHERE project_id = 'P2'`
	first := read(stamp)
	time.Sleep(50 * time.Millisecond)
	if second := read(stamp); first != second {
		t.Fatalf("a row's last_synced changed between two reads: %s then %s", first, second)
	}
}
