//go:build integration

package chmigrate_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Invariant the consumer of last_synced relies on: a reader that keeps a cursor and reads
// "last_synced > cursor" never permanently misses a row. These cells execute the ways a server stamp
// and the moment a row becomes visible to a reader can come apart, against a real ClickHouse with the
// migration applied. The contract they establish (and what they do NOT establish) is in the PR body.
func TestLastSyncedCursorContractAgainstRealClickHouse(t *testing.T) {
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

	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	millis := func(query string) int64 {
		t.Helper()
		var out int64
		if err := conn.QueryRow(ctx, query).Scan(&out); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return out
	}
	const cols = `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, valid_from, valid_to, updated_at) `
	const stampOf = `SELECT toUnixTimestamp64Milli(last_synced) FROM team_project_ownership WHERE project_id = '%s'`
	stamp := func(project string) int64 { return millis(fmt.Sprintf(stampOf, project)) }

	// Legacy rows, then the real migration file.
	exec(ownershipBeforeLastSynced)
	exec(cols + `VALUES ('o', 'jira', 'T', 'LEGACY', 'LEGACY', 'native', '2020-01-01 00:00:00', NULL, '2020-01-01 00:00:00')`)
	cursorBeforeMigration := time.Now().UTC().UnixMilli()
	time.Sleep(20 * time.Millisecond)
	sql, err := os.ReadFile("sql/099_team_project_ownership_last_synced.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(sql)) {
		exec(statement)
	}

	// Cell 1, MATERIALIZE: a cursor taken before the migration sees every legacy row afterwards.
	if got := stamp("LEGACY"); got <= cursorBeforeMigration {
		t.Fatalf("legacy last_synced %d is not after a cursor taken before the migration (%d): the row would be invisible to it", got, cursorBeforeMigration)
	}

	// Cell 2, part commit: a slow insert (2 s inside its SELECT) and a fast insert that commits while it
	// runs. The fast row becomes visible first; the question is where the slow row's stamp lands.
	var wg sync.WaitGroup
	var slowVisible time.Time
	wg.Add(1)
	go func() {
		defer wg.Done()
		exec(cols + `SELECT 'o', 'jira', 'T', 'SLOW', 'SLOW', 'native', toDateTime64('2020-01-01 00:00:00', 3, 'UTC'), NULL, toDateTime64('2020-01-01 00:00:00', 3, 'UTC') FROM numbers(1) WHERE sleepEachRow(2) = 0`)
		slowVisible = time.Now().UTC()
	}()
	time.Sleep(500 * time.Millisecond)
	exec(cols + `VALUES ('o', 'jira', 'T', 'FAST', 'FAST', 'native', '2020-01-01 00:00:00', NULL, '2020-01-01 00:00:00')`)
	cursor := millis(`SELECT toUnixTimestamp64Milli(max(last_synced)) FROM team_project_ownership`)
	wg.Wait()
	slow := stamp("SLOW")
	t.Logf("cell 2: fast stamp/cursor=%d slow stamp=%d slow visible at=%d", cursor, slow, slowVisible.UnixMilli())
	if slow > slowVisible.UnixMilli() {
		t.Fatalf("a row's last_synced (%d) is later than the moment its insert returned (%d): a stamp must never be in the future of visibility", slow, slowVisible.UnixMilli())
	}
	// The gap the consumer's safety lag must cover, pinned: the slow row is stamped when its insert
	// starts, so it is stamped BEFORE the fast row that became visible first, and it becomes visible
	// only after the 2 s statement. If ClickHouse ever stamps at completion this fails, and the
	// consumer contract in the PR body must be re-derived rather than left as written.
	if slow >= cursor {
		t.Fatalf("the slow row's stamp %d is not before the fast row's cursor %d: the stamp no longer precedes visibility, re-derive the consumer lag contract", slow, cursor)
	}
	if gap := slowVisible.UnixMilli() - slow; gap < 1500 {
		t.Fatalf("stamp-to-visibility gap = %d ms, want at least 1500 ms for a statement that slept 2 s", gap)
	}

	// Cell 3, async insert: the stamp is the server's flush time, never earlier than the client's send.
	sendAt := time.Now().UTC().UnixMilli()
	asyncCtx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"async_insert": 1, "wait_for_async_insert": 1, "async_insert_busy_timeout_ms": 1500}))
	if err := conn.Exec(asyncCtx, cols+`VALUES ('o', 'jira', 'T', 'ASYNC', 'ASYNC', 'native', '2020-01-01 00:00:00', NULL, '2020-01-01 00:00:00')`); err != nil {
		t.Fatal(err)
	}
	asyncDone := time.Now().UTC().UnixMilli()
	asyncStamp := stamp("ASYNC")
	t.Logf("cell 3: async send=%d stamp=%d returned=%d", sendAt, asyncStamp, asyncDone)
	if asyncStamp < sendAt || asyncStamp > asyncDone {
		t.Fatalf("async stamp %d outside [send %d, return %d]", asyncStamp, sendAt, asyncDone)
	}

	// Cell 4, ReplacingMergeTree dedup: the surviving version is the one with the greatest updated_at
	// (equal updated_at: the last inserted), and its stamp is the surviving row's stamp. A later-stamped
	// row with an OLDER updated_at is a stale write that a merge drops.
	exec(cols + `VALUES ('o', 'jira', 'T', 'RMT', 'RMT', 'native', '2020-01-01 00:00:00', NULL, '2020-01-01 00:00:20')`)
	newerVersionStamp := stamp("RMT")
	time.Sleep(20 * time.Millisecond)
	exec(cols + `VALUES ('o', 'jira', 'T', 'RMT', 'RMT', 'native', '2020-01-01 00:00:00', NULL, '2020-01-01 00:00:10')`) // older version, later stamp
	if got := millis(`SELECT toUnixTimestamp64Milli(max(last_synced)) FROM team_project_ownership WHERE project_id = 'RMT'`); got <= newerVersionStamp {
		t.Fatalf("raw read: the stale write's stamp %d is not after the kept version's %d", got, newerVersionStamp)
	}
	exec(`OPTIMIZE TABLE team_project_ownership FINAL`)
	if got := stamp("RMT"); got != newerVersionStamp {
		t.Fatalf("after dedup the surviving row carries stamp %d, want the greater-updated_at version's %d", got, newerVersionStamp)
	}
	// Equal updated_at (one provider stamp for a whole write): the later insert survives with its own, later, stamp.
	exec(cols + `VALUES ('o', 'jira', 'T', 'EQ', 'EQ', 'native', '2020-01-01 00:00:00', NULL, '2020-01-01 00:00:30')`)
	first := stamp("EQ")
	time.Sleep(20 * time.Millisecond)
	exec(cols + `VALUES ('o', 'jira', 'T', 'EQ', 'EQ', 'native', '2020-01-01 00:00:00', NULL, '2020-01-01 00:00:30')`)
	exec(`OPTIMIZE TABLE team_project_ownership FINAL`)
	if got := stamp("EQ"); got <= first {
		t.Fatalf("equal updated_at: surviving stamp %d, want the later insert's (> %d)", got, first)
	}
}
