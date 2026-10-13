//go:build integration

package providersync

import (
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// CHAOS-9548 reader memory. An org with 88 (team, project) pairs is synced 100
// times. "Before" is what a writer did: every sync writes every pair again.
// "After" is what a writer does now: a pair is written once. The open-row read
// of the writers (FINAL, valid_to IS NULL) runs on both and ClickHouse's own
// query log gives rows read and peak memory. Merges are stopped so a background
// merge cannot flatter the "before" case.
func TestReaderMemoryOfTheOpenOwnershipReadBeforeAndAfter(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES team_project_ownership"); err != nil {
		t.Fatal(err)
	}
	const pairs, syncs = 88, 100
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	key := "PLAT"
	seed := func(org string, syncCount int) {
		t.Helper()
		for sync := 0; sync < syncCount; sync++ {
			// One insert per sync: ClickHouse folds equal keys inside one insert block.
			batch, err := conn.PrepareBatch(ctx, `INSERT INTO team_project_ownership
				(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`)
			if err != nil {
				t.Fatal(err)
			}
			for pair := 0; pair < pairs; pair++ {
				if err := batch.Append(org, "jira", fmt.Sprintf("jira:team-%d", pair%11), fmt.Sprintf("project-%d", pair), &key,
					"jira_legacy", uint8(1), uint16(100), int32(10), base, (*time.Time)(nil), base.Add(time.Duration(sync)*time.Minute)); err != nil {
					t.Fatal(err)
				}
			}
			if err := batch.Send(); err != nil {
				t.Fatal(err)
			}
		}
	}
	seed("memory-before", syncs)
	seed("memory-after", 1)

	measure := func(org string) (rows int, readRows, memory uint64) {
		t.Helper()
		comment := fmt.Sprintf("chaos9548-%s-%d", org, time.Now().UnixNano())
		qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
		open, err := jiraOpenCatalogOwnership(qctx, conn, org)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
			t.Skipf("the query log is not readable by this user: %v", err)
		}
		if err := conn.QueryRow(ctx, `SELECT read_rows, memory_usage FROM system.query_log
			WHERE log_comment = ? AND type = 'QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1`, comment).Scan(&readRows, &memory); err != nil {
			t.Skipf("the query log has no row for the read: %v", err)
		}
		return len(open), readRows, memory
	}
	beforeOpen, beforeRead, beforeMemory := measure("memory-before")
	afterOpen, afterRead, afterMemory := measure("memory-after")
	t.Logf("reader memory: pairs=%d syncs=%d", pairs, syncs)
	t.Logf("before (every pair written at every sync): open rows=%d read_rows=%d peak_memory_bytes=%d", beforeOpen, beforeRead, beforeMemory)
	t.Logf("after  (a pair written once):               open rows=%d read_rows=%d peak_memory_bytes=%d", afterOpen, afterRead, afterMemory)
	if beforeOpen != pairs || afterOpen != pairs {
		t.Fatalf("open rows before %d after %d, want %d for both: the read must give the same answer", beforeOpen, afterOpen, pairs)
	}
	if afterRead*10 > beforeRead {
		t.Fatalf("rows read after %d is not an order of magnitude under before %d", afterRead, beforeRead)
	}
}
