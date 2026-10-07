//go:build integration

package workerservice

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// partitionKeyTables are the four work-item daily tables whose engine is
// ReplacingMergeTree and whose sorting key has work_scope_id and no repo_id.
var partitionKeyTables = []string{
	"work_item_metrics_daily", "work_item_user_metrics_daily",
	"work_item_state_durations_daily", "estimate_coverage_metrics_daily",
}

type partitionKeyItem struct {
	repo  uuid.UUID
	id    string
	scope string
}

func insertPartitionKeyItems(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time, items ...partitionKeyItem) {
	t.Helper()
	created, started, completed := day.Add(9*time.Hour), day.Add(9*time.Hour+30*time.Minute), day.Add(10*time.Hour)
	points := 3.0
	itemBatch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
		repo_id, work_item_id, provider, type, status, project_id, assignees, created_at, started_at,
		completed_at, story_points, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	transitionBatch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_transitions (
		repo_id, work_item_id, occurred_at, provider, from_status, to_status, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if err := itemBatch.Append(item.repo, item.id, "github", "issue", "done", item.scope, []string{"dev@example.com"},
			created, &started, &completed, &points, orgID, day); err != nil {
			t.Fatal(err)
		}
		if err := transitionBatch.Append(item.repo, item.id, started, "github", "todo", "in_progress", orgID, day); err != nil {
			t.Fatal(err)
		}
		if err := transitionBatch.Append(item.repo, item.id, completed, "github", "in_progress", "done", orgID, day); err != nil {
			t.Fatal(err)
		}
	}
	if err := itemBatch.Send(); err != nil {
		t.Fatal(err)
	}
	if err := transitionBatch.Send(); err != nil {
		t.Fatal(err)
	}
}

func partitionKeyRows(t *testing.T, ctx context.Context, conn driver.Conn, table, orgID string, day time.Time) uint64 {
	t.Helper()
	var rows uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM `+table+` WHERE org_id = ? AND day = ?`, orgID, day).Scan(&rows); err != nil {
		t.Fatalf("%s: %v", table, err)
	}
	return rows
}

// The daily job computes one day one repository partition at a time. Four of
// its tables replace rows by a sorting key that has work_scope_id and no
// repo_id. This test writes the rows of two repository partitions and of the
// nil-repository partition of one day through the real daily families and
// then forces the merge.
//
// When each partition has its own work scope, every row stays.
//
// When one work scope spans the three partitions, the partitions write one
// key and the merge keeps the row of the partition that ran last: the row
// holds the items of one partition only. That is a limit of the data model
// of the four tables (CHAOS-8849), asserted here as it is today. A fix of
// CHAOS-8849 must change the second case of this test.
func TestDailyJobRepositoryPartitionsOfOneDayDoNotShareAKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	day := time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)
	for _, shape := range []struct {
		name   string
		scopes [3]string
		// written and merged are the row counts of partitionKeyTables, in
		// order, before and after the forced merge.
		written, merged [4]uint64
	}{
		{"each partition has its own work scope", [3]string{"scope-a", "scope-b", "scope-none"},
			[4]uint64{3, 3, 6, 3}, [4]uint64{3, 3, 6, 3}},
		{"one work scope spans the three partitions (CHAOS-8849)", [3]string{"scope-shared", "scope-shared", "scope-shared"},
			[4]uint64{3, 3, 6, 3}, [4]uint64{1, 1, 2, 1}},
	} {
		t.Run(shape.name, func(t *testing.T) {
			orgID, repoA, repoB := uuid.NewString(), uuid.New(), uuid.New()
			insertTouchedRepo(t, ctx, rig.conn, orgID, repoA, "acme/api", "github")
			insertTouchedRepo(t, ctx, rig.conn, orgID, repoB, "acme/web", "github")
			insertPartitionKeyItems(t, ctx, rig.conn, orgID, day,
				partitionKeyItem{repoA, "gh:acme/api#1", shape.scopes[0]},
				partitionKeyItem{repoB, "gh:acme/web#1", shape.scopes[1]},
				partitionKeyItem{uuid.Nil, "gh:none#1", shape.scopes[2]})
			rig.fullRecompute(t, ctx, orgID, day)
			for index, table := range partitionKeyTables {
				written := partitionKeyRows(t, ctx, rig.conn, table, orgID, day)
				if err := rig.conn.Exec(ctx, `OPTIMIZE TABLE `+table+` FINAL`); err != nil {
					t.Fatal(err)
				}
				merged := partitionKeyRows(t, ctx, rig.conn, table, orgID, day)
				if written != shape.written[index] || merged != shape.merged[index] {
					t.Errorf("%s: %d rows written, %d after the merge; want %d written and %d after the merge",
						table, written, merged, shape.written[index], shape.merged[index])
				}
			}
		})
	}
}
