//go:build integration

package workerservice

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// partitionKeyTables are the four work-item daily tables whose engine is
// ReplacingMergeTree and whose sorting key has work_scope_id and no repo_id: a
// row of these tables is the row of a work scope.
var partitionKeyTables = []string{
	"work_item_metrics_daily", "work_item_user_metrics_daily",
	"work_item_state_durations_daily", "estimate_coverage_metrics_daily",
}

type partitionKeyItem struct {
	repo  uuid.UUID
	id    string
	scope string
	// points and synced default to 3 story points and the day.
	points float64
	synced time.Time
	// open is an estimated item of the backlog: created on the day, not
	// started, not completed. Every other item is completed on the day.
	open bool
}

func insertPartitionKeyItems(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time, items ...partitionKeyItem) {
	t.Helper()
	created, started, completed := day.Add(9*time.Hour), day.Add(9*time.Hour+30*time.Minute), day.Add(10*time.Hour)
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
		points, synced := 3.0, day
		if item.points != 0 {
			points = item.points
		}
		if !item.synced.IsZero() {
			synced = item.synced
		}
		if item.open {
			if err := itemBatch.Append(item.repo, item.id, "github", "issue", "todo", item.scope, []string{"dev@example.com"},
				created, (*time.Time)(nil), (*time.Time)(nil), &points, orgID, synced); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := itemBatch.Append(item.repo, item.id, "github", "issue", "done", item.scope, []string{"dev@example.com"},
			created, &started, &completed, &points, orgID, synced); err != nil {
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

// workScopeRow is what the four tables hold for one work scope of one day,
// from the newest row of each key. itemsCompletedInAnyVersion is the smallest
// items_completed of every stored row version, merged or not: a version that
// holds the items of a part of the scope shows here.
type workScopeRow struct {
	itemsCompleted, itemsCompletedInAnyVersion uint64
	storyPoints                                float64
	userItemsCompleted                         uint64
	stateItemsTouched                          uint64
	estimatedItems                             uint64
}

func readWorkScopeRow(t *testing.T, ctx context.Context, conn driver.Conn, orgID, scope string, day time.Time) workScopeRow {
	t.Helper()
	var row workScopeRow
	for _, read := range []struct {
		query string
		into  []any
	}{
		{`SELECT toUInt64(sum(items_completed)), toFloat64(sum(story_points_completed)) FROM work_item_metrics_daily FINAL
			WHERE org_id = ? AND work_scope_id = ? AND day = ?`, []any{&row.itemsCompleted, &row.storyPoints}},
		{`SELECT toUInt64(min(items_completed)) FROM work_item_metrics_daily
			WHERE org_id = ? AND work_scope_id = ? AND day = ?`, []any{&row.itemsCompletedInAnyVersion}},
		{`SELECT toUInt64(sum(items_completed)) FROM work_item_user_metrics_daily FINAL
			WHERE org_id = ? AND work_scope_id = ? AND day = ?`, []any{&row.userItemsCompleted}},
		{`SELECT toUInt64(max(items_touched)) FROM work_item_state_durations_daily FINAL
			WHERE org_id = ? AND work_scope_id = ? AND day = ?`, []any{&row.stateItemsTouched}},
		{`SELECT toUInt64(sum(estimated_count)) FROM estimate_coverage_metrics_daily FINAL
			WHERE org_id = ? AND work_scope_id = ? AND day = ?`, []any{&row.estimatedItems}},
	} {
		if err := conn.QueryRow(ctx, read.query, orgID, scope, day).Scan(read.into...); err != nil {
			t.Fatalf("%s: %v", read.query, err)
		}
	}
	return row
}

func (rig *touchedRig) listedRecompute(t *testing.T, ctx context.Context, orgID string, day time.Time, repositories ...uuid.UUID) {
	t.Helper()
	listed := make([]daily.RepositoryID, 0, len(repositories))
	for _, repository := range repositories {
		listed = append(listed, daily.RepositoryID(repository.String()))
	}
	run := rig.startRun(t, ctx, func(tx pgx.Tx) (daily.Run, error) {
		return rig.store.StartRunTx(ctx, tx, daily.StartRunRequest{
			OrganizationID: orgID, TargetDay: day, Generation: "post-sync:" + uuid.NewString(), RepositoryIDs: listed,
		}, nilPartitionPublisher{})
	})
	rig.dispatchAndRun(t, ctx, run)
}

// The daily job runs one repository partition at a time, and four of its
// tables replace rows by a sorting key that has work_scope_id and no repo_id.
// The work-item families therefore compute a work scope once over the items
// of every repository (CHAOS-8849). This test writes the rows of two
// repositories and of the nil repository of one day through the real daily
// families and then forces the merge.
//
// When each repository has its own work scope, every row stays.
//
// When one work scope spans the three, the tables hold one row of each key
// for the scope, and the row holds the items of all three: no stored version
// holds the items of a part of the scope.
func TestDailyJobWritesOneRowOfAWorkScopeOverAllItsRepositories(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	day := time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)
	for _, shape := range []struct {
		name   string
		scopes [3]string
		// merged are the row counts of partitionKeyTables, in order, after
		// the forced merge. want is the row of the first scope.
		merged [4]uint64
		want   workScopeRow
	}{
		{"each repository has its own work scope", [3]string{"scope-a", "scope-b", "scope-none"},
			[4]uint64{3, 3, 6, 3}, workScopeRow{1, 1, 3, 1, 1, 1}},
		{"one work scope spans the two repositories and the nil repository", [3]string{"scope-shared", "scope-shared", "scope-shared"},
			[4]uint64{1, 1, 2, 1}, workScopeRow{3, 3, 9, 3, 3, 3}},
	} {
		t.Run(shape.name, func(t *testing.T) {
			orgID, repoA, repoB := uuid.NewString(), uuid.New(), uuid.New()
			insertTouchedRepo(t, ctx, rig.conn, orgID, repoA, "acme/api", "github")
			insertTouchedRepo(t, ctx, rig.conn, orgID, repoB, "acme/web", "github")
			insertPartitionKeyItems(t, ctx, rig.conn, orgID, day,
				partitionKeyItem{repo: repoA, id: "gh:acme/api#1", scope: shape.scopes[0]},
				partitionKeyItem{repo: repoB, id: "gh:acme/web#1", scope: shape.scopes[1]},
				partitionKeyItem{repo: uuid.Nil, id: "gh:none#1", scope: shape.scopes[2]},
				partitionKeyItem{repo: repoA, id: "gh:acme/api#2", scope: shape.scopes[0], open: true},
				partitionKeyItem{repo: repoB, id: "gh:acme/web#2", scope: shape.scopes[1], open: true},
				partitionKeyItem{repo: uuid.Nil, id: "gh:none#2", scope: shape.scopes[2], open: true})
			rig.fullRecompute(t, ctx, orgID, day)
			if got := readWorkScopeRow(t, ctx, rig.conn, orgID, shape.scopes[0], day); got != shape.want {
				t.Errorf("work scope %s = %+v, want %+v", shape.scopes[0], got, shape.want)
			}
			for index, table := range partitionKeyTables {
				if err := rig.conn.Exec(ctx, `OPTIMIZE TABLE `+table+` FINAL`); err != nil {
					t.Fatal(err)
				}
				if merged := partitionKeyRows(t, ctx, rig.conn, table, orgID, day); merged != shape.merged[index] {
					t.Errorf("%s: %d rows after the merge, want %d", table, merged, shape.merged[index])
				}
			}
			if got := readWorkScopeRow(t, ctx, rig.conn, orgID, shape.scopes[0], day); got != shape.want {
				t.Errorf("work scope %s after the merge = %+v, want %+v", shape.scopes[0], got, shape.want)
			}
		})
	}
}

// A run of listed repositories (the touched-day run of the post-sync fan-out)
// must leave a work scope that a listed repository shares with an unlisted one
// as a run of every repository leaves it.
func TestDailyRunOfListedRepositoriesComputesASharedWorkScopeOverEveryRepository(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	day := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC)
	for _, shape := range []struct {
		name   string
		listed func(repoA, repoB uuid.UUID) uuid.UUID
	}{
		{"a repository is listed", func(repoA, _ uuid.UUID) uuid.UUID { return repoA }},
		{"the nil repository is listed", func(_, _ uuid.UUID) uuid.UUID { return uuid.Nil }},
	} {
		t.Run(shape.name, func(t *testing.T) {
			orgID, repoA, repoB := uuid.NewString(), uuid.New(), uuid.New()
			insertTouchedRepo(t, ctx, rig.conn, orgID, repoA, "acme/api", "github")
			insertTouchedRepo(t, ctx, rig.conn, orgID, repoB, "acme/web", "github")
			insertPartitionKeyItems(t, ctx, rig.conn, orgID, day,
				partitionKeyItem{repo: repoA, id: "gh:acme/api#1", scope: "scope-shared"},
				partitionKeyItem{repo: repoB, id: "gh:acme/web#1", scope: "scope-shared"},
				partitionKeyItem{repo: uuid.Nil, id: "gh:none#1", scope: "scope-shared"},
				partitionKeyItem{repo: repoB, id: "gh:acme/web#2", scope: "scope-web-only"})
			rig.fullRecompute(t, ctx, orgID, day)

			listed := shape.listed(repoA, repoB)
			insertPartitionKeyItems(t, ctx, rig.conn, orgID, day,
				partitionKeyItem{repo: listed, id: "gh:new#1", scope: "scope-shared"})
			rig.listedRecompute(t, ctx, orgID, day, listed)
			afterListed := derivedDaySnapshot(t, ctx, rig.conn, orgID, day)
			if got, want := readWorkScopeRow(t, ctx, rig.conn, orgID, "scope-shared", day), (workScopeRow{4, 3, 12, 4, 4, 0}); got != want {
				t.Errorf("shared work scope after the run of the listed repository = %+v, want %+v", got, want)
			}

			rig.fullRecompute(t, ctx, orgID, day)
			if full := derivedDaySnapshot(t, ctx, rig.conn, orgID, day); !reflect.DeepEqual(afterListed, full) {
				t.Errorf("after the run of the listed repository\n got %v\nfull recompute %v", afterListed, full)
			}
		})
	}
}

// One work item id stored under two repository ids is one item: it counts
// once, and the row with the newest last_synced is the item.
func TestDailyJobCountsAWorkItemStoredUnderTwoRepositoriesOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	day := time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC)
	orgID, repoA, repoB := uuid.NewString(), uuid.New(), uuid.New()
	insertTouchedRepo(t, ctx, rig.conn, orgID, repoA, "acme/api", "github")
	insertTouchedRepo(t, ctx, rig.conn, orgID, repoB, "acme/api", "github")
	insertPartitionKeyItems(t, ctx, rig.conn, orgID, day,
		partitionKeyItem{repo: repoA, id: "gh:acme/api#1", scope: "acme/api", points: 3, synced: day.Add(11 * time.Hour)},
		partitionKeyItem{repo: repoB, id: "gh:acme/api#1", scope: "acme/api", points: 5, synced: day.Add(12 * time.Hour)},
		partitionKeyItem{repo: repoB, id: "gh:acme/api#2", scope: "acme/api", points: 2})
	rig.fullRecompute(t, ctx, orgID, day)
	if got, want := readWorkScopeRow(t, ctx, rig.conn, orgID, "acme/api", day), (workScopeRow{2, 2, 7, 2, 2, 0}); got != want {
		t.Errorf("work scope = %+v, want %+v: two items, and 5 + 2 story points of the newest rows", got, want)
	}
}
