//go:build integration

package workerservice

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// issue_type_metrics_daily and work_item_completion_daily keep every row
// version (plain MergeTree), and computed_at is a DateTime of one second. Two
// runs that write one key in the same second store two versions of the key
// with the same computed_at. derivedDaySnapshot must then read ONE of them
// whatever the insert order: the version with the highest hash of the row, the
// rule of the served readers on a computed_at tie (CHAOS-14430).
var tiedVersionTables = []string{"issue_type_metrics_daily", "work_item_completion_daily"}

const tiedVersionStamp = "2006-01-02 15:04:05"

// seedListedRunOfRepositoryA is the a_repository_is_listed shape: a full
// recompute, one new item of repository A, then the run of repository A.
func seedListedRunOfRepositoryA(t *testing.T, ctx context.Context, rig *touchedRig, day time.Time) (string, uuid.UUID) {
	t.Helper()
	orgID, repoA, repoB := uuid.NewString(), uuid.New(), uuid.New()
	insertTouchedRepo(t, ctx, rig.conn, orgID, repoA, "acme/api", "github")
	insertTouchedRepo(t, ctx, rig.conn, orgID, repoB, "acme/web", "github")
	insertPartitionKeyItems(t, ctx, rig.conn, orgID, day,
		partitionKeyItem{repo: repoA, id: "gh:acme/api#1", scope: "scope-shared"},
		partitionKeyItem{repo: repoB, id: "gh:acme/web#1", scope: "scope-shared"},
		partitionKeyItem{repo: uuid.Nil, id: "gh:none#1", scope: "scope-shared"},
		partitionKeyItem{repo: repoB, id: "gh:acme/web#2", scope: "scope-web-only"})
	rig.fullRecompute(t, ctx, orgID, day)
	insertPartitionKeyItems(t, ctx, rig.conn, orgID, day,
		partitionKeyItem{repo: repoA, id: "gh:new#1", scope: "scope-shared"})
	rig.listedRecompute(t, ctx, orgID, day, repoA)
	return orgID, repoA
}

// copyRepositoryVersion stores again the rows of one repository written at
// from, with computed_at set to to.
func copyRepositoryVersion(t *testing.T, ctx context.Context, conn driver.Conn, table, orgID string, day time.Time, repo uuid.UUID, from, to time.Time) {
	t.Helper()
	rowsAt := func(at time.Time) uint64 {
		var rows uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM `+table+`
			WHERE org_id = ? AND day = ? AND toString(repo_id) = ? AND computed_at = toDateTime(?, 'UTC')`,
			orgID, day, repo.String(), at.UTC().Format(tiedVersionStamp)).Scan(&rows); err != nil {
			t.Fatalf("%s: %v", table, err)
		}
		return rows
	}
	copied, before := rowsAt(from), rowsAt(to)
	source, err := conn.Query(ctx, `SELECT * REPLACE (toDateTime(?, 'UTC') AS computed_at) FROM `+table+`
		WHERE org_id = ? AND day = ? AND toString(repo_id) = ? AND computed_at = toDateTime(?, 'UTC')`,
		to.UTC().Format(tiedVersionStamp), orgID, day, repo.String(), from.UTC().Format(tiedVersionStamp))
	if err != nil {
		t.Fatalf("%s: read version: %v", table, err)
	}
	columns := source.ColumnTypes()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO `+table)
	if err != nil {
		t.Fatalf("%s: prepare copy: %v", table, err)
	}
	for source.Next() {
		pointers := make([]any, len(columns))
		for index, column := range columns {
			pointers[index] = reflect.New(column.ScanType()).Interface()
		}
		if err := source.Scan(pointers...); err != nil {
			t.Fatalf("%s: scan version: %v", table, err)
		}
		values := make([]any, len(pointers))
		for index, pointer := range pointers {
			values[index] = reflect.ValueOf(pointer).Elem().Interface()
		}
		if err := batch.Append(values...); err != nil {
			t.Fatalf("%s: append copy: %v", table, err)
		}
	}
	if err := source.Err(); err != nil {
		t.Fatalf("%s: read version: %v", table, err)
	}
	source.Close()
	if err := batch.Send(); err != nil {
		t.Fatalf("%s: copy version: %v", table, err)
	}
	for polls := 0; rowsAt(to) < before+copied; polls++ {
		if polls == 40 {
			t.Fatalf("%s: the copied version is not readable after %d polls", table, polls)
		}
		time.Sleep(250 * time.Millisecond)
		t.Logf("%s: copied version not readable yet (poll %d)", table, polls+1)
	}
}

func TestDerivedDaySnapshotReadsOneVersionOfAKeyTiedOnComputedAt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	day := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC)
	for _, round := range []int{1, 2} {
		for _, shape := range []struct {
			name      string
			freshLast bool
		}{
			{"the older version is stored last", false},
			{"the newer version is stored last", true},
		} {
			t.Run(fmt.Sprintf("round %d/%s", round, shape.name), func(t *testing.T) {
				orgID, repoA := seedListedRunOfRepositoryA(t, ctx, rig, day)
				for _, table := range tiedVersionTables {
					var oldest, newest time.Time
					if err := rig.conn.QueryRow(ctx, `SELECT min(computed_at), max(computed_at) FROM `+table+`
						WHERE org_id = ? AND day = ? AND toString(repo_id) = ?`, orgID, day, repoA.String()).Scan(&oldest, &newest); err != nil {
						t.Fatalf("%s: %v", table, err)
					}
					if newest.After(oldest) {
						copyRepositoryVersion(t, ctx, rig.conn, table, orgID, day, repoA, oldest, newest)
					}
					if shape.freshLast {
						copyRepositoryVersion(t, ctx, rig.conn, table, orgID, day, repoA, newest, newest)
					}

					rows, err := rig.conn.Query(ctx, `SELECT row, cityHash64(row) FROM (
						SELECT toString(tuple(* EXCEPT (computed_at))) AS row FROM `+table+`
						WHERE org_id = ? AND day = ? AND toString(repo_id) = ? AND computed_at = toDateTime(?, 'UTC'))`,
						orgID, day, repoA.String(), newest.UTC().Format(tiedVersionStamp))
					if err != nil {
						t.Fatalf("%s: %v", table, err)
					}
					versions := map[string]uint64{}
					want, wantHash := "", uint64(0)
					for rows.Next() {
						var row string
						var hash uint64
						if err := rows.Scan(&row, &hash); err != nil {
							t.Fatal(err)
						}
						versions[row] = hash
						if want == "" || hash > wantHash {
							want, wantHash = row, hash
						}
					}
					if err := rows.Err(); err != nil {
						t.Fatal(err)
					}
					rows.Close()
					if len(versions) != 2 {
						t.Fatalf("%s: %d distinct versions at the newest computed_at, want 2 (a tie of an older and a newer version): %v", table, len(versions), versions)
					}

					var got []string
					picked, err := rig.conn.Query(ctx, `SELECT toString(tuple(* EXCEPT (computed_at))) FROM `+derivedDaySources[table]+`
						WHERE toString(repo_id) = ?`, orgID, day, repoA.String())
					if err != nil {
						t.Fatalf("%s: %v", table, err)
					}
					for picked.Next() {
						var row string
						if err := picked.Scan(&row); err != nil {
							t.Fatal(err)
						}
						got = append(got, row)
					}
					if err := picked.Err(); err != nil {
						t.Fatal(err)
					}
					picked.Close()
					if len(got) != 1 || got[0] != want {
						t.Errorf("%s: the snapshot read %v of the tied key, want the one version with the highest row hash %s (versions %v)", table, got, want, versions)
					}
				}
			})
		}
	}
}

// A row with a NULL column (repo_id of the nil repository, a churn_loc with
// no sized pull request) counts in the snapshot hash like any other row.
func TestDerivedDaySnapshotHashCountsEveryRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	day := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC)
	orgID, _ := seedListedRunOfRepositoryA(t, ctx, rig, day)
	for table, source := range derivedDaySources {
		var rows, nullHashes uint64
		if err := rig.conn.QueryRow(ctx, `SELECT count(), countIf(isNull(`+derivedRowHash+`)) FROM `+source, orgID, day).Scan(&rows, &nullHashes); err != nil {
			t.Fatalf("%s: %v", table, err)
		}
		if rows == 0 {
			t.Errorf("%s: no row on the seeded day; the cell proves nothing for it", table)
		}
		if nullHashes != 0 {
			t.Errorf("%s: %d of %d rows have a NULL row hash and do not count in the snapshot", table, nullHashes, rows)
		}
	}
}
