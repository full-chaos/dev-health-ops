//go:build integration

package report

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Throwaway leg (do not merge): the report source of a key with two versions in the same computed_at second.

const (
	samePickOrg  = "f0f0f0f0-0000-4000-8000-000000014603"
	samePickRepo = "f0f0f0f0-0000-4000-8000-0000000000b1"
	samePickAt   = "2026-08-21 10:00:00"
)

type samePickTable struct {
	name     string
	insert   string
	versionA []any
	versionB []any
	valueA   string
	valueB   string
	value    string
	keyWhere string
	hash     string
	replaces bool
}

func samePickTables() []samePickTable {
	return []samePickTable{
		{
			name: "issue_type_metrics_daily",
			insert: `INSERT INTO issue_type_metrics_daily (repo_id, day, provider, team_id, issue_type_norm, created_count,
				completed_count, active_count, cycle_p50_hours, cycle_p90_hours, lead_p50_hours, computed_at, org_id)
				VALUES (?, '2026-08-21', 'github', 'team-a', 'issue', ?, ?, ?, 0.5, 0.5, ?, toDateTime(?, 'UTC'), ?)`,
			versionA: []any{samePickRepo, uint32(1), uint32(1), uint32(1), 2.5, samePickAt, samePickOrg},
			versionB: []any{samePickRepo, uint32(2), uint32(2), uint32(2), nil, samePickAt, samePickOrg},
			valueA:   "1",
			valueB:   "2",
			value:    "completed_count",
			keyWhere: "toString(repo_id) = '" + samePickRepo + "' AND day = '2026-08-21' AND provider = 'github' AND team_id = 'team-a' AND issue_type_norm = 'issue'",
			hash:     "cityHash64(tuple(*))",
		},
		{
			name: "work_item_completion_daily",
			insert: `INSERT INTO work_item_completion_daily (org_id, repo_id, day, team_id, delivery_units, work_items_completed,
				prs_merged, churn_loc, cycle_p50_hours, computed_at)
				VALUES (?, ?, '2026-08-21', 'team-a', ?, ?, 0, ?, 0.5, toDateTime(?, 'UTC'))`,
			versionA: []any{samePickOrg, samePickRepo, uint32(3), uint32(3), uint64(10), samePickAt},
			versionB: []any{samePickOrg, samePickRepo, uint32(4), uint32(4), nil, samePickAt},
			valueA:   "3",
			valueB:   "4",
			value:    "work_items_completed",
			keyWhere: "toString(repo_id) = '" + samePickRepo + "' AND day = '2026-08-21' AND team_id = 'team-a'",
			hash:     "cityHash64(tuple(*))",
		},
		{
			name: "work_item_metrics_daily",
			insert: `INSERT INTO work_item_metrics_daily (day, provider, work_scope_id, team_id, team_name, items_started,
				items_completed, items_started_unassigned, items_completed_unassigned, wip_count_end_of_day,
				wip_unassigned_end_of_day, cycle_time_p50_hours, bug_completed_ratio, story_points_completed, computed_at, org_id, row_view)
				VALUES ('2026-08-21', 'jira', 'scope-a', 'team-a', 'Team A', 0, ?, 0, 0, 0, 0, ?, 0, 0, toDateTime(?, 'UTC'), ?, 'org')`,
			versionA: []any{uint32(3), 4.0, samePickAt, samePickOrg},
			versionB: []any{uint32(5), nil, samePickAt, samePickOrg},
			valueA:   "3",
			valueB:   "5",
			value:    "items_completed",
			keyWhere: "day = '2026-08-21' AND provider = 'jira' AND work_scope_id = 'scope-a' AND team_id = 'team-a' AND row_view = 'org'",
			hash:     "tupleElement(" + rowview.WorkItemMetricsNewestOrder + ", 2)",
			replaces: true,
		},
	}
}

func samePickInsert(t *testing.T, ctx context.Context, conn driver.Conn, table samePickTable, versions ...[]any) {
	t.Helper()
	if err := conn.Exec(ctx, "TRUNCATE TABLE "+table.name); err != nil {
		t.Fatal(err)
	}
	for _, version := range versions {
		if err := conn.Exec(ctx, table.insert, version...); err != nil {
			t.Fatalf("%s: %v", table.name, err)
		}
	}
}

// samePickHashes is the stored value and the order hash of each stored version of the key.
func samePickHashes(t *testing.T, ctx context.Context, conn driver.Conn, table samePickTable) map[string]uint64 {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT v, isNull(h), ifNull(h, 0) FROM (SELECT toString(`+table.value+`) AS v, `+table.hash+` AS h
		FROM `+table.name+` WHERE org_id = ? AND `+table.keyWhere+`)`, samePickOrg)
	if err != nil {
		t.Fatalf("%s: %v", table.name, err)
	}
	defer rows.Close()
	hashes := map[string]uint64{}
	for rows.Next() {
		var value string
		var null uint8
		var hash uint64
		if err := rows.Scan(&value, &null, &hash); err != nil {
			t.Fatal(err)
		}
		if null != 0 {
			t.Errorf("%s: the order hash of version %s is NULL", table.name, value)
		}
		hashes[value] = hash
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return hashes
}

// samePickReads reads the key through the real report source n times and returns the distinct values served.
func samePickReads(t *testing.T, ctx context.Context, conn driver.Conn, table samePickTable, n int) map[string]int {
	t.Helper()
	served := map[string]int{}
	for read := 0; read < n; read++ {
		rows, err := conn.Query(ctx, `SELECT toString(`+table.value+`) FROM `+dedupFromSource(table.name)+` WHERE org_id = ? AND `+table.keyWhere, samePickOrg)
		if err != nil {
			t.Fatalf("%s: %v", table.name, err)
		}
		count := 0
		for rows.Next() {
			var value string
			if err := rows.Scan(&value); err != nil {
				t.Fatal(err)
			}
			served[value]++
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if count != 1 {
			t.Errorf("%s: read %d gave %d rows of the key, want 1", table.name, read, count)
		}
	}
	return served
}

func TestReportSourcePicksOneVersionOfAKeyTiedInOneSecond(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, table := range samePickTables() {
		t.Run(table.name, func(t *testing.T) {
			samePickInsert(t, ctx, conn, table, table.versionA, table.versionB)
			hashes := samePickHashes(t, ctx, conn, table)
			if len(hashes) != 2 {
				t.Fatalf("%s: %d distinct versions stored (%v), want 2", table.name, len(hashes), hashes)
			}
			want, wantHash := "", uint64(0)
			for value, hash := range hashes {
				if want == "" || hash > wantHash {
					want, wantHash = value, hash
				}
			}
			var hashList []uint64
			for _, hash := range hashes {
				hashList = append(hashList, hash)
			}
			if hashList[0] == hashList[1] {
				t.Errorf("%s: both versions have the order hash %d", table.name, hashList[0])
			}
			t.Logf("SAME-SECOND %s: hashes %v; the higher hash serves %s", table.name, hashes, want)

			check := func(phase string, served map[string]int, want string) {
				t.Helper()
				t.Logf("SAME-SECOND %s %s: served %v", table.name, phase, served)
				if len(served) != 1 || served[want] == 0 {
					t.Errorf("%s %s: served %v over the reads, want %s on every read", table.name, phase, served, want)
				}
			}
			check("A then B, 2 parts, 20 reads", samePickReads(t, ctx, conn, table, 20), want)

			merged := func(order string, last string) {
				t.Helper()
				if err := conn.Exec(ctx, "OPTIMIZE TABLE "+table.name+" FINAL"); err != nil {
					t.Fatal(err)
				}
				served := samePickReads(t, ctx, conn, table, 20)
				if !table.replaces {
					check(order+", after OPTIMIZE FINAL, 20 reads", served, want)
					return
				}
				// ReplacingMergeTree(computed_at) keeps the last inserted version of a tie (ops #6798 RISK-NOTES).
				t.Logf("SERVED-FLIP %s %s: before the merge the higher order hash serves %s; after OPTIMIZE FINAL the last inserted serves %s (stored %v)",
					table.name, order, want, last, samePickHashes(t, ctx, conn, table))
				check(order+", after OPTIMIZE FINAL (last inserted kept), 20 reads", served, last)
			}
			merged("A then B", table.valueB)

			samePickInsert(t, ctx, conn, table, table.versionB, table.versionA)
			if again := samePickHashes(t, ctx, conn, table); again[want] != wantHash {
				t.Errorf("%s: the hash of version %s moved with the insert order: %v then %v", table.name, want, hashes, again)
			}
			check("B then A, 2 parts, 20 reads", samePickReads(t, ctx, conn, table, 20), want)
			merged("B then A", table.valueA)
		})
	}
}
