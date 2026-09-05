//go:build integration

package chschema

import (
	"context"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// replacingMergeTreeTable is one row of the CHAOS-4902 AUTHORITATIVE sweep
// population: the engine after all migrations apply, read from
// system.tables, rather than a regex over migration text (which answered
// 88 against the schema's original 85 -- see the count test below for the
// current count and its own history).
type replacingMergeTreeTable struct {
	name       string
	sortingKey string
	engineFull string
}

// engineFullVersionColumn extracts a ReplacingMergeTree's version-column
// argument from system.tables' engine_full text, e.g.
// "ReplacingMergeTree(computed_at) PARTITION BY ... ORDER BY ..." ->
// "computed_at". An EMPTY capture (bare "ReplacingMergeTree()", no
// argument) means the engine has no declared version column, so ClickHouse
// merges pick an ARBITRARY row on collapse -- exactly the dedup-key gap
// this sweep exists to catch, so it is reported, not silently accepted.
var engineFullVersionColumn = regexp.MustCompile(`ReplacingMergeTree\(([^)]*)\)`)

// sweepReplacingMergeTreeTables applies the full migration chain to a fresh
// ClickHouse container and reads every Replacing* table's name, sorting
// key, and full engine declaration. Shared by both tests below so the
// (expensive) container start + migration apply happens exactly once.
func sweepReplacingMergeTreeTables(t *testing.T) []replacingMergeTreeTable {
	t.Helper()

	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	// A remote-DSN instance (containers.remoteDSN) sets Instance.cleanup to
	// drop the scratch database it created -- the only thing standing
	// between a shared server and an unbounded pile of abandoned databases
	// (harness.go's own doc comment on Instance.cleanup). A container
	// instance's cleanup is a plain Terminate, already reaped by
	// testcontainers' ryuk on process exit even without this -- but the
	// remote-DSN path has no such reaper, so this call is load-bearing
	// there specifically. CHAOS-4902 r2 finding: this helper is called
	// twice (once per test) and neither call was ever closed.
	//
	// The error IS checked (CHAOS-4902 r3 finding): a discarded `_ =` here
	// would let a failed remote DROP DATABASE (remote.go's own "ORPHANED,
	// drop failed" log line) pass silently -- logged under -v, but nothing
	// fails the test, so an orphaned scratch database coexists with a
	// green run. t.Error, not t.Fatal: cleanup runs after the test body
	// has already determined its own pass/fail, so there is nothing left
	// to abort into.
	t.Cleanup(func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close clickhouse instance: %v", err)
		}
	})
	Apply(ctx, t, instance)

	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := parsed.User.Password()
	// HTTP protocol EXPLICITLY. The native client against an 8123 endpoint
	// reports "[handshake] unexpected packet [72] from server" -- 72 is 'H',
	// the first byte of an HTTP response. That error reads like a schema or
	// auth problem and is neither.
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{parsed.Host},
		Auth: clickhouse.Auth{
			Database: strings.TrimPrefix(parsed.Path, "/"),
			Username: parsed.User.Username(),
			Password: password,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	rows, err := conn.Query(ctx,
		"SELECT name, sorting_key, engine_full FROM system.tables "+
			"WHERE database = currentDatabase() AND engine LIKE 'Replacing%' ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var tables []replacingMergeTreeTable
	for rows.Next() {
		var table replacingMergeTreeTable
		if err := rows.Scan(&table.name, &table.sortingKey, &table.engineFull); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].name < tables[j].name })

	// A CONTROL, because "the schema did not apply" and "the schema has no
	// Replacing tables" produce the same empty answer, and this whole ticket
	// is about two states that look identical from outside.
	var total uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM system.tables WHERE database = currentDatabase()").Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total == 0 {
		t.Fatal("zero tables of ANY engine after Apply -- the migration chain did not run, so a wrong Replacing count would mean nothing")
	}

	return tables
}

// TestSweepReplacingMergeTreeTablesMatchesTheAuthoritativeCount is a
// recurrence guard, not a one-shot enumeration: asserts len == wantCount
// rather than printing it, so a future migration that adds or drops a
// ReplacingMergeTree table without updating the dedup sweep's scope fails
// this test instead of silently drifting.
//
// A prior regex-over-migration-text instrument answered 88; this schema
// read originally answered 85 (the 3-table diff was the instrument's own
// error list). 85 -> 86, CHAOS-4441 (#2171): merging main into this branch
// pulled in 085_work_unit_supersessions.sql, whose sidecar table
// (`work_unit_supersessions`) is itself a ReplacingMergeTree -- confirmed
// by re-running this exact test on bigboy (`found 86 ... table(s), want
// ... 85`, `work_unit_supersessions` present in the printed list, and no
// OTHER new migration in main's drift touches a Replacing table). This is
// the recurrence guard correctly catching a real, legitimate schema change,
// not a bug in the guard -- the fix is updating the count to match reality,
// same as every prior number in this constant's own history.
// 86 -> 88, CHAOS-3092 PR-B: 086_work_item_attribution_backstop_runs.sql adds
// TWO ReplacingMergeTree tables, both keyed on completed_at --
// `work_item_attribution_backstop_runs` and
// `work_item_attribution_backstop_scoped_runs` -- the CHAOS-2433
// write-then-marker run-marker pair, same shape as #2177's
// work_unit_membership_runs/_scoped_runs.
// 88 -> 91, CHAOS-4291: 087_complexity_tables_replacing_merge_tree.py
// converts all THREE complexity tables from plain MergeTree to
// ReplacingMergeTree(computed_at) -- `file_complexity_snapshots`,
// `repo_complexity_daily`, `team_complexity_daily` -- fixing the 6-20x
// append duplication the family had (measured live: 6.85x/6.01x/20.0x
// before this migration). Confirmed the only drift: the failing run's
// printed table list contained exactly these three names plus the
// existing 88, no other Replacing table appeared or vanished.
func TestSweepReplacingMergeTreeTablesMatchesTheAuthoritativeCount(t *testing.T) {
	const wantCount = 91

	tables := sweepReplacingMergeTreeTables(t)
	if len(tables) != wantCount {
		names := make([]string, len(tables))
		for i, table := range tables {
			names[i] = table.name
		}
		t.Fatalf("found %d ReplacingMergeTree table(s), want the CHAOS-4902 authoritative count %d -- "+
			"a migration added or dropped a Replacing* table without updating the dedup sweep's scope; tables: %v",
			len(tables), wantCount, names)
	}
}

// TestSweepReplacingMergeTreeTablesDeclareADedupKey is the per-table
// property CHAOS-4902's dedup fixtures assume, not just the count: every
// Replacing* table must have (1) a non-empty sorting key (ORDER BY -- the
// natural key merges collapse on) and (2) a non-empty ReplacingMergeTree
// version-column argument. A bare `ReplacingMergeTree()` with no version
// argument makes merges pick an ARBITRARY surviving row -- exactly the
// silent-corruption shape CHAOS-4902's FINAL/argMax fixtures assume cannot
// happen. The count test above proves the POPULATION is stable; this test
// proves every member of that population is actually dedup-safe by
// construction, not just present.
func TestSweepReplacingMergeTreeTablesDeclareADedupKey(t *testing.T) {
	tables := sweepReplacingMergeTreeTables(t)
	if len(tables) == 0 {
		t.Fatal("zero ReplacingMergeTree tables -- the count test should have already failed on this")
	}

	var missingSortingKey, missingVersionColumn []string
	for _, table := range tables {
		if strings.TrimSpace(table.sortingKey) == "" {
			missingSortingKey = append(missingSortingKey, table.name)
		}

		match := engineFullVersionColumn.FindStringSubmatch(table.engineFull)
		if match == nil || strings.TrimSpace(match[1]) == "" {
			missingVersionColumn = append(missingVersionColumn, table.name)
		}
	}

	if len(missingSortingKey) > 0 {
		t.Errorf("%d table(s) have NO sorting key (ORDER BY) -- merges have no natural key to collapse on: %v",
			len(missingSortingKey), missingSortingKey)
	}
	if len(missingVersionColumn) > 0 {
		t.Errorf("%d table(s) declare ReplacingMergeTree with NO version-column argument -- "+
			"a bare ReplacingMergeTree() picks an ARBITRARY row on merge, which is exactly the corruption "+
			"class this sweep exists to catch: %v", len(missingVersionColumn), missingVersionColumn)
	}
}
