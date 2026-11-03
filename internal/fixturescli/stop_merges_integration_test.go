//go:build integration

package fixturescli

import (
	"strings"
	"testing"
)

// CHAOS-11783: the package runs on one shared ClickHouse server, so stopMerges must stop the merges of the
// tables of ITS database only. A server-wide SYSTEM STOP MERGES would stay on for every later test and make
// OPTIMIZE TABLE ... FINAL fail in a database that never asked for it.
func TestStopMergesStopsOnlyTheMergesOfItsOwnDatabase(t *testing.T) {
	stopped := startClickHouse(t)
	other := startClickHouse(t)
	for _, ch := range []clickHouse{stopped, other} {
		clickHouseHTTP(t, ch.httpDSN, "CREATE TABLE merge_probe (id UInt64) ENGINE = MergeTree ORDER BY id")
	}
	stopMerges(t, stopped.httpDSN)
	for _, ch := range []clickHouse{stopped, other} {
		clickHouseHTTP(t, ch.httpDSN, "INSERT INTO merge_probe VALUES (1)")
		clickHouseHTTP(t, ch.httpDSN, "INSERT INTO merge_probe VALUES (2)")
	}
	activeParts := func(dsn string) string {
		return strings.TrimSpace(clickHouseHTTP(t, dsn,
			"SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'merge_probe' AND active FORMAT TSV"))
	}
	clickHouseHTTP(t, other.httpDSN, "OPTIMIZE TABLE merge_probe FINAL")
	if got := activeParts(other.httpDSN); got != "1" {
		t.Fatalf("a database that never stopped its merges holds %s active parts after OPTIMIZE FINAL, want 1", got)
	}
	if got := activeParts(stopped.httpDSN); got != "2" {
		t.Fatalf("the database whose merges were stopped holds %s active parts, want 2", got)
	}
}
