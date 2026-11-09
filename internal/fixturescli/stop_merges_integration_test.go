//go:build integration

package fixturescli

import (
	"strings"
	"testing"
	"time"
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

// A refreshable view appends rows on the server clock, so stopMerges stops the refreshable views of ITS
// database too, and only those: the same view in another database on the shared server still refreshes.
func TestStopMergesStopsOnlyTheRefreshesOfItsOwnDatabase(t *testing.T) {
	stopped := startClickHouse(t)
	other := startClickHouse(t)
	for _, ch := range []clickHouse{stopped, other} {
		clickHouseHTTP(t, ch.httpDSN, "CREATE TABLE refresh_probe (at DateTime64(6, 'UTC')) ENGINE = MergeTree ORDER BY at")
		clickHouseHTTP(t, ch.httpDSN, "CREATE MATERIALIZED VIEW refresh_probe_view REFRESH EVERY 1 SECOND APPEND TO refresh_probe AS SELECT now64(6, 'UTC') AS at")
	}
	stopMerges(t, stopped.httpDSN)
	rows := func(dsn string) string {
		return strings.TrimSpace(clickHouseHTTP(t, dsn, "SELECT count() FROM refresh_probe FORMAT TSV"))
	}
	stoppedBefore, otherBefore := rows(stopped.httpDSN), rows(other.httpDSN)
	deadline := time.Now().Add(15 * time.Second)
	for rows(other.httpDSN) == otherBefore {
		if time.Now().After(deadline) {
			t.Fatalf("a database that never stopped its views appended no refresh row in 15 s (still %s rows)", otherBefore)
		}
		time.Sleep(500 * time.Millisecond)
	}
	time.Sleep(3 * time.Second)
	if got := rows(stopped.httpDSN); got != stoppedBefore {
		t.Fatalf("the database whose views were stopped went from %s to %s refresh rows", stoppedBefore, got)
	}
}
