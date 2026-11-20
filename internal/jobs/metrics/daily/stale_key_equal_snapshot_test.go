package daily

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

const equalSnapshotRepo = "14161000-0000-4000-8000-0000000141a1"

// equalSnapshotConn serves one live key stored at storedAt, and when the marker batch is prepared (after the helper's
// read) it records a competing computed row of the same key at competingAt (the snapshot when unset): an overlapping
// run that wrote between the read and the insert.
type equalSnapshotConn struct {
	driver.Conn
	table       StaleKeyTable
	storedAt    time.Time
	snapshot    time.Time
	competingAt time.Time
	competing   time.Time
	markers     []time.Time
}

type equalSnapshotRows struct {
	driver.Rows
	values []any
	read   bool
}

func (r *equalSnapshotRows) Next() bool {
	if r.read {
		return false
	}
	r.read = true
	return true
}

func (r *equalSnapshotRows) Scan(dest ...any) error {
	if len(dest) != len(r.values) {
		return fmt.Errorf("scan destinations=%d values=%d", len(dest), len(r.values))
	}
	for i, value := range r.values {
		switch ptr := dest[i].(type) {
		case *string:
			*ptr = value.(string)
		case *time.Time:
			*ptr = value.(time.Time)
		default:
			return fmt.Errorf("destination %d has type %T", i, dest[i])
		}
	}
	return nil
}

func (*equalSnapshotRows) Err() error   { return nil }
func (*equalSnapshotRows) Close() error { return nil }

func (c *equalSnapshotConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	values := []any{equalSnapshotRepo, c.storedAt}
	for range c.table.Carried {
		values = append(values, "old")
	}
	return &equalSnapshotRows{values: values}, nil
}

func (c *equalSnapshotConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.competing = c.snapshot
	if !c.competingAt.IsZero() {
		c.competing = c.competingAt
	}
	return &equalSnapshotBatch{conn: c}, nil
}

type equalSnapshotBatch struct {
	driver.Batch
	conn *equalSnapshotConn
}

func (b *equalSnapshotBatch) Append(values ...any) error {
	b.conn.markers = append(b.conn.markers, values[2+len(b.conn.table.Keys)].(time.Time))
	return nil
}

func (*equalSnapshotBatch) Send() error { return nil }

type snapshotMarkerCell struct {
	table StaleKeyTable
	path  string
}

// snapshotMarkerCells is every snapshot-stamped table on the partition path, and on the organization-cleanup path
// when the finalizer of a whole-organization run retracts it.
func snapshotMarkerCells(t *testing.T) []snapshotMarkerCell {
	t.Helper()
	orgPath := map[string]bool{}
	for _, table := range OrganizationRunStaleKeyTables() {
		orgPath[table.Table] = true
	}
	var cells []snapshotMarkerCell
	for _, table := range teamkeytables.All() {
		if _, stamped := staleKeySnapshot(table, time.Unix(0, 0)); !stamped {
			continue
		}
		cells = append(cells, snapshotMarkerCell{table, "partition-recompute"})
		if orgPath[table.Table] {
			cells = append(cells, snapshotMarkerCell{table, "organization-cleanup"})
		}
	}
	if len(cells) != 12 {
		t.Fatalf("%d snapshot-stamped table x path cells, want 12 (8 tables on the partition path, 4 on the organization path)", len(cells))
	}
	return cells
}

// runSnapshotMarkerCell runs one path at snapshot over the double and returns the deferred keys it logged and, on the
// organization path, the count it returned.
func runSnapshotMarkerCell(t *testing.T, cell snapshotMarkerCell, conn *equalSnapshotConn, snapshot time.Time) (logged, returned int) {
	t.Helper()
	var captured bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&captured, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(previous)
	returned = -1
	if cell.path == "partition-recompute" {
		if _, err := supersedeStaleTeamKeys(context.Background(), conn, cell.table, "org-14161", snapshot,
			newStaleKeyScope([]string{equalSnapshotRepo}), nil, snapshot); err != nil {
			t.Fatal(err)
		}
	} else {
		result, err := retractStaleTeamKeysOutsideRun(context.Background(), conn, cell.table, "org-14161", snapshot,
			nil, func(context.Context) (staleKeyScope, error) { return staleKeyScope{}, nil }, snapshot)
		if err != nil {
			t.Fatal(err)
		}
		returned = result.deferred
	}
	for _, line := range bytes.Split(bytes.TrimSpace(captured.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if record["msg"] != StaleKeysSnapshotDeferredLogMessage {
			continue
		}
		if record["level"] != "WARN" || record["table"] != cell.table.Table || record["organization_id"] != "org-14161" {
			t.Fatalf("deferred line %v, want WARN of table %s and org-14161", record, cell.table.Table)
		}
		logged += int(record["deferred_keys"].(float64))
	}
	return logged, returned
}

// A row of zeros must never take the newest version of its key from a computed row of the same snapshot that lands
// between the helper's read and its insert: the tables are ReplacingMergeTree(computed_at), so a merge keeps the last
// inserted row of an equal computed_at (the marker), and an argMax reader takes either.
func TestStaleKeyMarkerLosesEqualSnapshotWrittenAfterRead(t *testing.T) {
	snapshot := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	for _, cell := range snapshotMarkerCells(t) {
		for _, storedAgo := range []time.Duration{time.Second, 5 * time.Second} {
			t.Run(fmt.Sprintf("%s/%s/stored-%s", cell.table.Table, cell.path, storedAgo), func(t *testing.T) {
				conn := &equalSnapshotConn{table: cell.table, storedAt: snapshot.Add(-storedAgo), snapshot: snapshot}
				runSnapshotMarkerCell(t, cell, conn, snapshot)
				for _, marker := range conn.markers {
					if !marker.After(conn.storedAt) {
						t.Fatalf("marker at %s does not retire the stored row at %s", marker, conn.storedAt)
					}
					if !conn.competing.IsZero() && !marker.Before(conn.competing) {
						t.Errorf("HIDDEN: marker computed_at=%s retracted=1 is not older than the competing computed row computed_at=%s retracted=0",
							marker.Format("15:04:05"), conn.competing.Format("15:04:05"))
					}
				}
				t.Logf("stored=%s markers=%d competing=%s", conn.storedAt.Format("15:04:05"), len(conn.markers), conn.competing.Format("15:04:05"))
			})
		}
	}
}

// The boundaries of the rule: the marker is one second below the snapshot S, and only a row stored at S-2s or older
// gets one. The two named residuals are pinned at their exact boundary: R1, a computed row of snapshot S-1s that lands
// after the read ties the marker; R2, a row stored at S-1s gets no marker this run and is counted in a WARN line.
func TestStaleKeySnapshotMarkerBoundaries(t *testing.T) {
	snapshot := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	marker := snapshot.Add(-time.Second)
	for _, cell := range snapshotMarkerCells(t) {
		name := cell.table.Table + "/" + cell.path
		t.Run(name+"/stored-S-2s-retired-at-S-1s", func(t *testing.T) {
			conn := &equalSnapshotConn{table: cell.table, storedAt: snapshot.Add(-2 * time.Second), snapshot: snapshot}
			logged, returned := runSnapshotMarkerCell(t, cell, conn, snapshot)
			if len(conn.markers) != 1 || !conn.markers[0].Equal(marker) {
				t.Fatalf("markers %v, want exactly one at %s for a row stored at %s", conn.markers, marker, conn.storedAt)
			}
			if logged != 0 || returned > 0 {
				t.Fatalf("deferred logged=%d returned=%d, want 0", logged, returned)
			}
		})
		t.Run(name+"/R2-stored-S-1s-deferred", func(t *testing.T) {
			conn := &equalSnapshotConn{table: cell.table, storedAt: marker, snapshot: snapshot}
			logged, returned := runSnapshotMarkerCell(t, cell, conn, snapshot)
			if len(conn.markers) != 0 {
				t.Fatalf("markers %v for a row stored at S-1s, want none (R2: deferred to a later snapshot)", conn.markers)
			}
			if logged != 1 {
				t.Fatalf("deferred keys logged %d, want 1", logged)
			}
			if cell.path == "organization-cleanup" && returned != 1 {
				t.Fatalf("deferred keys returned %d, want 1", returned)
			}
		})
		t.Run(name+"/stored-S-not-deferred", func(t *testing.T) {
			conn := &equalSnapshotConn{table: cell.table, storedAt: snapshot, snapshot: snapshot}
			logged, returned := runSnapshotMarkerCell(t, cell, conn, snapshot)
			if len(conn.markers) != 0 || logged != 0 || returned > 0 {
				t.Fatalf("markers %v logged=%d returned=%d for a row of the same snapshot, want a no-op", conn.markers, logged, returned)
			}
		})
		t.Run(name+"/R1-competing-row-of-snapshot-S-1s-ties-the-marker", func(t *testing.T) {
			conn := &equalSnapshotConn{table: cell.table, storedAt: snapshot.Add(-5 * time.Second), snapshot: snapshot, competingAt: marker}
			runSnapshotMarkerCell(t, cell, conn, snapshot)
			if len(conn.markers) != 1 || !conn.markers[0].Equal(conn.competing) {
				t.Fatalf("markers %v, competing row of snapshot S-1s at %s: R1 is a tie at exactly S-1s", conn.markers, conn.competing)
			}
		})
	}
}

// R2 is deferred, not lost: the next run of the repository-day, with a later snapshot, writes the marker of a row
// stored one second below the earlier snapshot.
func TestStaleKeyDeferredMarkerIsWrittenByTheNextRun(t *testing.T) {
	first := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	next := first.Add(time.Second)
	stored := first.Add(-time.Second)
	for _, cell := range snapshotMarkerCells(t) {
		t.Run(cell.table.Table+"/"+cell.path, func(t *testing.T) {
			conn := &equalSnapshotConn{table: cell.table, storedAt: stored, snapshot: first}
			if logged, _ := runSnapshotMarkerCell(t, cell, conn, first); len(conn.markers) != 0 || logged != 1 {
				t.Fatalf("first run: markers %v deferred %d, want none and 1", conn.markers, logged)
			}
			conn.snapshot = next
			if logged, _ := runSnapshotMarkerCell(t, cell, conn, next); logged != 0 {
				t.Fatalf("next run deferred %d, want 0", logged)
			}
			if len(conn.markers) != 1 || !conn.markers[0].Equal(first) || !conn.markers[0].After(stored) {
				t.Fatalf("next run markers %v, want exactly one at %s, newer than the row stored at %s", conn.markers, first, stored)
			}
		})
	}
}
