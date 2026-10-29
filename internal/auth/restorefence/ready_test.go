package restorefence

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// fakeRow answers a scan by destination type: a string is the WAL file name of
// the timeline read, a bool the fence-record lookup.
type fakeRow struct {
	walFile string
	fenced  bool
	err     error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	switch d := dest[0].(type) {
	case *string:
		*d = r.walFile
	case *bool:
		*d = r.fenced
	}
	return nil
}

type fakeQuerier struct {
	timeline fakeRow
	state    fakeRow
	sql      []string
	args     [][]any
}

func (f *fakeQuerier) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	f.sql = append(f.sql, sql)
	f.args = append(f.args, args)
	if strings.Contains(sql, "pg_walfile_name") {
		return f.timeline
	}
	return f.state
}

const walFileOnTimeline2 = "000000020000000000000007"

// promotingDB models a database that is promoted (the live timeline moves from 1 to 2, and only timeline 1 has a fence
// row) right after its first statement returns: the reviewer's interleaving. A statement that names both the timeline
// and the fence table is evaluated at one instant; two separate statements can mix two timelines.
type promotingDB struct {
	live    int64
	rows    map[int64]bool
	promote bool
	calls   []string
	mixed   bool
	err     error
}

func (d *promotingDB) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	d.calls = append(d.calls, sql)
	n := len(d.calls)
	defer func() {
		if n == 1 && d.promote {
			d.live = 2
		}
	}()
	if d.err != nil {
		return fakeRow{err: d.err}
	}
	hasTimeline, hasState := strings.Contains(sql, "pg_walfile_name"), strings.Contains(sql, "restore_fence_state")
	switch {
	case hasTimeline && hasState:
		return fakeRow{fenced: d.rows[d.live]}
	case hasTimeline:
		return fakeRow{walFile: map[int64]string{1: "000000010000000000000007", 2: walFileOnTimeline2}[d.live]}
	default:
		tl, _ := args[0].(int64)
		if tl != d.live {
			d.mixed = true
		}
		return fakeRow{fenced: d.rows[tl]}
	}
}

// Round1122 P1: the timeline and the fence row are read in ONE statement. A promotion between two statements made
// the old code answer ready for the unfenced timeline 2 from the fence row of the old timeline 1.
func TestCheckNeverMixesTwoTimelinesAcrossAPromotion(t *testing.T) {
	db := &promotingDB{live: 1, rows: map[int64]bool{1: true}, promote: true}
	_ = Check(context.Background(), db, testSchema(t))
	if db.mixed {
		t.Fatal("the fence row was read for a timeline that was no longer the live one (ready from the old primary's row)")
	}
	if len(db.calls) != 1 {
		t.Fatalf("Check ran %d statements; the timeline and the row must be read together (want 1)", len(db.calls))
	}
	for _, want := range []string{"pg_walfile_name(pg_current_wal_insert_lsn())", `"auth".restore_fence_state`} {
		if !strings.Contains(db.calls[0], want) {
			t.Errorf("the one statement does not contain %s: %s", want, db.calls[0])
		}
	}
}

func TestCheckIsReadyOnlyWhenTheCurrentTimelineHasAFenceRecord(t *testing.T) {
	if err := Check(context.Background(), &promotingDB{live: 2, rows: map[int64]bool{2: true}}, testSchema(t)); err != nil {
		t.Fatalf("a fenced timeline is not ready: %v", err)
	}
	if err := Check(context.Background(), &promotingDB{live: 2, rows: map[int64]bool{1: true}}, testSchema(t)); !errors.Is(err, ErrNotFenced) {
		t.Fatalf("an unfenced timeline (a row exists only for timeline 1) = %v, want ErrNotFenced", err)
	}
}

// pg_current_wal_insert_lsn raises on a standby ("recovery is in progress"): that, and every other failure of the
// statement, is NOT ready, and the driver error text never reaches the caller.
func TestCheckIsNotReadyWhenTheStatementFails(t *testing.T) {
	for _, msg := range []string{"recovery is in progress", "relation does not exist"} {
		db := &promotingDB{live: 1, rows: map[int64]bool{1: true}, err: errors.New(msg)}
		err := Check(context.Background(), db, testSchema(t))
		if !errors.Is(err, ErrCheckFailed) {
			t.Fatalf("%q = %v, want ErrCheckFailed", msg, err)
		}
		if strings.Contains(err.Error(), msg) {
			t.Fatalf("the driver error text %q reached the check error", msg)
		}
	}
}

func TestParseTimelineTakesTheFirstEightHexCharacters(t *testing.T) {
	cases := map[string]int64{
		"000000010000000000000001": 1,
		"000000020000000000000007": 2,
		"0000000A0000000100000003": 10,
		"FFFFFFFF0000000000000001": 4294967295,
	}
	for name, want := range cases {
		got, err := ParseTimeline(name)
		if err != nil || got != want {
			t.Errorf("ParseTimeline(%q) = %d, %v; want %d", name, got, err, want)
		}
	}
	for _, bad := range []string{"", "x", "0000000000000000000000000", "00000000"} {
		if _, err := ParseTimeline(bad); err == nil {
			t.Errorf("ParseTimeline(%q) accepted", bad)
		}
	}
}
