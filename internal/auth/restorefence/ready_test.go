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

func TestCheckIsReadyOnlyWhenTheCurrentTimelineHasAFenceRecord(t *testing.T) {
	q := &fakeQuerier{timeline: fakeRow{walFile: walFileOnTimeline2}, state: fakeRow{fenced: true}}
	if err := Check(context.Background(), q, testSchema(t)); err != nil {
		t.Fatalf("a fenced timeline is not ready: %v", err)
	}
	if len(q.sql) != 2 || !strings.Contains(q.sql[0], "pg_walfile_name(pg_current_wal_insert_lsn())") || !strings.Contains(q.sql[1], `"auth".restore_fence_state`) {
		t.Fatalf("the check does not compare the state with the live timeline: %v", q.sql)
	}
	if len(q.args[1]) != 1 || q.args[1][0] != int64(2) {
		t.Fatalf("the state lookup is not keyed by the parsed timeline 2: %v", q.args[1])
	}
	q.state = fakeRow{fenced: false}
	if err := Check(context.Background(), q, testSchema(t)); !errors.Is(err, ErrNotFenced) {
		t.Fatalf("an unfenced timeline = %v, want ErrNotFenced", err)
	}
}

// pg_walfile_name raises on a standby ("recovery is in progress"): that, and
// every other failure to read the timeline or the state, is NOT ready.
func TestCheckIsNotReadyWhenTheTimelineCannotBeRead(t *testing.T) {
	q := &fakeQuerier{timeline: fakeRow{err: errors.New("recovery is in progress")}, state: fakeRow{fenced: true}}
	err := Check(context.Background(), q, testSchema(t))
	if !errors.Is(err, ErrCheckFailed) {
		t.Fatalf("a timeline read that fails (in recovery) = %v, want ErrCheckFailed", err)
	}
	if strings.Contains(err.Error(), "recovery is in progress") {
		t.Fatal("the driver error text reached the check error")
	}
	if len(q.sql) != 1 {
		t.Fatalf("the state was read although the timeline was not: %v", q.sql)
	}
}

func TestCheckFailsClosedWhenTheStateCannotBeRead(t *testing.T) {
	q := &fakeQuerier{timeline: fakeRow{walFile: walFileOnTimeline2}, state: fakeRow{err: errors.New("relation does not exist")}}
	err := Check(context.Background(), q, testSchema(t))
	if !errors.Is(err, ErrCheckFailed) {
		t.Fatalf("an unreadable state = %v, want ErrCheckFailed", err)
	}
	if strings.Contains(err.Error(), "does not exist") {
		t.Fatal("the driver error text reached the check error")
	}
}

func TestCheckRefusesAWalFileNameItCannotReadATimelineFrom(t *testing.T) {
	for _, name := range []string{"", "0000000", "ZZZZZZZZ0000000000000007", "000000000000000000000007", "00000002"} {
		q := &fakeQuerier{timeline: fakeRow{walFile: name}, state: fakeRow{fenced: true}}
		if err := Check(context.Background(), q, testSchema(t)); !errors.Is(err, ErrCheckFailed) {
			t.Errorf("WAL file name %q = %v, want ErrCheckFailed", name, err)
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
