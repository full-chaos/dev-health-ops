package teamcreated

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type fakeConn struct {
	rows  map[string]time.Time
	err   error
	query string
	args  []any
}

func (f *fakeConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	f.query, f.args = query, args
	if f.err != nil {
		return nil, f.err
	}
	rows := &fakeRows{index: -1}
	for id, created := range f.rows {
		rows.ids = append(rows.ids, id)
		rows.times = append(rows.times, created)
	}
	return rows, nil
}

type fakeRows struct {
	driver.Rows
	ids   []string
	times []time.Time
	index int
}

func (r *fakeRows) Next() bool { r.index++; return r.index < len(r.ids) }
func (r *fakeRows) Scan(dest ...any) error {
	*dest[0].(*string) = r.ids[r.index]
	*dest[1].(*time.Time) = r.times[r.index]
	return nil
}
func (r *fakeRows) Close() error { return nil }
func (r *fakeRows) Err() error   { return nil }

func TestCarryReturnsStoredTimeAndForFallsBackForNewTeam(t *testing.T) {
	original := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	first := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)
	conn := &fakeConn{rows: map[string]time.Time{"team-a": original}}
	carried, err := Carry(context.Background(), conn, "org-1", []string{"team-a", "team-b"})
	if err != nil {
		t.Fatal(err)
	}
	if got := For(carried, "team-a", first); !got.Equal(original) {
		t.Fatalf("existing team: %v, want %v", got, original)
	}
	if got := For(carried, "team-b", first); !got.Equal(first) {
		t.Fatalf("new team: %v, want first write %v", got, first)
	}
	if conn.query != Query {
		t.Fatalf("query = %q", conn.query)
	}
}

func TestCarryReadsOldestVersionNotFinal(t *testing.T) {
	// The read must see unmerged old versions: FINAL would hide them, and max
	// instead of min would move the creation time forward on every update.
	for _, forbidden := range []string{"FINAL", "max(", "argMax"} {
		if strings.Contains(Query, forbidden) {
			t.Fatalf("Query contains %q: %s", forbidden, Query)
		}
	}
	for _, required := range []string{"min(coalesce(created_at, updated_at))", "org_id = {org_id:String}", "id IN {team_ids:Array(String)}", "GROUP BY id"} {
		if !strings.Contains(Query, required) {
			t.Fatalf("Query lacks %q: %s", required, Query)
		}
	}
}

func TestCarryFailsClosed(t *testing.T) {
	boom := errors.New("clickhouse unavailable")
	if _, err := Carry(context.Background(), &fakeConn{err: boom}, "org-1", []string{"a"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the query error", err)
	}
	if _, err := Carry(context.Background(), nil, "org-1", []string{"a"}); !errors.Is(err, ErrInvalidArguments) {
		t.Fatalf("nil conn: %v", err)
	}
	var typedNil *fakeConn
	if _, err := Carry(context.Background(), typedNil, "org-1", []string{"a"}); !errors.Is(err, ErrInvalidArguments) {
		t.Fatalf("typed-nil conn: %v", err)
	}
	if _, err := Carry(context.Background(), &fakeConn{}, " ", []string{"a"}); !errors.Is(err, ErrInvalidArguments) {
		t.Fatalf("blank org: %v", err)
	}
	if got, err := Carry(context.Background(), &fakeConn{}, "org-1", nil); err != nil || got != nil {
		t.Fatalf("no ids: %v %v", got, err)
	}
}

func TestForCutsANewTeamsTimeToTheStoredMicroseconds(t *testing.T) {
	first := time.Date(2026, 5, 6, 7, 8, 9, 123456789, time.UTC)
	want := time.Date(2026, 5, 6, 7, 8, 9, 123456000, time.UTC)
	if got := For(nil, "team-a", first); !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
