package atlassianteams

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamcreated"
)

type createdConn struct {
	driver.Conn
	created    map[string]time.Time
	createdErr error
	batch      *createdBatch
}

func (c *createdConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	if query == teamcreated.Query {
		if c.createdErr != nil {
			return nil, c.createdErr
		}
		rows := &createdRows{index: -1}
		for id, at := range c.created {
			rows.ids, rows.times = append(rows.ids, id), append(rows.times, at)
		}
		return rows, nil
	}
	return &createdRows{index: -1}, nil // the manual_members read: no stored team
}

func (c *createdConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.batch = &createdBatch{}
	return c.batch, nil
}

type createdRows struct {
	driver.Rows
	ids   []string
	times []time.Time
	index int
}

func (r *createdRows) Next() bool { r.index++; return r.index < len(r.ids) }
func (r *createdRows) Scan(dest ...any) error {
	if len(dest) != 2 {
		return fmt.Errorf("unexpected scan shape %d", len(dest))
	}
	*dest[0].(*string) = r.ids[r.index]
	*dest[1].(*time.Time) = r.times[r.index]
	return nil
}
func (r *createdRows) Close() error { return nil }
func (r *createdRows) Err() error   { return nil }

type createdBatch struct {
	driver.Batch
	appended [][]any
}

func (b *createdBatch) Append(v ...any) error { b.appended = append(b.appended, v); return nil }
func (b *createdBatch) Send() error           { return nil }
func (b *createdBatch) Abort() error          { return nil }

// The catalog write carries created_at: a stored team keeps its time, a new
// team takes its own updated_at, a retired team keeps its time, and a failed
// read aborts the write before a batch is prepared.
func TestWriteTeamsCarriesCreatedAt(t *testing.T) {
	const org = "org-1"
	updated := time.Date(2026, 9, 26, 12, 0, 0, 123456789, time.UTC)
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	original := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	retiredAt := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	teams := []TeamRow{
		{ID: "stored", TeamUUID: uuid.New(), Name: "stored", IsActive: 1, UpdatedAt: updated, OrgID: org, Provider: Provider, NativeTeamKey: "stored"},
		{ID: "fresh", TeamUUID: uuid.New(), Name: "fresh", IsActive: 1, UpdatedAt: updated, OrgID: org, Provider: Provider, NativeTeamKey: "fresh"},
	}
	retired := []inactiveTeam{{id: "retired", teamUUID: uuid.New(), name: "retired"}}
	createdOf := func(conn *createdConn, id string) time.Time {
		t.Helper()
		for _, row := range conn.batch.appended {
			if row[0] == id {
				created, ok := row[len(row)-1].(time.Time)
				if !ok {
					t.Fatalf("%s: last value is %T, want created_at", id, row[len(row)-1])
				}
				return created
			}
		}
		t.Fatalf("%s did not reach the batch", id)
		return time.Time{}
	}

	conn := &createdConn{created: map[string]time.Time{"stored": original, "retired": retiredAt}}
	if err := writeTeams(context.Background(), conn, org, teams, retired, now, false, nil); err != nil {
		t.Fatal(err)
	}
	if got := createdOf(conn, "stored"); !got.Equal(original) {
		t.Errorf("stored team created_at = %v, want %v", got, original)
	}
	if got, want := createdOf(conn, "fresh"), updated.Truncate(time.Microsecond); !got.Equal(want) {
		t.Errorf("new team created_at = %v, want %v", got, want)
	}
	if got := createdOf(conn, "retired"); !got.Equal(retiredAt) {
		t.Errorf("retired team created_at = %v, want %v", got, retiredAt)
	}

	failing := &createdConn{createdErr: errors.New("clickhouse unavailable")}
	if err := writeTeams(context.Background(), failing, org, teams, retired, now, false, nil); err == nil {
		t.Fatal("expected the write to fail")
	}
	if failing.batch != nil {
		t.Fatal("a batch was prepared although created_at could not be carried")
	}
}
