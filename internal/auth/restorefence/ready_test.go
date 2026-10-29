package restorefence

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

type fakeRow struct {
	value bool
	err   error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(*bool)) = r.value
	return nil
}

type fakeQuerier struct {
	row fakeRow
	sql string
}

func (f *fakeQuerier) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	f.sql = sql
	return f.row
}

func TestCheckIsReadyOnlyWhenTheCurrentTimelineHasAFenceRecord(t *testing.T) {
	q := &fakeQuerier{row: fakeRow{value: true}}
	if err := Check(context.Background(), q, testSchema(t)); err != nil {
		t.Fatalf("a fenced timeline is not ready: %v", err)
	}
	if !strings.Contains(q.sql, `"auth".restore_fence_timeline()`) || !strings.Contains(q.sql, `"auth".restore_fence_state`) {
		t.Fatalf("the probe does not compare the state with the live timeline: %s", q.sql)
	}
	q.row = fakeRow{value: false}
	if err := Check(context.Background(), q, testSchema(t)); !errors.Is(err, ErrNotFenced) {
		t.Fatalf("an unfenced timeline = %v, want ErrNotFenced", err)
	}
}

func TestCheckFailsClosedWhenTheStateCannotBeRead(t *testing.T) {
	q := &fakeQuerier{row: fakeRow{err: errors.New("function does not exist")}}
	err := Check(context.Background(), q, testSchema(t))
	if !errors.Is(err, ErrCheckFailed) {
		t.Fatalf("an unreadable state = %v, want ErrCheckFailed", err)
	}
	if strings.Contains(err.Error(), "function does not exist") {
		t.Fatal("the driver error text reached the check error")
	}
}
