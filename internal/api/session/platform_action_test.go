package session

import (
	"context"
	"errors"
	"reflect"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeRows is one result set; Scan assigns each non-nil value to its
// destination of the same type.
type fakeRows struct {
	pgx.Rows
	rows [][]any
	at   int
}

func (f *fakeRows) Next() bool { f.at++; return f.at <= len(f.rows) }
func (f *fakeRows) Close()     {}
func (f *fakeRows) Err() error { return nil }
func (f *fakeRows) Scan(dest ...any) error {
	for i, d := range dest {
		if v := f.rows[f.at-1][i]; v != nil {
			reflect.ValueOf(d).Elem().Set(reflect.ValueOf(v))
		}
	}
	return nil
}

// fakeTx answers the user read with one row, every membership read with none
// and stops at the first write: reaching Exec means the refresh was refused
// as a non-member and the family revoked.
type fakeTx struct {
	pgx.Tx
	user  []any
	execs int
}

func (f *fakeTx) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if len(sql) > 0 && containsFrom(sql, "FROM users") {
		return &fakeRows{rows: [][]any{f.user}}, nil
	}
	return &fakeRows{}, nil
}

func (f *fakeTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	f.execs++
	return pgconn.CommandTag{}, errors.New("stop at the first write")
}

func containsFrom(sql, part string) bool {
	for i := 0; i+len(part) <= len(sql); i++ {
		if sql[i:i+len(part)] == part {
			return true
		}
	}
	return false
}

func userValues(id uuid.UUID, superuser bool) []any {
	return []any{id, "u@example.com", nil, nil, nil, nil, nil, true, true, superuser, int64(0)}
}
