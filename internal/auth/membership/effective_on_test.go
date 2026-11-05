package membership

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type fakeRow struct {
	values []any
	err    error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, v := range r.values {
		switch d := dest[i].(type) {
		case **string:
			if v == nil {
				*d = nil
			} else {
				s := v.(string)
				*d = &s
			}
		case *string:
			*d = v.(string)
		}
	}
	return nil
}

// recordingQueryer answers every read with one row and records the statements.
type recordingQueryer struct {
	row  fakeRow
	sqls []string
}

func (q *recordingQueryer) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	q.sqls = append(q.sqls, sql)
	return q.row
}

func str(s string) any { return s }

func TestEffectiveOnReadsThePublicRowOnly(t *testing.T) {
	cases := map[string]struct {
		row  fakeRow
		want Effective
	}{
		"public role is an active member": {fakeRow{values: []any{str("admin")}}, Effective{StatusActive, "admin", true}},
		"no public row is not found":      {fakeRow{err: pgx.ErrNoRows}, Effective{}},
		"NULL public role is not found":   {fakeRow{values: []any{nil}}, Effective{}},
	}
	for name, c := range cases {
		q := &recordingQueryer{row: c.row}
		got, err := EffectiveOn(context.Background(), q, uuid.New(), uuid.New())
		if err != nil || got != c.want {
			t.Errorf("%s: %+v, %v; want %+v", name, got, err, c.want)
		}
		if len(q.sqls) != 1 {
			t.Fatalf("%s: %d statements, want exactly one", name, len(q.sqls))
		}
		if !strings.Contains(q.sqls[0], "public.memberships") || strings.Contains(q.sqls[0], "auth") {
			t.Errorf("%s: statement %q, want public.memberships and no auth table", name, q.sqls[0])
		}
	}
}

func TestEffectiveOnReadFailureIsAStoreError(t *testing.T) {
	q := &recordingQueryer{row: fakeRow{err: errors.New("connection reset")}}
	if _, err := EffectiveOn(context.Background(), q, uuid.New(), uuid.New()); !errors.Is(err, ErrStore) {
		t.Errorf("read failure: %v, want ErrStore", err)
	}
}
