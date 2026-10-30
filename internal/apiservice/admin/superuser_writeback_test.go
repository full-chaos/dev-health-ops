package admin

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// rowDB answers every users read with one row whose is_superuser is the
// switched read's value, and records every statement it executes.
type rowDB struct {
	superuser bool
	execs     []recordedExec
}

type recordedExec struct {
	sql  string
	args []any
}

func (d *rowDB) Begin(context.Context) (pgx.Tx, error) {
	return nil, errors.New("rowDB: no transactions")
}

func (d *rowDB) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	d.execs = append(d.execs, recordedExec{sql: sql, args: args})
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (d *rowDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("rowDB: no queries")
}

func (d *rowDB) QueryRow(context.Context, string, ...any) pgx.Row { return superuserRow{d.superuser} }

type superuserRow struct{ superuser bool }

// Scan fills only is_superuser (the eleventh of fullUserColumns).
func (r superuserRow) Scan(dest ...any) error {
	if p, ok := dest[10].(*bool); ok {
		*p = r.superuser
	}
	return nil
}

// todaysUpdateSQL is updateUser's statement before the switch existed.
const todaysUpdateSQL = `
UPDATE users SET email = $2, username = $3, full_name = $4, avatar_url = $5, is_active = $6, is_verified = $7, is_superuser = $8, updated_at = $9
WHERE id = $1`

// While the platform roles are owned, a users update that does not name
// is_superuser writes no is_superuser: the row it read carries the switched
// (auth) value, and writing it back would move the public column. Off, the
// statement is today's, with the row's own value.
func TestAdminProfileUpdateWritesNoSuperuserWhileOwned(t *testing.T) {
	name := "renamed"
	for _, owned := range []bool{true, false} {
		for _, read := range []bool{true, false} {
			db := &rowDB{superuser: read}
			store := pgStore{Pool: db, PlatformRolesOwned: owned}
			if _, err := store.updateUser(context.Background(), uuid.New(), userUpdate{FullName: &name}); err != nil {
				t.Fatalf("owned %v: %v", owned, err)
			}
			if len(db.execs) != 1 {
				t.Fatalf("owned %v: %d statements, want 1", owned, len(db.execs))
			}
			written := db.execs[0]
			if owned {
				if strings.Contains(written.sql, "is_superuser") {
					t.Errorf("owned, switched read %v: the profile update writes is_superuser: %s", read, written.sql)
				}
				continue
			}
			if written.sql != todaysUpdateSQL || len(written.args) != 9 || written.args[7] != read {
				t.Errorf("off, read %v: want today's statement writing the row's own value: %s %v", read, written.sql, written.args)
			}
		}
	}
}
