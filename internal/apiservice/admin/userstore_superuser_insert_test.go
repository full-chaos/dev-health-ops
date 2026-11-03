package admin

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

type insertRecorder struct {
	failingDB
	sql  []string
	args [][]any
}

func (r *insertRecorder) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.sql = append(r.sql, sql)
	r.args = append(r.args, args)
	return pgconn.CommandTag{}, nil
}

var insertColumns = regexp.MustCompile(`(?is)insert\s+into\s+users\s*\(([^)]*)\)\s*values\s*\(([^)]*(?:\([^)]*\))?[^)]*)\)`)

// insertUser names is_superuser only for a true value: a false value leaves
// the column to its default (false), so the api role needs no write grant on it.
func TestInsertUserNamesIsSuperuserOnlyWhenTrue(t *testing.T) {
	for _, superuser := range []bool{false, true} {
		recorder := &insertRecorder{failingDB: failingDB{t}}
		store := pgStore{Pool: recorder}
		if _, err := store.insertUser(context.Background(), userCreateInput{IsSuperuser: superuser}); err != nil {
			t.Fatalf("superuser=%v: %v", superuser, err)
		}
		if len(recorder.sql) != 1 {
			t.Fatalf("superuser=%v: %d statements, want 1", superuser, len(recorder.sql))
		}
		named := strings.Contains(strings.ToLower(recorder.sql[0]), "is_superuser")
		if named != superuser {
			t.Errorf("superuser=%v: statement names is_superuser = %v:\n%s", superuser, named, recorder.sql[0])
		}
		m := insertColumns.FindStringSubmatch(recorder.sql[0])
		if m == nil {
			t.Fatalf("superuser=%v: not an insert into users: %s", superuser, recorder.sql[0])
		}
		columns := strings.Split(m[1], ",")
		if got, want := len(strings.Split(m[2], ",")), len(columns); got != want {
			t.Errorf("superuser=%v: %d columns but %d values", superuser, want, got)
		}
		if superuser {
			found := false
			for _, arg := range recorder.args[0] {
				if b, ok := arg.(bool); ok && b {
					found = true
				}
			}
			if !found {
				t.Errorf("superuser=true: no true argument in %v", recorder.args[0])
			}
		}
	}
}
