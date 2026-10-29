package restorefence

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestInitStatementsWriteOneInitialRowForTheTimelineOnly(t *testing.T) {
	sql := initInsertSQL(testSchema(t))
	for _, want := range []string{`INSERT INTO "auth".restore_fence_state`, "'initial'", "'init'"} {
		if !strings.Contains(sql, want) {
			t.Errorf("init insert lacks %q", want)
		}
	}
	if strings.Contains(sql, "ON CONFLICT") {
		t.Error("init refuses an existing row; it never merges into one")
	}
	if got := initExistsSQL(testSchema(t)); !strings.Contains(got, "WHERE timeline_id = $1") || strings.Contains(got, "manifest_sha256") {
		t.Errorf("a row of ANY kind for the timeline refuses init: %s", got)
	}
}

func TestInitHelpSaysItIsOnlyForADatabaseThatIsNotARestore(t *testing.T) {
	if !strings.Contains(InitHelp, "only on a database that is NOT a restore") {
		t.Error("the init help must carry the warning")
	}
}

type scriptedRow struct {
	v       bool
	walFile string
}

func (r scriptedRow) Scan(dest ...any) error {
	switch d := dest[0].(type) {
	case *bool:
		*d = r.v
	case *string:
		*d = r.walFile
	}
	return nil
}

type scriptedDB struct {
	recovery, exists bool
	queries          []string
	execs            int
}

func (d *scriptedDB) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	d.queries = append(d.queries, sql)
	switch {
	case strings.Contains(sql, "pg_is_in_recovery"):
		return scriptedRow{v: d.recovery}
	case strings.Contains(sql, "pg_walfile_name"):
		return scriptedRow{walFile: "00000002000000000000000A"}
	default:
		return scriptedRow{v: d.exists}
	}
}

func (d *scriptedDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	d.execs++
	return pgconn.CommandTag{}, nil
}

// -if-absent never reaches the existing-row check on a server in recovery: the recovery refusal comes first and writes nothing.
func TestInitRefusesInRecoveryBeforeAnythingElse(t *testing.T) {
	db := &scriptedDB{recovery: true, exists: true}
	if _, err := initInTx(context.Background(), db, testSchema(t)); !errors.Is(err, ErrInitRefused) {
		t.Fatalf("init in recovery = %v, want ErrInitRefused (not errInitExists, which -if-absent would turn into success)", err)
	}
	if db.execs != 0 || len(db.queries) != 1 {
		t.Errorf("a refused init ran %d queries and %d writes after the recovery check", len(db.queries), db.execs)
	}
}

func TestInitWritesNothingWhenTheTimelineHasARow(t *testing.T) {
	db := &scriptedDB{exists: true}
	if _, err := initInTx(context.Background(), db, testSchema(t)); !errors.Is(err, errInitExists) || db.execs != 0 {
		t.Fatalf("err = %v, writes = %d", err, db.execs)
	}
	db = &scriptedDB{}
	tl, err := initInTx(context.Background(), db, testSchema(t))
	if err != nil || tl != 2 || db.execs != 1 {
		t.Fatalf("timeline %d, err %v, writes %d; want 2, nil, 1", tl, err, db.execs)
	}
}
