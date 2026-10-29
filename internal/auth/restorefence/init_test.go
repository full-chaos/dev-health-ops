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
	walFile          string
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
		if d.walFile != "" {
			return scriptedRow{walFile: d.walFile}
		}
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
	if _, err := initInTx(context.Background(), db, testSchema(t), 0); !errors.Is(err, ErrInitRefused) {
		t.Fatalf("init in recovery = %v, want ErrInitRefused (not errInitExists, which -if-absent would turn into success)", err)
	}
	if db.execs != 0 || len(db.queries) != 1 {
		t.Errorf("a refused init ran %d queries and %d writes after the recovery check", len(db.queries), db.execs)
	}
}

func TestInitWritesNothingWhenTheTimelineHasARow(t *testing.T) {
	db := &scriptedDB{exists: true, walFile: "00000001000000000000000A"}
	if _, err := initInTx(context.Background(), db, testSchema(t), 0); !errors.Is(err, errInitExists) || db.execs != 0 {
		t.Fatalf("err = %v, writes = %d", err, db.execs)
	}
	db = &scriptedDB{walFile: "00000001000000000000000A"}
	tl, err := initInTx(context.Background(), db, testSchema(t), 0)
	if err != nil || tl != 1 || db.execs != 1 {
		t.Fatalf("timeline %d, err %v, writes %d; want 1, nil, 1", tl, err, db.execs)
	}
}

// Round1115 P1: a promoted restored volume (not in recovery, timeline 2, no row) must not get an initial row
// from a bring-up that does not name the timeline.
func TestInitRefusesAFirstRecordOnATimelineAboveOneUnlessItIsNamed(t *testing.T) {
	db := &scriptedDB{} // the scripted WAL file is on timeline 2, no row
	if _, err := initInTx(context.Background(), db, testSchema(t), 0); !errors.Is(err, ErrInitRefused) || db.execs != 0 {
		t.Fatalf("init on timeline 2 with nothing named = %v, writes %d; want ErrInitRefused and no write", err, db.execs)
	}
	db = &scriptedDB{}
	if _, err := initInTx(context.Background(), db, testSchema(t), 3); !errors.Is(err, ErrInitRefused) || db.execs != 0 {
		t.Fatalf("init naming timeline 3 on timeline 2 = %v, writes %d; want ErrInitRefused", err, db.execs)
	}
	db = &scriptedDB{}
	if tl, err := initInTx(context.Background(), db, testSchema(t), 2); err != nil || tl != 2 || db.execs != 1 {
		t.Fatalf("init naming the live timeline 2 = %d, %v, writes %d; want 2, nil, 1", tl, err, db.execs)
	}
}

func TestInitNamingATimelineThatIsNotLiveRefusesOnTimelineOne(t *testing.T) {
	db := &scriptedDB{walFile: "00000001000000000000000A"}
	if _, err := initInTx(context.Background(), db, testSchema(t), 0); err != nil || db.execs != 1 {
		t.Fatalf("init on timeline 1 naming nothing = %v, writes %d; want a write", err, db.execs)
	}
	db = &scriptedDB{walFile: "00000001000000000000000A"}
	if _, err := initInTx(context.Background(), db, testSchema(t), 5); !errors.Is(err, ErrInitRefused) || db.execs != 0 {
		t.Fatalf("init naming timeline 5 on timeline 1 = %v, writes %d; want ErrInitRefused", err, db.execs)
	}
}

// -if-absent is the compose path and never names a timeline: combining the two is refused before any connection.
func TestInitOptionsRefuseIfAbsentWithANamedTimeline(t *testing.T) {
	if err := (InitOptions{IfAbsent: true, Timeline: 2}).validate(); err == nil {
		t.Fatal("-if-absent with -timeline was accepted")
	}
	if err := (InitOptions{IfAbsent: true}).validate(); err != nil {
		t.Fatalf("-if-absent alone: %v", err)
	}
	if err := (InitOptions{Timeline: 2}).validate(); err != nil {
		t.Fatalf("-timeline alone: %v", err)
	}
	if err := (InitOptions{Timeline: -1}).validate(); err == nil {
		t.Fatal("a negative timeline was accepted")
	}
}
