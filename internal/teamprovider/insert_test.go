package teamprovider_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamprovider"
	"github.com/full-chaos/dev-health-ops/internal/teamprovider/teamprovidertest"
)

type recordingExec struct {
	calls int
	query string
	args  []any
	err   error
}

func (r *recordingExec) Exec(_ context.Context, query string, args ...any) error {
	r.calls++
	r.query, r.args = query, args
	return r.err
}

var insertColumns = []string{"id", "team_uuid", "name", "description", "manual_members", "is_active", "updated_at", "org_id", "created_at", "deleted_at"}

// Insert sends ONE statement: the writer's columns, then the provider
// identity, selected from the stored versions of the same ids inside it.
func TestInsertIsOneStatementThatReadsTheStoredIdentity(t *testing.T) {
	conn := &recordingExec{}
	updated := time.Date(2026, 10, 17, 12, 0, 0, 123456000, time.FixedZone("x", 2*3600))
	gone := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	teamUUID := uuid.New()
	writes := []teamprovider.Write{
		{Values: []any{"gh:a", teamUUID, "A", (*string)(nil), []string{"m@example.com"}, true, updated, "org-1", updated, (*time.Time)(nil)},
			ProviderTeamID: " 4711 ", ProviderName: "A", Gone: teamprovider.GoneCleared},
		{Values: []any{"gh:b", teamUUID, "B", nil, []string{}, uint8(0), updated, "org-1", nil, nil}, Gone: teamprovider.GoneKept},
		{Values: []any{"gh:c", teamUUID, "C", nil, []string{}, uint8(1), updated, "org-1", updated, nil},
			ProviderTeamID: "9", Gone: teamprovider.GoneStated, GoneAt: &gone},
	}
	if err := teamprovider.Insert(context.Background(), conn, "org-1", insertColumns, writes); err != nil {
		t.Fatal(err)
	}
	if conn.calls != 1 {
		t.Fatalf("Exec calls = %d, want 1", conn.calls)
	}
	wantHead := "INSERT INTO teams (" + strings.Join(insertColumns, ", ") + ", " + teamprovider.Columns + ") SELECT "
	if !strings.HasPrefix(conn.query, wantHead) {
		t.Fatalf("statement head = %.200q", conn.query)
	}
	for _, part := range []string{
		"argMaxIf(provider_team_id, (updated_at, last_synced), provider_team_id != '')",
		"argMaxIf(provider_name, (updated_at, last_synced), provider_name != '')",
		"nullIf(argMax(ifNull(gone_at, toDateTime64(0, 6)), (updated_at, last_synced)), toDateTime64(0, 6))",
		"FROM teams WHERE org_id = {org_id:String} AND id IN {team_ids:Array(String)} GROUP BY id",
		"ON stored.stored_id = incoming.v0", "ORDER BY incoming.ordinal",
		"{v6:Array(DateTime64(6, 'UTC'))}", "{v3:Array(Nullable(String))}", "{v1:Array(UUID)}",
	} {
		if !strings.Contains(conn.query, part) {
			t.Errorf("statement does not hold %q", part)
		}
	}
	columns, got, err := teamprovidertest.Decode(conn.query, conn.args)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(columns, insertColumns) {
		t.Fatalf("columns = %v", columns)
	}
	if len(got) != 3 {
		t.Fatalf("rows = %d", len(got))
	}
	if v := got[0].Values; v[0] != "gh:a" || v[5] != uint8(1) || !v[6].(time.Time).Equal(updated) || v[9] != nil {
		t.Errorf("row 0 values = %#v", v)
	}
	if got[0].ProviderTeamID != "4711" || got[0].Gone != teamprovider.GoneCleared || got[0].GoneAt != nil {
		t.Errorf("row 0 identity = %+v", got[0])
	}
	if got[1].ProviderTeamID != "" || got[1].ProviderName != "" || got[1].Gone != teamprovider.GoneKept {
		t.Errorf("row 1 identity = %+v", got[1])
	}
	if got[2].Gone != teamprovider.GoneStated || got[2].GoneAt == nil || !got[2].GoneAt.Equal(gone) {
		t.Errorf("row 2 identity = %+v", got[2])
	}
}

func TestInsertRefusesAWriteItCannotMakeSafely(t *testing.T) {
	row := func(org string) teamprovider.Write {
		return teamprovider.Write{Values: []any{"gh:a", uuid.New(), "A", nil, []string{}, uint8(1), time.Now(), org, nil, nil}}
	}
	cases := map[string]struct {
		columns []string
		rows    []teamprovider.Write
	}{
		"an identity column in the list": {append(append([]string{}, insertColumns[:9]...), "gone_at"), []teamprovider.Write{row("org-1")}},
		"an unknown column":              {append(append([]string{}, insertColumns[:9]...), "members"), []teamprovider.Write{row("org-1")}},
		"no id column":                   {append([]string{"name"}, insertColumns[1:]...), []teamprovider.Write{row("org-1")}},
		"a row of another organization":  {insertColumns, []teamprovider.Write{row("org-2")}},
		"a row of the wrong width":       {insertColumns, []teamprovider.Write{{Values: []any{"gh:a"}}}},
		"a time column with no time":     {insertColumns, []teamprovider.Write{{Values: []any{"gh:a", uuid.New(), "A", nil, []string{}, uint8(1), "2026", "org-1", nil, nil}}}},
	}
	for name, c := range cases {
		conn := &recordingExec{}
		if err := teamprovider.Insert(context.Background(), conn, "org-1", c.columns, c.rows); !errors.Is(err, teamprovider.ErrInvalidWrite) {
			t.Errorf("%s: err = %v, want ErrInvalidWrite", name, err)
		}
		if conn.calls != 0 {
			t.Errorf("%s: a refused write ran a statement", name)
		}
	}
	if err := teamprovider.Insert(context.Background(), nil, "org-1", insertColumns, []teamprovider.Write{row("org-1")}); !errors.Is(err, teamprovider.ErrInvalidArguments) {
		t.Errorf("nil connection: %v", err)
	}
	conn := &recordingExec{}
	if err := teamprovider.Insert(context.Background(), conn, "org-1", insertColumns, nil); err != nil || conn.calls != 0 {
		t.Errorf("no rows: err %v, calls %d; want no statement", err, conn.calls)
	}
}

func TestStatedColumnsAreTheListBeforeTheIdentity(t *testing.T) {
	got, err := teamprovider.StatedColumns("INSERT INTO teams (id,name, created_at,provider_team_id,provider_name,gone_at)")
	if err != nil || !reflect.DeepEqual(got, []string{"id", "name", "created_at"}) {
		t.Fatalf("StatedColumns = %v, %v", got, err)
	}
	if _, err := teamprovider.StatedColumns("INSERT INTO teams (id,name,provider_team_id)"); !errors.Is(err, teamprovider.ErrInvalidWrite) {
		t.Fatalf("a list without the identity: %v", err)
	}
}

// Restate is one INSERT..SELECT from teams FINAL: every teams column is
// selected from the stored row (members and is_active computed from it), the
// identity by Insert's rule, and a deleted row is not selected.
func TestRestateIsOneStatementFromTheStoredRow(t *testing.T) {
	conn := &recordingExec{}
	rows := []teamprovider.Restatement{
		{ID: "custom:eng", AddMembers: []string{"admin@example.com"}, Activate: true},
		{ID: "gh:ops"},
	}
	if _, err := teamprovider.Restate(context.Background(), conn, "org-1", rows); err != nil {
		t.Fatal(err)
	}
	if conn.calls != 1 {
		t.Fatalf("Exec calls = %d, want 1", conn.calls)
	}
	head := conn.query[:strings.Index(conn.query, ") SELECT ")]
	for column := range teamprovider.ColumnTypes() {
		if !strings.Contains(head, column) {
			t.Errorf("the statement does not write %s", column)
		}
	}
	for _, part := range []string{
		"INSERT INTO teams (",
		teamprovider.Columns + ") SELECT ",
		"arrayConcat(t.manual_members, arrayDistinct(arrayFilter(m -> NOT has(t.manual_members, m), incoming.add_members)))",
		"if(incoming.activate = 1, toUInt8(1), t.is_active)",
		"t.updated_at",
		"FROM teams AS t FINAL",
		"argMaxIf(provider_team_id, (updated_at, last_synced), provider_team_id != '')",
		"min(coalesce(created_at, updated_at))",
		"WHERE t.org_id = {org_id:String} AND t.deleted_at IS NULL",
	} {
		if !strings.Contains(conn.query, part) {
			t.Errorf("the statement does not hold %q", part)
		}
	}
}

func TestRestateRefusesARowItCannotWrite(t *testing.T) {
	for name, rows := range map[string][]teamprovider.Restatement{
		"no team id":    {{ID: ""}},
		"a repeated id": {{ID: "gh:a"}, {ID: "gh:a", Activate: true}},
	} {
		conn := &recordingExec{}
		if _, err := teamprovider.Restate(context.Background(), conn, "org-1", rows); !errors.Is(err, teamprovider.ErrInvalidWrite) {
			t.Errorf("%s: err = %v, want ErrInvalidWrite", name, err)
		}
		if conn.calls != 0 {
			t.Errorf("%s: a refused write ran a statement", name)
		}
	}
	if _, err := teamprovider.Restate(context.Background(), nil, "org-1", []teamprovider.Restatement{{ID: "gh:a"}}); !errors.Is(err, teamprovider.ErrInvalidArguments) {
		t.Errorf("nil connection: %v", err)
	}
	conn := &recordingExec{}
	if written, err := teamprovider.Restate(context.Background(), conn, "org-1", nil); err != nil || written != 0 || conn.calls != 0 {
		t.Errorf("no rows: %d, %v, calls %d; want no statement", written, err, conn.calls)
	}
}
