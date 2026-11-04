package projectionreader

import (
	"context"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func authSchema(t *testing.T) authschema.ValidatedIdentifier {
	t.Helper()
	id, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

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
		reflect.ValueOf(d).Elem().Set(reflect.ValueOf(f.rows[f.at-1][i]))
	}
	return nil
}

type fakeQuerier struct {
	rows [][]any
	sql  []string
	args [][]any
}

func (f *fakeQuerier) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	f.sql = append(f.sql, sql)
	f.args = append(f.args, args)
	return &fakeRows{rows: f.rows}, nil
}

// C20: the reader holds one capability, a read, and both statements are one SELECT.
func TestC20ReaderCapabilityIsSelectOnly(t *testing.T) {
	typ := reflect.TypeOf((*Querier)(nil)).Elem()
	if typ.NumMethod() != 1 || typ.Method(0).Name != "Query" {
		t.Fatalf("Querier has %d methods; it must be exactly Query", typ.NumMethod())
	}
	for name, sql := range map[string]string{"page": PageSQL(authSchema(t)), "ids": IDsSQL(authSchema(t))} {
		if !regexp.MustCompile(`(?is)^\s*SELECT\b`).MatchString(sql) {
			t.Fatalf("%s: not a SELECT: %q", name, sql)
		}
		for _, word := range []string{"INSERT", "UPDATE", "DELETE", "TRUNCATE", "FOR UPDATE", ";"} {
			if strings.Contains(strings.ToUpper(sql), word) {
				t.Errorf("%s: the statement holds %s: %q", name, word, sql)
			}
		}
		if !strings.Contains(sql, authschema.Quote(authSchema(t))+".credential_projection") {
			t.Errorf("%s: the table is not schema-qualified: %q", name, sql)
		}
	}
}

// C21: the walk is bound to the external_push rows and ordered by the byte order of the key.
func TestC21ReaderSQLIsExternalPushKeysetInCollateC(t *testing.T) {
	page := PageSQL(authSchema(t))
	for _, want := range []string{"domain_owner = 'external_push'", "external_reference > $1", `ORDER BY external_reference COLLATE "C"`, "LIMIT $2"} {
		if !strings.Contains(page, want) {
			t.Errorf("page statement lacks %q: %q", want, page)
		}
	}
	ids := IDsSQL(authSchema(t))
	for _, want := range []string{"domain_owner = 'external_push'", "external_reference = ANY($1)"} {
		if !strings.Contains(ids, want) {
			t.Errorf("ids statement lacks %q: %q", want, ids)
		}
	}
	for name, sql := range map[string]string{"page": page, "ids": ids} {
		if strings.Contains(sql, "'acr'") {
			t.Errorf("%s statement names the acr rows: %q", name, sql)
		}
		for _, col := range []string{"fingerprint", "terminal_at", "principal_id", "registry_id", "owner_subject", "SELECT *"} {
			if strings.Contains(sql, col) {
				t.Errorf("%s statement names %s: %q", name, col, sql)
			}
		}
	}
}

func TestC21bNewReaderBoundsThePageSize(t *testing.T) {
	q := &fakeQuerier{}
	for _, bad := range []int{0, -1, MaxPageSize + 1} {
		if _, err := NewReader(q, authSchema(t), bad); err == nil {
			t.Errorf("page size %d accepted", bad)
		}
	}
	if _, err := NewReader(nil, authSchema(t), 10); err == nil {
		t.Error("nil querier accepted")
	}
	for _, ok := range []int{1, DefaultPageSize, MaxPageSize} {
		if _, err := NewReader(q, authSchema(t), ok); err != nil {
			t.Errorf("page size %d refused: %v", ok, err)
		}
	}
}

func TestC21cReadPageMapsRowsAndPassesTheKeysetBounds(t *testing.T) {
	org := uuid.MustParse("AAAAAAAA-0000-4000-8000-00000000000A")
	src := "11111111-0000-4000-8000-000000000001"
	exp := "2027-01-02T03:04:05.123456Z"
	q := &fakeQuerier{rows: [][]any{
		{"tok-1", org, (*string)(nil), "active", []string{"ingest:write"}, (*string)(nil)},
		{"tok-2", org, &src, "expired", []string{"a", "b"}, &exp},
	}}
	r, err := NewReader(q, authSchema(t), 2)
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.ReadPage(context.Background(), "tok-0")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.args) != 1 || !reflect.DeepEqual(q.args[0], []any{"tok-0", 2}) {
		t.Fatalf("bounds = %v", q.args)
	}
	if len(got) != 2 || got[0].ID != "tok-1" || got[0].Organization != "aaaaaaaa-0000-4000-8000-00000000000a" ||
		got[0].SourceID != "" || got[0].Status != pushlifecycle.StatusActive || got[0].ExpiresAt != nil ||
		!reflect.DeepEqual(got[0].Scopes, []string{"ingest:write"}) {
		t.Fatalf("row 1 = %+v", got[0])
	}
	want, _ := time.Parse(time.RFC3339Nano, exp)
	if got[1].SourceID != src || got[1].Status != pushlifecycle.StatusExpired || got[1].ExpiresAt == nil || !got[1].ExpiresAt.Equal(want) {
		t.Fatalf("row 2 = %+v", got[1])
	}
}

func TestC21dReadByIDsRefusesMoreThanTheBoundAndAnEmptyListReadsNothing(t *testing.T) {
	q := &fakeQuerier{}
	r, err := NewReader(q, authSchema(t), 2)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := r.ReadByIDs(context.Background(), nil); err != nil || len(got) != 0 || len(q.sql) != 0 {
		t.Fatalf("empty ids: %v %v, %d statements", got, err, len(q.sql))
	}
	if _, err := r.ReadByIDs(context.Background(), []string{"a", "b", "c"}); err == nil {
		t.Fatal("more ids than the page size accepted")
	}
}
