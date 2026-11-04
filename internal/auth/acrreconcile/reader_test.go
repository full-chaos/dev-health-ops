package acrreconcile

import (
	"context"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

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

// The reader can read and nothing else: its only capability has one method.
func TestTheReaderCapabilityIsASingleQueryMethod(t *testing.T) {
	typ := reflect.TypeOf((*Querier)(nil)).Elem()
	if typ.NumMethod() != 1 || typ.Method(0).Name != "Query" {
		t.Fatalf("Querier has %d methods; it must be exactly Query", typ.NumMethod())
	}
}

// The statement is one SELECT of the projection, bound to the acr domain and one organization, in keyset order.
func TestPageSQLIsOneBoundedSelect(t *testing.T) {
	sql := PageSQL(authSchema(t))
	if !regexp.MustCompile(`(?is)^\s*SELECT\b`).MatchString(sql) {
		t.Fatalf("not a SELECT: %q", sql)
	}
	for _, word := range []string{"INSERT", "UPDATE", "DELETE", "TRUNCATE", "FOR UPDATE", ";"} {
		if strings.Contains(strings.ToUpper(sql), word) {
			t.Errorf("the statement holds %s: %q", word, sql)
		}
	}
	for _, want := range []string{"domain_owner = 'acr'", "organization_id = $1", "external_reference > $2", "ORDER BY external_reference", "LIMIT $3"} {
		if !strings.Contains(sql, want) {
			t.Errorf("the statement lacks %q: %q", want, sql)
		}
	}
	if !strings.Contains(sql, authschema.Quote(authSchema(t))+".credential_projection") {
		t.Errorf("the table is not schema-qualified: %q", sql)
	}
}

type neverQuerier struct{}

func (neverQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, nil }

func TestNewReaderRefusesAPageSizeBelowOne(t *testing.T) {
	if _, err := NewReader(neverQuerier{}, authSchema(t), 0); err == nil {
		t.Fatal("page size 0 accepted")
	}
	if _, err := NewReader(nil, authSchema(t), 10); err == nil {
		t.Fatal("nil querier accepted")
	}
	if _, err := NewReader(neverQuerier{}, authSchema(t), 10); err != nil {
		t.Fatalf("valid reader refused: %v", err)
	}
}

type fakeQuerier struct{}

func (*fakeQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, nil }

func TestNewReaderRefusesATypedNilQuerier(t *testing.T) {
	var typedNil *fakeQuerier
	if _, err := NewReader(typedNil, authSchema(t), 10); err == nil {
		t.Fatal("typed-nil querier accepted")
	}
}

func TestNewReaderAcceptsANonNilPointerQuerier(t *testing.T) {
	if _, err := NewReader(&fakeQuerier{}, authSchema(t), 10); err != nil {
		t.Fatalf("non-nil pointer querier refused: %v", err)
	}
}

type expiryRows struct {
	pgx.Rows
	expires *string
	done    bool
}

func (r *expiryRows) Next() bool { d := r.done; r.done = true; return !d }
func (r *expiryRows) Close()     {}
func (r *expiryRows) Err() error { return nil }
func (r *expiryRows) Scan(dest ...any) error {
	*(dest[0].(*string)) = "mp-1"
	*(dest[1].(*int64)) = 1
	*(dest[2].(*string)) = "active"
	*(dest[3].(**string)) = r.expires
	return nil
}

type expiryQuerier struct{ expires *string }

func (q expiryQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return &expiryRows{expires: q.expires}, nil
}

func readOneExpiry(t *testing.T, expires *string) ([]Projected, error) {
	t.Helper()
	r, err := NewReader(expiryQuerier{expires}, authSchema(t), 10)
	if err != nil {
		t.Fatal(err)
	}
	return r.ReadPage(context.Background(), uuid.New(), "")
}

func TestReadPageRefusesAnEmptyStoredExpiry(t *testing.T) {
	empty := ""
	if _, err := readOneExpiry(t, &empty); err == nil {
		t.Fatal("empty non-NULL expiry accepted")
	}
}

func TestReadPageNullExpiryIsNoExpiry(t *testing.T) {
	got, err := readOneExpiry(t, nil)
	if err != nil || len(got) != 1 || got[0].ExpiresAt != nil {
		t.Fatalf("NULL expiry: %+v %v", got, err)
	}
}

func TestReadPageParsesAValidExpiry(t *testing.T) {
	v := "2026-11-03T10:00:00.5Z"
	got, err := readOneExpiry(t, &v)
	want, _ := time.Parse(time.RFC3339Nano, v)
	if err != nil || len(got) != 1 || got[0].ExpiresAt == nil || !got[0].ExpiresAt.Equal(want) {
		t.Fatalf("valid expiry: %+v %v", got, err)
	}
}
