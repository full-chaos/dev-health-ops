package acrreconcile

import (
	"context"
	"reflect"
	"regexp"
	"strings"
	"testing"

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
