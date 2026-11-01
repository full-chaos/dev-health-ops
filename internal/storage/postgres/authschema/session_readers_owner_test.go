package authschema

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ownerProbeQuerier answers the reader existence probe with true and the
// owner probe with ownerRole (no rows when empty). Any Exec is recorded.
type ownerProbeQuerier struct {
	ownerRole string
	execs     []string
}

type ownerProbeRow struct {
	value string
	found bool
}

func (r ownerProbeRow) Scan(dest ...any) error {
	if !r.found {
		return pgx.ErrNoRows
	}
	switch d := dest[0].(type) {
	case *bool:
		*d = true
	case *string:
		*d = r.value
	}
	return nil
}

func (q *ownerProbeQuerier) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	if strings.Contains(sql, "SELECT EXISTS") {
		return ownerProbeRow{found: true}
	}
	return ownerProbeRow{value: q.ownerRole, found: q.ownerRole != ""}
}

func (q *ownerProbeQuerier) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	q.execs = append(q.execs, sql)
	return pgconn.CommandTag{}, nil
}

func (q *ownerProbeQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("unexpected Query")
}

func (q *ownerProbeQuerier) Begin(context.Context) (pgx.Tx, error) {
	return nil, errors.New("unexpected Begin")
}

// A session reader that owns, or inherits from the owner of, the auth schema
// or any of its tables or sequences is refused before any statement is built:
// the reconcile's REVOKE would strip the owner's own ACL entries.
func TestSessionReaderThatOwnsTheSchemaIsRefusedBeforeAnyStatement(t *testing.T) {
	options := Options{Schema: "auth", RuntimeRole: "auth_runtime", SessionReaderRoles: []string{"owner_reader"}}
	schemaID, err := NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	q := &ownerProbeQuerier{ownerRole: "auth_owner"}
	statements, err := sessionReaderStatements(context.Background(), q, options, schemaID)
	if !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("owner reader: err = %v, want ErrInvalidOptions", err)
	}
	for _, name := range []string{"owner_reader", "auth_owner"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("refusal %q does not name %q", err, name)
		}
	}
	if len(statements) != 0 || len(q.execs) != 0 {
		t.Errorf("statements = %d, execs = %d, want none", len(statements), len(q.execs))
	}

	clean := &ownerProbeQuerier{}
	statements, err = sessionReaderStatements(context.Background(), clean, options, schemaID)
	if err != nil || len(statements) == 0 {
		t.Errorf("reader without ownership: %d statements, err = %v, want statements and no error", len(statements), err)
	}
}
