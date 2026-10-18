package authschema

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type recordingPool struct {
	user     string
	executed []string
}

type valueRow struct{ value string }

func (r valueRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.value
	return nil
}

func (p *recordingPool) record(sql string, args []any) {
	p.executed = append(p.executed, sql)
	for _, arg := range args {
		p.executed = append(p.executed, fmt.Sprint(arg))
	}
}

func (p *recordingPool) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	p.record(sql, args)
	return valueRow{p.user}
}

func (p *recordingPool) Acquire(context.Context) (pooledConn, error) { return &recordingConn{p}, nil }

type recordingConn struct{ pool *recordingPool }

var errStopped = errors.New("recording stub stops here")

func (c *recordingConn) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	c.pool.record(sql, args)
	return pgconn.CommandTag{}, nil
}

func (c *recordingConn) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	c.pool.record(sql, args)
	return nil, errStopped
}

func (c *recordingConn) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	c.pool.record(sql, args)
	return valueRow{c.pool.user}
}

func (c *recordingConn) Begin(context.Context) (pgx.Tx, error) { return nil, errStopped }
func (c *recordingConn) Release()                              {}
func (c *recordingConn) Conn() *pgx.Conn                       { return nil }

// TestApplySendsNoStatementCarryingAnUnvalidatedIdentifier pins what reached
// the wire, not how many calls were made: Apply must read current_user BEFORE
// it can validate (Validate takes the migration role), so "zero database
// calls" is false on correct code. The invariant is that no executed SQL or
// bound argument contains the rejected identifier.
func TestApplySendsNoStatementCarryingAnUnvalidatedIdentifier(t *testing.T) {
	const injected = `evil";drop schema public;--`
	cases := map[string]Options{
		"schema":       {Schema: injected, RuntimeRole: "app_runtime"},
		"runtime role": {Schema: "auth", RuntimeRole: injected},
	}
	for name, options := range cases {
		t.Run(name, func(t *testing.T) {
			pool := &recordingPool{user: "migrator"}
			_, err := apply(context.Background(), pool, options)
			if !errors.Is(err, ErrInvalidOptions) {
				t.Fatalf("err = %v, want ErrInvalidOptions", err)
			}
			for _, sent := range pool.executed {
				if strings.Contains(sent, "drop schema public") {
					t.Fatalf("unvalidated identifier reached the database: %q", sent)
				}
			}
		})
	}
}

// TestApplyRecorderSeesValidIdentifiers keeps the test above from passing
// vacuously: on valid options the same recorder must see the schema name in
// DDL, so an empty recording cannot be mistaken for a clean one.
func TestApplyRecorderSeesValidIdentifiers(t *testing.T) {
	pool := &recordingPool{user: "migrator"}
	_, err := apply(context.Background(), pool, Options{Schema: "auth_probe", RuntimeRole: "app_runtime"})
	if !errors.Is(err, ErrMigrationFailed) {
		t.Fatalf("err = %v, want ErrMigrationFailed from the stub stopping the run", err)
	}
	var sawCurrentUser, sawSchemaDDL bool
	for _, sent := range pool.executed {
		sawCurrentUser = sawCurrentUser || strings.Contains(sent, "current_user")
		sawSchemaDDL = sawSchemaDDL || strings.Contains(sent, `CREATE SCHEMA IF NOT EXISTS "auth_probe"`)
	}
	if !sawCurrentUser || !sawSchemaDDL {
		t.Fatalf("recorder missed expected statements (current_user=%v, schema DDL=%v): %q",
			sawCurrentUser, sawSchemaDDL, pool.executed)
	}
}
