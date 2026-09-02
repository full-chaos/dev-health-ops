package authschema

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// currentUser reports the role the migration connection actually
// authenticated as.
//
// It is read from the SESSION rather than parsed out of the DSN because the
// two can differ -- a DSN with no user falls back to the OS user, and a
// connection can be redirected by a service file or PGUSER. The distinctness
// check in Options.Validate is only meaningful against the role that will
// really own the created objects, so it has to come from here.
func currentUser(ctx context.Context, pool *pgxpool.Pool) (string, error) {
	var role string
	if err := pool.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		return "", fmt.Errorf("%w: reading current_user", ErrMigrationFailed)
	}
	return role, nil
}

// ensureSchemaAndVersionTable creates the auth schema and this lineage's
// bookkeeping table if they are absent.
//
// Both are IF NOT EXISTS so a re-run is a no-op, and both are created by the
// migration role, which makes the migration role their OWNER -- that ownership
// is what keeps DDL out of the runtime role's reach without needing a REVOKE.
func ensureSchemaAndVersionTable(ctx context.Context, conn *pgx.Conn, options Options) error {
	schema := quoteIdentifier(options.Schema)
	if _, err := conn.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+schema); err != nil {
		return fmt.Errorf("%w: creating schema", ErrMigrationFailed)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			version    integer      PRIMARY KEY,
			name       text         NOT NULL,
			applied_at timestamptz  NOT NULL DEFAULT now()
		)`, schema, quoteIdentifier(versionTable)),
	); err != nil {
		return fmt.Errorf("%w: creating the version table", ErrMigrationFailed)
	}
	return nil
}

// appliedVersions reads the lineage positions this database already holds.
func appliedVersions(ctx context.Context, conn *pgx.Conn, schema string) (map[int]struct{}, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT version FROM %s.%s`, quoteIdentifier(schema), quoteIdentifier(versionTable),
	))
	if err != nil {
		return nil, fmt.Errorf("%w: reading applied versions", ErrMigrationFailed)
	}
	defer rows.Close()

	applied := make(map[int]struct{})
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("%w: reading an applied version", ErrMigrationFailed)
		}
		applied[version] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading applied versions", ErrMigrationFailed)
	}
	return applied, nil
}

// ErrNotCurrent reports a database behind the embedded lineage head.
var ErrNotCurrent = errors.New("auth schema is not at the embedded lineage head")

// Check reports the database's current lineage position WITHOUT issuing any
// DDL.
//
// This is what a deployment gate calls: it answers "is this database ready for
// this binary" without the authority to change the answer. It deliberately
// does not create the schema or the version table -- an absent schema is
// reported as version 0 and ErrNotCurrent, not silently provisioned, because a
// check that can mutate is not a check.
func Check(ctx context.Context, pool *pgxpool.Pool, schema string) (current int, head int, err error) {
	if pool == nil {
		return 0, 0, fmt.Errorf("%w: no connection pool", ErrInvalidOptions)
	}
	if err := ValidateIdentifier(schema); err != nil {
		return 0, 0, err
	}
	head, err = HeadVersion()
	if err != nil {
		return 0, 0, err
	}

	var present bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_catalog.pg_tables
			WHERE schemaname = $1 AND tablename = $2
		)`, schema, versionTable,
	).Scan(&present); err != nil {
		return 0, head, fmt.Errorf("%w: looking for the version table", ErrMigrationFailed)
	}
	if !present {
		return 0, head, fmt.Errorf("%w: version table absent (current 0, head %d)", ErrNotCurrent, head)
	}

	if err := pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT coalesce(max(version), 0) FROM %s.%s`,
		quoteIdentifier(schema), quoteIdentifier(versionTable),
	)).Scan(&current); err != nil {
		return 0, head, fmt.Errorf("%w: reading the current version", ErrMigrationFailed)
	}
	if current != head {
		return current, head, fmt.Errorf("%w: current %d, head %d", ErrNotCurrent, current, head)
	}
	return current, head, nil
}

// redactPGError renders a driver error safely.
//
// A *pgconn.PgError carries the SQLSTATE, the message and the failing
// statement position -- all of which describe THIS package's own embedded SQL
// and are safe and useful. Anything else can be a connection-level error whose
// text renders host, port and user, so it is reduced to its type.
func redactPGError(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return fmt.Sprintf("%s: %s", pgErr.Code, pgErr.Message)
	}
	return "connection or protocol error"
}
