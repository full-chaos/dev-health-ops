package chmigrate

import (
	"context"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// connDB is DB over a ClickHouse connection. Every statement runs in the
// connection's default database (the DSN's database).
type connDB struct {
	conn     driver.Conn
	database string
}

// NewConnDB wraps conn and reads which database it migrates.
func NewConnDB(ctx context.Context, conn driver.Conn) (DB, string, error) {
	var database string
	if err := conn.QueryRow(ctx, "SELECT currentDatabase()").Scan(&database); err != nil {
		return nil, "", err
	}
	return &connDB{conn: conn, database: database}, database, nil
}

func (c *connDB) Exec(ctx context.Context, statement string) error {
	return c.conn.Exec(ctx, statement)
}

func (c *connDB) AppliedVersions(ctx context.Context) (map[string]bool, error) {
	var exists uint64
	if err := c.conn.QueryRow(ctx,
		"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = ?", SchemaMigrationsTable,
	).Scan(&exists); err != nil {
		return nil, err
	}
	applied := map[string]bool{}
	if exists == 0 {
		return applied, nil
	}
	rows, err := c.conn.Query(ctx, "SELECT version FROM "+SchemaMigrationsTable)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		applied[version] = true
	}
	return applied, rows.Err()
}

func (c *connDB) ObjectCreate(ctx context.Context, name string) (string, bool, error) {
	rows, err := c.conn.Query(ctx,
		"SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = ?", name)
	if err != nil {
		return "", false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return "", false, rows.Err()
	}
	var create string
	if err := rows.Scan(&create); err != nil {
		return "", false, err
	}
	return StripDatabase(create, c.database), true, rows.Err()
}

func (c *connDB) RowCount(ctx context.Context, table string) (uint64, error) {
	var count uint64
	err := c.conn.QueryRow(ctx, "SELECT count() FROM "+quoteIdentifier(table)).Scan(&count)
	return count, err
}

// StripDatabase removes database's name as a qualifier from a CREATE
// statement read back from system.tables, the same normalization the baseline
// capture applies to CaptureDatabase.
func StripDatabase(statement, database string) string {
	return strings.NewReplacer("`"+database+"`.", "", database+".", "").Replace(statement)
}
