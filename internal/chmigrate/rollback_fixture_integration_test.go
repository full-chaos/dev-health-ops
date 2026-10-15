//go:build integration

package chmigrate_test

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// useSchemaMigrations gives a hand-built fixture the schema_migrations table of
// the head baseline (its CREATE statement, not a copy) with the migration's
// version already recorded, as the migrator leaves it after the upgrade. A
// rollback file ends by deleting that row, so a fixture that runs one needs the
// table. It returns a count of the migration's version rows.
func useSchemaMigrations(t *testing.T, ctx context.Context, conn driver.Conn, version string) func() uint64 {
	t.Helper()
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	create := ""
	for _, object := range baseline.Objects {
		if object.Name == chmigrate.SchemaMigrationsTable {
			create = object.Create
		}
	}
	if create == "" {
		t.Fatalf("the baseline holds no %s", chmigrate.SchemaMigrationsTable)
	}
	if err := conn.Exec(ctx, create); err != nil {
		t.Fatalf("%s: %v", create, err)
	}
	if err := conn.Exec(ctx, "INSERT INTO "+chmigrate.SchemaMigrationsTable+" (version, applied_at) VALUES ('"+version+"', now64(3))"); err != nil {
		t.Fatal(err)
	}
	return func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM "+chmigrate.SchemaMigrationsTable+" WHERE version = ?", version).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
}
