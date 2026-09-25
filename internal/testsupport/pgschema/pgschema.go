// Package pgschema gives Go integration tests the REAL Postgres schema: the
// pgmigrate baseline and chain, the same schema the Alembic heads produce.
// Tests use it instead of hand-writing CREATE TABLE for production tables
// (Trap #412; the guard is TestHandWrittenTestDDLMatchesTheMigratedSchema in
// internal/pgmigrate). A hand-written table drifts: #3134 made a query read
// integrations.credential_id and the tests' own `integrations` table lacked
// it (CHAOS-6717).
package pgschema

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// Apply builds the migrated schema in the database behind pool. It fails the
// test on any error; it never leaves a half-built schema to be read as a
// smaller one.
func Apply(ctx context.Context, t testing.TB, pool *pgxpool.Pool) {
	t.Helper()
	ApplyURI(ctx, t, pool.Config().ConnString())
}

// ApplyURI is Apply for a test that holds a connection string, not a pool.
func ApplyURI(ctx context.Context, t testing.TB, uri string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatalf("pgschema: connect: %v", err)
	}
	defer conn.Close(context.Background())
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatalf("pgschema: load the baseline: %v", err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatalf("pgschema: load the chain: %v", err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		t.Fatalf("pgschema: apply the migrated schema: %v", err)
	}
}
