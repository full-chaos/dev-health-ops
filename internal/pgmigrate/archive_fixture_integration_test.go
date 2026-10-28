//go:build integration

package pgmigrate_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// archivedUpgradeTo brings the database behind conn to revision through the chain as it was released.
func archivedUpgradeTo(t *testing.T, ctx context.Context, conn *pgx.Conn, revision string) {
	t.Helper()
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, pgmigrate.ChainThrough(chain, revision)); err != nil {
		t.Fatalf("upgrade to %s: %v", revision, err)
	}
}
