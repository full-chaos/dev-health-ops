//go:build integration

package restorefence

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// export reads the legacy public tables: the owner login passes, the least-privilege migrator stops at the first legacy
// section. The fence runs on the owner login (Secret auth-migrate-database-owner); the migrator gets no grant.
func TestExportRunsAsTheOwnerAndStopsAsTheLeastPrivilegeMigrator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)

	var owner string
	if err := db.admin.QueryRow(ctx, `SELECT current_user`).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	migrator := strings.Replace(db.runtimeRole, "fence_runtime", "fence_migrator", 1)
	if _, err := db.admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`, authschema.Quote(mustIdentifier(t, migrator)))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(db.admin, migrator, t.Logf) })
	if _, err := authschema.OwnerMove(ctx, db.admin, authschema.OwnerMoveOptions{
		Schema: "auth", From: owner, To: migrator, RuntimeRole: db.runtimeRole, Apply: true,
	}); err != nil {
		t.Fatalf("OwnerMove to the migrator: %v", err)
	}
	cfg := db.admin.Config().Copy()
	cfg.ConnConfig.RuntimeParams["options"] = "-c role=" + migrator
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	_, err = ExportManifest(ctx, pool, db.schema)
	if !errors.Is(err, ErrExport) || !strings.Contains(err.Error(), "reading section public.") {
		t.Fatalf("export as the least-privilege migrator = %v, want ErrExport naming a public section", err)
	}
	if _, err := ExportManifest(ctx, db.admin, db.schema); err != nil {
		t.Errorf("export as the owner: %v", err)
	}
}
