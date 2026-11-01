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

// restore-fence apply writes legacy public tables, so it stays on the owner login. As the least-privilege migrator it
// stops in the preflight, names the missing privilege, and changes no row.
func TestApplyAsTheLeastPrivilegeMigratorRefusesBeforeAnyWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	db.stateT(t, ctx)
	manifest := exportEnd(t, ctx, db)

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

	revisionsBefore, stateBefore := db.revisions(t, ctx), db.stateRows(t, ctx)
	_, err = Apply(ctx, pool, db.schema, manifest, Options{Apply: true})
	if !errors.Is(err, ErrPrivileges) {
		t.Fatalf("apply as the least-privilege migrator = %v, want ErrPrivileges", err)
	}
	if !strings.Contains(err.Error(), "public.") {
		t.Errorf("the refusal %q names no public table privilege", err)
	}
	if fmt.Sprint(db.revisions(t, ctx)) != fmt.Sprint(revisionsBefore) || db.stateRows(t, ctx) != stateBefore {
		t.Error("a refused apply changed rows")
	}

	if _, err := Apply(ctx, db.admin, db.schema, manifest, Options{Apply: true}); err != nil {
		t.Errorf("apply on the owner login after the move: %v", err)
	}
}

func (d *fenceDB) stateRows(t *testing.T, ctx context.Context) int {
	t.Helper()
	var n int
	if err := d.admin.QueryRow(ctx, `SELECT count(*) FROM auth.restore_fence_state`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
