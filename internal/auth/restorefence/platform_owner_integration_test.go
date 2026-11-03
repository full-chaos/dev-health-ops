//go:build integration

package restorefence

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// platGrantOwner inserts a platform_owner assignment inside audit.Commit as the pool's role. The fence's pool is the
// table owner (the migration role), the only role the owner trigger lets write the row.
func (d *fenceDB) platGrantOwner(t *testing.T, ctx context.Context, assignment, principal string) {
	t.Helper()
	d.commit(t, ctx, "platform_role.granted", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `INSERT INTO auth.platform_role_assignments (id, principal_id, role_key) VALUES ($1, $2, 'platform_owner')`,
			platAssignment(assignment), platPrincipal(principal))
		return err
	})
}

// A platform_owner row revoked after T comes back with the restore. The fence runs as the table owner, so the owner
// trigger lets its DELETE through; an owner row granted after T is named by id and never inserted.
func TestTheFenceDeletesARevokedOwnerRowAndNamesAnOwnerGrantAfterT(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := platPair(t, ctx, map[string]string{"ga": "alice"})
	for _, d := range []*fenceDB{restored, end} {
		d.platGrantOwner(t, ctx, "go", "alice")
	}
	end.platRevoke(t, ctx, "go")
	end.platGrantOwner(t, ctx, "gn", "bob")
	report, err := Apply(ctx, restored.admin, restored.schema, exportEnd(t, ctx, end), Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !restored.platHas(t, ctx, "ga") || restored.platHas(t, ctx, "go") || restored.platHas(t, ctx, "gn") {
		t.Errorf("restored assignments %v: want ga kept, the revoked owner row go deleted, the new owner row gn not inserted", restored.platIDs(t, ctx))
	}
	ids, ok := actionIDs(report, ActionRegrantPlatformRole)
	if !ok || len(ids) != 1 || ids[0] != platAssignment("gn").String() {
		t.Errorf("regrant_platform_role ids %v (named %v), want exactly the new owner row gn", ids, ok)
	}
	if _, ok := actionIDs(report, ActionNoActivePlatformAdmin); ok {
		t.Errorf("alice is still an active platform_admin: %+v", report.Actions)
	}
}
