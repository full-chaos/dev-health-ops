//go:build integration

package authschema

import (
	"context"
	"testing"
	"time"
)

// Phase 5 runs the grants Job as the NOSUPERUSER migrator after the owner move, with the organizations switch on:
// the migrator must hold GRANT OPTION on every column OrganizationWriteGrants gives the runtime role.
func TestGrantsAsTheMigratorAfterTheMoveWithOrgsOwned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	options := env.options(true)
	options.OrgsOwned = true
	if _, err := Apply(ctx, env.migrator, options); err != nil {
		t.Fatalf("grants Job as the migrator, orgs owned: %v", err)
	}
	for _, column := range OrganizationWriteGrants().Update {
		var held bool
		if err := env.admin.QueryRow(ctx, `SELECT has_column_privilege($1, 'public.organizations', $2, 'UPDATE')`, env.runtimeRole, column).Scan(&held); err != nil || !held {
			t.Errorf("the runtime role lacks UPDATE (%s) on public.organizations (%v)", column, err)
		}
	}
}
