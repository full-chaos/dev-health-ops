//go:build integration

package usersbackfill

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// ownedRuntime is a second runtime login on the shared database, granted with
// AUTH_USERS_OWNED on (the users-owned grants are switch-gated); e.runtime
// keeps the switch-off grants.
const (
	ownedRuntimeRole     = "owned_runtime"
	ownedRuntimePassword = "owned-runtime-test-only-not-a-credential"
)

var (
	ownedRuntimeOnce sync.Once
	ownedRuntimePool *pgxpool.Pool
	ownedRuntimeErr  error
)

func (e *reconEnv) ownedRuntime(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ownedRuntimeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		role, password := ownedRuntimeRole, ownedRuntimePassword
		if _, err := e.admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
			ownedRuntimeErr = err
			return
		}
		reconClose = append(reconClose, func() { containers.DropRole(e.admin, role, func(string, ...any) {}) })
		if _, err := authschema.Apply(ctx, e.admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
			ownedRuntimeErr = fmt.Errorf("Apply with users owned: %w", err)
			return
		}
		cfg := e.admin.Config().Copy()
		cfg.ConnConfig.User, cfg.ConnConfig.Password = role, password
		ownedRuntimePool, ownedRuntimeErr = pgxpool.NewWithConfig(ctx, cfg)
		if ownedRuntimeErr == nil {
			reconClose = append(reconClose, ownedRuntimePool.Close)
		}
	})
	if ownedRuntimeErr != nil {
		t.Fatal(ownedRuntimeErr)
	}
	return ownedRuntimePool
}
