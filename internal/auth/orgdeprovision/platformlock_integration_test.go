//go:build integration

package orgdeprovision

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

var (
	platformActorOnce sync.Once
	platformActorID   uuid.UUID
	platformActorErr  error
)

func (e *env) platformStore(t *testing.T, ctx context.Context) (*platformrole.Store, uuid.UUID) {
	t.Helper()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	roles, err := platformrole.New(platformrole.Config{Pool: e.admin, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	platformActorOnce.Do(func() {
		platformActorID, _ = e.seedUser(t, ctx)
		_, platformActorErr = roles.Bootstrap(ctx, platformActorID)
	})
	if platformActorErr != nil {
		t.Fatalf("bootstrap the platform administrator: %v", platformActorErr)
	}
	return roles, platformActorID
}

// A deprovision takes the platform role write lock shared before its principal rows, so a grant of the platform role
// waits (one exclusive advisory request waiting in pg_locks) until the deprovision that authorized under its own locks commits.
func TestDeprovisionHoldsThePlatformRoleWriteLockShared(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	roles, grantor := e.platformStore(t, ctx)
	orgID := e.seedOrg(t, ctx)
	actor, _ := e.seedUser(t, ctx)
	e.add(t, ctx, orgID, actor, "owner")
	inAuthorize, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	defer free()
	hold := func(context.Context, audit.TxOps, uuid.UUID) error {
		close(inAuthorize)
		<-release
		return nil
	}
	verbErr, grantErr := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := e.service.Deprovision(ctx, Input{OrgID: orgID, Actor: Actor{PrincipalID: &actor}, Authorize: hold})
		verbErr <- err
	}()
	select {
	case <-inAuthorize:
	case err := <-verbErr:
		t.Fatalf("the deprovision ended before its Authorize: %v", err)
	case <-ctx.Done():
		t.Fatal("hang guard expired before Authorize")
	}
	go func() {
		_, err := roles.Grant(ctx, platformrole.GrantInput{Actor: grantor, Principal: actor, Role: platformrole.PlatformAdmin})
		grantErr <- err
	}()
	for {
		var n int
		if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted AND mode = 'ExclusiveLock'`).Scan(&n); err != nil {
			t.Fatalf("read pg_locks: %v", err)
		}
		if n == 1 {
			break
		}
		select {
		case err := <-grantErr:
			t.Fatalf("the grant returned while the deprovision is held (err %v), want it waiting on the advisory lock", err)
		case <-ctx.Done():
			t.Fatal("hang guard expired waiting for one exclusive advisory waiter")
		case <-time.After(50 * time.Millisecond):
		}
	}
	free()
	for name, ch := range map[string]chan error{"deprovision": verbErr, "grant": grantErr} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("%s: got error %v, want nil", name, err)
			}
		case <-ctx.Done():
			t.Fatalf("%s did not finish: hang guard expired", name)
		}
	}
}
