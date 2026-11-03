//go:build integration

package invite

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
		platformActorID = e.seedUser(t, ctx, address(), true)
		_, platformActorErr = roles.Bootstrap(ctx, platformActorID)
	})
	if platformActorErr != nil {
		t.Fatalf("bootstrap the platform administrator: %v", platformActorErr)
	}
	return roles, platformActorID
}

// exclusiveAdvisoryWaiters counts the exclusive advisory lock requests that wait.
func (e *env) exclusiveAdvisoryWaiters(t *testing.T, ctx context.Context) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted AND mode = 'ExclusiveLock'`).Scan(&n); err != nil {
		t.Fatalf("read pg_locks: %v", err)
	}
	return n
}

// Create and Revoke take the platform role write lock shared before anything else, so a grant of the platform role
// waits (one exclusive advisory request waiting in pg_locks) until the verb that decided under its own locks commits.
func TestCreateAndRevokeHoldThePlatformRoleWriteLockShared(t *testing.T) {
	e := getEnv(t)
	for _, verb := range []string{"create", "revoke"} {
		t.Run(verb, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			roles, grantor := e.platformStore(t, ctx)
			f := e.invite(t, ctx, "member")
			actor := f.inviter
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
				if verb == "create" {
					_, err := e.store.Create(ctx, CreateInput{OrgID: f.org, Email: address(), Role: "member", InvitedBy: actor,
						Actor: Actor{PrincipalID: &actor}, Authorize: hold, PublicAudit: &PublicAudit{}})
					verbErr <- err
					return
				}
				verbErr <- e.store.Revoke(ctx, RevokeInput{OrgID: f.org, InviteID: f.created.ID, Actor: Actor{PrincipalID: &actor}, Authorize: hold})
			}()
			select {
			case <-inAuthorize:
			case err := <-verbErr:
				t.Fatalf("%s ended before its Authorize: %v", verb, err)
			case <-ctx.Done():
				t.Fatal("hang guard expired before Authorize")
			}
			go func() {
				_, err := roles.Grant(ctx, platformrole.GrantInput{Actor: grantor, Principal: actor, Role: platformrole.PlatformAdmin})
				grantErr <- err
			}()
			for e.exclusiveAdvisoryWaiters(t, ctx) != 1 {
				select {
				case err := <-grantErr:
					t.Fatalf("%s: the grant returned while the verb is held (err %v), want it waiting on the advisory lock", verb, err)
				case <-ctx.Done():
					t.Fatalf("%s: hang guard expired waiting for one exclusive advisory waiter", verb)
				case <-time.After(50 * time.Millisecond):
				}
			}
			free()
			for name, ch := range map[string]chan error{"verb": verbErr, "grant": grantErr} {
				select {
				case err := <-ch:
					if err != nil {
						t.Fatalf("%s: %s: got error %v, want nil", verb, name, err)
					}
				case <-ctx.Done():
					t.Fatalf("%s: %s did not finish: hang guard expired", verb, name)
				}
			}
		})
	}
}
