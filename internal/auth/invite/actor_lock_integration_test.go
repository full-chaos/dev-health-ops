//go:build integration

package invite

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// Create and Revoke hold the actor's auth.principals and public.users rows
// (steps 1 and 2 of the one lock order) while Authorize decides: another
// writer of either row waits, so the rule reads the rows as they stay until
// the commit.
func TestCreateAndRevokeHoldTheActorsRowsWhileAuthorizing(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, verb := range []string{"create", "revoke"} {
		t.Run(verb, func(t *testing.T) {
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
			done := make(chan error, 1)
			go func() {
				if verb == "create" {
					_, err := e.store.Create(ctx, CreateInput{OrgID: f.org, Email: address(), Role: "member", InvitedBy: actor,
						Actor: Actor{PrincipalID: &actor}, Authorize: hold, PublicAudit: &PublicAudit{}})
					done <- err
					return
				}
				done <- e.store.Revoke(ctx, RevokeInput{OrgID: f.org, InviteID: f.created.ID, Actor: Actor{PrincipalID: &actor}, Authorize: hold})
			}()
			select {
			case <-inAuthorize:
			case err := <-done:
				t.Fatalf("%s ended before its Authorize: %v", verb, err)
			}
			for _, table := range []string{"auth.principals", "public.users"} {
				_, err := e.admin.Exec(ctx, `SELECT 1 FROM `+table+` WHERE id = $1 FOR UPDATE NOWAIT`, actor)
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
					t.Fatalf("%s: the actor's %s row is not held while Authorize runs (lock probe: %v)", verb, table, err)
				}
			}
			free()
			if err := <-done; err != nil {
				t.Fatalf("%s: %v", verb, err)
			}
		})
	}
}
