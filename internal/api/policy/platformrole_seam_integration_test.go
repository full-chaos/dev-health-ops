//go:build integration

package policy

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
)

type seamRead func(PGStore, context.Context, uuid.UUID) (bool, bool, error)

var seamReads = map[string]seamRead{
	"UserState": func(s PGStore, ctx context.Context, id uuid.UUID) (bool, bool, error) {
		state, ok, err := s.UserState(ctx, id)
		return state.IsSuperuser, ok, err
	},
	"AccessUser": func(s PGStore, ctx context.Context, id uuid.UUID) (bool, bool, error) {
		row, ok, err := s.AccessUser(ctx, id)
		return row.State.IsSuperuser, ok, err
	},
}

// While owned, is_superuser is the auth assignment under the store's rule
// (held, not expired, principal not disabled) and public is_superuser is not
// read; off, it is public is_superuser as before.
func TestUserStateReadsTheAssignmentWhileOwned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, true)
	assigned := env.User(ctx, t, false, true)
	publicOnly := env.User(ctx, t, true, false)
	expired := env.User(ctx, t, true, true)
	env.Expire(ctx, t, expired)
	disabled := env.User(ctx, t, true, true)
	env.Disable(ctx, t, disabled)
	on := PGStore{Pool: env.Reader, AuthSchema: "auth", PlatformRolesOwned: true}
	off := PGStore{Pool: env.Reader, AuthSchema: "auth"}
	for name, read := range seamReads {
		for _, c := range []struct {
			who     string
			id      uuid.UUID
			on, off bool
		}{
			{"an assignment, public false", assigned, true, false},
			{"public superuser, no assignment", publicOnly, false, true},
			{"an expired assignment", expired, false, true},
			{"a disabled principal", disabled, false, true},
		} {
			for store, want := range map[*PGStore]bool{&on: c.on, &off: c.off} {
				got, ok, err := read(*store, ctx, c.id)
				if err != nil || !ok || got != want {
					t.Errorf("%s, %s, owned %v: superuser %v ok %v err %v, want %v", name, c.who, store.PlatformRolesOwned, got, ok, err, want)
				}
			}
		}
	}
}

// AMENDMENT 2: while owned, a read of the auth tables that fails (here the
// reader grant lacks the platform role read) is the store's grant error,
// never "not a superuser".
func TestUserStateAuthReadErrorIsStoreUnavailableWhileOwned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, false)
	id := env.User(ctx, t, true, true)
	on := PGStore{Pool: env.Reader, AuthSchema: "auth", PlatformRolesOwned: true}
	for name, read := range seamReads {
		got, ok, err := read(on, ctx, id)
		if err == nil || MissingGrant(err) == "" || ok || got {
			t.Errorf("%s: superuser %v ok %v err %v, want the grant error", name, got, ok, err)
		}
	}
}
