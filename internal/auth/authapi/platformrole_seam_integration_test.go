//go:build integration

package authapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
)

// While owned, StateOn's is_superuser is the auth assignment held by an active public.users row; off, it is public is_superuser. A failed auth read while owned is the
// users store error, never "not a superuser".
func TestAuthServiceStateOnSameRules(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, true)
	assigned := env.User(ctx, t, false, true)
	publicOnly := env.User(ctx, t, true, false)
	expired := env.ExpiredUser(ctx, t, true)
	disabled := env.User(ctx, t, true, true)
	env.Disable(ctx, t, disabled)
	inactive := env.User(ctx, t, true, true)
	pgseed.SetUserState(ctx, t, env.Admin, inactive.String(), false, true, 0)
	on := PGUsers{Pool: env.Admin, AuthSchema: env.Schema, PlatformRolesOwned: true}
	off := PGUsers{Pool: env.Admin, AuthSchema: env.Schema}
	for _, c := range []struct {
		who     string
		id      uuid.UUID
		on, off bool
	}{
		{"an assignment, public false", assigned, true, false},
		{"public superuser, no assignment", publicOnly, false, true},
		{"an expired assignment", expired, false, true},
		{"a disabled auth copy of an active public user: public wins", disabled, true, true},
		{"an inactive public user with an assignment", inactive, false, true},
	} {
		for _, store := range []PGUsers{on, off} {
			want := c.off
			if store.PlatformRolesOwned {
				want = c.on
			}
			state, found, err := store.State(ctx, c.id)
			if err != nil || !found || state.IsSuperuser != want {
				t.Errorf("%s, owned %v: %+v found %v err %v, want superuser %v", c.who, store.PlatformRolesOwned, state, found, err, want)
			}
		}
	}

	ungranted := platformroleseam.Start(ctx, t, false)
	id := ungranted.User(ctx, t, true, true)
	state, found, err := PGUsers{Pool: ungranted.Reader, AuthSchema: ungranted.Schema, PlatformRolesOwned: true}.State(ctx, id)
	if !errors.Is(err, errUsersStore) || found || state.IsSuperuser {
		t.Fatalf("auth read error while owned: %+v found %v err %v, want the users store error", state, found, err)
	}
}
