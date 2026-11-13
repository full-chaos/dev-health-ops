package admin

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

// While users are owned, go-api's own store writes no is_active: the local writer refuses, so go-api can never leave
// a reactivated public.users row beside a disabled auth principal. The PATCH that activates or deactivates a user is
// forwarded to auth-service, which writes both schemas in one commit (CHAOS-13684, pm D7015; the forward is
// TestUsersOwnedPatchIsActiveIsServed).
func TestUsersOwnedLocalStoreRefusesAnIsActiveWrite(t *testing.T) {
	for _, active := range []bool{true, false} {
		for _, rolesOwned := range []bool{true, false} {
			store := pgStore{UsersOwned: true, PlatformRolesOwned: rolesOwned}
			if _, err := store.updateUser(context.Background(), uuid.New(), userUpdate{IsActive: &active}); !errors.Is(err, ErrUsersOwned) {
				t.Errorf("is_active %v, roles owned %v: store updateUser = %v, want ErrUsersOwned", active, rolesOwned, err)
			}
		}
	}
}
