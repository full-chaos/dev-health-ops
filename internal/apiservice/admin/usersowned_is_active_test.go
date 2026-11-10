package admin

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// At phase 4 (AUTH_USERS_OWNED on, prod since CHAOS-11505) go-api writes no is_active, so it can never leave a
// reactivated public.users row beside a disabled auth principal: the PATCH /admin/users/{user_id} that activates or
// deactivates a user (the sent is_active differs from the stored one) is refused 409 operation_moved with no write
// and no auth-service call, with platform roles owned too, and the store write itself refuses (CHAOS-13209: acp
// relies on this for flip-gate row 4 = 0). The handler reads the stored row first: only a real change is refused
// (PLAN-13380-v4 Q9). A change that opens the local writer goes red here first.
func TestUsersOwnedRefusesAnIsActiveChange(t *testing.T) {
	for _, active := range []bool{true, false} {
		for _, rolesOwned := range []bool{true, false} {
			body := `{"is_active":false}`
			if active {
				body = `{"is_active":true}`
			}
			stored := activeStored()
			stored.active = !active
			db, users := ownedDB(stored), &fakeUsersAdmin{}
			code, answer := ownedCall{route: "update", body: body}.run(ownedHandlers(db, users, rolesOwned))
			if code != http.StatusConflict || !strings.Contains(answer, `"reason":"operation_moved"`) {
				t.Errorf("is_active %v, roles owned %v: %d %q, want 409 operation_moved", active, rolesOwned, code, answer)
			}
			if len(users.verbs) != 0 || len(wrote(db)) != 0 {
				t.Errorf("is_active %v, roles owned %v: auth calls %v, writes %q, want none", active, rolesOwned, users.verbs, wrote(db))
			}
			store := pgStore{UsersOwned: true, PlatformRolesOwned: rolesOwned}
			if _, err := store.updateUser(context.Background(), uuid.New(), userUpdate{IsActive: &active}); !errors.Is(err, ErrUsersOwned) {
				t.Errorf("is_active %v, roles owned %v: store updateUser = %v, want ErrUsersOwned", active, rolesOwned, err)
			}
		}
	}
}
