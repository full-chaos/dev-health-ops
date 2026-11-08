package admin

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// At phase 4 (AUTH_USERS_OWNED on, prod since CHAOS-11505) go-api writes no is_active, so it can never leave a
// reactivated public.users row beside a disabled auth principal: the PATCH /admin/users/{user_id} that activates or
// deactivates a user is refused 409 operation_moved before any database work, with platform roles owned too, and the
// store write itself refuses (CHAOS-13209: acp relies on this for flip-gate row 4 = 0). A change that opens the
// local writer goes red here first.
func TestUsersOwnedRefusesAnIsActiveChange(t *testing.T) {
	for _, active := range []bool{true, false} {
		for _, rolesOwned := range []bool{true, false} {
			body := `{"is_active":false}`
			if active {
				body = `{"is_active":true}`
			}
			h := &handlers{store: pgStore{Pool: failingDB{t}, UsersOwned: true, PlatformRolesOwned: rolesOwned},
				logger: slog.New(slog.NewTextHandler(&strings.Builder{}, nil))}
			request := httptest.NewRequest(http.MethodPatch, "/api/v1/admin/users/"+uuid.NewString(), strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.SetPathValue("user_id", uuid.NewString())
			recorder := httptest.NewRecorder()
			h.updateUser(recorder, request)
			if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), `"reason":"operation_moved"`) {
				t.Errorf("is_active %v, roles owned %v: %d %q, want 409 operation_moved", active, rolesOwned, recorder.Code, recorder.Body.String())
			}
			store := pgStore{UsersOwned: true, PlatformRolesOwned: rolesOwned}
			if _, err := store.updateUser(context.Background(), uuid.New(), userUpdate{IsActive: &active}); !errors.Is(err, ErrUsersOwned) {
				t.Errorf("is_active %v, roles owned %v: store updateUser = %v, want ErrUsersOwned", active, rolesOwned, err)
			}
		}
	}
}
