//go:build integration

package admin

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
)

// While platform roles are owned, a superuser TARGET keeps its protection when its principal is disabled, as legacy
// read the target's stored is_superuser column: a caller who is not a superuser may not write to it (the guarded
// write, the transfer-ownership target and the impersonation target read the stored holding). The disabled user still
// holds no authority.
func TestOwnedDisabledSuperuserTargetKeepsItsProtection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, true)
	h := &handlers{store: pgStore{Pool: env.Admin, PlatformRolesOwned: true}, logger: slog.New(slog.DiscardHandler)}

	disabledSuper := env.User(ctx, t, false, true)
	pgseed.SetUserState(ctx, t, env.Admin, disabledSuper.String(), false, false, 0)
	env.Disable(ctx, t, disabledSuper)
	plain := env.User(ctx, t, false, false)
	orgAdmin := &policy.User{UserID: uuid.NewString(), IsSuperuser: false}
	orgAdmin.ID = uuid.MustParse(orgAdmin.UserID)

	guarded := func(target uuid.UUID) int {
		t.Helper()
		w := httptest.NewRecorder()
		h.superuserGuardedWrite(ctx, w, orgAdmin, nil, "update_user", func(g *superuserTx) func() {
			if g.refuse(target) {
				return nil
			}
			return func() { w.WriteHeader(http.StatusOK) }
		})
		return w.Code
	}
	if code := guarded(disabledSuper); code != http.StatusForbidden {
		t.Errorf("a non-superuser write to a disabled superuser target answered %d, want 403", code)
	}
	if code := guarded(plain); code != http.StatusOK {
		t.Errorf("a non-superuser write to a plain user answered %d, want 200", code)
	}
	if super, err := h.store.isSuperuser(ctx, disabledSuper); err != nil || !super {
		t.Errorf("transfer target read of the disabled superuser = %v, %v; want true", super, err)
	}
	if target, err := h.store.userByID(ctx, disabledSuper); err != nil || target == nil || !target.IsSuperuser {
		t.Errorf("impersonation target read of the disabled superuser = %+v, %v; want is_superuser true", target, err)
	}
	authority, err := policy.PlatformRoleRead{Owned: true}.Superuser("is_superuser", "users.id")
	if err != nil {
		t.Fatal(err)
	}
	var held bool
	if err := env.Admin.QueryRow(ctx, `SELECT `+authority+` FROM users WHERE id = $1`, disabledSuper).Scan(&held); err != nil || held {
		t.Errorf("authority of the disabled superuser = %v, %v; want false", held, err)
	}
}
