//go:build integration

package admin

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
)

// While owned, every admin read of is_superuser is the auth platform role:
// the impersonation target, the user rows, the superuser count, and the
// guarded write's target lock (a non-superuser caller cannot write to an
// assignment holder).
func TestAdminSuperuserReadsFollowThePlatformRoleWhileOwned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, true)
	publicOnly := env.User(ctx, t, true, false)
	assigned := env.User(ctx, t, false, true)
	env.User(ctx, t, false, true)
	for _, owned := range []bool{true, false} {
		store := pgStore{Pool: env.Admin, PlatformRolesOwned: owned, AuthSchema: "auth"}
		for who, want := range map[uuid.UUID]bool{publicOnly: !owned, assigned: owned} {
			target, err := store.userByID(ctx, who)
			if err != nil || target == nil || target.IsSuperuser != want {
				t.Errorf("owned %v: impersonation target %s: %+v %v, want superuser %v", owned, who, target, err, want)
			}
			full, err := store.fullUserByID(ctx, who)
			if err != nil || full == nil || full.IsSuperuser != want {
				t.Errorf("owned %v: user row %s: %+v %v, want superuser %v", owned, who, full, err, want)
			}
		}
		stats, err := store.platformStats(ctx, time.Now().Add(-time.Hour))
		if want := map[bool]int64{true: 2, false: 1}[owned]; err != nil || stats.superuserCount != want {
			t.Errorf("owned %v: superuser count %+v %v, want %d", owned, stats, err, want)
		}
		h := &handlers{store: store, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		caller := &policy.User{UserID: uuid.NewString(), ID: uuid.New()}
		for who, wantRefused := range map[uuid.UUID]bool{publicOnly: !owned, assigned: owned} {
			recorder := httptest.NewRecorder()
			h.superuserGuardedWrite(ctx, recorder, caller, nil, "update_user", func(g *superuserTx) func() {
				if g.refuse(who) {
					return nil
				}
				return func() { recorder.WriteHeader(http.StatusOK) }
			})
			if refused := recorder.Code == http.StatusForbidden; refused != wantRefused {
				t.Errorf("owned %v: guarded write to %s: status %d, want refused %v", owned, who, recorder.Code, wantRefused)
			}
		}
	}
}
