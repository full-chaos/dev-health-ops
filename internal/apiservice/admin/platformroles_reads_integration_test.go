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

// While owned, a profile-only update (the HTTP PATCH and the dho verb) leaves
// public users.is_superuser as it was, in both directions: the row it reads
// carries the auth value, and none of it is written back. Off, the column is
// written with its own value, unchanged too.
func TestAdminProfileUpdateLeavesThePublicColumnWhileOwned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, true)
	public := func(id uuid.UUID) bool {
		t.Helper()
		var superuser bool
		if err := env.Admin.QueryRow(ctx, `SELECT is_superuser FROM public.users WHERE id = $1`, id).Scan(&superuser); err != nil {
			t.Fatal(err)
		}
		return superuser
	}
	caller := &policy.User{UserID: uuid.NewString(), ID: uuid.New(), IsSuperuser: true}
	for _, owned := range []bool{true, false} {
		publicOnly := env.User(ctx, t, true, false)
		assigned := env.User(ctx, t, false, true)
		store := pgStore{Pool: env.Admin, PlatformRolesOwned: owned, AuthSchema: "auth"}
		h := &handlers{store: store, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		operator := Operator{Pool: env.Admin, PlatformRolesOwned: owned, AuthSchema: "auth"}
		for who, before := range map[uuid.UUID]bool{publicOnly: true, assigned: false} {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPatch, "/api/v1/admin/users/"+who.String(), nil)
			request.SetPathValue("user_id", who.String())
			request = withBody(`{"full_name":"renamed"}`)(request.WithContext(policy.WithUser(request.Context(), caller)))
			h.updateUser(recorder, request)
			if recorder.Code != http.StatusOK || public(who) != before {
				t.Errorf("owned %v: PATCH full_name of %s: status %d %s, public is_superuser %v, want %v", owned, who, recorder.Code, recorder.Body.String(), public(who), before)
			}
			id, name := who.String(), "renamed again"
			if _, err := operator.UpdateUser(ctx, UpdateUserInput{ID: &id, FullName: &name}); err != nil || public(who) != before {
				t.Errorf("owned %v: dho users update of %s: %v, public is_superuser %v, want %v", owned, who, err, public(who), before)
			}
		}
	}
}

// The transfer-ownership guard's target read follows the switch: for a
// non-superuser caller and an organization with no owner, a target that holds
// only the platform role is refused (403) while owned and passes the guard
// (400, not an owner) while off; a public-only superuser the reverse.
func TestTransferOwnershipGuardReadsThePlatformRoleWhileOwned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, true)
	publicOnly := env.User(ctx, t, true, false)
	assigned := env.User(ctx, t, false, true)
	org := env.Member(ctx, t, env.User(ctx, t, false, false), "member")
	caller := &policy.User{UserID: uuid.NewString(), ID: uuid.New()}
	for _, owned := range []bool{true, false} {
		h := &handlers{store: pgStore{Pool: env.Admin, PlatformRolesOwned: owned, AuthSchema: "auth"}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		for who, refused := range map[uuid.UUID]bool{assigned: owned, publicOnly: !owned} {
			recorder := httptest.NewRecorder()
			h.transferOwnershipInAuth(ctx, recorder, caller, org, who)
			want := map[bool]int{true: http.StatusForbidden, false: http.StatusBadRequest}[refused]
			if recorder.Code != want {
				t.Errorf("owned %v: transfer to %s: status %d %s, want %d", owned, who, recorder.Code, recorder.Body.String(), want)
			}
		}
	}
}
