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
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
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

// While owned, a guarded write waits for a platform role grant in flight and
// then decides on it: conn A holds the platform role write lock and has
// inserted the target's assignment, uncommitted; the guarded write of a
// non-superuser caller blocks on the lock (seen in pg_locks, not timed) and,
// once A commits, refuses with 403. Off, the guarded write does not wait.
func TestGuardedWriteWaitsForAnInFlightPlatformRoleGrant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, true)
	caller := &policy.User{UserID: uuid.NewString(), ID: uuid.New()}
	for _, owned := range []bool{true, false} {
		target := env.User(ctx, t, false, false)
		locked, release, granted := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() {
			assignment, principal := uuid.New(), target.String()
			granted <- audit.Commit(ctx, env.Admin, env.Schema, audit.Mutation{
				Apply: func(ctx context.Context, tx audit.TxOps) error {
					if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, rule.WriteLock); err != nil {
						return err
					}
					if _, err := tx.Exec(ctx, `INSERT INTO auth.platform_role_assignments (id, principal_id, role_key) VALUES ($1, $2, $3)`, assignment, target, rule.PlatformAdmin); err != nil {
						return err
					}
					close(locked)
					<-release
					return nil
				},
				Audit: audit.AuditEvent{EventType: "auth.platform_role.granted", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &principal,
					Attributes: map[string]any{"reason_code": "role_granted", "target_kind": "principal", "target_id": principal}},
				Events: []audit.OutboxEvent{{AggregateType: "platform_role_assignment", AggregateID: provisioning.WireID("platform_role_assignment", assignment.String()),
					EventType: "platform_role_assignment.granted", Change: map[string]any{"status": "active"}}},
			})
		}()
		select {
		case <-locked:
		case err := <-granted:
			t.Fatalf("owned %v: the grant ended before it held the lock: %v", owned, err)
		}
		h := &handlers{store: pgStore{Pool: env.Admin, PlatformRolesOwned: owned, AuthSchema: "auth"}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		answered := make(chan int, 1)
		go func() {
			recorder := httptest.NewRecorder()
			h.superuserGuardedWrite(ctx, recorder, caller, nil, "update_user", func(g *superuserTx) func() {
				if g.refuse(target) {
					return nil
				}
				return func() { recorder.WriteHeader(http.StatusOK) }
			})
			answered <- recorder.Code
		}()
		if !owned {
			if code := <-answered; code != http.StatusOK {
				t.Errorf("off: the guarded write answered %d while the grant was in flight, want 200", code)
			}
			close(release)
			if err := <-granted; err != nil {
				t.Fatal(err)
			}
			continue
		}
		for waiting := false; !waiting; {
			select {
			case code := <-answered:
				t.Fatalf("owned: the guarded write answered %d before the grant committed: it did not wait for the platform role write lock", code)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			default:
			}
			if err := env.Admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted)`).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
		}
		close(release)
		if err := <-granted; err != nil {
			t.Fatal(err)
		}
		if code := <-answered; code != http.StatusForbidden {
			t.Errorf("owned: the guarded write answered %d after the grant committed, want 403", code)
		}
	}
}
