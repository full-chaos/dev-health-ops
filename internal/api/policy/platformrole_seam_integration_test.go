//go:build integration

package policy

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
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
	expired := env.ExpiredUser(ctx, t, true)
	disabled := env.User(ctx, t, true, true)
	env.Disable(ctx, t, disabled)
	service := serviceAccountWithAssignment(t, ctx, env)
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
			{"a service account principal", service, false, false},
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

// serviceAccountWithAssignment is a public user whose auth principal is a
// service account holding platform_admin: the platform role rule holds only
// for a user principal.
func serviceAccountWithAssignment(t *testing.T, ctx context.Context, env *platformroleseam.Env) uuid.UUID {
	t.Helper()
	id := uuid.New()
	pgseed.User(ctx, t, env.Admin, id.String())
	principal, assignment := id.String(), uuid.New()
	err := audit.Commit(ctx, env.Admin, env.Schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if _, err := tx.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'service_account', 'svc')`, id); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO auth.platform_role_assignments (id, principal_id, role_key) VALUES ($1, $2, 'platform_admin')`, assignment, id)
			return err
		},
		Audit: audit.AuditEvent{EventType: "auth.principal.projected", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &principal,
			Attributes: map[string]any{"reason_code": "principal_projected", "target_kind": "principal", "target_id": principal}},
		Events: []audit.OutboxEvent{
			{AggregateType: "principal", AggregateID: provisioning.WireID("principal", principal), EventType: "principal.created", Change: map[string]any{"status": "active"}},
			{AggregateType: "platform_role_assignment", AggregateID: provisioning.WireID("platform_role_assignment", assignment.String()),
				EventType: "platform_role_assignment.granted", Change: map[string]any{"status": "active"}},
		},
	})
	if err != nil {
		t.Fatalf("plant a service account with an assignment: %v", err)
	}
	return id
}
