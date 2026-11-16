package identity

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// eraseFakeTx records each step of applyAdminErase. Its reads answer the principal, an active public.users row, two
// memberships and one external identity; failOn makes the first statement with that prefix fail with code.
type eraseFakeTx struct {
	steps          []string
	events         []audit.OutboxEvent
	noPrincipal    bool
	failOn, code   string
	memberships    []string
	identities     []string
	providers      []string
	publicIsActive bool
}

type eraseRow struct {
	err  error
	scan func(dest ...any)
}

func (r eraseRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	r.scan(dest...)
	return nil
}

func (f *eraseFakeTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	f.steps = append(f.steps, "exec:"+sql)
	if f.failOn != "" && strings.HasPrefix(sql, f.failOn) {
		return pgconn.CommandTag{}, &pgconn.PgError{Code: f.code}
	}
	return pgconn.NewCommandTag("DELETE 1"), nil
}

func (f *eraseFakeTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	switch {
	case strings.Contains(sql, ".principals WHERE id = $1 AND kind = 'user' FOR UPDATE"):
		f.steps = append(f.steps, "lock:principal")
		if f.noPrincipal {
			return eraseRow{err: pgx.ErrNoRows}
		}
		return eraseRow{scan: func(dest ...any) { *dest[0].(*uuid.UUID) = uuid.New() }}
	case strings.Contains(sql, "FROM public.users WHERE id = $1 FOR UPDATE"):
		f.steps = append(f.steps, "lock:public.users")
		return eraseRow{scan: func(dest ...any) { *dest[0].(*bool) = f.publicIsActive }}
	case strings.Contains(sql, ".memberships WHERE principal_id = $1"):
		f.steps = append(f.steps, "read:memberships")
		return eraseRow{scan: func(dest ...any) { *dest[0].(*[]string) = f.memberships }}
	case strings.Contains(sql, ".external_identities WHERE principal_id = $1"):
		f.steps = append(f.steps, "read:identities")
		return eraseRow{scan: func(dest ...any) {
			*dest[0].(*[]string) = f.identities
			*dest[1].(*[]string) = f.providers
		}}
	}
	f.steps = append(f.steps, "unexpected:"+sql)
	return eraseRow{err: errors.New("unexpected read")}
}

func (f *eraseFakeTx) AddEvent(events ...audit.OutboxEvent) error {
	for _, e := range events {
		f.steps = append(f.steps, "event:"+e.EventType)
	}
	f.events = append(f.events, events...)
	return nil
}

func (f *eraseFakeTx) FlushAudit(context.Context) error {
	f.steps = append(f.steps, "flush:audit")
	return nil
}

func newEraseFake() *eraseFakeTx {
	return &eraseFakeTx{publicIsActive: true, memberships: []string{uuid.NewString(), uuid.NewString()},
		identities: []string{uuid.NewString()}, providers: []string{"github"}}
}

// The erase takes the delete's lock order (the principal, then the public.users row), revokes the user's own platform
// roles, queues the events of its own rows, writes the queued audit rows, and only then deletes: the auth memberships,
// public.users, auth.users, the principal.
func TestAdminForceDeleteApplyOrder(t *testing.T) {
	f := newEraseFake()
	revoked := false
	result, err := applyAdminErase(context.Background(), f, `"auth"`, uuid.New(), func(context.Context) error {
		revoked = true
		f.steps = append(f.steps, "revoke:platform_roles")
		return nil
	})
	if err != nil || !revoked || !result.WasActive {
		t.Fatalf("erase: %v, revoked %v, was_active %v; want nil, true, true", err, revoked, result.WasActive)
	}
	want := []string{
		"lock:principal", "lock:public.users", "revoke:platform_roles", "read:memberships", "read:identities",
		"event:membership.removed", "event:membership.removed", "event:external_identity.unlinked", "flush:audit",
		`exec:DELETE FROM "auth".memberships WHERE principal_id = $1`,
		`exec:UPDATE "public"."billing_audit_log" SET "actor_id" = NULL WHERE "actor_id" = $1`,
		`exec:UPDATE "public"."impersonation_sessions" SET "admin_user_id" = NULL WHERE "admin_user_id" = $1`,
		`exec:UPDATE "public"."impersonation_sessions" SET "target_user_id" = NULL WHERE "target_user_id" = $1`,
		`exec:UPDATE "public"."refunds" SET "initiated_by" = NULL WHERE "initiated_by" = $1`,
		`exec:DELETE FROM public.users WHERE id = $1`,
		`exec:DELETE FROM "auth".users WHERE principal_id = $1`,
		`exec:DELETE FROM "auth".principals WHERE id = $1`,
	}
	if !reflect.DeepEqual(f.steps, want) {
		t.Errorf("steps:\n got %q\nwant %q", f.steps, want)
	}
	for _, e := range f.events {
		if err := provisioning.CheckEvent(e.AggregateType, e.AggregateID, e.EventType, e.Change); err != nil {
			t.Errorf("event %s: %v", e.EventType, err)
		}
	}
}

// A foreign key that refuses a delete (23001 RESTRICT, 23503 NO ACTION) is ErrReferenceBlocked, and nothing after the
// refused statement runs; Commit rolls the rest back. Another database error is returned as itself.
func TestAdminForceDeleteRefusesBlockedHistoryWritesNothing(t *testing.T) {
	for _, c := range []struct {
		name, failOn, code string
		want               error
	}{
		{"public restrict", "DELETE FROM public.users", "23001", ErrReferenceBlocked},
		{"public no action", "DELETE FROM public.users", "23503", ErrReferenceBlocked},
		{"principal restrict", `DELETE FROM "auth".principals`, "23001", ErrReferenceBlocked},
		{"another failure", "DELETE FROM public.users", "40001", nil},
	} {
		f := newEraseFake()
		f.failOn, f.code = c.failOn, c.code
		_, err := applyAdminErase(context.Background(), f, `"auth"`, uuid.New(), func(context.Context) error { return nil })
		if c.want != nil && !errors.Is(err, c.want) {
			t.Errorf("%s: %v, want %v", c.name, err, c.want)
		}
		if c.want == nil && (err == nil || errors.Is(err, ErrReferenceBlocked)) {
			t.Errorf("%s: %v, want the database error itself", c.name, err)
		}
		last := f.steps[len(f.steps)-1]
		if !strings.HasPrefix(last, "exec:"+c.failOn) {
			t.Errorf("%s: last step %q, want the refused %q", c.name, last, c.failOn)
		}
	}
	f := newEraseFake()
	f.noPrincipal = true
	if _, err := applyAdminErase(context.Background(), f, `"auth"`, uuid.New(), func(context.Context) error { return nil }); !errors.Is(err, ErrUserNotFound) {
		t.Errorf("unknown principal: %v, want ErrUserNotFound", err)
	}
	if len(f.steps) != 1 {
		t.Errorf("unknown principal: steps %q, want the lock only", f.steps)
	}
}

// The erase row names the admin as actor and no subject column (the principal is gone when Commit writes it): the
// user's id is in its attributes. The mutation carries the soft delete's two events and no off-stream reason, and its
// refusals log at WARN.
func TestAdminForceDeleteAuditRowKeepsTheIdInItsAttributes(t *testing.T) {
	store := lazyOwnedStore(t)
	var got audit.Mutation
	old := commitMutation
	commitMutation = func(_ context.Context, _ *pgxpool.Pool, _ authschema.ValidatedIdentifier, m audit.Mutation) error {
		got = m
		return nil
	}
	t.Cleanup(func() { commitMutation = old })
	principal, actor := uuid.New(), uuid.New()
	none := func(context.Context, audit.TxOps) error { return nil }
	if _, err := store.AdminForceDeleteUser(context.Background(), AdminForceDeleteRequest{Principal: principal, Actor: actor,
		Authorize: none, RevokeRoles: none}); err != nil {
		t.Fatal(err)
	}
	a := got.Audit
	if a.EventType != "auth.user.erased" || a.SubjectPrincipalID != nil || a.ActorPrincipalID == nil || *a.ActorPrincipalID != actor.String() {
		t.Errorf("audit row %+v: want auth.user.erased, no subject, actor %s", a, actor)
	}
	if a.Attributes["target_id"] != principal.String() || a.Attributes["target_kind"] != "principal" || a.Attributes["reason_code"] != "principal_erased" {
		t.Errorf("audit attributes %v: want target_id %s, target_kind principal, reason_code principal_erased", a.Attributes, principal)
	}
	if got.OffStream != "" || len(got.Events) != 2 || got.Events[0].EventType != "principal.disabled" ||
		got.Events[1].EventType != "user.tokens_invalidated" || got.Events[0].Change["status"] != "disabled" {
		t.Errorf("mutation off_stream %q events %+v, want none and principal.disabled (disabled), user.tokens_invalidated", got.OffStream, got.Events)
	}
	if !reflect.DeepEqual(got.Refusals, []error{ErrUserNotFound, ErrReferenceBlocked}) {
		t.Errorf("refusals %v", got.Refusals)
	}
	if _, err := store.AdminForceDeleteUser(context.Background(), AdminForceDeleteRequest{Principal: principal, Actor: actor, Authorize: none}); err == nil {
		t.Error("an erase without its role revoke was accepted")
	}
}
