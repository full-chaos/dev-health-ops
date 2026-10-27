//go:build integration

package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func (e *env) publicUser(t *testing.T, ctx context.Context, id uuid.UUID) {
	t.Helper()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users (id, email, username, full_name, token_version) VALUES ($1, $2, $3, 'Last Login', 0)`,
		id, id.String()+"@example.test", id.String()); err != nil {
		t.Fatalf("seed public.users: %v", err)
	}
}

func (e *env) lastLogin(t *testing.T, ctx context.Context, id uuid.UUID) (*time.Time, time.Time) {
	t.Helper()
	var last *time.Time
	var updated time.Time
	if err := e.admin.QueryRow(ctx, `SELECT last_login_at, updated_at FROM public.users WHERE id = $1`, id).Scan(&last, &updated); err != nil {
		t.Fatalf("read public.users: %v", err)
	}
	return last, updated
}

// Users owned: each sign-in stamps public.users.last_login_at and updated_at
// with the session's issued_at, to the microsecond, once per sign-in.
func TestSignInStampsPublicLastLoginWhenUsersOwned(t *testing.T) {
	ctx := testContext(t)
	e := newEnvOwned(t, ctx, true)
	e.store = e.newStore(t, e.keyText, Config{UsersOwned: true})
	principal := e.principal(t, ctx)
	e.publicUser(t, ctx, principal)
	for i := 0; i < 2; i++ {
		issued, err := e.store.CreateAudited(WithSignIn(ctx), principal, nil, []string{"pwd"}, signInEvent(principal))
		if err != nil {
			t.Fatalf("CreateAudited: %v", err)
		}
		last, updated := e.lastLogin(t, ctx, principal)
		if last == nil {
			t.Fatalf("sign-in %d: last_login_at is NULL", i+1)
		}
		var want time.Time
		if err := e.admin.QueryRow(ctx, `SELECT issued_at FROM auth.sessions WHERE id = $1`, issued.Session.ID.String()).Scan(&want); err != nil {
			t.Fatalf("read issued_at: %v", err)
		}
		want = want.UTC()
		if !last.UTC().Equal(want) || !updated.UTC().Equal(want) {
			t.Fatalf("sign-in %d: last_login_at %v updated_at %v, want issued_at %v", i+1, last.UTC(), updated.UTC(), want)
		}
	}
}

// Users not owned: go-api stamps; auth-service writes nothing to public.users.
func TestSignInWritesNothingToPublicUsersWhenNotOwned(t *testing.T) {
	ctx := testContext(t)
	e := newEnvOwned(t, ctx, false)
	principal := e.principal(t, ctx)
	e.publicUser(t, ctx, principal)
	if _, err := e.store.CreateAudited(WithSignIn(ctx), principal, nil, []string{"pwd"}, signInEvent(principal)); err != nil {
		t.Fatalf("CreateAudited: %v", err)
	}
	if last, _ := e.lastLogin(t, ctx, principal); last != nil {
		t.Fatalf("last_login_at = %v, want NULL", last)
	}
}

// A sign-in whose audit row fails leaves no session and no stamp.
func TestSignInStampRollsBackWithTheSession(t *testing.T) {
	ctx := testContext(t)
	e := newEnvOwned(t, ctx, true)
	e.store = e.newStore(t, e.keyText, Config{UsersOwned: true})
	principal := e.principal(t, ctx)
	e.publicUser(t, ctx, principal)
	if _, err := e.admin.Exec(ctx, `CREATE FUNCTION auth.refuse_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit refused'; END $$;
		CREATE TRIGGER refuse_audit BEFORE INSERT ON auth.security_audit_events FOR EACH ROW EXECUTE FUNCTION auth.refuse_audit()`); err != nil {
		t.Fatalf("install trigger: %v", err)
	}
	if _, err := e.store.CreateAudited(WithSignIn(ctx), principal, nil, []string{"pwd"}, signInEvent(principal)); err == nil {
		t.Fatal("CreateAudited succeeded with a refused audit row")
	}
	if last, _ := e.lastLogin(t, ctx, principal); last != nil {
		t.Fatalf("last_login_at = %v after a rolled back sign-in, want NULL", last)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, principal.String()); n != 0 {
		t.Fatalf("%d sessions after a rolled back sign-in", n)
	}
}

// A user with no public.users row cannot sign in under users owned: no stamp
// means the transaction fails and no session is written.
func TestSignInWithoutPublicUserFailsWhenUsersOwned(t *testing.T) {
	ctx := testContext(t)
	e := newEnvOwned(t, ctx, true)
	e.store = e.newStore(t, e.keyText, Config{UsersOwned: true})
	principal := e.principal(t, ctx)
	_, err := e.store.CreateAudited(WithSignIn(ctx), principal, nil, []string{"pwd"}, signInEvent(principal))
	if err == nil || errors.Is(err, ErrPrincipalUnavailable) {
		t.Fatalf("err = %v, want a store failure", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, principal.String()); n != 0 {
		t.Fatal("a session was written")
	}
}

// CreateAudited (the issueSession exchange) is not a sign-in: it never stamps.
func TestCreateAuditedLeavesLastLoginUnchangedWhenUsersOwned(t *testing.T) {
	ctx := testContext(t)
	e := newEnvOwned(t, ctx, true)
	e.store = e.newStore(t, e.keyText, Config{UsersOwned: true})
	principal := e.principal(t, ctx)
	e.publicUser(t, ctx, principal)
	if _, err := e.store.CreateAudited(ctx, principal, nil, []string{"pwd"}, signInEvent(principal)); err != nil {
		t.Fatalf("CreateAudited: %v", err)
	}
	if last, _ := e.lastLogin(t, ctx, principal); last != nil {
		t.Fatalf("last_login_at = %v, want NULL", last)
	}
}

// A disabled principal's sign-in under users owned is refused before the
// stamp: no session, no audit row, public.users untouched.
func TestSignInForADisabledPrincipalWritesNothingWhenUsersOwned(t *testing.T) {
	ctx := testContext(t)
	e := newEnvOwned(t, ctx, true)
	e.store = e.newStore(t, e.keyText, Config{UsersOwned: true})
	principal := e.principal(t, ctx)
	e.publicUser(t, ctx, principal)
	planted := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET last_login_at = $2, updated_at = $2 WHERE id = $1`, principal, planted); err != nil {
		t.Fatalf("plant last_login_at: %v", err)
	}
	e.guarded(t, ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, principal.String())

	const sessions = `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`
	const audits = `SELECT count(*) FROM auth.security_audit_events`
	sessionsBefore := e.count(t, ctx, sessions, principal.String())
	auditsBefore := e.count(t, ctx, audits)
	lastBefore, updatedBefore := e.lastLogin(t, ctx, principal)
	if lastBefore == nil || !lastBefore.UTC().Equal(planted) {
		t.Fatalf("planted last_login_at = %v, want %v", lastBefore, planted)
	}

	_, err := e.store.CreateAudited(WithSignIn(ctx), principal, nil, []string{"pwd"}, signInEvent(principal))
	if !errors.Is(err, ErrPrincipalUnavailable) {
		t.Fatalf("err = %v, want ErrPrincipalUnavailable", err)
	}
	if n := e.count(t, ctx, sessions, principal.String()); n != sessionsBefore {
		t.Fatalf("sessions %d -> %d for a refused sign-in", sessionsBefore, n)
	}
	if n := e.count(t, ctx, audits); n != auditsBefore {
		t.Fatalf("audit rows %d -> %d for a refused sign-in", auditsBefore, n)
	}
	lastAfter, updatedAfter := e.lastLogin(t, ctx, principal)
	if lastAfter == nil || !lastAfter.Equal(*lastBefore) || !updatedAfter.Equal(updatedBefore) {
		t.Fatalf("public.users changed: last_login_at %v -> %v, updated_at %v -> %v", lastBefore, lastAfter, updatedBefore, updatedAfter)
	}
}
