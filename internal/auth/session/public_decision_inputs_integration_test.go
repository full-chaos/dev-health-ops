//go:build integration

package session

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/copyproject"
)

// publicOnlyUser seeds an active public.users row with NO auth copy.
func (e *env) publicOnlyUser(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users (id, email, username, full_name, is_active, is_verified, token_version)
VALUES ($1, $2, $3, 'Public Only', true, true, 4)`, id, "po-"+id.String()+"@example.test", "po-"+id.String()); err != nil {
		t.Fatalf("seed public.users: %v", err)
	}
	return id
}

// publicOnlyOrg seeds an active public.organizations row with NO auth copy.
func (e *env) publicOnlyOrg(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'public-only')`, id, "po-"+id.String()[:8]); err != nil {
		t.Fatalf("seed public.organizations: %v", err)
	}
	return id
}

func (e *env) copies(t *testing.T, ctx context.Context, user uuid.UUID, org *uuid.UUID) (principals, users, orgs int) {
	t.Helper()
	principals = e.count(t, ctx, `SELECT count(*) FROM auth.principals WHERE id = $1`, user.String())
	users = e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1`, user.String())
	if org != nil {
		orgs = e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1`, org.String())
	}
	return
}

// Every entry that starts a session goes through insertSessionAs, so each
// serves a public user and organization that have no auth copy, projecting
// both in the session's transaction, and the session carries the public
// token_version.
func TestIssueForAPublicUserAndOrganizationWithNoCopyProjectsThem(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	for name, issue := range map[string]func(user uuid.UUID, org *uuid.UUID) (Issued, error){
		"Create": func(user uuid.UUID, org *uuid.UUID) (Issued, error) {
			return e.store.Create(ctx, user, org, []string{"pwd"})
		},
		"CreateAudited": func(user uuid.UUID, org *uuid.UUID) (Issued, error) {
			return e.store.CreateAudited(ctx, user, org, []string{"pwd"}, signInEvent(user))
		},
		"CreateInTx": func(user uuid.UUID, org *uuid.UUID) (Issued, error) {
			var out Issued
			err := audit.Commit(ctx, e.runtime, e.schema, audit.Mutation{
				Apply: func(ctx context.Context, tx audit.TxOps) error {
					var err error
					out, err = e.store.CreateInTx(ctx, tx, user, org, []string{"pwd"})
					return err
				},
				Audit:     signInEvent(user),
				OffStream: audit.OffStreamSessionLifecycle,
			})
			return out, err
		},
	} {
		t.Run(name, func(t *testing.T) {
			user, org := e.publicOnlyUser(t, ctx), e.publicOnlyOrg(t, ctx)
			if p, u, o := e.copies(t, ctx, user, &org); p+u+o != 0 {
				t.Fatalf("seed has copies: %d %d %d", p, u, o)
			}
			issued, err := issue(user, &org)
			if err != nil {
				t.Fatalf("issue: %v", err)
			}
			if issued.Session.PrincipalID != user || issued.Session.OrganizationID == nil || *issued.Session.OrganizationID != org {
				t.Fatalf("session = %+v", issued.Session)
			}
			if issued.Session.LegacyTokenVersion == nil || *issued.Session.LegacyTokenVersion != 4 {
				t.Fatalf("pin = %v, want the public token_version 4", issued.Session.LegacyTokenVersion)
			}
			if p, u, o := e.copies(t, ctx, user, &org); p != 1 || u != 1 || o != 1 {
				t.Fatalf("copies after the issue = %d %d %d, want 1 1 1", p, u, o)
			}
			if n := e.count(t, ctx, `SELECT count(*) FROM auth.principals WHERE id = $1 AND disabled_at IS NULL`, user.String()); n != 1 {
				t.Fatalf("the projected copy of an active user is disabled")
			}
		})
	}
}

// public.users.is_active decides: an inactive public user is refused whatever
// its auth copy says, and a refusal projects nothing.
func TestIssueRefusesAnInactivePublicUserAndProjectsNothing(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	withCopy := e.principal(t, ctx)
	e.deactivate(t, ctx, withCopy)
	if _, err := e.store.Create(ctx, withCopy, nil, []string{"pwd"}); !errors.Is(err, ErrPrincipalUnavailable) {
		t.Fatalf("Create for an inactive public user with an active copy = %v, want ErrPrincipalUnavailable", err)
	}
	noCopy := e.publicOnlyUser(t, ctx)
	e.deactivate(t, ctx, noCopy)
	org := e.publicOnlyOrg(t, ctx)
	if _, err := e.store.Create(ctx, noCopy, &org, []string{"pwd"}); !errors.Is(err, ErrPrincipalUnavailable) {
		t.Fatalf("Create for an inactive public user with no copy = %v, want ErrPrincipalUnavailable", err)
	}
	if p, u, o := e.copies(t, ctx, noCopy, &org); p+u+o != 0 {
		t.Fatalf("a refused issue projected copies: %d %d %d", p, u, o)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id IN ($1, $2)`, withCopy.String(), noCopy.String()); n != 0 {
		t.Fatalf("%d sessions after the refusals", n)
	}
}

// A disabled auth copy of an active public user does not decide: the issue is
// served and the copy is left as it is.
func TestIssueIgnoresADisabledCopyOfAnActivePublicUser(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.principal(t, ctx)
	e.guarded(t, ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, user.String())
	if _, err := e.store.Create(ctx, user, nil, []string{"pwd"}); err != nil {
		t.Fatalf("Create with a disabled copy of an active public user = %v, want a session", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.principals WHERE id = $1 AND disabled_at IS NOT NULL`, user.String()); n != 1 {
		t.Fatal("the issue changed the copy")
	}
}

// An auth principal with no public.users row is refused: public is the authority.
func TestIssueRefusesAnAuthPrincipalWithNoPublicRow(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'auth-only')`, id.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.Create(ctx, id, nil, []string{"pwd"}); !errors.Is(err, ErrPrincipalUnavailable) {
		t.Fatalf("Create for an auth-only principal = %v, want ErrPrincipalUnavailable", err)
	}
}

// An organization public.organizations does not hold cannot be projected: the
// issue is refused with ErrOrganizationUnavailable and writes nothing.
func TestIssueRefusesAnOrganizationWithNoPublicRow(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.publicOnlyUser(t, ctx)
	org := uuid.New()
	if _, err := e.store.Create(ctx, user, &org, []string{"pwd"}); !errors.Is(err, ErrOrganizationUnavailable) {
		t.Fatalf("Create for a missing organization = %v, want ErrOrganizationUnavailable", err)
	}
	if p, u, o := e.copies(t, ctx, user, &org); p+u+o != 0 {
		t.Fatalf("a refused issue left copies: %d %d %d", p, u, o)
	}
}

// B1 does not read the organization's status: an inactive public organization
// does not refuse the issue (authorize decides it).
func TestIssueDoesNotRefuseAnInactivePublicOrganization(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.principal(t, ctx)
	org := e.publicOnlyOrg(t, ctx)
	if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET is_active = false WHERE id = $1`, org); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.Create(ctx, user, &org, []string{"pwd"}); err != nil {
		t.Fatalf("Create for an inactive public organization = %v, want a session", err)
	}
}

// The first sign-ins of one public user and organization, all at once: one
// copy of each, every issue served, no deadlock (the user is projected before
// the organization in every transaction).
func TestConcurrentFirstIssuesProjectOneCopy(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user, org := e.publicOnlyUser(t, ctx), e.publicOnlyOrg(t, ctx)
	const n = 8
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.store.Create(ctx, user, &org, []string{"pwd"})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent issue: %v", err)
		}
	}
	if p, u, o := e.copies(t, ctx, user, &org); p != 1 || u != 1 || o != 1 {
		t.Fatalf("copies = %d %d %d, want 1 1 1", p, u, o)
	}
	if got := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, user.String()); got != n {
		t.Fatalf("%d sessions, want %d", got, n)
	}
}

// An auth.users row that holds the public user's login key under another
// principal refuses the issue with a store error and writes nothing.
func TestIssueRefusesAnEmailCollisionAndWritesNothing(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.publicOnlyUser(t, ctx)
	var email string
	if err := e.admin.QueryRow(ctx, `SELECT email FROM public.users WHERE id = $1`, user).Scan(&email); err != nil {
		t.Fatal(err)
	}
	other := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'holder')`, other.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.users (principal_id, email, email_lower) VALUES ($1, $2, lower($2))`, other.String(), email); err != nil {
		t.Fatal(err)
	}
	// The collision itself, not a later statement of the aborted transaction, ends the issue.
	_, err := e.store.Create(ctx, user, nil, []string{"pwd"})
	if !errors.Is(err, ErrStore) || !errors.Is(err, copyproject.ErrEmailCollision) {
		t.Fatalf("Create with a colliding login key = %v, want a store error carrying copyproject.ErrEmailCollision", err)
	}
	if p, u, _ := e.copies(t, ctx, user, nil); p+u != 0 {
		t.Fatalf("a refused issue left copies: %d %d", p, u)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, user.String()); n != 0 {
		t.Fatalf("%d sessions", n)
	}
}

// The refresh exchange decides on public.users.is_active (B2): an inactive
// public user is refused with an active auth copy, a disabled copy of an
// active user is served, and a session whose user has no public row is refused.
func TestExchangeDecidesOnThePublicUser(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)

	off := e.principal(t, ctx)
	offSession := e.create(t, ctx, off)
	e.deactivate(t, ctx, off)
	if got := e.exchange(t, ctx, e.store, offSession.Credential); got.Reason != ReasonPrincipalDeactivated || got.Credential.Reveal() != "" {
		t.Fatalf("exchange for an inactive public user = %s, want principal_deactivated and no credential", got.Reason)
	}

	on := e.principal(t, ctx)
	onSession := e.create(t, ctx, on)
	e.guarded(t, ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, on.String())
	if got := e.exchange(t, ctx, e.store, onSession.Credential); got.Reason != ReasonAccepted {
		t.Fatalf("exchange for an active public user with a disabled copy = %s, want accepted", got.Reason)
	}

	gone := e.principal(t, ctx)
	goneSession := e.create(t, ctx, gone)
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.users WHERE id = $1`, gone.String()); err != nil {
		t.Fatal(err)
	}
	if got := e.exchange(t, ctx, e.store, goneSession.Credential); got.Reason != ReasonPrincipalDeactivated {
		t.Fatalf("exchange for a session whose user has no public row = %s, want principal_deactivated", got.Reason)
	}
}
