//go:build integration

package identity

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
)

var errAfterJoin = errors.New("identity test: the caller fails after the join")

// joinCommit runs insertJoinMembership in one audit.Commit, as a join caller
// does, then after (nil = none).
func joinCommit(ctx context.Context, s *Store, m membershipJoin, after func(context.Context, audit.TxOps) error) error {
	subject, org, target := m.Principal.String(), m.OrgID.String(), m.ID.String()
	return audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := s.insertJoinMembership(ctx, tx, m); err != nil {
				return err
			}
			if after != nil {
				return after(ctx, tx)
			}
			return nil
		},
		Audit: audit.AuditEvent{
			EventType: "auth.membership.added", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject, OrganizationID: &org,
			Attributes: map[string]any{"reason_code": "membership_added", "target_kind": "membership", "target_id": target},
		},
		Events: []audit.OutboxEvent{{
			AggregateType: "membership", AggregateID: provisioning.WireID("membership", target),
			EventType: "membership.activated", Change: map[string]any{"status": "active"},
		}},
	})
}

// joinFixture is a user principal with its public.users row and an
// organization in both schemas.
func (e *env) joinFixture(t *testing.T, ctx context.Context) (user, org uuid.UUID) {
	t.Helper()
	user = e.seedUser(t, ctx, uniqueEmail("join"), true, true)
	e.project(t, ctx, user)
	return user, e.seedAuthOwnedOrg(t, ctx)
}

func (e *env) memberRows(t *testing.T, ctx context.Context, user, org uuid.UUID) (public, auth int) {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM public.memberships WHERE user_id = $1 AND org_id = $2`, user, org),
		e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE principal_id = $1 AND organization_id = $2`, user, org)
}

func (e *env) requireNoMemberRows(t *testing.T, ctx context.Context, user, org uuid.UUID) {
	t.Helper()
	if p, a := e.memberRows(t, ctx, user, org); p != 0 || a != 0 {
		t.Fatalf("membership rows after a failed join: public=%d auth=%d, want 0 and 0", p, a)
	}
	if n := e.auditRows(t, ctx, "auth.membership.added", user.String()); n != 0 {
		t.Fatalf("audit rows after a failed join = %d, want 0", n)
	}
}

// A committed join is visible to go-api's membership reader at once: the
// public row carries the join's id, role and instant, as go-api's own JIT row.
func TestJoinMembershipVisibleToGoAPIReaderInSameCommit(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	user, org := e.joinFixture(t, ctx)
	m := membershipJoin{ID: uuid.New(), Principal: user, OrgID: org, Role: "admin"}
	if err := joinCommit(ctx, e.owned, m, nil); err != nil {
		t.Fatalf("join: %v", err)
	}
	// go-api's reader (internal/api/session/store.go membershipRoles), on its own connection.
	var role string
	if err := e.admin.QueryRow(ctx, `SELECT role FROM memberships WHERE user_id = $1::uuid AND org_id = $2::uuid`, user, org).Scan(&role); err != nil {
		t.Fatalf("go-api membership reader: %v", err)
	}
	if role != "admin" {
		t.Fatalf("go-api reader role = %q, want admin", role)
	}
	var pubID, authID uuid.UUID
	var authStatus, authRole string
	var sameJoined, publicOneInstant, authOneInstant, invitedNull bool
	if err := e.admin.QueryRow(ctx, `SELECT p.id, a.id, a.status, a.role, p.joined_at = a.joined_at,
	p.joined_at = p.created_at AND p.created_at = p.updated_at,
	a.joined_at = a.created_at AND a.created_at = a.updated_at, p.invited_by_id IS NULL
FROM public.memberships p JOIN auth.memberships a ON a.principal_id = p.user_id AND a.organization_id = p.org_id
WHERE p.user_id = $1 AND p.org_id = $2`, user, org).
		Scan(&pubID, &authID, &authStatus, &authRole, &sameJoined, &publicOneInstant, &authOneInstant, &invitedNull); err != nil {
		t.Fatalf("read both rows: %v", err)
	}
	if pubID != m.ID || authID != m.ID || authStatus != "active" || authRole != "admin" || !sameJoined || !publicOneInstant || !authOneInstant || !invitedNull {
		t.Fatalf("rows differ: public id %v auth id %v (want %v), auth %s/%s, same joined_at %v, public one instant %v, auth one instant %v, invited_by NULL %v",
			pubID, authID, m.ID, authStatus, authRole, sameJoined, publicOneInstant, authOneInstant, invitedNull)
	}
}

// A failure at the auth write (the organization is not in the auth schema)
// also removes the public row written before it.
func TestJoinMembershipAuthWriteFailureLeavesNoRows(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	user := e.seedUser(t, ctx, uniqueEmail("join-authfail"), true, true)
	e.project(t, ctx, user)
	org := e.seedOrg(t, ctx)
	err := joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: org, Role: "member"}, nil)
	if err == nil {
		t.Fatal("join onto an organization the auth schema does not hold succeeded")
	}
	e.requireNoMemberRows(t, ctx, user, org)
}

// A failure at the public write (the organization is not in public) fails the
// join closed with ErrLegacyProjection.
func TestJoinMembershipPublicWriteFailureLeavesNoRows(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	user, org := e.joinFixture(t, ctx)
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.organizations WHERE id = $1`, org); err != nil {
		t.Fatal(err)
	}
	// Put the public organization back, so no auth-only organization stays in the shared database.
	t.Cleanup(func() {
		if _, err := e.admin.Exec(context.Background(), `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'identity-test')`, org, "org-"+org.String()); err != nil {
			t.Errorf("restore the public organization: %v", err)
		}
	})
	err := joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: org, Role: "member"}, nil)
	if !errors.Is(err, ErrLegacyProjection) {
		t.Fatalf("join with no public organization = %v, want ErrLegacyProjection", err)
	}
	e.requireNoMemberRows(t, ctx, user, org)
}

// An account with no public.users row is not joined.
func TestJoinMembershipNoPublicUserLeavesNoRows(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	email := uniqueEmail("join-nouser")
	user := e.seedUser(t, ctx, email, true, true)
	e.project(t, ctx, user)
	org := e.seedAuthOwnedOrg(t, ctx)
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.users WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	// Put the public user back (as seedUser wrote it), so no auth user without a public row stays in the shared database.
	t.Cleanup(func() {
		if _, err := e.admin.Exec(context.Background(), `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, '$2b$12$abcdefghijklmnopqrstuuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0', 'local', true, true, false, 0, now(), now())`, user, email); err != nil {
			t.Errorf("restore the public user: %v", err)
		}
	})
	err := joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: org, Role: "member"}, nil)
	if !errors.Is(err, ErrLegacyProjection) {
		t.Fatalf("join with no public user = %v, want ErrLegacyProjection", err)
	}
	e.requireNoMemberRows(t, ctx, user, org)
}

// A failure of the caller after both writes removes both rows.
func TestJoinMembershipFailureAfterBothWritesLeavesNoRows(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	user, org := e.joinFixture(t, ctx)
	var seenPublic, seenAuth int
	err := joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: org, Role: "member"},
		func(ctx context.Context, tx audit.TxOps) error {
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM public.memberships WHERE user_id = $1 AND org_id = $2`, user, org).Scan(&seenPublic); err != nil {
				return err
			}
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM auth.memberships WHERE principal_id = $1 AND organization_id = $2`, user, org).Scan(&seenAuth); err != nil {
				return err
			}
			return errAfterJoin
		})
	if !errors.Is(err, errAfterJoin) {
		t.Fatalf("join = %v, want the caller's error", err)
	}
	if seenPublic != 1 || seenAuth != 1 {
		t.Fatalf("inside the join: public=%d auth=%d, want 1 and 1", seenPublic, seenAuth)
	}
	e.requireNoMemberRows(t, ctx, user, org)
}

// A go-api membership in the organization makes the join a no-op: no auth row,
// no second public row.
func TestJoinMembershipPublicRowExistsWritesNothing(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	user, org := e.joinFixture(t, ctx)
	e.seedMembership(t, ctx, user, org)
	err := joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: org, Role: "admin"}, nil)
	if !errors.Is(err, errJoinAlreadyMember) {
		t.Fatalf("join of a public member = %v, want errJoinAlreadyMember", err)
	}
	if p, a := e.memberRows(t, ctx, user, org); p != 1 || a != 0 {
		t.Fatalf("rows: public=%d auth=%d, want 1 and 0", p, a)
	}
	var role string
	if err := e.admin.QueryRow(ctx, `SELECT role FROM public.memberships WHERE user_id = $1 AND org_id = $2`, user, org).Scan(&role); err != nil || role != "member" {
		t.Fatalf("public role = %q, %v; want the go-api row unchanged (member)", role, err)
	}
}

// An auth membership with no public row (go-api removed the member) is not
// re-added: ErrMembershipDiverged, nothing written.
func TestJoinMembershipDivergedAuthRowRefused(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	user, org := e.joinFixture(t, ctx)
	if err := joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: org, Role: "member"}, nil); err != nil {
		t.Fatalf("first join: %v", err)
	}
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.memberships WHERE user_id = $1 AND org_id = $2`, user, org); err != nil {
		t.Fatal(err)
	}
	err := joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: org, Role: "member"}, nil)
	if !errors.Is(err, ErrMembershipDiverged) {
		t.Fatalf("join over an auth-only membership = %v, want ErrMembershipDiverged", err)
	}
	if p, a := e.memberRows(t, ctx, user, org); p != 0 || a != 1 {
		t.Fatalf("rows: public=%d auth=%d, want 0 and 1", p, a)
	}
	// Match the pair again, so no auth-only membership stays in the shared database.
	e.seedMembership(t, ctx, user, org)
}

// Concurrent joins of one account to one organization write one row per table.
func TestJoinMembershipConcurrentJoinsWriteOneRowEach(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	user, org := e.joinFixture(t, ctx)
	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: org, Role: "member"}, nil)
		}()
	}
	wg.Wait()
	joined := 0
	for _, err := range errs {
		switch {
		case err == nil:
			joined++
		case errors.Is(err, errJoinAlreadyMember):
		default:
			t.Fatalf("concurrent join: %v", err)
		}
	}
	if joined != 1 {
		t.Fatalf("%d joins succeeded, want 1", joined)
	}
	if p, a := e.memberRows(t, ctx, user, org); p != 1 || a != 1 {
		t.Fatalf("rows: public=%d auth=%d, want 1 and 1", p, a)
	}
}

// The join waits for the public.users row lock go-api's own JIT join takes,
// and a go-api join committed under that lock makes it a no-op.
func TestJoinMembershipWaitsForGoAPIUserLock(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	user, org := e.joinFixture(t, ctx)
	goAPI, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = goAPI.Rollback(context.Background()) }()
	var locked uuid.UUID
	if err := goAPI.QueryRow(ctx, `SELECT id FROM users WHERE id = $1::uuid FOR UPDATE`, user).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: org, Role: "member"}, nil)
	}()
	role := e.runtime.Config().ConnConfig.User
	for waiting := 0; waiting == 0; {
		select {
		case err := <-done:
			t.Fatalf("the join returned (%v) while go-api held the public.users row lock", err)
		case <-ctx.Done():
			t.Fatal("the join never waited on the public.users row lock")
		default:
		}
		waiting = e.count(t, ctx, `SELECT count(*) FROM pg_stat_activity
WHERE usename = $1 AND wait_event_type = 'Lock' AND query LIKE '%FROM public.users WHERE id = $1 FOR UPDATE%'`, role)
		if waiting == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if _, err := goAPI.Exec(ctx, `INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'viewer', now(), now(), now())`, uuid.New(), org, user); err != nil {
		t.Fatal(err)
	}
	if err := goAPI.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, errJoinAlreadyMember) {
		t.Fatalf("join after a go-api join = %v, want errJoinAlreadyMember", err)
	}
	if p, a := e.memberRows(t, ctx, user, org); p != 1 || a != 0 {
		t.Fatalf("rows: public=%d auth=%d, want 1 and 0", p, a)
	}
}

// An open go-api admin add holds the public.users row (its foreign key check)
// so the join waits on the user lock; once the add commits the join is a no-op.
func TestJoinMembershipWaitsForGoAPIAdminAdd(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	user, org := e.joinFixture(t, ctx)
	adminAdd, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adminAdd.Rollback(context.Background()) }()
	if _, err := adminAdd.Exec(ctx, `INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), org, user); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: org, Role: "admin"}, nil)
	}()
	role := e.runtime.Config().ConnConfig.User
	for waiting := 0; waiting == 0; {
		select {
		case err := <-done:
			t.Fatalf("the join returned (%v) while the go-api add was open", err)
		case <-ctx.Done():
			t.Fatal("the join never waited on the public.users row lock")
		default:
		}
		waiting = e.count(t, ctx, `SELECT count(*) FROM pg_stat_activity
WHERE usename = $1 AND wait_event_type = 'Lock' AND query LIKE '%FROM public.users WHERE id = $1 FOR UPDATE%'`, role)
		if waiting == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if err := adminAdd.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, errJoinAlreadyMember) {
		t.Fatalf("join racing a go-api add = %v, want errJoinAlreadyMember", err)
	}
	if p, a := e.memberRows(t, ctx, user, org); p != 1 || a != 0 {
		t.Fatalf("rows: public=%d auth=%d, want 1 and 0", p, a)
	}
}

// Before the users cutover the join writes nothing (the public insert grant
// does not exist then).
func TestJoinMembershipRefusedWithoutUsersOwned(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	user, org := e.joinFixture(t, ctx)
	err := joinCommit(ctx, e.store, membershipJoin{ID: uuid.New(), Principal: user, OrgID: org, Role: "member"}, nil)
	if !errors.Is(err, ErrSignupNotAvailable) {
		t.Fatalf("join before the cutover = %v, want ErrSignupNotAvailable", err)
	}
	e.requireNoMemberRows(t, ctx, user, org)
}

// A join in a transaction that is not READ COMMITTED is refused before any
// lock or write: its existence check after the user lock would read a snapshot
// older than a go-api join that committed while it waited.
func TestJoinMembershipRefusedOutsideReadCommitted(t *testing.T) {
	e := getEnv(t)
	for _, level := range []string{"repeatable read", "serializable"} {
		t.Run(level, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			cfg := e.runtime.Config().Copy()
			cfg.MaxConns = 2
			cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = level
			pool, err := pgxpool.NewWithConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			s, err := New(Config{Pool: pool, Schema: e.store.schema, UsersOwnedByAuth: true})
			if err != nil {
				t.Fatal(err)
			}
			user, org := e.joinFixture(t, ctx)
			err = joinCommit(ctx, s, membershipJoin{ID: uuid.New(), Principal: user, OrgID: org, Role: "member"}, nil)
			if !errors.Is(err, ErrJoinNotReadCommitted) {
				t.Fatalf("join under %s = %v, want ErrJoinNotReadCommitted", level, err)
			}
			e.requireNoMemberRows(t, ctx, user, org)
		})
	}
}

// A role outside the organization roles writes nothing.
func TestJoinMembershipRefusesUnknownRole(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	user, org := e.joinFixture(t, ctx)
	err := joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: org, Role: "superuser"}, nil)
	if !errors.Is(err, ErrBackfillRole) {
		t.Fatalf("join with an unknown role = %v, want ErrBackfillRole", err)
	}
	e.requireNoMemberRows(t, ctx, user, org)
}

// The join writes only in its organization.
func TestJoinMembershipOnlyInTheJoinOrg(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	user, org := e.joinFixture(t, ctx)
	other := e.seedAuthOwnedOrg(t, ctx)
	if err := joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: org, Role: "member"}, nil); err != nil {
		t.Fatalf("join: %v", err)
	}
	if p, a := e.memberRows(t, ctx, user, other); p != 0 || a != 0 {
		t.Fatalf("rows in the other organization: public=%d auth=%d, want 0 and 0", p, a)
	}
	pub := e.count(t, ctx, `SELECT count(*) FROM public.memberships WHERE user_id = $1`, user)
	auth := e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE principal_id = $1`, user)
	if pub != 1 || auth != 1 {
		t.Fatalf("rows of the account: public=%d auth=%d, want 1 and 1", pub, auth)
	}
}

// The organization projection is its own commit before the join's, as at every
// sign-in: a refused join leaves that row and no membership.
func TestRefusedSignInJoinLeavesOnlyTheProjection(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	user, elsewhere := e.joinFixture(t, ctx)
	if err := joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: elsewhere, Role: "member"}, nil); err != nil {
		t.Fatalf("seed the other membership: %v", err)
	}
	org := e.seedOrg(t, ctx)
	err := e.owned.JoinOnSignIn(ctx, user, SSOJoin{OrgID: org, Role: "member"}, SelfSignUp(nil), nil)
	if !errors.Is(err, ErrMemberElsewhere) {
		t.Fatalf("join = %v, want ErrMemberElsewhere", err)
	}
	var projected int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1`, org).Scan(&projected); err != nil {
		t.Fatal(err)
	}
	if projected != 1 {
		t.Fatalf("%d projected organization rows, want 1", projected)
	}
	if p, a := e.memberRows(t, ctx, user, org); p != 0 || a != 0 {
		t.Fatalf("membership rows after a refused join: public=%d auth=%d, want 0 and 0", p, a)
	}
}
