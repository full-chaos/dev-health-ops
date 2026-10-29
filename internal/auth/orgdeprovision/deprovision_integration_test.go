//go:build integration

package orgdeprovision

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/invite"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/usersbackfill"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

type env struct {
	admin    *pgxpool.Pool
	runtime  *pgxpool.Pool
	orgs     *org.Store
	members  *membership.Store
	invites  *invite.Store
	identity *identity.Store
	service  *Service
	notOwned *Service
	secret   string
}

var (
	shared     *env
	sharedErr  error
	sharedOnce sync.Once
	closers    []func()
)

func TestMain(m *testing.M) {
	code := m.Run()
	for i := len(closers) - 1; i >= 0; i-- {
		closers[i]()
	}
	os.Exit(code)
}

func getEnv(t *testing.T) *env {
	t.Helper()
	sharedOnce.Do(func() { shared, sharedErr = newEnv(t) })
	if sharedErr != nil {
		t.Fatal(sharedErr)
	}
	return shared
}

func newEnv(t *testing.T) (*env, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		return nil, err
	}
	closers = append(closers, func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(closeCtx)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		return nil, fmt.Errorf("admin pool: %w", err)
	}
	closers = append(closers, admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("deprovision_runtime", instance)
	if err != nil {
		return nil, fmt.Errorf("derive role: %w", err)
	}
	password := "deprovision-test-" + uuid.NewString()
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		return nil, fmt.Errorf("create role: %w", err)
	}
	closers = append(closers, func() { containers.DropRole(admin, role, func(string, ...any) {}) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
		return nil, fmt.Errorf("authschema.Apply: %w", err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("runtime pool: %w", err)
	}
	closers = append(closers, runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		return nil, err
	}
	e := &env{admin: admin, runtime: runtime, secret: "deprovision-secret-" + uuid.NewString()}
	if e.orgs, err = org.New(org.Config{Pool: runtime, Schema: schema}); err != nil {
		return nil, err
	}
	if e.members, err = membership.New(membership.Config{Pool: runtime, Schema: schema, OwnsUsers: true}); err != nil {
		return nil, err
	}
	if e.invites, err = invite.New(invite.Config{Pool: runtime, Schema: schema, OwnsUsers: true, Memberships: e.members,
		Secret: e.secret, AcceptSecrets: []string{e.secret}}); err != nil {
		return nil, err
	}
	if e.identity, err = identity.New(identity.Config{Pool: runtime, Schema: schema, UsersOwnedByAuth: true}); err != nil {
		return nil, err
	}
	if e.service, err = New(Config{Pool: runtime, Schema: schema, Orgs: e.orgs, Memberships: e.members, Invites: e.invites}); err != nil {
		return nil, err
	}
	notOwnedMembers, err := membership.New(membership.Config{Pool: runtime, Schema: schema})
	if err != nil {
		return nil, err
	}
	notOwnedInvites, err := invite.New(invite.Config{Pool: runtime, Schema: schema, Memberships: notOwnedMembers,
		Secret: e.secret, AcceptSecrets: []string{e.secret}})
	if err != nil {
		return nil, err
	}
	if e.notOwned, err = New(Config{Pool: runtime, Schema: schema, Orgs: e.orgs, Memberships: notOwnedMembers, Invites: notOwnedInvites}); err != nil {
		return nil, err
	}
	return e, nil
}

func allowMembers(context.Context, audit.TxOps, membership.Locked) error { return nil }
func allowOrg(context.Context, audit.TxOps, uuid.UUID) error             { return nil }
func denyOrg(context.Context, audit.TxOps, uuid.UUID) error              { return ErrDenied }

func (e *env) seedUser(t *testing.T, ctx context.Context) (uuid.UUID, string) {
	t.Helper()
	id := uuid.New()
	email := "member-" + id.String()[:8] + "@example.com"
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, false, 0, now(), now())`, id, email); err != nil {
		t.Fatalf("seed public.users: %v", err)
	}
	if err := e.identity.Project(ctx, id); err != nil {
		t.Fatalf("Project: %v", err)
	}
	return id, email
}

func (e *env) seedOrg(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'deprovision-test')`,
		id, "org-"+id.String()[:8]); err != nil {
		t.Fatalf("seed public.organizations: %v", err)
	}
	if err := e.identity.ProjectOrganization(ctx, id); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	return id
}

func (e *env) add(t *testing.T, ctx context.Context, orgID, user uuid.UUID, role string) *membership.Membership {
	t.Helper()
	m, err := e.members.Add(ctx, membership.Change{OrgID: orgID, PrincipalID: user, Role: role, Authorize: allowMembers})
	if err != nil {
		t.Fatalf("Add %s: %v", role, err)
	}
	return m
}

func (e *env) invite(t *testing.T, ctx context.Context, orgID, inviter uuid.UUID, email string) *invite.Created {
	t.Helper()
	c, err := e.invites.Create(ctx, invite.CreateInput{OrgID: orgID, Email: email, Role: "member", InvitedBy: inviter, Authorize: allowOrg,
		PublicAudit: &invite.PublicAudit{}})
	if err != nil {
		t.Fatalf("invite Create: %v", err)
	}
	return c
}

func (e *env) count(t *testing.T, ctx context.Context, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

// live is what an organization still has: public membership rows, auth
// membership rows not removed, pending invites in either table, and whether
// its auth row is active.
type live struct {
	publicMembers, authMembers, authPending, publicPending int
	active                                                 bool
}

func (e *env) live(t *testing.T, ctx context.Context, orgID uuid.UUID) live {
	t.Helper()
	l := live{
		publicMembers: e.count(t, ctx, `SELECT count(*) FROM public.memberships WHERE org_id = $1`, orgID),
		authMembers:   e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND status <> 'removed'`, orgID),
		authPending:   e.count(t, ctx, `SELECT count(*) FROM auth.invites WHERE organization_id = $1 AND status = 'pending'`, orgID),
		publicPending: e.count(t, ctx, `SELECT count(*) FROM public.org_invites WHERE org_id = $1 AND status = 'pending'`, orgID),
	}
	l.active = e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NULL`, orgID) == 1
	return l
}

// requireReconciled reads the users-reconcile membership counts (CHAOS-10783)
// over the pairs of orgs only, so rows other tests leave cannot make it pass or
// fail: every pair these organizations hold agrees in both tables.
func (e *env) requireReconciled(t *testing.T, ctx context.Context, orgs ...uuid.UUID) {
	t.Helper()
	if len(orgs) == 0 {
		t.Fatal("requireReconciled needs the organizations to read")
	}
	tx, err := usersbackfill.OpenSnapshot(ctx, e.admin)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	for _, org := range orgs {
		d, err := usersbackfill.TxTarget{Tx: tx, Schema: schema}.MembershipDiffsOf(ctx, org)
		if err != nil {
			t.Fatal(err)
		}
		if d != (usersbackfill.MembershipDiffs{}) {
			t.Fatalf("reconcile of %v: %+v, want all 0", org, d)
		}
	}
}

// populated is an organization with every kind of membership and invite.
type populated struct {
	org                                    uuid.UUID
	owner, admin, member, suspended, goAPI uuid.UUID
	pendingAuth, pendingPublic, accepted   uuid.UUID
	members, invites                       int
	acceptedInviteAccepted, expiredRevoked bool
}

func (e *env) populate(t *testing.T, ctx context.Context) populated {
	t.Helper()
	p := populated{org: e.seedOrg(t, ctx)}
	p.owner, _ = e.seedUser(t, ctx)
	p.admin, _ = e.seedUser(t, ctx)
	p.member, _ = e.seedUser(t, ctx)
	p.suspended, _ = e.seedUser(t, ctx)
	p.goAPI, _ = e.seedUser(t, ctx)
	e.add(t, ctx, p.org, p.owner, "owner")
	e.add(t, ctx, p.org, p.admin, "admin")
	e.add(t, ctx, p.org, p.member, "member")
	e.add(t, ctx, p.org, p.suspended, "viewer")
	if _, err := e.members.Suspend(ctx, membership.Change{OrgID: p.org, PrincipalID: p.suspended, Authorize: allowMembers}); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	// A membership go-api wrote before auth-service owned users: a public row only.
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at, created_at, updated_at)
VALUES (gen_random_uuid(), $1, $2, 'member', now(), now(), now())`, p.goAPI, p.org); err != nil {
		t.Fatal(err)
	}
	p.members = 5
	p.pendingAuth = e.invite(t, ctx, p.org, p.owner, "pending-"+uuid.NewString()[:8]+"@example.com").ID
	// An invite go-api made before: a public row only.
	p.pendingPublic = uuid.New()
	_, digest := signedtoken.Build(p.pendingPublic, e.secret)
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.org_invites (id, org_id, email, role, token_hash, invited_by_id, status, expires_at, created_at, updated_at)
VALUES ($1, $2, $3, 'viewer', $4, $5, 'pending', now() + interval '1 day', now(), now())`,
		p.pendingPublic, p.org, "public-"+uuid.NewString()[:8]+"@example.com", digest, p.owner); err != nil {
		t.Fatal(err)
	}
	joiner, email := e.seedUser(t, ctx)
	accepted := e.invite(t, ctx, p.org, p.owner, email)
	if _, err := e.invites.Accept(ctx, invite.AcceptInput{Token: accepted.Token, PrincipalID: joiner, PublicAudit: &invite.PublicAudit{}}); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	p.accepted = accepted.ID
	p.members++
	p.invites = 2
	if got := e.live(t, ctx, p.org); got != (live{publicMembers: 5, authMembers: 5, authPending: 1, publicPending: 2, active: true}) {
		t.Fatalf("populated organization = %+v", got)
	}
	// The go-api-only rows are a reconcile class: a test that leaves the
	// organization live ends it, so the whole-database reconcile of another
	// test reads only its own rows.
	t.Cleanup(func() {
		if _, err := e.service.Deprovision(context.Background(), Input{OrgID: p.org, Authorize: allowOrg}); err != nil {
			t.Errorf("cleanup deprovision: %v", err)
		}
	})
	return p
}

// One commit ends everything: every pending invite revoked in both tables
// (a go-api-only one taken over), every live membership removed in both
// tables, the organization suspended, with one audit row and one event per
// change; an accepted invite stays accepted, and the tables reconcile.
func TestDeprovisionEndsEverythingInOneCommit(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	p := e.populate(t, ctx)
	actor, _ := e.seedUser(t, ctx)
	res, err := e.service.Deprovision(ctx, Input{OrgID: p.org, Actor: Actor{PrincipalID: &actor}, Authorize: allowOrg})
	if err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	if *res != (Result{RemovedMemberships: p.members, RevokedInvites: p.invites, Suspended: true}) {
		t.Fatalf("result = %+v, want %d removed, %d revoked, suspended", res, p.members, p.invites)
	}
	if got := e.live(t, ctx, p.org); got != (live{}) {
		t.Fatalf("after the deprovision = %+v, want nothing live", got)
	}
	for _, id := range []uuid.UUID{p.pendingAuth, p.pendingPublic} {
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.invites a JOIN public.org_invites p USING (id)
WHERE a.id = $1 AND a.status = 'revoked' AND p.status = 'revoked'`, id); n != 1 {
			t.Fatalf("invite %v is not revoked in both tables", id)
		}
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.invites a JOIN public.org_invites p USING (id)
WHERE a.id = $1 AND a.status = 'accepted' AND p.status = 'accepted'`, p.accepted); n != 1 {
		t.Fatal("the accepted invite changed")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND organization_id = $2
AND actor_principal_id = $3 AND attributes->>'new_status' = 'suspended'`, EventDeprovisioned, p.org.String(), actor.String()); n != 1 {
		t.Fatalf("deprovision audit rows = %d, want 1", n)
	}
	for event, want := range map[string]int{"membership.removed": p.members, "invite.revoked": p.invites, "organization.suspended": 1} {
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = $1
AND payload->>'organization_id' = $2`, event, "org_"+p.org.String()); n != want {
			t.Fatalf("%s events = %d, want %d", event, n, want)
		}
	}
	e.requireReconciled(t, ctx, p.org)

	// A repeat is a no-op: nothing written.
	again, err := e.service.Deprovision(ctx, Input{OrgID: p.org, Actor: Actor{PrincipalID: &actor}, Authorize: allowOrg})
	if err != nil || *again != (Result{}) {
		t.Fatalf("repeat = %+v, %v; want a no-op", again, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND organization_id = $2`,
		EventDeprovisioned, p.org.String()); n != 1 {
		t.Fatalf("a repeat wrote an audit row (%d rows)", n)
	}
}

// A failure anywhere in the commit leaves everything as it was.
func TestDeprovisionFailureRollsBackEverything(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	p := e.populate(t, ctx)
	before := e.live(t, ctx, p.org)
	fn := "deprovision_fail_" + uuid.NewString()[:8]
	if _, err := e.admin.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION public.%s() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF NEW.org_id = '%s' THEN RAISE EXCEPTION 'planted failure'; END IF; RETURN NEW; END $$`, fn, p.org)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, fmt.Sprintf(`CREATE TRIGGER %s BEFORE UPDATE ON public.org_invites FOR EACH ROW EXECUTE FUNCTION public.%s()`, fn, fn)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = e.admin.Exec(context.Background(), fmt.Sprintf(`DROP TRIGGER %s ON public.org_invites`, fn))
		_, _ = e.admin.Exec(context.Background(), fmt.Sprintf(`DROP FUNCTION public.%s()`, fn))
	})
	_, err := e.service.Deprovision(ctx, Input{OrgID: p.org, Authorize: allowOrg})
	if !errors.Is(err, ErrStore) {
		t.Fatalf("Deprovision with a planted failure = %v, want ErrStore", err)
	}
	if got := e.live(t, ctx, p.org); got != before {
		t.Fatalf("after a failed deprovision = %+v, want %+v", got, before)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND organization_id = $2`,
		EventDeprovisioned, p.org.String()); n != 0 {
		t.Fatalf("a failed deprovision wrote %d audit rows", n)
	}
}

// Authorize decides first: a refused actor gets ErrDenied, for a missing
// organization too, and nothing changes; users not owned refuses.
func TestDeprovisionRefusals(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	p := e.populate(t, ctx)
	before := e.live(t, ctx, p.org)
	for name, auth := range map[string]Authorize{"nil": nil, "deny": denyOrg} {
		if _, err := e.service.Deprovision(ctx, Input{OrgID: p.org, Authorize: auth}); !errors.Is(err, ErrDenied) {
			t.Fatalf("%s Authorize = %v, want ErrDenied", name, err)
		}
		if _, err := e.service.Deprovision(ctx, Input{OrgID: uuid.New(), Authorize: auth}); !errors.Is(err, ErrDenied) {
			t.Fatalf("%s Authorize, missing organization = %v, want ErrDenied", name, err)
		}
	}
	if _, err := e.service.Deprovision(ctx, Input{OrgID: uuid.New(), Authorize: allowOrg}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing organization = %v, want ErrNotFound", err)
	}
	if _, err := e.notOwned.Deprovision(ctx, Input{OrgID: p.org, Authorize: allowOrg}); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("users not owned = %v, want ErrNotOwned", err)
	}
	if got := e.live(t, ctx, p.org); got != before {
		t.Fatalf("after refusals = %+v, want %+v", got, before)
	}
}

// After a deprovision the membership and invite verbs refuse: an add, an
// invite create, and an accept of a token minted before.
func TestVerbsRefuseOnADeprovisionedOrganization(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	orgID := e.seedOrg(t, ctx)
	owner, _ := e.seedUser(t, ctx)
	e.add(t, ctx, orgID, owner, "owner")
	joiner, email := e.seedUser(t, ctx)
	minted := e.invite(t, ctx, orgID, owner, email)
	if _, err := e.service.Deprovision(ctx, Input{OrgID: orgID, Authorize: allowOrg}); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	later, _ := e.seedUser(t, ctx)
	if _, err := e.members.Add(ctx, membership.Change{OrgID: orgID, PrincipalID: later, Role: "member", Authorize: allowMembers}); !errors.Is(err, membership.ErrOrgSuspended) {
		t.Fatalf("Add after deprovision = %v, want ErrOrgSuspended", err)
	}
	if _, err := e.invites.Create(ctx, invite.CreateInput{OrgID: orgID, Email: "late-" + uuid.NewString()[:8] + "@example.com", Role: "member",
		InvitedBy: owner, Authorize: allowOrg, PublicAudit: &invite.PublicAudit{}}); !errors.Is(err, invite.ErrOrgSuspended) {
		t.Fatalf("Create after deprovision = %v, want ErrOrgSuspended", err)
	}
	if _, err := e.invites.Accept(ctx, invite.AcceptInput{Token: minted.Token, PrincipalID: joiner, PublicAudit: &invite.PublicAudit{}}); !errors.Is(err, invite.ErrInviteInvalid) {
		t.Fatalf("Accept after deprovision = %v, want ErrInviteInvalid", err)
	}
	// A pending invite of a suspended organization is unusable too.
	other := e.seedOrg(t, ctx)
	e.add(t, ctx, other, owner, "owner")
	second, address := e.seedUser(t, ctx)
	pending := e.invite(t, ctx, other, owner, address)
	if err := e.orgs.Suspend(ctx, other, nil, org.Actor{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.invites.Accept(ctx, invite.AcceptInput{Token: pending.Token, PrincipalID: second, PublicAudit: &invite.PublicAudit{}}); !errors.Is(err, invite.ErrInviteInvalid) {
		t.Fatalf("Accept in a suspended organization = %v, want ErrInviteInvalid", err)
	}
	if got := e.live(t, ctx, orgID); got != (live{}) {
		t.Fatalf("deprovisioned organization after the refused verbs = %+v, want nothing live", got)
	}
}

func isDeadlock(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40P01"
}

// A deprovision and an accept of one invite, started together: no deadlock,
// and one order wins cleanly. Accept first: the membership is made, then
// removed by the deprovision, and the invite stays accepted. Deprovision
// first: the invite is revoked and the accept refused. Either way nothing is
// live after.
func TestDeprovisionAgainstAConcurrentAccept(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	wins := map[string]int{}
	var orgs []uuid.UUID
	for round := range 20 {
		orgID := e.seedOrg(t, ctx)
		orgs = append(orgs, orgID)
		owner, _ := e.seedUser(t, ctx)
		e.add(t, ctx, orgID, owner, "owner")
		joiner, email := e.seedUser(t, ctx)
		minted := e.invite(t, ctx, orgID, owner, email)
		acceptErr, depErr := make(chan error, 1), make(chan error, 1)
		var res *Result
		go func() {
			_, err := e.invites.Accept(ctx, invite.AcceptInput{Token: minted.Token, PrincipalID: joiner, PublicAudit: &invite.PublicAudit{}})
			acceptErr <- err
		}()
		go func() {
			var err error
			res, err = e.service.Deprovision(ctx, Input{OrgID: orgID, Authorize: allowOrg})
			depErr <- err
		}()
		a, d := <-acceptErr, <-depErr
		if isDeadlock(a) || isDeadlock(d) {
			t.Fatalf("round %d: deadlock: accept %v, deprovision %v", round, a, d)
		}
		if d != nil {
			t.Fatalf("round %d: deprovision: %v", round, d)
		}
		var status string
		if err := e.admin.QueryRow(ctx, `SELECT a.status FROM auth.invites a JOIN public.org_invites p USING (id)
WHERE a.id = $1 AND a.status = p.status`, minted.ID).Scan(&status); err != nil {
			t.Fatalf("round %d: invite rows disagree: %v", round, err)
		}
		switch {
		case a == nil:
			wins["accept"]++
			if status != "accepted" || res.RemovedMemberships != 2 || res.RevokedInvites != 0 {
				t.Fatalf("round %d: accept first: invite %s, result %+v", round, status, res)
			}
		case errors.Is(a, invite.ErrInviteInvalid):
			wins["deprovision"]++
			if status != "revoked" || res.RemovedMemberships != 1 || res.RevokedInvites != 1 {
				t.Fatalf("round %d: deprovision first: invite %s, result %+v", round, status, res)
			}
		default:
			t.Fatalf("round %d: accept = %v, want success or ErrInviteInvalid", round, a)
		}
		if got := e.live(t, ctx, orgID); got != (live{}) {
			t.Fatalf("round %d: after both = %+v, want nothing live", round, got)
		}
	}
	t.Logf("first: %v", wins)
	e.requireReconciled(t, ctx, orgs...)
}

// deprovisionRetried is a deprovision that is run again after ErrStateMoved,
// as go-api does on the 409.
func (e *env) deprovisionRetried(ctx context.Context, orgID uuid.UUID) error {
	_, err := e.service.Deprovision(ctx, Input{OrgID: orgID, Authorize: allowOrg})
	if errors.Is(err, ErrStateMoved) {
		_, err = e.service.Deprovision(ctx, Input{OrgID: orgID, Authorize: allowOrg})
	}
	return err
}

// A deprovision against membership verbs and an invite create on the same
// organization, started together: no deadlock, and nothing is live after all
// of them. One member is go-api's (a public row only), so the deprovision
// inserts its auth row, naming a principal a concurrent remove holds FOR
// UPDATE.
func TestDeprovisionAgainstConcurrentMembershipVerbs(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var orgs []uuid.UUID
	for round := range 10 {
		orgID := e.seedOrg(t, ctx)
		orgs = append(orgs, orgID)
		owner, _ := e.seedUser(t, ctx)
		e.add(t, ctx, orgID, owner, "owner")
		member, _ := e.seedUser(t, ctx)
		e.add(t, ctx, orgID, member, "member")
		goAPI, _ := e.seedUser(t, ctx)
		if _, err := e.admin.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at, created_at, updated_at)
VALUES (gen_random_uuid(), $1, $2, 'member', now(), now(), now())`, goAPI, orgID); err != nil {
			t.Fatal(err)
		}
		newcomer, _ := e.seedUser(t, ctx)
		verbs := []func() error{
			func() error {
				_, err := e.members.Add(ctx, membership.Change{OrgID: orgID, PrincipalID: newcomer, Role: "viewer", Authorize: allowMembers})
				return err
			},
			func() error {
				_, err := e.members.ChangeRole(ctx, membership.Change{OrgID: orgID, PrincipalID: member, Role: "admin", Authorize: allowMembers})
				return err
			},
			func() error {
				_, err := e.members.Remove(ctx, membership.Change{OrgID: orgID, PrincipalID: goAPI, Authorize: allowMembers})
				return err
			},
			func() error {
				_, err := e.invites.Create(ctx, invite.CreateInput{OrgID: orgID, Email: "race-" + uuid.NewString()[:8] + "@example.com",
					Role: "member", InvitedBy: owner, Authorize: allowOrg, PublicAudit: &invite.PublicAudit{}})
				return err
			},
			func() error { return e.deprovisionRetried(ctx, orgID) },
		}
		errs := make(chan error, len(verbs))
		for _, verb := range verbs {
			go func() { errs <- verb() }()
		}
		for range verbs {
			err := <-errs
			if isDeadlock(err) {
				t.Fatalf("round %d: deadlock: %v", round, err)
			}
			if err != nil && !errors.Is(err, membership.ErrOrgSuspended) && !errors.Is(err, invite.ErrOrgSuspended) &&
				!errors.Is(err, membership.ErrStateMoved) {
				t.Fatalf("round %d: %v", round, err)
			}
		}
		if got := e.live(t, ctx, orgID); got != (live{}) {
			t.Fatalf("round %d: after all = %+v, want nothing live", round, got)
		}
	}
	e.requireReconciled(t, ctx, orgs...)
}

// The deprovision takes no public.users row: it completes while go-api holds
// a member's users row (its guard lock).
func TestDeprovisionTakesNoUsersLock(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p := e.populate(t, ctx)
	goAPI, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = goAPI.Rollback(context.Background()) }()
	var locked uuid.UUID
	for _, user := range []uuid.UUID{p.member, p.goAPI} {
		if err := goAPI.QueryRow(ctx, `SELECT id FROM public.users WHERE id = $1 FOR UPDATE`, user).Scan(&locked); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() {
		_, err := e.service.Deprovision(ctx, Input{OrgID: p.org, Authorize: allowOrg})
		done <- err
	}()
	role := e.runtime.Config().ConnConfig.User
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Deprovision: %v", err)
			}
			return
		case <-ctx.Done():
			t.Fatal("the deprovision did not finish")
		default:
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM pg_stat_activity WHERE usename = $1 AND wait_event_type = 'Lock'`, role); n != 0 {
			t.Fatalf("the deprovision waits on a lock (%d waiting) while go-api holds only users rows", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The principal rows a deprovision's inserts name are taken before the
// organization row: while a membership verb holds a go-api member's principal
// (step 1), the deprovision waits there and holds no lock on the
// organization row, so the verb can still take it.
func TestDeprovisionLocksPrincipalsBeforeTheOrganization(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	p := e.populate(t, ctx)
	verb, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = verb.Rollback(context.Background()) }()
	if _, err := verb.Exec(ctx, `SELECT id FROM auth.principals WHERE id = $1 FOR UPDATE`, p.goAPI); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := e.service.Deprovision(ctx, Input{OrgID: p.org, Authorize: allowOrg})
		done <- err
	}()
	role := e.runtime.Config().ConnConfig.User
	for waiting := 0; waiting == 0; {
		select {
		case err := <-done:
			t.Fatalf("the deprovision returned (%v) while a verb held a principal it names", err)
		case <-ctx.Done():
			t.Fatal("the deprovision never waited on the principal row")
		default:
		}
		waiting = e.count(t, ctx, `SELECT count(*) FROM pg_stat_activity
WHERE usename = $1 AND wait_event_type = 'Lock' AND query LIKE '%.principals WHERE id = ANY($1::uuid[]) ORDER BY id FOR KEY SHARE%'`, role)
		if waiting == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	// The verb goes on to its organization lock (step 3): free.
	var active bool
	if err := verb.QueryRow(ctx, `SELECT disabled_at IS NULL FROM auth.organizations WHERE id = $1 FOR SHARE NOWAIT`, p.org).Scan(&active); err != nil {
		t.Fatalf("the organization row is locked while the deprovision waits on a principal: %v", err)
	}
	if err := verb.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	if got := e.live(t, ctx, p.org); got != (live{}) {
		t.Fatalf("after the deprovision = %+v, want nothing live", got)
	}
}
