//go:build integration

package invite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

type env struct {
	admin    *pgxpool.Pool
	runtime  *pgxpool.Pool
	store    *Store
	notOwned *Store
	members  *membership.Store
	identity *identity.Store
	secret   string
	now      time.Time
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
	role, err := containers.RoleName("invite_runtime", instance)
	if err != nil {
		return nil, fmt.Errorf("derive role: %w", err)
	}
	password := "invite-test-" + uuid.NewString()
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
	members, err := membership.New(membership.Config{Pool: runtime, Schema: schema, OwnsUsers: true})
	if err != nil {
		return nil, err
	}
	e := &env{admin: admin, runtime: runtime, members: members, secret: "invite-secret-" + uuid.NewString(),
		now: time.Now().UTC()}
	clock := func() time.Time { return e.now }
	if e.store, err = New(Config{Pool: runtime, Schema: schema, OwnsUsers: true, Memberships: members,
		Secret: e.secret, AcceptSecrets: []string{e.secret}, Now: clock}); err != nil {
		return nil, err
	}
	if e.notOwned, err = New(Config{Pool: runtime, Schema: schema, Memberships: members,
		Secret: e.secret, AcceptSecrets: []string{e.secret}, Now: clock}); err != nil {
		return nil, err
	}
	if e.identity, err = identity.New(identity.Config{Pool: runtime, Schema: schema, UsersOwnedByAuth: true}); err != nil {
		return nil, err
	}
	return e, nil
}

func allow(context.Context, audit.TxOps, uuid.UUID) error { return nil }

func (e *env) seedUser(t *testing.T, ctx context.Context, email string, verified bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, $3, false, 0, now(), now())`, id, email, verified); err != nil {
		t.Fatalf("seed public.users: %v", err)
	}
	if err := e.identity.Project(ctx, id); err != nil {
		t.Fatalf("Project: %v", err)
	}
	return id
}

func (e *env) seedOrg(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'invite-test')`,
		id, "org-"+id.String()[:8]); err != nil {
		t.Fatalf("seed public.organizations: %v", err)
	}
	if err := e.identity.ProjectOrganization(ctx, id); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	return id
}

func address() string { return "invitee-" + uuid.NewString()[:8] + "@example.com" }

func (e *env) count(t *testing.T, ctx context.Context, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

// fixture is an organization, its inviter (a member) and an invite to email.
type fixture struct {
	org, inviter uuid.UUID
	email        string
	created      *Created
}

func (e *env) invite(t *testing.T, ctx context.Context, role string) fixture {
	t.Helper()
	f := fixture{org: e.seedOrg(t, ctx), email: address()}
	f.inviter = e.seedUser(t, ctx, address(), true)
	if _, err := e.members.Add(ctx, membership.Change{OrgID: f.org, PrincipalID: f.inviter, Role: "owner",
		Authorize: func(context.Context, audit.TxOps, membership.Locked) error { return nil }}); err != nil {
		t.Fatalf("seed the inviter's membership: %v", err)
	}
	created, err := e.store.Create(ctx, CreateInput{OrgID: f.org, Email: f.email, Role: role, InvitedBy: f.inviter,
		Authorize: allow, PublicAudit: &PublicAudit{}})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	f.created = created
	return f
}

type inviteRows struct {
	publicStatus, authStatus string
	sameHash                 bool
}

func (e *env) inviteRows(t *testing.T, ctx context.Context, id uuid.UUID) inviteRows {
	t.Helper()
	var r inviteRows
	if err := e.admin.QueryRow(ctx, `SELECT
  COALESCE((SELECT status FROM public.org_invites WHERE id = $1), ''),
  COALESCE((SELECT status FROM auth.invites WHERE id = $1), ''),
  COALESCE((SELECT p.token_hash = a.token_hash FROM public.org_invites p JOIN auth.invites a ON a.id = p.id WHERE p.id = $1), false)`, id).
		Scan(&r.publicStatus, &r.authStatus, &r.sameHash); err != nil {
		t.Fatalf("read invite rows: %v", err)
	}
	return r
}

// A create writes both invite rows with the token's digest, one audit row, one
// event and one go-api audit_logs row; the token itself is stored nowhere.
func TestCreateWritesBothRowsAndNeverTheToken(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	f := e.invite(t, ctx, "member")
	sum := sha256.Sum256([]byte(f.created.Token))
	digest := hex.EncodeToString(sum[:])
	if got := e.inviteRows(t, ctx, f.created.ID); got != (inviteRows{"pending", "pending", true}) {
		t.Fatalf("invite rows = %+v", got)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.invites WHERE id = $1 AND token_hash = $2`, f.created.ID, digest); n != 1 {
		t.Fatalf("auth.invites rows with the token digest = %d, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.invite.created' AND organization_id = $1`, f.org.String()); n != 1 {
		t.Fatalf("audit rows = %d, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'invite.created' AND aggregate_id = $1`, "inv_"+f.created.ID.String()); n != 1 {
		t.Fatalf("outbox rows = %d, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.audit_logs WHERE org_id = $1 AND action = 'member_invited' AND resource_id = $2`, f.org, f.created.ID.String()); n != 1 {
		t.Fatalf("public audit_logs member_invited rows = %d, want 1", n)
	}
	for _, q := range []string{
		`SELECT count(*) FROM auth.invites WHERE row_to_json(invites)::text LIKE '%' || $1 || '%'`,
		`SELECT count(*) FROM public.org_invites WHERE row_to_json(org_invites)::text LIKE '%' || $1 || '%'`,
		`SELECT count(*) FROM auth.security_audit_events e WHERE row_to_json(e)::text LIKE '%' || $1 || '%'`,
		`SELECT count(*) FROM auth.auth_outbox_events o WHERE row_to_json(o)::text LIKE '%' || $1 || '%'`,
		`SELECT count(*) FROM public.audit_logs l WHERE row_to_json(l)::text LIKE '%' || $1 || '%'`,
	} {
		if n := e.count(t, ctx, q, f.created.Token); n != 0 {
			t.Fatalf("the token is stored: %q found %d rows", q, n)
		}
	}
	if _, err := e.store.Create(ctx, CreateInput{OrgID: f.org, Email: f.email, Role: "member", InvitedBy: f.inviter,
		Authorize: allow, PublicAudit: &PublicAudit{}}); !errors.Is(err, ErrPendingExists) {
		t.Fatalf("a second pending invite = %v, want ErrPendingExists", err)
	}
}

// An accept adds the membership with the invite's role and inviter and marks
// both invite rows accepted, in one commit, with one go-api audit_logs row.
func TestAcceptAddsTheMembershipAndMarksBothInviteRows(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	f := e.invite(t, ctx, "admin")
	user := e.seedUser(t, ctx, f.email, true)
	got, err := e.store.Accept(ctx, AcceptInput{Token: f.created.Token, PrincipalID: user, PublicAudit: &PublicAudit{}})
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if got.InviteID != f.created.ID || got.OrgID != f.org || got.Role != "admin" {
		t.Fatalf("Accepted = %+v", got)
	}
	if r := e.inviteRows(t, ctx, f.created.ID); r.publicStatus != "accepted" || r.authStatus != "accepted" {
		t.Fatalf("invite rows = %+v, want accepted in both", r)
	}
	var role string
	var inviter uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT p.role, p.invited_by_id FROM public.memberships p
JOIN auth.memberships a ON a.organization_id = p.org_id AND a.principal_id = p.user_id AND a.status = 'active' AND a.role = p.role
WHERE p.org_id = $1 AND p.user_id = $2`, f.org, user).Scan(&role, &inviter); err != nil || role != "admin" || inviter != f.inviter {
		t.Fatalf("membership role=%q inviter=%v err=%v, want admin by %v in both tables", role, inviter, err, f.inviter)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.audit_logs WHERE org_id = $1 AND action = 'member_joined'`, f.org); n != 1 {
		t.Fatalf("public audit_logs member_joined rows = %d, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type IN ('invite.accepted', 'membership.activated') AND aggregate_id IN ($1, $2)`,
		"inv_"+f.created.ID.String(), "mbr_"+got.MembershipID.String()); n != 2 {
		t.Fatalf("accept events = %d, want invite.accepted and membership.activated", n)
	}
}

// A token is single use: after an accept or a revoke, and once expired, it is
// refused like an unknown or badly signed token: one error, one text.
func TestEveryUnusableTokenGetsTheSameRefusal(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	accepted := e.invite(t, ctx, "member")
	user := e.seedUser(t, ctx, accepted.email, true)
	if _, err := e.store.Accept(ctx, AcceptInput{Token: accepted.created.Token, PrincipalID: user, PublicAudit: &PublicAudit{}}); err != nil {
		t.Fatal(err)
	}
	revoked := e.invite(t, ctx, "member")
	if err := e.store.Revoke(ctx, RevokeInput{OrgID: revoked.org, InviteID: revoked.created.ID, Authorize: allow}); err != nil {
		t.Fatal(err)
	}
	revokedUser := e.seedUser(t, ctx, revoked.email, true)
	expired := e.invite(t, ctx, "member")
	expiredUser := e.seedUser(t, ctx, expired.email, true)
	unknown, _ := signedtoken.Build(uuid.New(), e.secret)
	badSignature, _ := signedtoken.Build(uuid.New(), "another-"+uuid.NewString())
	cases := map[string]AcceptInput{
		"replayed after accept": {Token: accepted.created.Token, PrincipalID: user},
		"revoked":               {Token: revoked.created.Token, PrincipalID: revokedUser},
		"expired":               {Token: expired.created.Token, PrincipalID: expiredUser},
		"unknown":               {Token: unknown, PrincipalID: user},
		"bad signature":         {Token: badSignature, PrincipalID: user},
		"not a token":           {Token: "x", PrincipalID: user},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			in.PublicAudit = &PublicAudit{}
			if name == "expired" {
				// The store's clock, one second past the invite's expiry.
				saved := e.now
				e.now = expired.created.ExpiresAt.Add(time.Second)
				defer func() { e.now = saved }()
			}
			if _, err := e.store.Accept(ctx, in); err != ErrInviteInvalid {
				t.Fatalf("Accept = %v, want exactly ErrInviteInvalid", err)
			}
		})
	}
	if r := e.inviteRows(t, ctx, revoked.created.ID); r.publicStatus != "revoked" || r.authStatus != "revoked" {
		t.Fatalf("revoked invite rows = %+v", r)
	}
	if err := e.store.Revoke(ctx, RevokeInput{OrgID: accepted.org, InviteID: accepted.created.ID, Authorize: allow}); !errors.Is(err, ErrNotPending) {
		t.Fatalf("revoke of an accepted invite = %v, want ErrNotPending", err)
	}
}

func TestAcceptRefusesAnUnverifiedOrOtherAddressAndWritesNothing(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	f := e.invite(t, ctx, "member")
	unverified := e.seedUser(t, ctx, f.email, false)
	other := e.seedUser(t, ctx, address(), true)
	for name, c := range map[string]struct {
		user uuid.UUID
		want error
	}{"unverified": {unverified, ErrEmailUnverified}, "other address": {other, ErrEmailMismatch}} {
		if _, err := e.store.Accept(ctx, AcceptInput{Token: f.created.Token, PrincipalID: c.user, PublicAudit: &PublicAudit{}}); !errors.Is(err, c.want) {
			t.Fatalf("%s: Accept = %v, want %v", name, err, c.want)
		}
	}
	if r := e.inviteRows(t, ctx, f.created.ID); r.publicStatus != "pending" || r.authStatus != "pending" {
		t.Fatalf("invite rows = %+v, want pending", r)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.memberships WHERE org_id = $1 AND user_id IN ($2, $3)`, f.org, unverified, other); n != 0 {
		t.Fatalf("memberships = %d, want 0", n)
	}
}

// An invite go-api made before auth-service owned users has only its public
// row; an accept takes it over into auth.invites in the same commit.
func TestAcceptTakesOverAnInviteThatExistsOnlyInPublic(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, inviter, email := e.seedOrg(t, ctx), e.seedUser(t, ctx, address(), true), address()
	id := uuid.New()
	token, digest := signedtoken.Build(id, e.secret)
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.org_invites (id, org_id, email, role, token_hash, invited_by_id, status, expires_at, created_at, updated_at)
VALUES ($1, $2, $3, 'viewer', $4, $5, 'pending', now() + interval '1 day', now(), now())`, id, org, email, digest, inviter); err != nil {
		t.Fatal(err)
	}
	user := e.seedUser(t, ctx, email, true)
	if _, err := e.store.Accept(ctx, AcceptInput{Token: token, PrincipalID: user, PublicAudit: &PublicAudit{}}); err != nil {
		t.Fatalf("Accept of a public-only invite: %v", err)
	}
	if r := e.inviteRows(t, ctx, id); r != (inviteRows{"accepted", "accepted", true}) {
		t.Fatalf("invite rows = %+v, want accepted in both with one digest", r)
	}
}

// Two accepts of one token at once: exactly one adds the membership.
func TestConcurrentAcceptsOfOneTokenWriteOnce(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := e.invite(t, ctx, "member")
	user := e.seedUser(t, ctx, f.email, true)
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := e.store.Accept(ctx, AcceptInput{Token: f.created.Token, PrincipalID: user, PublicAudit: &PublicAudit{}})
			errs <- err
		}()
	}
	var ok, invalid int
	for range 2 {
		switch err := <-errs; {
		case err == nil:
			ok++
		case err == ErrInviteInvalid:
			invalid++
		default:
			t.Fatalf("Accept: %v", err)
		}
	}
	if ok != 1 || invalid != 1 {
		t.Fatalf("ok=%d invalid=%d, want 1 and 1", ok, invalid)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.audit_logs WHERE org_id = $1 AND action = 'member_joined'`, f.org); n != 1 {
		t.Fatalf("member_joined rows = %d, want 1", n)
	}
}

// A failure planted on go-api's invite row rolls back the membership, both
// invite rows and every audit and event row of the accept.
func TestForcedFailureInTheAcceptLeavesEverythingUnchanged(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	f := e.invite(t, ctx, "member")
	user := e.seedUser(t, ctx, f.email, true)
	fn := "invite_test_fail_" + uuid.NewString()[:8]
	for _, s := range []string{
		fmt.Sprintf(`CREATE FUNCTION public.%s() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN RAISE EXCEPTION 'planted failure' USING ERRCODE = 'P0001'; END $$`, fn),
		fmt.Sprintf(`CREATE TRIGGER %s BEFORE UPDATE ON public.org_invites FOR EACH ROW EXECUTE FUNCTION public.%s()`, fn, fn),
	} {
		if _, err := e.admin.Exec(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = e.admin.Exec(context.Background(), fmt.Sprintf(`DROP TRIGGER %s ON public.org_invites`, fn))
		_, _ = e.admin.Exec(context.Background(), fmt.Sprintf(`DROP FUNCTION public.%s()`, fn))
	})
	if _, err := e.store.Accept(ctx, AcceptInput{Token: f.created.Token, PrincipalID: user, PublicAudit: &PublicAudit{}}); err == nil {
		t.Fatal("Accept with a planted failure succeeded")
	}
	if r := e.inviteRows(t, ctx, f.created.ID); r.publicStatus != "pending" || r.authStatus != "pending" {
		t.Fatalf("invite rows = %+v, want pending", r)
	}
	for _, q := range []string{
		`SELECT count(*) FROM public.memberships WHERE org_id = $1 AND user_id = $2`,
		`SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2`,
	} {
		if n := e.count(t, ctx, q, f.org, user); n != 0 {
			t.Fatalf("%q = %d, want 0", q, n)
		}
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.audit_logs WHERE org_id = $1 AND action = 'member_joined'`, f.org); n != 0 {
		t.Fatalf("member_joined rows = %d, want 0", n)
	}
}

func TestEveryVerbRefusesWhenUsersAreNotOwned(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, inviter := e.seedOrg(t, ctx), e.seedUser(t, ctx, address(), true)
	if _, err := e.notOwned.Create(ctx, CreateInput{OrgID: org, Email: address(), Role: "member", InvitedBy: inviter, Authorize: allow, PublicAudit: &PublicAudit{}}); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("Create = %v, want ErrNotOwned", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.org_invites WHERE org_id = $1`, org); n != 0 {
		t.Fatalf("invites = %d, want 0", n)
	}
}

// A refused actor gets ErrDenied for a missing organization too; nothing is
// written.
func TestCreateRefusalsAreDeniedToARefusedActor(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	inviter := e.seedUser(t, ctx, address(), true)
	refuse := func(context.Context, audit.TxOps, uuid.UUID) error { return ErrDenied }
	for name, org := range map[string]uuid.UUID{"existing organization": e.seedOrg(t, ctx), "missing organization": uuid.New()} {
		if _, err := e.store.Create(ctx, CreateInput{OrgID: org, Email: address(), Role: "member", InvitedBy: inviter, Authorize: refuse, PublicAudit: &PublicAudit{}}); !errors.Is(err, ErrDenied) {
			t.Fatalf("%s: Create = %v, want ErrDenied", name, err)
		}
	}
	if _, err := e.store.Create(ctx, CreateInput{OrgID: uuid.New(), Email: address(), Role: "member", InvitedBy: inviter, Authorize: allow, PublicAudit: &PublicAudit{}}); !errors.Is(err, ErrOrgNotFound) {
		t.Fatalf("accepted actor, missing organization: Create = %v, want ErrOrgNotFound", err)
	}
}

// An account that is not active when the accept holds its users row (locked
// from step 2) is refused, with nothing written: no membership, the invite
// still pending in both tables, no audit row, no go-api member_joined row.
func TestAcceptRefusesAnInactiveAccountUnderItsLock(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	f := e.invite(t, ctx, "member")
	user := e.seedUser(t, ctx, f.email, true)
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	_, err := e.store.Accept(ctx, AcceptInput{Token: f.created.Token, PrincipalID: user, PublicAudit: &PublicAudit{}})
	if !errors.Is(err, ErrAccountInactive) {
		t.Fatalf("Accept by an inactive account = %v, want ErrAccountInactive", err)
	}
	if got := e.inviteRows(t, ctx, f.created.ID); got.publicStatus != "pending" || got.authStatus != "pending" {
		t.Fatalf("invite rows after a refused accept = %+v, want pending in both", got)
	}
	if n := e.count(t, ctx, `SELECT (SELECT count(*) FROM public.memberships WHERE org_id = $1 AND user_id = $2)
  + (SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2)
  + (SELECT count(*) FROM public.audit_logs WHERE org_id = $1 AND action = 'member_joined')
  + (SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.invite.accepted' AND organization_id = $1)`, f.org, user); n != 0 {
		t.Fatalf("a refused accept wrote %d rows", n)
	}
}
