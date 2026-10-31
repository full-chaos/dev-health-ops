//go:build integration

package serviceaccount

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/invite"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
	"github.com/full-chaos/dev-health-ops/internal/auth/orgdeprovision"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

type env struct {
	admin    *pgxpool.Pool
	runtime  *pgxpool.Pool
	schema   authschema.ValidatedIdentifier
	members  *membership.Store
	sa       *Store
	notOwned *Store
	deprov   *orgdeprovision.Service
	identity *identity.Store
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
	role, err := containers.RoleName("serviceaccount_runtime", instance)
	if err != nil {
		return nil, fmt.Errorf("derive role: %w", err)
	}
	const password = "serviceaccount-test-only-not-a-credential"
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
	e := &env{admin: admin, runtime: runtime, schema: schema}
	sessions, err := session.New(session.Config{Pool: runtime, Schema: schema,
		SuccessorKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString([]byte("sa-sessions-successor-key-000032")))})
	if err != nil {
		return nil, err
	}
	if e.members, err = membership.New(membership.Config{Pool: runtime, Schema: schema, OwnsUsers: true}); err != nil {
		return nil, err
	}
	orgs, err := org.New(org.Config{Pool: runtime, Schema: schema, Sessions: sessions})
	if err != nil {
		return nil, err
	}
	secret := "sa-invite-secret-" + uuid.NewString()
	invites, err := invite.New(invite.Config{Pool: runtime, Schema: schema, OwnsUsers: true, Memberships: e.members, Secret: secret, AcceptSecrets: []string{secret}})
	if err != nil {
		return nil, err
	}
	if e.deprov, err = orgdeprovision.New(orgdeprovision.Config{Pool: runtime, Schema: schema, Orgs: orgs, Memberships: e.members, Invites: invites, Sessions: sessions}); err != nil {
		return nil, err
	}
	if e.identity, err = identity.New(identity.Config{Pool: runtime, Schema: schema, UsersOwnedByAuth: true}); err != nil {
		return nil, err
	}
	if e.sa, err = New(Config{Pool: runtime, Schema: schema, Locks: e.members, OwnsUsers: true}); err != nil {
		return nil, err
	}
	if e.notOwned, err = New(Config{Pool: runtime, Schema: schema, Locks: e.members}); err != nil {
		return nil, err
	}
	return e, nil
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func allowAll(context.Context, audit.TxOps, membership.Locked) error { return nil }

func (e *env) seedUser(t *testing.T) uuid.UUID {
	t.Helper()
	ctx := ctxT(t)
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, false, 0, now(), now())`, id, "sa-"+id.String()[:8]+"@example.test"); err != nil {
		t.Fatalf("seed public.users: %v", err)
	}
	if err := e.identity.Project(ctx, id); err != nil {
		t.Fatalf("Project: %v", err)
	}
	return id
}

func (e *env) seedOrg(t *testing.T) uuid.UUID {
	t.Helper()
	ctx := ctxT(t)
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'sa-test')`, id, "org-"+id.String()[:8]); err != nil {
		t.Fatalf("seed public.organizations: %v", err)
	}
	if err := e.identity.ProjectOrganization(ctx, id); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	return id
}

func (e *env) member(t *testing.T, orgID uuid.UUID) uuid.UUID {
	t.Helper()
	u := e.seedUser(t)
	if _, err := e.members.Add(ctxT(t), membership.Change{OrgID: orgID, PrincipalID: u, Role: "member", Authorize: allowAll}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	return u
}

func (e *env) removeMember(t *testing.T, orgID, u uuid.UUID) {
	t.Helper()
	if _, err := e.members.Remove(ctxT(t), membership.Change{OrgID: orgID, PrincipalID: u, Authorize: allowAll}); err != nil {
		t.Fatalf("Remove: %v", err)
	}
}

// commitSeed runs sql through audit.Commit: the security tables refuse any
// other write.
func (e *env) commitSeed(t *testing.T, sql string, args ...any) {
	t.Helper()
	err := audit.Commit(ctxT(t), e.admin, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, sql, args...)
			return err
		},
		Audit:     audit.AuditEvent{EventType: "credential.test", Outcome: audit.OutcomeAllowed},
		OffStream: audit.OffStreamWorkloadRegistry,
	})
	if err != nil {
		t.Fatalf("guarded write through audit.Commit: %v", err)
	}
}

func (e *env) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctxT(t), sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

func (e *env) create(t *testing.T, orgID, owner uuid.UUID) uuid.UUID {
	t.Helper()
	id, err := e.sa.Create(ctxT(t), CreateInput{OrgID: orgID, Name: "bot-" + uuid.NewString()[:8], Owner: owner}, Actor{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return id
}

func (e *env) writes(t *testing.T, id uuid.UUID) string {
	t.Helper()
	var s string
	if err := e.admin.QueryRow(ctxT(t), `SELECT
(SELECT status || '/' || (disabled_at IS NOT NULL)::text || '/' || owner_principal_id::text FROM auth.service_accounts WHERE principal_id = $1) || '/' ||
(SELECT (disabled_at IS NOT NULL)::text FROM auth.principals WHERE id = $1) || '/' ||
(SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1)::text || '/' ||
(SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_id = $2)::text`, id, wire(id)).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSACreateWritesRowsAuditOutboxAndRevision(t *testing.T) {
	e := getEnv(t)
	orgID := e.seedOrg(t)
	owner, actor := e.member(t, orgID), e.seedUser(t)
	id, err := e.sa.Create(ctxT(t), CreateInput{OrgID: orgID, Name: "ci-bot", Owner: owner}, Actor{PrincipalID: &actor})
	if err != nil {
		t.Fatal(err)
	}
	var kind, status string
	var gotOwner, createdBy uuid.UUID
	if err := e.admin.QueryRow(ctxT(t), `SELECT p.kind, sa.status, sa.owner_principal_id, sa.created_by FROM auth.principals p
JOIN auth.service_accounts sa ON sa.principal_id = p.id WHERE p.id = $1`, id).Scan(&kind, &status, &gotOwner, &createdBy); err != nil {
		t.Fatal(err)
	}
	if kind != "service_account" || status != "active" || gotOwner != owner || createdBy != actor {
		t.Fatalf("rows %s %s %s %s", kind, status, gotOwner, createdBy)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1 AND event_type = 'auth.service_account.created'
AND actor_principal_id = $2 AND attributes->>'reason_code' = 'principal_provisioned' AND attributes->>'principal_id' = $3`, id, actor, owner.String()); n != 1 {
		t.Fatalf("%d created audit rows, want 1", n)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_type = 'service_account' AND aggregate_id = $1 AND event_type = 'service_account.created'`, wire(id)); n != 1 {
		t.Fatalf("%d created outbox events, want 1", n)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.aggregate_revisions WHERE aggregate_type = 'service_account' AND aggregate_id = $1 AND revision = 1`, wire(id)); n != 1 {
		t.Fatalf("%d revision rows, want 1", n)
	}
}

func TestSACreateRefusals(t *testing.T) {
	e := getEnv(t)
	orgID, other := e.seedOrg(t), e.seedOrg(t)
	owner := e.member(t, orgID)
	e.create(t, orgID, owner)
	first, _ := e.sa.List(ctxT(t), orgID)
	taken := first[0].Name
	stranger := e.seedUser(t)
	disabled := e.member(t, orgID)
	e.commitSeed(t, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, disabled)
	svc := e.create(t, orgID, owner)
	suspended := e.seedOrg(t)
	suspendedOwner := e.member(t, suspended)
	e.commitSeed(t, `UPDATE auth.organizations SET disabled_at = now() WHERE id = $1`, suspended)
	cases := []struct {
		cell string
		in   CreateInput
		want error
	}{
		{"name taken", CreateInput{OrgID: orgID, Name: taken, Owner: owner}, ErrNameTaken},
		{"not a member", CreateInput{OrgID: orgID, Name: "n1", Owner: stranger}, ErrOwnerNotMember},
		{"owner disabled", CreateInput{OrgID: orgID, Name: "n2", Owner: disabled}, ErrOwnerDisabled},
		{"owner is a service account", CreateInput{OrgID: orgID, Name: "n3", Owner: svc}, ErrOwnerNotUser},
		{"owner unknown", CreateInput{OrgID: orgID, Name: "n4", Owner: uuid.New()}, ErrOwnerNotUser},
		{"org suspended", CreateInput{OrgID: suspended, Name: "n5", Owner: suspendedOwner}, ErrOrgNotActive},
		{"org unknown", CreateInput{OrgID: uuid.New(), Name: "n6", Owner: owner}, ErrOrgNotFound},
	}
	for _, c := range cases {
		before := e.count(t, `SELECT (SELECT count(*) FROM auth.service_accounts) + (SELECT count(*) FROM auth.principals WHERE kind = 'service_account') + (SELECT count(*) FROM auth.security_audit_events)`)
		if _, err := e.sa.Create(ctxT(t), c.in, Actor{}); !errors.Is(err, c.want) {
			t.Errorf("%s: %v, want %v", c.cell, err, c.want)
		}
		if after := e.count(t, `SELECT (SELECT count(*) FROM auth.service_accounts) + (SELECT count(*) FROM auth.principals WHERE kind = 'service_account') + (SELECT count(*) FROM auth.security_audit_events)`); after != before {
			t.Errorf("%s: a refusal wrote %d rows", c.cell, after-before)
		}
	}
	if _, err := e.sa.Create(ctxT(t), CreateInput{OrgID: other, Name: taken, Owner: e.member(t, other)}, Actor{}); err != nil {
		t.Errorf("the same name in another organization: %v", err)
	}
}

func TestSADisableSetsRowAndPrincipalAndIsRefusedTwice(t *testing.T) {
	e := getEnv(t)
	orgID := e.seedOrg(t)
	id := e.create(t, orgID, e.member(t, orgID))
	if err := e.sa.Disable(ctxT(t), id, Actor{}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Split(e.writes(t, id), "/"); got[0] != "disabled" || got[1] != "true" || got[3] != "true" {
		t.Fatalf("state %v", got)
	}
	before := e.writes(t, id)
	if err := e.sa.Disable(ctxT(t), id, Actor{}); !errors.Is(err, ErrAlreadyInState) {
		t.Fatalf("second Disable: %v", err)
	}
	if e.writes(t, id) != before {
		t.Fatal("a refused Disable wrote")
	}
	if err := e.sa.Disable(ctxT(t), e.seedUser(t), Actor{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Disable of a user: %v", err)
	}
}

func TestSALostOwnerKeepsTheAccountAndBlocksEnable(t *testing.T) {
	e := getEnv(t)
	orgID := e.seedOrg(t)
	owner := e.member(t, orgID)
	id := e.create(t, orgID, owner)
	e.removeMember(t, orgID, owner)
	info, err := e.sa.Lookup(ctxT(t), id)
	if err != nil || !info.Active || info.OwnerMember || !info.OwnerEnabled {
		t.Fatalf("after the owner left: %+v %v, want active, owner enabled, not a member", info, err)
	}
	if err := e.sa.Disable(ctxT(t), id, Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := e.sa.Enable(ctxT(t), id, Actor{}); !errors.Is(err, ErrOwnerNotMember) {
		t.Fatalf("Enable with a lost owner: %v", err)
	}
	next := e.member(t, orgID)
	if err := e.sa.SetOwner(ctxT(t), id, next, Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := e.sa.Enable(ctxT(t), id, Actor{}); err != nil {
		t.Fatalf("Enable after SetOwner: %v", err)
	}
	if got := strings.Split(e.writes(t, id), "/"); got[0] != "active" || got[1] != "false" || got[3] != "false" {
		t.Fatalf("state %v", got)
	}
}

func TestSASetOwnerAuditAndEventNameTheNewOwner(t *testing.T) {
	e := getEnv(t)
	orgID := e.seedOrg(t)
	old, next := e.member(t, orgID), e.member(t, orgID)
	id := e.create(t, orgID, old)
	if err := e.sa.SetOwner(ctxT(t), id, old, Actor{}); !errors.Is(err, ErrAlreadyInState) {
		t.Fatalf("same owner: %v", err)
	}
	if err := e.sa.SetOwner(ctxT(t), id, e.seedUser(t), Actor{}); !errors.Is(err, ErrOwnerNotMember) {
		t.Fatalf("non-member owner: %v", err)
	}
	if err := e.sa.SetOwner(ctxT(t), id, next, Actor{}); err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1 AND event_type = 'auth.service_account.owner_changed'
AND attributes->>'reason_code' = 'service_account_owner_changed' AND attributes->>'principal_id' = $2`, id, next.String()); n != 1 {
		t.Fatalf("%d owner_changed audit rows naming the new owner, want 1", n)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_id = $1 AND event_type = 'service_account.owner_changed'`, wire(id)); n != 1 {
		t.Fatalf("%d owner_changed events, want 1", n)
	}
	info, _ := e.sa.Lookup(ctxT(t), id)
	if info.Owner != next {
		t.Fatalf("owner %s, want %s", info.Owner, next)
	}
}

func TestSANotOwnedRefusesBeforeAnyWrite(t *testing.T) {
	e := getEnv(t)
	orgID := e.seedOrg(t)
	owner := e.member(t, orgID)
	id := e.create(t, orgID, owner)
	before := e.count(t, `SELECT (SELECT count(*) FROM auth.service_accounts) + (SELECT count(*) FROM auth.security_audit_events)`)
	if _, err := e.notOwned.Create(ctxT(t), CreateInput{OrgID: orgID, Name: "x1", Owner: owner}, Actor{}); !errors.Is(err, ErrNotOwned) {
		t.Errorf("Create: %v", err)
	}
	if err := e.notOwned.Enable(ctxT(t), id, Actor{}); !errors.Is(err, ErrNotOwned) {
		t.Errorf("Enable: %v", err)
	}
	if err := e.notOwned.SetOwner(ctxT(t), id, owner, Actor{}); !errors.Is(err, ErrNotOwned) {
		t.Errorf("SetOwner: %v", err)
	}
	if after := e.count(t, `SELECT (SELECT count(*) FROM auth.service_accounts) + (SELECT count(*) FROM auth.security_audit_events)`); after != before {
		t.Errorf("a refused verb wrote %d rows", after-before)
	}
	if err := e.notOwned.Disable(ctxT(t), id, Actor{}); err != nil {
		t.Errorf("Disable must work with users not owned: %v", err)
	}
}

func TestSAEnableRefusesAStateThatChangedBeforeTheLocks(t *testing.T) {
	e := getEnv(t)
	orgID := e.seedOrg(t)
	old, next := e.member(t, orgID), e.member(t, orgID)
	id := e.create(t, orgID, old)
	if err := e.sa.Disable(ctxT(t), id, Actor{}); err != nil {
		t.Fatal(err)
	}
	moved := *e.sa
	moved.read = func(ctx context.Context, id uuid.UUID) (*Info, error) {
		info, err := e.sa.Lookup(ctx, id)
		if err == nil {
			e.commitSeed(t, `UPDATE auth.service_accounts SET owner_principal_id = $2 WHERE principal_id = $1`, id, next)
		}
		return info, err
	}
	before := e.writes(t, id)
	if err := moved.Enable(ctxT(t), id, Actor{}); !errors.Is(err, ErrStateChanged) {
		t.Fatalf("Enable on a moved owner: %v", err)
	}
	if got := e.writes(t, id); strings.Split(got, "/")[0] != "disabled" || strings.Split(before, "/")[0] != "disabled" {
		t.Fatalf("a refused Enable wrote: %s", got)
	}
}

func TestSADirectWriteOutsideCommitIsRefused(t *testing.T) {
	e := getEnv(t)
	orgID := e.seedOrg(t)
	id := e.create(t, orgID, e.member(t, orgID))
	_, err := e.runtime.Exec(ctxT(t), `UPDATE auth.service_accounts SET status = 'disabled', disabled_at = now() WHERE principal_id = $1`, id)
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "42501" {
		t.Fatalf("direct UPDATE: %v, want 42501", err)
	}
}

func isDeadlock(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "40P01"
}

type held struct {
	locked  chan struct{}
	release chan struct{}
	once    sync.Once
}

func newHeld() *held { return &held{locked: make(chan struct{}), release: make(chan struct{})} }

func (h *held) authorize(context.Context, audit.TxOps, membership.Locked) error {
	close(h.locked)
	<-h.release
	return nil
}

func (h *held) free() { h.once.Do(func() { close(h.release) }) }

// waitsOn blocks until a backend of the runtime pool waits on a lock in a
// statement containing text, and fails by name when none does.
func (e *env) waitsOn(t *testing.T, cell, text string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if e.count(t, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query ILIKE '%' || $1 || '%' AND pid <> pg_backend_pid()`, text) > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s: no backend waits on a lock in a statement with %q; the choreography drifted", cell, text)
}

// P-SA1: a removal of the owner holds the owner's principal row FOR UPDATE
// (step 1) after Authorize; Enable and Create name that owner and wait for it,
// then judge the owner as it is after the removal. No deadlock.
func TestSAPairMembershipRemoveOfTheOwnerAndEnableAndCreate(t *testing.T) {
	const cell = "P-SA1/owner removal"
	e := getEnv(t)
	orgID := e.seedOrg(t)
	owner := e.member(t, orgID)
	id := e.create(t, orgID, owner)
	if err := e.sa.Disable(ctxT(t), id, Actor{}); err != nil {
		t.Fatal(err)
	}
	h := newHeld()
	t.Cleanup(h.free)
	removeErr := make(chan error, 1)
	go func() {
		_, err := e.members.Remove(ctxT(t), membership.Change{OrgID: orgID, PrincipalID: owner, Authorize: h.authorize})
		removeErr <- err
	}()
	select {
	case <-h.locked:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s: the removal never reached Authorize", cell)
	}
	enableErr, createErr := make(chan error, 1), make(chan error, 1)
	go func() { enableErr <- e.sa.Enable(ctxT(t), id, Actor{}) }()
	go func() {
		_, err := e.sa.Create(ctxT(t), CreateInput{OrgID: orgID, Name: "p-sa1", Owner: owner}, Actor{})
		createErr <- err
	}()
	e.waitsOn(t, cell, "principals")
	h.free()
	for name, ch := range map[string]chan error{"remove": removeErr, "enable": enableErr, "create": createErr} {
		select {
		case err := <-ch:
			if isDeadlock(err) {
				t.Fatalf("%s: %s deadlocked: %v", cell, name, err)
			}
			switch name {
			case "remove":
				if err != nil {
					t.Fatalf("%s: remove: %v", cell, err)
				}
			default:
				if !errors.Is(err, ErrOwnerNotMember) {
					t.Fatalf("%s: %s: %v, want ErrOwnerNotMember after the removal", cell, name, err)
				}
			}
		case <-time.After(60 * time.Second):
			t.Fatalf("%s: %s did not finish", cell, name)
		}
	}
}

// P-SA2: an organization deprovision holds the organization row FOR UPDATE
// (step 3) after Authorize; Create takes the owner's principal rows first
// (step 1, compatible with the deprovision's KEY SHARE), then waits on the
// organization row, and finds the organization suspended.
func TestSAPairOrgDeprovisionAndCreate(t *testing.T) {
	const cell = "P-SA2/org deprovision"
	e := getEnv(t)
	orgID := e.seedOrg(t)
	owner := e.member(t, orgID)
	locked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	t.Cleanup(free)
	depErr := make(chan error, 1)
	go func() {
		_, err := e.deprov.Deprovision(ctxT(t), orgdeprovision.Input{OrgID: orgID, Authorize: func(context.Context, audit.TxOps, uuid.UUID) error {
			close(locked)
			<-release
			return nil
		}})
		depErr <- err
	}()
	select {
	case <-locked:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s: the deprovision never reached Authorize", cell)
	}
	createErr := make(chan error, 1)
	go func() {
		_, err := e.sa.Create(ctxT(t), CreateInput{OrgID: orgID, Name: "p-sa2", Owner: owner}, Actor{})
		createErr <- err
	}()
	e.waitsOn(t, cell, "organizations")
	free()
	if err := <-depErr; err != nil {
		t.Fatalf("%s: deprovision: %v", cell, err)
	}
	if err := <-createErr; isDeadlock(err) || !errors.Is(err, ErrOrgNotActive) {
		t.Fatalf("%s: create: %v, want ErrOrgNotActive", cell, err)
	}
}

// P-SA3: a writer that holds the account's principal row (Disable's first
// lock) and then asks for the service_accounts row, against Enable. Enable
// takes the principal row before the account row (steps 1 and 7), so it waits
// for the holder and no cycle closes; an Enable that took the account row
// first deadlocks here.
func TestSAPairDisableOrderAndEnable(t *testing.T) {
	const cell = "P-SA3/principal then account"
	e := getEnv(t)
	orgID := e.seedOrg(t)
	id := e.create(t, orgID, e.member(t, orgID))
	if err := e.sa.Disable(ctxT(t), id, Actor{}); err != nil {
		t.Fatal(err)
	}
	tx, err := e.admin.Begin(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctxT(t), `SELECT id FROM auth.principals WHERE id = $1 FOR UPDATE`, id); err != nil {
		t.Fatal(err)
	}
	enableErr := make(chan error, 1)
	go func() { enableErr <- e.sa.Enable(ctxT(t), id, Actor{}) }()
	e.waitsOn(t, cell, "principals")
	if _, err := tx.Exec(ctxT(t), `SELECT principal_id FROM auth.service_accounts WHERE principal_id = $1 FOR UPDATE`, id); err != nil {
		t.Fatalf("%s: the holder's second lock: %v", cell, err)
	}
	if err := tx.Commit(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-enableErr:
		if err != nil {
			t.Fatalf("%s: Enable: %v", cell, err)
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("%s: Enable did not finish", cell)
	}
}
