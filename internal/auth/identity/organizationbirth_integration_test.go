//go:build integration

package identity

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/registerorg"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func birthContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func (e *env) orgless(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	user := e.seedUser(t, ctx, uniqueEmail("birth"), true, true)
	e.project(t, ctx, user)
	return user
}

func allowCreate(context.Context, audit.TxOps) error { return nil }

// wantBothOrgRows proves one organization in both schemas with one slug and name.
func (e *env) wantBothOrgRows(t *testing.T, ctx context.Context, org uuid.UUID, slug, name string) {
	t.Helper()
	if n := e.count(t, ctx, `SELECT count(*) FROM public.organizations p JOIN auth.organizations a ON a.id = p.id
WHERE p.id = $1 AND p.slug = $2 AND a.slug = $2 AND p.name = $3 AND a.name = $3 AND p.is_active AND a.disabled_at IS NULL`,
		org, slug, name); n != 1 {
		t.Fatalf("organization %s in both schemas with slug %q and name %q: %d rows, want 1", org, slug, name, n)
	}
}

// wantOwner proves one owner membership in both schemas under one id and instant.
func (e *env) wantOwner(t *testing.T, ctx context.Context, org, user, id uuid.UUID) {
	t.Helper()
	if n := e.count(t, ctx, `SELECT count(*) FROM public.memberships p JOIN auth.memberships a ON a.id = p.id
WHERE p.id = $1 AND p.org_id = $2 AND p.user_id = $3 AND a.organization_id = $2 AND a.principal_id = $3
  AND p.role = 'owner' AND a.role = 'owner' AND a.status = 'active' AND p.joined_at = a.joined_at`, id, org, user); n != 1 {
		t.Fatalf("owner membership %s in both schemas: %d rows, want 1", id, n)
	}
}

func (e *env) orgWrites(t *testing.T, ctx context.Context) [4]int {
	t.Helper()
	return [4]int{e.count(t, ctx, `SELECT count(*) FROM public.organizations`), e.count(t, ctx, `SELECT count(*) FROM auth.organizations`),
		e.count(t, ctx, `SELECT count(*) FROM public.memberships`), e.count(t, ctx, `SELECT count(*) FROM auth.memberships`)}
}

// failWhen raises on an INSERT into table when the row matches condition, until the test ends.
func (e *env) failWhen(t *testing.T, ctx context.Context, table, condition string) {
	t.Helper()
	name := "birth_fail_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := e.admin.Exec(ctx, `CREATE FUNCTION `+name+`() RETURNS trigger LANGUAGE plpgsql AS $f$
BEGIN IF `+condition+` THEN RAISE EXCEPTION 'planted failure'; END IF; RETURN NEW; END $f$`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `CREATE TRIGGER `+name+` BEFORE INSERT ON `+table+` FOR EACH ROW EXECUTE FUNCTION `+name+`()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = e.admin.Exec(ctx, `DROP TRIGGER `+name+` ON `+table)
		_, _ = e.admin.Exec(ctx, `DROP FUNCTION `+name+`()`)
	})
}

func TestBirthRegisterWritesAuthOrgAndOwner(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	email := uniqueEmail("register")
	hash := testBcrypt
	user, err := e.owned.ProvisionLocal(ctx, LocalProvisionRequest{Email: email, PasswordHash: &hash, Organization: &NewOrganization{Name: "Acme Corp"}})
	if err != nil {
		t.Fatal(err)
	}
	var org, membership uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT org_id, id FROM public.memberships WHERE user_id = $1`, user).Scan(&org, &membership); err != nil {
		t.Fatal(err)
	}
	e.wantBothOrgRows(t, ctx, org, registerorg.Slug("Acme Corp", user), "Acme Corp")
	e.wantOwner(t, ctx, org, user, membership)
	if n := e.outboxRows(t, ctx, "organization.created", org.String()); n != 1 {
		t.Fatalf("organization.created events = %d, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'membership.activated' AND aggregate_id = 'mem_' || $1`, membership.String()); n != 1 {
		t.Fatalf("membership.activated events = %d, want 1", n)
	}
	if n := e.auditRows(t, ctx, "auth.membership.added", user.String()); n != 1 {
		t.Fatalf("auth.membership.added rows = %d, want 1", n)
	}
}

func TestBirthRegisterAuditNamesTheOrganization(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	hash := testBcrypt
	user, err := e.owned.ProvisionLocal(ctx, LocalProvisionRequest{Email: uniqueEmail("audit"), PasswordHash: &hash, Organization: &NewOrganization{Name: "Named"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.owned.SetEmailVerified(ctx, user, time.Now(), Actor{}); err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events s JOIN public.memberships m ON m.org_id = s.organization_id
WHERE s.event_type = 'auth.user.email_verified' AND s.subject_principal_id = $1 AND m.user_id = $1`, user); n != 1 {
		t.Fatalf("the verification audit row names the organization on %d rows, want 1", n)
	}
}

func TestBirthEqualRowsProjectNothing(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	born, err := e.owned.OnboardOrganization(ctx, OnboardOrganizationRequest{Principal: e.orgless(t, ctx), Name: "Quiet Org"})
	if err != nil {
		t.Fatal(err)
	}
	before := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE organization_id = $1`, born.ID)
	if err := e.store.ProjectOrganization(ctx, born.ID); err != nil {
		t.Fatal(err)
	}
	if after := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE organization_id = $1`, born.ID); after != before {
		t.Fatalf("the projection wrote %d audit rows on an equal organization", after-before)
	}
}

// TestNewOrgSignsInWithOrgsOwned: with AUTH_ORGS_OWNED the projection creates
// nothing, so each birth must leave the auth row. The public-only org is the control.
func TestNewOrgSignsInWithOrgsOwned(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	orgsOwned, err := New(Config{Pool: e.runtime, Schema: schema, UsersOwnedByAuth: true, OrgsOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := orgsOwned.ProjectOrganization(ctx, e.seedOrg(t, ctx)); !errors.Is(err, ErrOrganizationNotFound) {
		t.Fatalf("control, a public-only org: err = %v, want ErrOrganizationNotFound", err)
	}
	hash := testBcrypt
	registered, err := e.owned.ProvisionLocal(ctx, LocalProvisionRequest{Email: uniqueEmail("signin"), PasswordHash: &hash, Organization: &NewOrganization{Name: "Signs In"}})
	if err != nil {
		t.Fatal(err)
	}
	var registeredOrg uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT org_id FROM public.memberships WHERE user_id = $1`, registered).Scan(&registeredOrg); err != nil {
		t.Fatal(err)
	}
	onboarded, err := e.owned.OnboardOrganization(ctx, OnboardOrganizationRequest{Principal: e.orgless(t, ctx), Name: "Signs In Too"})
	if err != nil {
		t.Fatal(err)
	}
	owner := e.orgless(t, ctx)
	created, err := e.owned.CreateOrganization(ctx, CreateOrganizationRequest{Actor: e.orgless(t, ctx), Slug: "signs-in-" + owner.String()[:8], Name: "Admin Org",
		Settings: []byte(`{}`), Tier: "community", ManagedBy: "stripe", Owner: &owner, Authorize: allowCreate})
	if err != nil {
		t.Fatal(err)
	}
	for name, id := range map[string]uuid.UUID{"register": registeredOrg, "onboard": onboarded.ID, "admin create": created.ID} {
		if err := orgsOwned.ProjectOrganization(ctx, id); err != nil {
			t.Errorf("%s: sign-in projection: %v", name, err)
		}
		if err := orgsOwned.EnsureOrganization(ctx, id); err != nil {
			t.Errorf("%s: provisioning projection: %v", name, err)
		}
	}
}

func TestOnboardOrgOneCommit(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	user := e.orgless(t, ctx)
	description := "Organization created during onboarding"
	born, err := e.owned.OnboardOrganization(ctx, OnboardOrganizationRequest{Principal: user, Name: "Acme Ops", Description: &description,
		RequestMetadata: []byte(`{"ip_address":"192.0.2.1"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if born.MembershipID == nil {
		t.Fatal("no membership id")
	}
	slug := registerorg.OnboardSlug("Acme Ops", user)
	if born.Slug != slug {
		t.Fatalf("slug %q, want %q", born.Slug, slug)
	}
	e.wantBothOrgRows(t, ctx, born.ID, slug, "Acme Ops")
	e.wantOwner(t, ctx, born.ID, user, *born.MembershipID)
	if n := e.count(t, ctx, `SELECT count(*) FROM public.organizations WHERE id = $1 AND tier = 'community' AND managed_by = 'stripe'
AND settings::text = '{}' AND description IS NULL AND stripe_customer_id IS NULL AND onboarding_integration_skipped_at IS NULL`, born.ID); n != 1 {
		t.Fatal("the public row differs from go-api's onboarding row")
	}
	var changes, metadata []byte
	var resource, action, stored, status string
	var actor uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT user_id, action, resource_type, description, status, changes::text, request_metadata::text
FROM public.audit_logs WHERE org_id = $1`, born.ID).Scan(&actor, &action, &resource, &stored, &status, &changes, &metadata); err != nil {
		t.Fatalf("go-api audit row: %v", err)
	}
	if actor != user || action != "create" || resource != "organization" || stored != description || status != "success" {
		t.Fatalf("go-api audit row: %v %q %q %q %q", actor, action, resource, stored, status)
	}
	var change struct {
		Name string `json:"organization_name"`
	}
	if err := json.Unmarshal(changes, &change); err != nil || change.Name != "Acme Ops" {
		t.Fatalf("changes %s: %v", changes, err)
	}
	if !strings.Contains(string(metadata), "192.0.2.1") {
		t.Fatalf("request metadata %s", metadata)
	}
	if n := e.auditRows(t, ctx, "auth.membership.added", user.String()); n != 1 {
		t.Fatalf("auth.membership.added rows = %d, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.organization.created' AND organization_id = $1 AND actor_principal_id = $2`, born.ID, user); n != 1 {
		t.Fatalf("auth.organization.created rows = %d, want 1", n)
	}
}

func TestOnboardOrgAuditFailureWritesNothing(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	user := e.orgless(t, ctx)
	e.failWhen(t, ctx, "auth.security_audit_events", `NEW.subject_principal_id = '`+user.String()+`' AND NEW.event_type = 'auth.membership.added'`)
	before := e.orgWrites(t, ctx)
	if _, err := e.owned.OnboardOrganization(ctx, OnboardOrganizationRequest{Principal: user, Name: "Fails"}); err == nil {
		t.Fatal("the commit succeeded past a failing audit row")
	}
	if after := e.orgWrites(t, ctx); after != before {
		t.Fatalf("rows %v -> %v after a failed commit", before, after)
	}
}

func TestOnboardOrgPublicAuditFailureWritesNothing(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	user := e.orgless(t, ctx)
	e.failWhen(t, ctx, "public.audit_logs", `NEW.user_id = '`+user.String()+`'`)
	before := e.orgWrites(t, ctx)
	if _, err := e.owned.OnboardOrganization(ctx, OnboardOrganizationRequest{Principal: user, Name: "Fails Too"}); err == nil {
		t.Fatal("the commit succeeded past a failing go-api audit row")
	}
	if after := e.orgWrites(t, ctx); after != before {
		t.Fatalf("rows %v -> %v after a failed commit", before, after)
	}
}

func TestOnboardOrgRefusesAMember(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	t.Run("public row only", func(t *testing.T) {
		user := e.orgless(t, ctx)
		if _, err := e.admin.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), user, e.seedOrg(t, ctx)); err != nil {
			t.Fatal(err)
		}
		if _, err := e.owned.OnboardOrganization(ctx, OnboardOrganizationRequest{Principal: user, Name: "Second"}); !errors.Is(err, ErrOrganizationMember) {
			t.Fatalf("err = %v, want ErrOrganizationMember", err)
		}
	})
	t.Run("auth row only", func(t *testing.T) {
		user, org := e.joinFixture(t, ctx)
		if err := joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: org, Role: "member"}, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := e.admin.Exec(ctx, `DELETE FROM public.memberships WHERE user_id = $1`, user); err != nil {
			t.Fatal(err)
		}
		if _, err := e.owned.OnboardOrganization(ctx, OnboardOrganizationRequest{Principal: user, Name: "Third"}); !errors.Is(err, ErrOrganizationMember) {
			t.Fatalf("err = %v, want ErrOrganizationMember", err)
		}
	})
}

func TestOnboardOrgRefusesAnInactiveAccount(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	refused := func(t *testing.T, user uuid.UUID) {
		t.Helper()
		if _, err := e.owned.OnboardOrganization(ctx, OnboardOrganizationRequest{Principal: user, Name: "Nope"}); !errors.Is(err, ErrAccountInactive) {
			t.Fatalf("err = %v, want ErrAccountInactive", err)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM public.memberships WHERE user_id = $1`, user); n != 0 {
			t.Fatalf("memberships = %d, want 0", n)
		}
	}
	t.Run("public row inactive", func(t *testing.T) {
		user := e.orgless(t, ctx)
		if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, user); err != nil {
			t.Fatal(err)
		}
		refused(t, user)
	})
	t.Run("principal disabled", func(t *testing.T) {
		user := e.seedUser(t, ctx, uniqueEmail("inactive"), false, true)
		e.project(t, ctx, user)
		if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_active = true WHERE id = $1`, user); err != nil {
			t.Fatal(err)
		}
		refused(t, user)
	})
}

func TestOnboardOrgRaceOneOrg(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	user := e.orgless(t, ctx)
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = e.owned.OnboardOrganization(ctx, OnboardOrganizationRequest{Principal: user, Name: "Race"})
		}()
	}
	wg.Wait()
	won := 0
	for _, err := range errs {
		switch {
		case err == nil:
			won++
		case !errors.Is(err, ErrOrganizationMember):
			t.Fatalf("a loser's err = %v", err)
		}
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.memberships WHERE user_id = $1`, user); won != 1 || n != 1 {
		t.Fatalf("won %d, memberships %d, want 1 and 1", won, n)
	}
}

// The purge leaves the suspended auth row, so the old slug stays taken in auth only.
func TestOnboardOrgSlugOfAPurgedOrgGetsASuffix(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	user := e.orgless(t, ctx)
	slug := registerorg.OnboardSlug("Second Life", user)
	first := e.seedOrg(t, ctx)
	if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET slug = $2, name = 'Second Life' WHERE id = $1`, first, slug); err != nil {
		t.Fatal(err)
	}
	if err := e.store.ProjectOrganization(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.organizations WHERE id = $1`, first); err != nil {
		t.Fatal(err)
	}
	born, err := e.owned.OnboardOrganization(ctx, OnboardOrganizationRequest{Principal: user, Name: "Second Life"})
	if err != nil {
		t.Fatalf("the slug of a purged organization kept the user orgless: %v", err)
	}
	if !strings.HasPrefix(born.Slug, slug+"-") || len(born.Slug) != len(slug)+9 {
		t.Fatalf("slug %q, want %q with one 8 hex suffix", born.Slug, slug)
	}
	e.wantBothOrgRows(t, ctx, born.ID, born.Slug, "Second Life")
}

func TestBirthSlugTakenInTheCommitIsACollision(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	taken := e.seedOrg(t, ctx)
	var slug string
	if err := e.admin.QueryRow(ctx, `SELECT slug FROM public.organizations WHERE id = $1`, taken).Scan(&slug); err != nil {
		t.Fatal(err)
	}
	user := e.orgless(t, ctx)
	b := newOrganizationBirth(slug, "Late", &user)
	err := audit.Commit(ctx, e.runtime, e.owned.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error { return e.owned.insertOrganizationBirth(ctx, tx, b) },
		Audit: organizationAudit("auth.organization.created", "organization_created", b.ID.String()),
		Events: []audit.OutboxEvent{{AggregateType: "organization", AggregateID: provisioning.WireID("organization", b.ID.String()),
			EventType: "organization.created", Change: map[string]any{"status": "active"}}},
	})
	if !errors.Is(err, ErrOrganizationSlugCollision) {
		t.Fatalf("err = %v, want ErrOrganizationSlugCollision", err)
	}
}

func TestCreateOrgWithOwnerOneCommit(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	actor, owner := e.orgless(t, ctx), e.orgless(t, ctx)
	slug := "made-" + owner.String()[:8]
	born, err := e.owned.CreateOrganization(ctx, CreateOrganizationRequest{Actor: actor, Slug: slug, Name: "Made", Settings: []byte(`{}`),
		Tier: "community", ManagedBy: "stripe", Owner: &owner, Authorize: allowCreate})
	if err != nil {
		t.Fatal(err)
	}
	e.wantBothOrgRows(t, ctx, born.ID, slug, "Made")
	e.wantOwner(t, ctx, born.ID, owner, *born.MembershipID)
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.organization.created' AND organization_id = $1 AND actor_principal_id = $2`, born.ID, actor); n != 1 {
		t.Fatalf("auth.organization.created rows naming the actor = %d, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.audit_logs WHERE org_id = $1`, born.ID); n != 0 {
		t.Fatalf("go-api audit rows = %d, want 0 as the local path", n)
	}
}

func TestCreateOrgNoOwnerWritesBothOrgRows(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	actor := e.orgless(t, ctx)
	slug := "empty-" + actor.String()[:8]
	born, err := e.owned.CreateOrganization(ctx, CreateOrganizationRequest{Actor: actor, Slug: slug, Name: "Empty", Settings: []byte(`{}`),
		Tier: "community", ManagedBy: "stripe", Authorize: allowCreate})
	if err != nil {
		t.Fatal(err)
	}
	e.wantBothOrgRows(t, ctx, born.ID, slug, "Empty")
	if born.MembershipID != nil {
		t.Fatalf("membership %v, want none", born.MembershipID)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.memberships WHERE org_id = $1`, born.ID); n != 0 {
		t.Fatalf("memberships = %d, want 0", n)
	}
}

func TestCreateOrgKeepsDescriptionSettingsTier(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	actor := e.orgless(t, ctx)
	description := "kept"
	born, err := e.owned.CreateOrganization(ctx, CreateOrganizationRequest{Actor: actor, Slug: "kept-" + actor.String()[:8], Name: "Kept", Description: &description,
		Settings: []byte(`{"a":1}`), Tier: "team", ManagedBy: "manual", Authorize: allowCreate})
	if err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.organizations WHERE id = $1 AND description = 'kept' AND settings::text = '{"a":1}' AND tier = 'team' AND managed_by = 'manual'`, born.ID); n != 1 {
		t.Fatal("the public row lost a field")
	}
}

func TestCreateOrgRefusesANonSuperuserOnTheTx(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	actor := e.orgless(t, ctx)
	sawTx := false
	before := e.orgWrites(t, ctx)
	_, err := e.owned.CreateOrganization(ctx, CreateOrganizationRequest{Actor: actor, Slug: "refused-" + actor.String()[:8], Name: "Refused", Settings: []byte(`{}`),
		Tier: "community", ManagedBy: "stripe", Authorize: func(context.Context, audit.TxOps) error {
			sawTx = true
			return ErrOrganizationDenied
		}})
	if !errors.Is(err, ErrOrganizationDenied) || !sawTx {
		t.Fatalf("err = %v, ran on the tx = %v", err, sawTx)
	}
	if after := e.orgWrites(t, ctx); after != before {
		t.Fatalf("rows %v -> %v after a refusal", before, after)
	}
}

// Authorize reads the platform roles, so a role write must wait for the verb,
// or the verb for it: the verb asks the shared lock before its other locks.
func TestCreateOrgRaceWithRoleRevoke(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	actor := e.orgless(t, ctx)
	tx, err := e.runtime.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, rule.WriteLock); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := e.owned.CreateOrganization(ctx, CreateOrganizationRequest{Actor: actor, Slug: "held-" + actor.String()[:8], Name: "Held", Settings: []byte(`{}`),
			Tier: "community", ManagedBy: "stripe", Authorize: allowCreate})
		done <- err
	}()
	for e.count(t, ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted`) == 0 {
		select {
		case err := <-done:
			t.Fatalf("the verb ended (%v) under an exclusive role write lock", err)
		case <-ctx.Done():
			t.Fatal("the verb never asked for the role write lock")
		case <-time.After(20 * time.Millisecond):
		}
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
