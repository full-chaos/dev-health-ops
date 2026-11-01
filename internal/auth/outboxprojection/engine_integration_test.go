//go:build integration

package outboxprojection

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

type env struct {
	admin    *pgxpool.Pool
	runtime  *pgxpool.Pool
	schema   authschema.ValidatedIdentifier
	identity *identity.Store
	engine   *Engine
}

var (
	closers []func()
	fenced  = map[bool]*env{}
	mu      sync.Mutex
)

func TestMain(m *testing.M) {
	code := m.Run()
	for i := len(closers) - 1; i >= 0; i-- {
		closers[i]()
	}
	os.Exit(code)
}

func getEnv(t *testing.T, fence bool) *env {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	if e := fenced[fence]; e != nil {
		return e
	}
	e, err := newEnv(t, fence)
	if err != nil {
		t.Fatal(err)
	}
	fenced[fence] = e
	return e
}

func newEnv(t *testing.T, fence bool) (*env, error) {
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
		return nil, err
	}
	closers = append(closers, admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("outboxprojection_runtime", instance)
	if err != nil {
		return nil, err
	}
	const password = "outboxprojection-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		return nil, err
	}
	closers = append(closers, func() { containers.DropRole(admin, role, func(string, ...any) {}) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
		return nil, err
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User, cfg.ConnConfig.Password = role, password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	closers = append(closers, runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		return nil, err
	}
	if fence {
		if _, err := restorefence.Init(ctx, admin, schema, restorefence.InitOptions{IfAbsent: true}); err != nil {
			return nil, err
		}
	}
	id, err := identity.New(identity.Config{Pool: runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		return nil, err
	}
	e := &env{admin: admin, runtime: runtime, schema: schema, identity: id}
	e.engine, err = New(Config{Pool: runtime, Schema: schema, ProjectOrganization: func(ctx context.Context, org uuid.UUID) (bool, error) {
		err := id.ProjectOrganization(ctx, org)
		if errors.Is(err, identity.ErrOrganizationNotFound) {
			return false, nil
		}
		return err == nil, err
	}})
	return e, err
}

func (e *env) org(t *testing.T) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(context.Background(), `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'projection')`, id, "org-"+id.String()[:8]); err != nil {
		t.Fatal(err)
	}
	return id
}

func (e *env) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%q: %v", sql, err)
	}
	return n
}

type spec struct {
	org      uuid.UUID
	source   string
	id       string
	revision int
	kind     string
	change   contracts.ExternalPushLifecycleChange
	at       time.Time
}

func (s spec) input() Input {
	return Input{Domain: DomainExternalPush, Event: contracts.ExternalPushLifecycleEvent{
		AggregateType: contracts.ExternalPushCredentialAggregate, AggregateID: s.id, EventType: s.kind,
		IdempotencyKey: fmt.Sprintf("external_push_credential:%s:%d", s.id, s.revision),
		CreatedAt:      contracts.Timestamp{Time: s.at},
		Payload:        contracts.ExternalPushLifecyclePayload{Revision: contracts.Revision(s.revision), OrganizationID: "org_" + s.org.String(), SourceID: s.source, Change: s.change},
	}}
}

func created(org uuid.UUID, source string, scopes ...string) spec {
	return spec{org: org, source: source, id: uuid.NewString(), revision: 1, kind: contracts.ExternalPushCreated, at: time.Now().UTC().Truncate(time.Microsecond),
		change: contracts.ExternalPushLifecycleChange{Status: "active", Fingerprint: "0123456789abcdef", Scopes: scopes}}
}

func (s spec) next(kind string, change contracts.ExternalPushLifecycleChange) spec {
	s.revision++
	s.kind, s.change = kind, change
	s.at = s.at.Add(time.Second)
	return s
}

func (e *env) project(t *testing.T, in Input) Result {
	t.Helper()
	res, err := e.engine.Project(context.Background(), in)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	return res
}

func mustOutcome(t *testing.T, res Result, want Outcome) {
	t.Helper()
	if res.Outcome != want {
		t.Fatalf("outcome %s (class %s), want %s", res.Outcome, res.Class, want)
	}
}

func TestProjectCreatesPrincipalRegistryAndProjectionAndNoCursor(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	c := created(org, "src-a", "ingest:write")
	mustOutcome(t, e.project(t, c.input()), pushlifecycle.Applied)
	if n := e.count(t, `SELECT count(*) FROM auth.push_sources s JOIN auth.principals p ON p.id = s.principal_id WHERE s.organization_id = $1 AND s.source_id = 'src-a' AND p.kind = 'external_source'`, org); n != 1 {
		t.Fatalf("external_source principals = %d, want 1", n)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE domain_owner = 'external_push' AND external_reference = $1 AND credential_class = 'external_push_ingest_token' AND hash_algorithm = 'fingerprint' AND fingerprint = '0123456789abcdef' AND scopes = ARRAY['ingest:write']::auth.credential_scope[] AND revoked_at IS NULL`, c.id); n != 1 {
		t.Fatalf("registry rows = %d, want 1", n)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection p WHERE external_reference = $1 AND revision = 1 AND max_seen_revision = 1 AND status = 'active'`, c.id); n != 1 {
		t.Fatalf("projection rows = %d, want 1", n)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.projection_cursor`); n != 0 {
		t.Fatalf("the one call touched the reader's cursor: %d rows", n)
	}
	second := created(org, "src-a", "ingest:write")
	mustOutcome(t, e.project(t, second.input()), pushlifecycle.Applied)
	if n := e.count(t, `SELECT count(*) FROM auth.push_sources WHERE organization_id = $1 AND source_id = 'src-a'`, org); n != 1 {
		t.Fatalf("a second credential of the source made %d principals", n)
	}
}

func TestProjectDuplicateAndStaleWriteNothingAndAConflictIsNotApplied(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	c := created(org, "src-b", "ingest:write", "ingest:status")
	e.project(t, c.input())
	scoped := c.next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write"}})
	mustOutcome(t, e.project(t, scoped.input()), pushlifecycle.Applied)
	before := e.count(t, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.credential_projection.decided'`)
	mustOutcome(t, e.project(t, scoped.input()), pushlifecycle.Duplicate)
	mustOutcome(t, e.project(t, c.input()), pushlifecycle.Stale)
	other := scoped
	other.change.Scopes = []string{"ingest:status"}
	res := e.project(t, other.input())
	mustOutcome(t, res, OutcomeConflict)
	if res.Class != ClassConflict {
		t.Fatalf("class %s", res.Class)
	}
	if after := e.count(t, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.credential_projection.decided'`); after != before {
		t.Fatalf("a duplicate, a stale delivery or a conflict wrote %d audit rows", after-before)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND scopes = ARRAY['ingest:write']::auth.credential_scope[]`, c.id); n != 1 {
		t.Fatalf("the conflicting body changed the registry")
	}
}

func TestProjectGapAndAPendingParentAreRetriesThatWriteNothing(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	c := created(org, "src-c", "ingest:write")
	gap := c
	gap.revision, gap.kind = 3, contracts.ExternalPushRevoked
	res := e.project(t, gap.input())
	mustOutcome(t, res, pushlifecycle.Gap)
	if res.Class != ClassRetry {
		t.Fatalf("class %s", res.Class)
	}
	parent := created(org, "src-c", "ingest:write")
	child := spec{org: org, source: "src-c", id: uuid.NewString(), revision: 1, kind: contracts.ExternalPushRotated, at: time.Now().UTC().Truncate(time.Microsecond),
		change: contracts.ExternalPushLifecycleChange{Status: "active", Fingerprint: "fedcba9876543210", Scopes: []string{"ingest:write"}, RotationParentID: parent.id}}
	mustOutcome(t, e.project(t, child.input()), pushlifecycle.ParentPending)
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference IN ($1, $2)`, c.id, child.id); n != 0 {
		t.Fatalf("a retry outcome wrote %d projection rows", n)
	}
	mustOutcome(t, e.project(t, parent.input()), pushlifecycle.Applied)
	mustOutcome(t, e.project(t, child.input()), pushlifecycle.Applied)
}

func TestProjectAParentSeenAndWrongIsRefusedFinalAndRecorded(t *testing.T) {
	e := getEnv(t, true)
	org, other := e.org(t), e.org(t)
	parent := created(other, "src-d", "ingest:write")
	e.project(t, parent.input())
	child := spec{org: org, source: "src-d", id: uuid.NewString(), revision: 1, kind: contracts.ExternalPushRotated, at: time.Now().UTC().Truncate(time.Microsecond),
		change: contracts.ExternalPushLifecycleChange{Status: "active", Fingerprint: "fedcba9876543210", Scopes: []string{"ingest:write"}, RotationParentID: parent.id}}
	// The parent lives in another organization: not found inside this one, so a pending parent.
	mustOutcome(t, e.project(t, child.input()), pushlifecycle.ParentPending)
	widen := created(org, "src-d", "ingest:write")
	e.project(t, widen.input())
	wide := spec{org: org, source: "src-d", id: uuid.NewString(), revision: 1, kind: contracts.ExternalPushRotated, at: time.Now().UTC().Truncate(time.Microsecond),
		change: contracts.ExternalPushLifecycleChange{Status: "active", Fingerprint: "fedcba9876543210", Scopes: []string{"ingest:write", "ingest:status"}, RotationParentID: widen.id}}
	res := e.project(t, wide.input())
	mustOutcome(t, res, pushlifecycle.RefusedWiden)
	if res.Class != ClassRefused {
		t.Fatalf("class %s", res.Class)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection_digest WHERE external_reference = $1 AND outcome = 'refused_widen'`, wide.id); n != 1 {
		t.Fatalf("the final refusal left %d digest rows, want 1 (the reader must not read it again)", n)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1`, wide.id); n != 0 {
		t.Fatal("a refused rotation wrote a registry row")
	}
}

func TestProjectRevokeWithNoReasonIsDomainRevokedAndAMappedReasonKeepsItsValue(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	a, b := created(org, "src-e", "ingest:write"), created(org, "src-e", "ingest:write")
	e.project(t, a.input())
	e.project(t, b.input())
	mustOutcome(t, e.project(t, a.next(contracts.ExternalPushRevoked, contracts.ExternalPushLifecycleChange{Status: "revoked"}).input()), pushlifecycle.Applied)
	rb := b.next(contracts.ExternalPushRevoked, contracts.ExternalPushLifecycleChange{Status: "revoked"}).input()
	rb.RevokeReason = "compromised"
	mustOutcome(t, e.project(t, rb), pushlifecycle.Applied)
	for id, reason := range map[string]string{a.id: "domain_revoked", b.id: "compromised"} {
		if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND revoked_at IS NOT NULL AND revoked_reason = $2`, id, reason); n != 1 {
			t.Errorf("credential %s: revoked_reason %s not stored", id, reason)
		}
	}
	mustOutcome(t, e.project(t, a.next(contracts.ExternalPushRevoked, contracts.ExternalPushLifecycleChange{Status: "revoked"}).next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write"}}).input()), pushlifecycle.RefusedTerminal)
}

func TestProjectRotationAppliesWhenTheParentRevokedFirst(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	parent := created(org, "src-f", "ingest:write")
	e.project(t, parent.input())
	rotatedAt := parent.at.Add(2 * time.Second)
	revoke := parent.next(contracts.ExternalPushRevoked, contracts.ExternalPushLifecycleChange{Status: "revoked"})
	revoke.at = rotatedAt
	mustOutcome(t, e.project(t, revoke.input()), pushlifecycle.Applied)
	child := spec{org: org, source: "src-f", id: uuid.NewString(), revision: 1, kind: contracts.ExternalPushRotated, at: rotatedAt,
		change: contracts.ExternalPushLifecycleChange{Status: "active", Fingerprint: "fedcba9876543210", Scopes: []string{"ingest:write"}, RotationParentID: parent.id}}
	mustOutcome(t, e.project(t, child.input()), pushlifecycle.Applied)
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND rotation_parent_id IS NULL AND revoked_at IS NULL`, child.id); n != 1 {
		t.Fatal("the child row must be live with no registry parent link")
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1 AND rotation_parent_reference = $2`, child.id, parent.id); n != 1 {
		t.Fatal("the lineage is not in credential_projection")
	}
}

func TestProjectNarrowsFromTheRegistryAndNeverWidens(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	c := created(org, "src-g", "ingest:write", "ingest:status")
	e.project(t, c.input())
	mustOutcome(t, e.project(t, c.next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write"}}).input()), pushlifecycle.Applied)
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND scopes = ARRAY['ingest:write']::auth.credential_scope[]`, c.id); n != 1 {
		t.Fatal("the registry scopes were not narrowed")
	}
}

func TestProjectAnOrganizationWithNoAuthRowIsProjectedAndAnUnknownOneIsRefused(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t) // only in public.organizations: the sign-in projection has not run
	if n := e.count(t, `SELECT count(*) FROM auth.organizations WHERE id = $1`, org); n != 0 {
		t.Skip("the organization is already projected")
	}
	mustOutcome(t, e.project(t, created(org, "src-h", "ingest:write").input()), pushlifecycle.Applied)
	if n := e.count(t, `SELECT count(*) FROM auth.organizations WHERE id = $1`, org); n != 1 {
		t.Fatal("the organization was not projected before the store step")
	}
	res := e.project(t, created(uuid.New(), "src-h", "ingest:write").input())
	mustOutcome(t, res, OutcomeOrganizationUnknown)
	if res.Class != ClassRefused {
		t.Fatalf("class %s", res.Class)
	}
}

func TestProjectAcrDomainKeepsItsClassAndOwnerSubject(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	c := created(org, "acr", "context:read")
	c.id = "cred_" + uuid.NewString()[:12]
	in := c.input()
	in.Domain, in.AcrClass, in.OwnerSubject = DomainAcr, "acr_browser_refresh_token", "sub-1"
	mustOutcome(t, e.project(t, in), pushlifecycle.Applied)
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry r JOIN auth.credential_projection p ON p.registry_id = r.id WHERE r.domain_owner = 'acr' AND r.credential_class = 'acr_browser_refresh_token' AND p.acr_class = 'acr_browser_refresh_token' AND p.owner_subject = 'sub-1'`); n < 1 {
		t.Fatal("the acr row lost its class or owner subject")
	}
}

func TestProjectTwoWritersOfOneCredentialApplyOnce(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	c := created(org, "src-i", "ingest:write")
	var wg sync.WaitGroup
	results := make([]Outcome, 4)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := e.engine.Project(context.Background(), c.input())
			if err != nil {
				t.Errorf("Project: %v", err)
			}
			results[i] = res.Outcome
		}()
	}
	wg.Wait()
	applied := 0
	for _, o := range results {
		if o == pushlifecycle.Applied {
			applied++
		}
	}
	if applied != 1 {
		t.Fatalf("outcomes %v: applied %d times, want once", results, applied)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1`, c.id); n != 1 {
		t.Fatalf("registry rows %d, want 1", n)
	}
}

func TestProjectOnAnUnfencedTimelineWritesNothing(t *testing.T) {
	e := getEnv(t, false)
	org := e.org(t)
	c := created(org, "src-j", "ingest:write")
	res := e.project(t, c.input())
	mustOutcome(t, res, OutcomeUnfenced)
	if res.Class != ClassUnfenced {
		t.Fatalf("class %s", res.Class)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection`); n != 0 {
		t.Fatalf("an unfenced call wrote %d projection rows", n)
	}
}

func TestProjectMalformedEventWritesNothing(t *testing.T) {
	e := getEnv(t, true)
	c := created(e.org(t), "", "ingest:write")
	res := e.project(t, c.input())
	mustOutcome(t, res, OutcomeMalformed)
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection_digest WHERE external_reference = $1`, c.id); n != 0 {
		t.Fatal("a malformed event left a digest row")
	}
}
