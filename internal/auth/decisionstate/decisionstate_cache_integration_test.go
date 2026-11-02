//go:build integration

package decisionstate

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisioncache"
	"github.com/full-chaos/dev-health-ops/internal/auth/grantend"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/revisionfeed"
)

func (e *env) collecting(t *testing.T, owned bool) *Reader {
	t.Helper()
	r, err := New(Config{Pool: e.runtime, Schema: e.schema, Users: authapi.PGUsers{Pool: e.runtime, AuthSchema: e.schema, PlatformRolesOwned: owned},
		CollectDependencies: true})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func depsOf(t *testing.T, snap authorization.GrantSnapshot) ([]decisioncache.Dependency, []string) {
	t.Helper()
	ds, ok := snap.(authapi.DependencySnapshot)
	if !ok {
		t.Fatal("a collecting reader gave no DependencySnapshot")
	}
	return ds.Dependencies(), ds.SetTypes()
}

func revisionOf(deps []decisioncache.Dependency, typ, id string) (int64, bool) {
	for _, d := range deps {
		if d.AggregateType == typ && d.AggregateID == id {
			return d.Revision, true
		}
	}
	return 0, false
}

// PLAN-10842-C cell (a): the revisions come from aggregate_revisions in the same REPEATABLE READ transaction as the
// state, so a revision committed after the transaction began is not seen by it.
func TestTheDependencyRevisionsAreReadInTheDecisionTransaction(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	user, org := e.user(t, ctx, true, false), e.org(t, ctx)
	r := e.collecting(t, false)
	prn := "prn_" + user.String()
	var first, second []decisioncache.Dependency
	if err := r.Read(ctx, func(tx authapi.DecisionTx) error {
		snap, err := tx.Snapshot(ctx, user, org.String())
		if err != nil {
			return err
		}
		first, _ = depsOf(t, snap)
		e.armed(t, ctx, `UPDATE auth.aggregate_revisions SET revision = revision + 1 WHERE aggregate_type = 'principal' AND aggregate_id = $1`, prn)
		snap, err = tx.Snapshot(ctx, user, org.String())
		second, _ = depsOf(t, snap)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	a, ok := revisionOf(first, "principal", prn)
	if !ok || a < 1 {
		t.Fatalf("principal dependency %+v, want the projected principal at revision >= 1", first)
	}
	if b, _ := revisionOf(second, "principal", prn); b != a {
		t.Fatalf("revision %d inside the transaction after a commit, want %d (one snapshot)", b, a)
	}
	if _, ok := revisionOf(first, "organization", "org_"+org.String()); !ok {
		t.Fatalf("no organization dependency in %+v", first)
	}
	after := e.snapshotWith(t, ctx, r, user, org.String())
	deps, _ := depsOf(t, after)
	if c, _ := revisionOf(deps, "principal", prn); c != a+1 {
		t.Fatalf("a new transaction reads revision %d, want %d", c, a+1)
	}
}

func (e *env) snapshotWith(t *testing.T, ctx context.Context, r *Reader, principal uuid.UUID, org string) authorization.GrantSnapshot {
	t.Helper()
	var snap authorization.GrantSnapshot
	if err := r.Read(ctx, func(tx authapi.DecisionTx) error {
		var err error
		snap, err = tx.Snapshot(ctx, principal, org)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return snap
}

// PLAN-10842-C cell (b), pm Q2: a grant row written with no audited event has no revision row: revision 0. The set tags
// of an organization read are there.
func TestAGrantWithNoRevisionRowIsRevisionZero(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	user, org := e.user(t, ctx, true, false), e.org(t, ctx)
	var grant uuid.UUID
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ($1, $2, 'viewer', 'repository', 'acme/zero')`, user, org)
	if err := e.admin.QueryRow(ctx, `SELECT id FROM auth.resource_grants WHERE principal_id = $1 AND organization_id = $2`, user, org).Scan(&grant); err != nil {
		t.Fatal(err)
	}
	deps, sets := depsOf(t, e.snapshotWith(t, ctx, e.collecting(t, false), user, org.String()))
	if rev, ok := revisionOf(deps, "resource_grant", "grt_"+grant.String()); !ok || rev != 0 {
		t.Fatalf("grant dependency %d %v in %+v, want revision 0", rev, ok, deps)
	}
	if !slices.Equal(sets, []string{"membership", "resource_grant"}) {
		t.Fatalf("set types %q", sets)
	}
}

// PLAN-10842-C cells (c) and (e), pm Q1: Unfed reads go-api's public rows on every call, and a go-api admin write (no
// outbox event) shows in the next read at once.
func TestUnfedReadsTheGoAPIRowsAndSeesAnAdminWriteAtOnce(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	user, org := e.user(t, ctx, true, false), e.org(t, ctx)
	e.exec(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at) VALUES ($1, $2, $3, 'admin', now())`, uuid.New(), user, org)
	r := e.collecting(t, false)
	read := func() authapi.UnfedState {
		t.Helper()
		var u authapi.UnfedState
		if err := r.Read(ctx, func(tx authapi.DecisionTx) error {
			var err error
			u, err = tx.Unfed(ctx, user, org.String())
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return u
	}
	u := read()
	if !u.UserFound || !u.UserActive || u.Superuser == nil || *u.Superuser || !u.MembershipRoleFound || u.MembershipRole != "admin" || u.NowUnixNano <= 0 {
		t.Fatalf("unfed %+v, want an active non-superuser admin with a clock", u)
	}
	e.exec(t, ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, user)
	e.exec(t, ctx, `UPDATE public.memberships SET role = 'member' WHERE user_id = $1 AND org_id = $2`, user, org)
	if u := read(); u.UserActive || u.MembershipRole != "member" {
		t.Fatalf("unfed %+v after the go-api writes, want inactive and member", u)
	}
}

// PLAN-10842-C cell (d): the dependencies a real snapshot reads are evicted by the real writer's event. A grant revoke
// committed through audit.Commit with grantend's event reaches the cache through the consumer.
func TestARevokedGrantEventEvictsTheAllowThatReadIt(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	user, owner, org := e.user(t, ctx, true, false), e.user(t, ctx, true, false), e.org(t, ctx)
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ($1, $2, 'viewer', 'repository', 'acme/revoked')`, user, org)
	var grant uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT id FROM auth.resource_grants WHERE principal_id = $1 AND organization_id = $2`, user, org).Scan(&grant); err != nil {
		t.Fatal(err)
	}
	marks := &lazyMarks{}
	cache, err := decisioncache.New(decisioncache.Config{Catalog: policycatalog.Default(), Watermarks: marks})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := revisionfeed.NewConsumer(revisionfeed.Config{Invalidator: cache})
	if err != nil {
		t.Fatal(err)
	}
	marks.w = consumer.Watermarks()
	stamp := cache.Begin()
	deps, sets := depsOf(t, e.snapshotWith(t, ctx, e.collecting(t, false), user, org.String()))
	k := lowKey(user, org)
	if got := cache.Put(k, authorization.Decision{Allowed: true, Reason: authorization.ReasonAllowedByRole, PolicyRevision: k.PolicyRevision},
		decisioncache.Read{Dependencies: deps, SetTypes: sets, Stamp: stamp}); got != decisioncache.Stored {
		t.Fatalf("Put = %q", got)
	}
	// The audit request id is a UUID, as identity/store_integration_test.go:406 builds it (request := uuid.NewString()).
	requestID := uuid.NewString()
	if err := audit.Commit(ctx, e.runtime, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `UPDATE auth.resource_grants SET revoked_at = now(), revoked_by = $2 WHERE id = $1`, grant, owner)
			return err
		},
		Audit:  grantend.AuditFor(org, user, grant, grantend.Actor{PrincipalID: &owner, RequestID: &requestID}),
		Events: []audit.OutboxEvent{grantend.EventFor(grant)},
	}); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	var (
		payload []byte
		id      string
	)
	if err := e.admin.QueryRow(ctx, `SELECT aggregate_id, payload FROM auth.auth_outbox_events WHERE aggregate_type = 'resource_grant'
AND aggregate_id = $1 ORDER BY id DESC LIMIT 1`, "grt_"+grant.String()).Scan(&id, &payload); err != nil {
		t.Fatal(err)
	}
	var p contracts.ProvisioningPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.Handle(ctx, contracts.ProvisioningEvent{AggregateType: "resource_grant", AggregateID: id, Payload: p}); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.Get(k); ok {
		t.Fatal("the allow that read the grant was served after the grant's revoke event")
	}
}

// PLAN-10842-C cell (g), pm Q6: with the platform roles owned by auth, the platform role read is a set (tag
// platform_role_assignment) and is_superuser is not in Unfed.
func TestOwnedPlatformRolesAreATagNotAnUnfedValue(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	user := e.user(t, ctx, true, true)
	r := e.collecting(t, true)
	_, sets := depsOf(t, e.snapshotWith(t, ctx, r, user, ""))
	if !slices.Contains(sets, "platform_role_assignment") {
		t.Fatalf("set types %q, want platform_role_assignment while owned", sets)
	}
	var u authapi.UnfedState
	if err := r.Read(ctx, func(tx authapi.DecisionTx) error {
		var err error
		u, err = tx.Unfed(ctx, user, "")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if u.Superuser != nil {
		t.Fatalf("superuser %v in Unfed while the platform roles are owned", *u.Superuser)
	}
	k := lowKey(user, uuid.New())
	cache, err := decisioncache.New(decisioncache.Config{Catalog: policycatalog.Default(), Watermarks: &lazyMarks{w: revisionfeed.NewWatermarks()}})
	if err != nil {
		t.Fatal(err)
	}
	if got := cache.Put(k, authorization.Decision{Allowed: true, Reason: authorization.ReasonAllowedByRole, PolicyRevision: k.PolicyRevision},
		decisioncache.Read{SetTypes: sets, Stamp: cache.Begin()}); got != decisioncache.Stored {
		t.Fatalf("Put = %q", got)
	}
	if err := cache.Invalidate(ctx, "platform_role_assignment", "pra_any", 1); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.Get(k); ok {
		t.Fatal("a platform role assignment event left the owned-role allow in the cache")
	}
}

type lazyMarks struct{ w *revisionfeed.Watermarks }

func (l *lazyMarks) Latest(typ, id string) (int64, bool) { return l.w.Latest(typ, id) }

func lowKey(user, org uuid.UUID) decisioncache.Key {
	var action string
	for _, a := range policycatalog.Default().Actions() {
		if a.RiskClass == policycatalog.RiskLow && a.CacheClass == policycatalog.CacheDecisionBound {
			action = a.ID
			break
		}
	}
	return decisioncache.Key{PrincipalID: "prn_" + user.String(), SessionID: "ses_cell", OrganizationID: org.String(), Action: action,
		ResourceKind: "organization", PolicyRevision: policycatalog.Default().Version()}
}

func (e *env) stateVersion(t *testing.T, ctx context.Context, r *Reader, principal uuid.UUID, org string) string {
	t.Helper()
	var v string
	if err := r.Read(ctx, func(tx authapi.DecisionTx) error {
		var err error
		v, err = tx.StateVersion(ctx, principal, org)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return v
}

// stepChanges runs each step and requires the state version to change after each one.
func (e *env) stepChanges(t *testing.T, ctx context.Context, r *Reader, principal uuid.UUID, org string, steps []struct {
	name string
	run  func()
}) {
	t.Helper()
	before := e.stateVersion(t, ctx, r, principal, org)
	for _, s := range steps {
		s.run()
		after := e.stateVersion(t, ctx, r, principal, org)
		if after == before {
			t.Fatalf("%s: the state version did not change (%s)", s.name, after)
		}
		before = after
	}
}

type step = struct {
	name string
	run  func()
}

// r1 P1, pm option C: an insert, an update and a delete of a grant of the pair each change the state version, with no
// outbox event.
func TestTheStateVersionChangesOnAGrantInsertUpdateAndDelete(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	user, owner, org := e.user(t, ctx, true, false), e.user(t, ctx, true, false), e.org(t, ctx)
	r := e.collecting(t, false)
	grant := uuid.New()
	e.stepChanges(t, ctx, r, user, org.String(), []step{
		{"insert", func() {
			e.armed(t, ctx, `INSERT INTO auth.resource_grants (id, principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ($1, $2, $3, 'viewer', 'repository', 'acme/state')`, grant, user, org)
		}},
		{"update", func() {
			e.armed(t, ctx, `UPDATE auth.resource_grants SET revoked_at = now(), revoked_by = $2 WHERE id = $1`, grant, owner)
		}},
		{"delete", func() { e.armed(t, ctx, `DELETE FROM auth.resource_grants WHERE id = $1`, grant) }},
	})
}

// r1 P1, pm option C: a raw write of the principal, organization, membership or platform role assignment rows, with no
// outbox event, changes the state version.
func TestTheStateVersionChangesOnRawWritesWithNoEvent(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	t.Run("principals", func(t *testing.T) {
		principal := uuid.New()
		e.stepChanges(t, ctx, e.collecting(t, false), principal, "", []step{
			{"insert", func() { e.armed(t, ctx, `INSERT INTO auth.principals (id, kind) VALUES ($1, 'user')`, principal) }},
			{"update", func() { e.armed(t, ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, principal) }},
			{"delete", func() { e.armed(t, ctx, `DELETE FROM auth.principals WHERE id = $1`, principal) }},
		})
	})
	t.Run("organizations", func(t *testing.T) {
		user, org := e.user(t, ctx, true, false), uuid.New()
		e.stepChanges(t, ctx, e.collecting(t, false), user, org.String(), []step{
			{"insert", func() {
				e.armed(t, ctx, `INSERT INTO auth.organizations (id, slug, name) VALUES ($1, $2, 'state-version')`, org, "sv-"+org.String()[:8])
			}},
			{"update", func() { e.armed(t, ctx, `UPDATE auth.organizations SET disabled_at = now() WHERE id = $1`, org) }},
			{"delete", func() { e.armed(t, ctx, `DELETE FROM auth.organizations WHERE id = $1`, org) }},
		})
	})
	t.Run("memberships", func(t *testing.T) {
		user, org := e.user(t, ctx, true, false), e.org(t, ctx)
		e.stepChanges(t, ctx, e.collecting(t, false), user, org.String(), []step{
			{"insert", func() {
				e.armed(t, ctx, `INSERT INTO auth.memberships (organization_id, principal_id, status, role, joined_at) VALUES ($1, $2, 'active', 'admin', now())`, org, user)
			}},
			{"update", func() {
				e.armed(t, ctx, `UPDATE auth.memberships SET role = 'member' WHERE organization_id = $1 AND principal_id = $2`, org, user)
			}},
			{"delete", func() {
				e.armed(t, ctx, `DELETE FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2`, org, user)
			}},
		})
	})
	t.Run("platform_role_assignments", func(t *testing.T) {
		user := e.user(t, ctx, true, false)
		e.stepChanges(t, ctx, e.collecting(t, true), user, "", []step{
			{"insert", func() {
				e.armed(t, ctx, `INSERT INTO auth.platform_role_assignments (principal_id, role_key) VALUES ($1, 'platform_admin')`, user)
			}},
			{"update", func() {
				e.armed(t, ctx, `UPDATE auth.platform_role_assignments SET expires_at = now() + interval '1 day' WHERE principal_id = $1`, user)
			}},
			{"delete", func() { e.armed(t, ctx, `DELETE FROM auth.platform_role_assignments WHERE principal_id = $1`, user) }},
		})
	})
}

// pm option C: VACUUM FREEZE keeps xmin, so the state version of unchanged rows is stable across it.
func TestTheStateVersionIsStableAcrossVacuumFreeze(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	user, org := e.user(t, ctx, true, false), e.org(t, ctx)
	e.armed(t, ctx, `INSERT INTO auth.memberships (organization_id, principal_id, status, role, joined_at) VALUES ($1, $2, 'active', 'admin', now())`, org, user)
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ($1, $2, 'viewer', 'repository', 'acme/frozen')`, user, org)
	r := e.collecting(t, true)
	before := e.stateVersion(t, ctx, r, user, org.String())
	e.exec(t, ctx, `VACUUM (FREEZE) auth.principals, auth.organizations, auth.memberships, auth.resource_grants, auth.platform_role_assignments`)
	if after := e.stateVersion(t, ctx, r, user, org.String()); after != before {
		t.Fatalf("the state version changed across VACUUM FREEZE: %s -> %s", before, after)
	}
}
