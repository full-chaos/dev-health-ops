//go:build integration

package apiservice

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/metric/noop"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/accessrouting"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrrevoke"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/authplane"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
)

// CHAOS-13728: the F8p2b weak-cover class (RISK-F8P2B-admin-1 section 8, WEAK) against the REAL decision point. Each
// cell serves one request on go-api's real Routes (the production protection, newProtection) twice over one migrated
// database: the "off" side runs the live GO_API_AUTHZ_ENFORCE set (admin off), the "on" side the same set plus admin
// (the F8p2b flip). Both sides ask one decision point: auth-service's authorize routes built by authruntime's
// production builder (AuthorizeRoutesWithoutFeed) over the same database, reached through go-api's real pep client
// with a workload token its caller gate verifies. No verdict is faked: the evaluator reads decisionstate.
//
// Covered: rows 21-26 (/api/v1/admin/users, admin-or), row 27 (pull-stripe), M2 (GET /api/v1/admin/impersonate/status,
// impersonation.go:321) and the in-handler HoldsPlatform sites users.go:159 and :381, guards.go:20, :46 and :72,
// telemetry/report.go:38 and acrcredentials/routes.go:136.

// wcLiveEnforce is the live go-api enforce set (deploy main 4e773d98 values.prod.yaml:1891-1892, helm rev 620).
const wcLiveEnforce = "settings,teams,billing,integrations,members"

const (
	wcOff = 0
	wcOn  = 1
)

var wcSideName = [2]string{"off", "on"}

// wcMode is one AUTH_PLATFORM_ROLES_OWNED setting of go-api and auth-service.
type wcMode struct {
	name               string
	goOwned, authOwned bool
}

// m_split is UNREACHABLE IN PROD: the chart renders authService.platformRolesOwned to go-api, auth-service and the
// jobs from one value and refuses an extraEnv override (deploy/helm/dev-health/auth_platform_roles_owned_test.go:86
// TestPlatformRolesOwnedCannotBeSetThroughExtraEnv; go-api-deployment.yaml:160). It is the one form in which the
// CHAOS-13209 class (a users-row superuser with no auth platform_admin assignment) changes its answer.
var wcModes = []wcMode{
	{name: "m_off", goOwned: false, authOwned: false},
	{name: "m_on", goOwned: true, authOwned: true},
	{name: "m_split_unreachable_in_prod", goOwned: false, authOwned: true},
}

// wcExpect is what a caller's cells assert.
type wcExpect int

const (
	// wcParity: the on answer equals the off answer, status and body, on every row.
	wcParity wcExpect = iota
	// wcMeasure: as wcParity; a difference is a STOP (P2 d, P6), never a fix.
	wcMeasure
	// wcSplitChange: the accepted change of RISK section 4 / card F2 (only in m_split): off answers as a platform admin,
	// on as a caller with no platform role, on the platform-decided rows.
	wcSplitChange
	// wcG3Change: the known change the F8p2b G3 START gate keeps at 0 rows: off answers as a caller with no platform
	// role (legacy rule.HoldsSQL reads auth.principals), on as a platform admin (the decision input reads public.users,
	// authapi/users.go:82 PublicHoldsSQL), on the platform-decided rows.
	wcG3Change
	// wcRecorded: no assert on the answers; the IMPROW lines record legacy vs enforced (the named rig mode
	// "imp-switch-off (not prod)").
	wcRecorded
	// wcImpGate: as wcParity; on every row but the live-row exempt ones (status, stop, handoff consume) both sides are
	// the Scope's 403 with no decision point call and no user-admin call (AUTH_PROXY_IMPERSONATION on, as in prod).
	wcImpGate
)

// wcSpec is one caller class.
type wcSpec struct {
	label string
	// role is the caller's membership role in its own org; "" is none.
	role string
	// tokenOrg is whether the token names the caller's own org (a member's, or a foreign org for a non-member).
	tokenOrg bool
	// publicSuperuser and assigned are the users row's is_superuser and the auth platform_admin assignment.
	publicSuperuser, assigned bool
	expect                    wcExpect
	// modes limits the class to these modes; nil is every mode.
	modes []string
	// prepare changes the seeded caller before its cells (a G3 principal state, a platform_owner grant).
	prepare func(t *testing.T, ctx context.Context, w *wcWorld, c *wcCaller)
	// kind is a caller that is not a plain user token: "anon", "imp", "delegated".
	kind string
	// allRows runs the class on every admin-namespace row (wcAllRows), not only the weak-cover rows.
	allRows bool
	// impSwitchOn serves the class on the go-api handlers with AUTH_PROXY_IMPERSONATION on and an upstream, as in prod.
	impSwitchOn bool
}

var wcSpecs = []wcSpec{
	{label: "plain_orgless"},
	{label: "pa_orgless", publicSuperuser: true, assigned: true},
	{label: "owner", role: "owner", tokenOrg: true},
	{label: "admin", role: "admin", tokenOrg: true},
	{label: "member", role: "member", tokenOrg: true},
	{label: "pa_foreign_org", tokenOrg: true, publicSuperuser: true, assigned: true},
	{label: "anon", kind: "anon"},
	// The impersonation classes (pm/acp 04:18-04:19Z) on every admin-namespace row. imp_switch_on is the go-api
	// impersonation row with AUTH_PROXY_IMPERSONATION on as in prod (live render deploy f50e9710 rev 621 :2709-2710):
	// a GATE. delegated_imp is the prod impersonation form, auth-service's delegated session: a GATE. imp_switch_off is
	// the named rig mode "imp-switch-off (not prod)": recorded, not a gate.
	{label: "imp_switch_off_not_prod", kind: "imp", publicSuperuser: true, assigned: true, allRows: true, expect: wcRecorded},
	{label: "imp_switch_on", kind: "imp", publicSuperuser: true, assigned: true, allRows: true, impSwitchOn: true, expect: wcImpGate},
	{label: "delegated", kind: "delegated", role: "owner", tokenOrg: true, expect: wcMeasure},
	{label: "delegated_imp", kind: "delegated", role: "owner", tokenOrg: true, allRows: true, impSwitchOn: true, expect: wcParity},
	{label: "su_13209", publicSuperuser: true, expect: wcParity},
	{label: "po_only", publicSuperuser: true, expect: wcParity, prepare: wcGrantOwner},
	{label: "g3a_principal_delete_refused", publicSuperuser: true, assigned: true, expect: wcMeasure, modes: []string{"m_on"}, prepare: wcDropPrincipal},
	{label: "g3b_principal_disabled", publicSuperuser: true, assigned: true, expect: wcG3Change, modes: []string{"m_on"}, prepare: wcDisablePrincipal},
	{label: "g3c_principal_service_account", publicSuperuser: true, assigned: true, expect: wcG3Change, modes: []string{"m_on"}, prepare: wcServiceAccountPrincipal},
	{label: "g3d_public_inactive", publicSuperuser: true, assigned: true, expect: wcMeasure, modes: []string{"m_on"}, prepare: wcPublicInactive},
}

// wcExpectIn is the spec's expectation in mode m: the CHAOS-13209 class (su_13209, po_only) changes only in m_split.
func (s wcSpec) wcExpectIn(m wcMode) wcExpect {
	if (s.label == "su_13209" || s.label == "po_only") && m.goOwned != m.authOwned {
		return wcSplitChange
	}
	return s.expect
}

func (s wcSpec) inMode(m wcMode) bool { return s.modes == nil || slices.Contains(s.modes, m.name) }

// wcCaller is one seeded caller: one user, and per side its org, targets, token and session.
type wcCaller struct {
	spec wcSpec
	user uuid.UUID
	// sideUser is the caller's user per side: the same user, but for imp, one platform admin per side (each with its
	// own live impersonation row, so a stop on one side does not end the other's).
	sideUser [2]uuid.UUID
	// victim is a per-side org the platform org rows act on (delete included).
	victim  [2]uuid.UUID
	org     [2]uuid.UUID
	targets [2]map[string]uuid.UUID
	session [2]uuid.UUID
	token   [2]string
}

// wcTargets are the users each caller's rows act on, per side, all members (role member) of the caller's org except
// add, who is no member.
var wcTargets = []string{"get", "patch", "password", "delete", "add", "role", "remove", "transfer"}

// wcSides is one decision point and the two go-api handlers (off, on) that ask it, each with its user-admin stub.
type wcSides struct {
	plane    *authplane.Authorize
	handlers [2]http.Handler
	admins   [2]*wcUsersAdmin
}

type wcWorld struct {
	mode    wcMode
	env     *platformroleseam.Env
	key     authplane.Key
	main    wcSides
	impOn   wcSides
	namesMu sync.RWMutex
	names   map[string]string
	ids     *identity.Store
}

func (w *wcWorld) setName(id uuid.UUID, name string) {
	w.namesMu.Lock()
	defer w.namesMu.Unlock()
	w.names[id.String()] = name
}

func (w *wcWorld) nameOf(id string) (string, bool) {
	w.namesMu.RLock()
	defer w.namesMu.RUnlock()
	name, ok := w.names[id]
	return name, ok
}

// newSides starts a decision point over the world's database (its state wrapped by wrap when set) and the off and on
// go-api handlers that ask it.
func (w *wcWorld) newSides(t *testing.T, ctx context.Context, wrap func(authapi.DecisionState) authapi.DecisionState) wcSides {
	t.Helper()
	var s wcSides
	s.plane = authplane.StartAuthorize(t, ctx, w.env.URI, w.key, authplane.AuthorizeOptions{PlatformRolesOwned: w.mode.authOwned, UsersOwned: true, Wrap: wrap})
	s.handlers[wcOff], s.admins[wcOff] = w.goAPI(t, s.plane, wcLiveEnforce, AuthProxyConfig{})
	s.handlers[wcOn], s.admins[wcOn] = w.goAPI(t, s.plane, wcLiveEnforce+",admin", AuthProxyConfig{})
	return s
}

// impSwitchOnSides are the main decision point's go-api handlers with AUTH_PROXY_IMPERSONATION on and an upstream (a
// loopback stub that answers 503), as prod runs them: the Scope refuses an impersonating admin's own bearer.
func (w *wcWorld) impSwitchOnSides(t *testing.T) wcSides {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		http.Error(rw, `{"detail":"stub upstream"}`, http.StatusServiceUnavailable)
	}))
	t.Cleanup(upstream.Close)
	origin, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := AuthProxyConfig{Upstream: origin, Impersonation: true}
	if !proxy.ImpersonationOn() {
		t.Fatal("the proxy config does not turn impersonation through auth-service on")
	}
	s := wcSides{plane: w.main.plane}
	s.handlers[wcOff], s.admins[wcOff] = w.goAPI(t, s.plane, wcLiveEnforce, proxy)
	s.handlers[wcOn], s.admins[wcOn] = w.goAPI(t, s.plane, wcLiveEnforce+",admin", proxy)
	return s
}

type wcRevoker struct{}

// Revoke admits the admin actor only, so the actor the HoldsPlatform site chose is the answer (204 or 404).
func (wcRevoker) Revoke(_ context.Context, _, _, actorType, _ string) error {
	if actorType == acrrevoke.ActorAdmin {
		return nil
	}
	return acrrevoke.ErrNotFound
}

func newWCWorld(t *testing.T, ctx context.Context, mode wcMode) *wcWorld {
	t.Helper()
	w := &wcWorld{mode: mode, env: platformroleseam.Start(ctx, t, mode.authOwned), key: authplane.NewKey(t), names: map[string]string{}}
	var err error
	if w.ids, err = identity.New(identity.Config{Pool: w.env.Admin, Schema: w.env.Schema}); err != nil {
		t.Fatal(err)
	}
	var wrap func(authapi.DecisionState) authapi.DecisionState
	if wcPlantMainCells {
		var plants []wcPlant
		for _, p := range wcPlants {
			if wcModes[p.mode].name == mode.name {
				plants = append(plants, p)
			}
		}
		wrap = w.plantWrap(plants)
	}
	w.main = w.newSides(t, ctx, wrap)
	w.impOn = w.impSwitchOnSides(t)
	return w
}

// wcUsersAdmin stands in for go-api's client of auth-service's user-admin routes (deps.UsersAdmin): with users owned,
// the admin user writes after the guard go there. It records every call; a guard that lets a write through on one
// side only is a call count that differs. Rig limit: the post-guard body is this stub's answer on both sides.
type wcUsersAdmin struct {
	mu    sync.Mutex
	calls []string
}

var _ authprovision.UsersAdmin = (*wcUsersAdmin)(nil)

func (a *wcUsersAdmin) record(method string, actor, principal uuid.UUID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, method+" actor="+actor.String()+" principal="+principal.String())
}

func (a *wcUsersAdmin) take() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := a.calls
	a.calls = nil
	return out
}

func (a *wcUsersAdmin) CreateUser(_ context.Context, in authprovision.UserCreateRequest) (authprovision.CreatedUser, error) {
	a.record("CreateUser", in.Actor, uuid.Nil)
	return authprovision.CreatedUser{ID: uuid.New(), Email: in.Email}, nil
}

func (a *wcUsersAdmin) UpdateUser(_ context.Context, in authprovision.UserUpdateRequest) error {
	a.record("UpdateUser", in.Actor, in.Principal)
	return nil
}

func (a *wcUsersAdmin) SetUserPassword(_ context.Context, in authprovision.UserPasswordRequest) error {
	a.record("SetUserPassword", in.Actor, in.Principal)
	return nil
}

func (a *wcUsersAdmin) DeleteUser(_ context.Context, in authprovision.UserDeleteRequest) (authprovision.DeletedUser, error) {
	a.record("DeleteUser", in.Actor, in.Principal)
	return authprovision.DeletedUser{WasActive: true}, nil
}

// goAPI is go-api's handler as service.go builds it: newProtection over the database, the user access tokens routed by
// the endpoint-profile rows, the Enforcer over the profile rows with the enforce set, the production scope layers.
// Users are owned (AUTH_USERS_OWNED, as in prod) with the stub user-admin client.
func (w *wcWorld) goAPI(t *testing.T, plane *authplane.Authorize, enforce string, proxy AuthProxyConfig) (http.Handler, *wcUsersAdmin) {
	t.Helper()
	protected, err := newProtection(apiTestConfig(), w.env.Admin, w.mode.goOwned, proxy, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	if err := accessrouting.RouteUserTokens(protected.auth, w.key.Tokens.Verifier(t), noop.NewMeterProvider().Meter("wc"), quietLogger()); err != nil {
		t.Fatal(err)
	}
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		t.Fatal(err)
	}
	enf, err := policy.NewEnforcer(policy.EnforceConfig{Decider: plane.Client, Rows: rows,
		Switch: authzenforce.Parse(authzenforce.GoAPI, enforce), Logger: quietLogger()})
	if err != nil {
		t.Fatal(err)
	}
	protected.guard.SetEnforcer(enf)
	admin := &wcUsersAdmin{}
	deps := Deps{Pool: w.env.Admin, ClickHouse: unreachableClickHouse(t), Auth: protected.auth, Guard: protected.guard,
		PlatformRolesOwned: w.mode.goOwned, UsersOwned: true, UsersAdmin: admin, AuthSchema: authplane.Schema, ACRRevoker: wcRevoker{}, AuthProxy: proxy}
	layers := []func(http.Handler) http.Handler{protected.auth.ReadOnce, protected.scope.Principal, protected.scope.OrgScope, protected.scope.Impersonation}
	// As service.go builds it: newServer skips the scope layers on the proxied routes auth-service authenticates.
	server, err := newServer(apiTestConfig(), quietLogger(), Routes(deps, quietLogger()), nil, proxy.ScopeExemptRoutes(), layers...)
	if err != nil {
		t.Fatal(err)
	}
	return server.Handler(), admin
}

// exec runs one statement; bypass runs it with the triggers off (foreign keys and the audit commit guard included),
// for a principal state no writer makes (P2 c: a user's principal row of kind service_account).
func (w *wcWorld) exec(t *testing.T, ctx context.Context, bypass bool, sql string, args ...any) {
	t.Helper()
	tx, err := w.env.Admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if bypass {
		if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func (w *wcWorld) org(t *testing.T, ctx context.Context, name string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	pgseed.Org(ctx, t, w.env.Admin, id.String(), "free")
	if err := w.ids.ProjectOrganization(ctx, id); err != nil {
		t.Fatal(err)
	}
	w.setName(id, name)
	return id
}

// user is a public user with an auth principal (identity's backfill) and, when assigned, the platform_admin assignment.
func (w *wcWorld) user(t *testing.T, ctx context.Context, name string, publicSuperuser, assigned bool) uuid.UUID {
	t.Helper()
	id := w.env.User(ctx, t, publicSuperuser, assigned)
	w.setName(id, name)
	return id
}

func (w *wcWorld) member(t *testing.T, ctx context.Context, user, org uuid.UUID, role string) {
	t.Helper()
	pgseed.Membership(ctx, t, w.env.Admin, uuid.NewString(), user.String(), org.String(), role)
}

func (w *wcWorld) sessionRow(t *testing.T, ctx context.Context, principal uuid.UUID) uuid.UUID {
	t.Helper()
	id := uuid.New()
	hash := sha256.Sum256([]byte("c13728-session:" + id.String()))
	w.exec(t, ctx, false, `INSERT INTO auth.sessions (id, principal_id, token_hash, expires_at, legacy_token_version)
VALUES ($1, $2, $3, now() + interval '2 hours', 0)`, id, principal, hash[:])
	return id
}

// seed makes the caller of spec: its user, per side its org (its membership there when it has a role), its targets,
// its session and its token. imp and delegated get their own session shapes.
func (w *wcWorld) seed(t *testing.T, ctx context.Context, spec wcSpec) *wcCaller {
	t.Helper()
	c := &wcCaller{spec: spec}
	if spec.kind != "anon" {
		c.user = w.user(t, ctx, spec.label+".user", spec.publicSuperuser, spec.assigned)
	}
	c.sideUser = [2]uuid.UUID{c.user, c.user}
	if spec.kind == "imp" {
		c.sideUser[wcOn] = w.user(t, ctx, spec.label+".user", spec.publicSuperuser, spec.assigned)
	}
	var impOrg uuid.UUID
	if spec.kind == "imp" {
		impOrg = w.org(t, ctx, spec.label+".org")
	}
	for side := range 2 {
		if spec.kind == "imp" {
			c.org[side] = impOrg
		} else {
			c.org[side] = w.org(t, ctx, spec.label+".org")
		}
		if spec.role != "" {
			w.member(t, ctx, c.user, c.org[side], spec.role)
		}
		c.victim[side] = w.org(t, ctx, spec.label+".victim_org")
		c.targets[side] = map[string]uuid.UUID{}
		for _, name := range wcTargets {
			target := w.user(t, ctx, spec.label+".target_"+name, false, false)
			// The list rows search this tag, so a list answers this caller's targets only, whatever else was seeded.
			w.exec(t, ctx, false, `UPDATE users SET full_name = $2 WHERE id = $1`, target, wcSearchTag(spec.label)+name)
			if name != "add" {
				w.member(t, ctx, target, c.org[side], "member")
			}
			c.targets[side][name] = target
		}
		if spec.kind == "anon" || spec.kind == "delegated" {
			continue
		}
		c.session[side] = w.sessionRow(t, ctx, c.sideUser[side])
		claims := edtokens.Claims{UserID: c.sideUser[side], SessionID: &c.session[side]}
		if spec.tokenOrg {
			claims.OrgID = c.org[side].String()
		}
		c.token[side] = w.key.Tokens.MustMint(t, claims)
	}
	if spec.kind == "imp" {
		// One live go-api impersonation row per side's platform admin, on that side's member of the shared org.
		for side := range 2 {
			pgseed.ImpersonationSession(ctx, t, w.env.Admin, uuid.NewString(), c.sideUser[side].String(), c.targets[side]["get"].String(),
				impOrg.String(), "member", time.Now().Add(time.Hour), nil)
		}
	}
	if spec.kind == "delegated" {
		w.delegate(t, ctx, c)
	}
	if spec.prepare != nil {
		spec.prepare(t, ctx, w, c)
	}
	return c
}

// delegate gives the caller (the delegation's target, owner of its org) one delegated session per side, each made by
// its own platform admin actor (one live delegation per actor), and a token with the act claim naming it.
func (w *wcWorld) delegate(t *testing.T, ctx context.Context, c *wcCaller) {
	t.Helper()
	for side := range 2 {
		actor := w.user(t, ctx, c.spec.label+".actor", true, true)
		base := w.sessionRow(t, ctx, actor)
		id := uuid.New()
		hash := sha256.Sum256([]byte("c13728-delegated:" + id.String()))
		w.exec(t, ctx, false, `INSERT INTO auth.sessions (id, principal_id, token_hash, expires_at, legacy_token_version,
organization_id, actor_principal_id, delegated_from_session_id, delegation_reason, delegation_permitted_actions)
VALUES ($1, $2, $3, now() + interval '10 minutes', 0, $4, $5, $6, 'chaos-13728 cell', ARRAY['members.manage'])`,
			id, c.user, hash[:], c.org[side], actor, base)
		c.session[side] = id
		c.token[side] = w.key.Tokens.MustMint(t, edtokens.Claims{UserID: c.user, OrgID: c.org[side].String(), Actor: &actor, SessionID: &id})
	}
}

func wcGrantOwner(t *testing.T, ctx context.Context, w *wcWorld, c *wcCaller) {
	t.Helper()
	roles, err := platformrole.New(platformrole.Config{Pool: w.env.Admin, Schema: w.env.Schema})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := roles.GrantOwner(ctx, c.user); err != nil {
		t.Fatalf("grant platform_owner: %v", err)
	}
}

// wcDropPrincipal tries to delete the principal row (P2 a): the schema refuses it, so "a live platform_admin assignment
// whose principal row is missing" cannot exist. auth.users.principal_id (auth migration 0001:42) and
// platform_role_assignments.principal_id (0034:40-43, which replaced 0002:50's CASCADE) are ON DELETE RESTRICT. The
// caller stays a platform admin, and its cells measure parity.
func wcDropPrincipal(t *testing.T, ctx context.Context, w *wcWorld, c *wcCaller) {
	t.Helper()
	_, err := w.env.Admin.Exec(ctx, `DELETE FROM auth.principals WHERE id = $1`, c.user)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23001" {
		t.Fatalf("delete of an assignment holder's principal: %v, want the RESTRICT refusal 23001", err)
	}
	var principals, assignments int
	if err := w.env.Admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM auth.principals WHERE id = $1),
(SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1)`, c.user).Scan(&principals, &assignments); err != nil {
		t.Fatal(err)
	}
	if principals != 1 || assignments != 1 {
		t.Fatalf("after the refused delete: %d principals, %d assignments; want 1 and 1", principals, assignments)
	}
	t.Logf("RESTRICT|13728|g3a|principal_delete=%s|principals=1|assignments=1", pgErr.Code)
}

func wcDisablePrincipal(t *testing.T, ctx context.Context, w *wcWorld, c *wcCaller) {
	t.Helper()
	w.env.Disable(ctx, t, c.user)
}

func wcServiceAccountPrincipal(t *testing.T, ctx context.Context, w *wcWorld, c *wcCaller) {
	t.Helper()
	w.exec(t, ctx, true, `UPDATE auth.principals SET kind = 'service_account' WHERE id = $1`, c.user)
}

func wcPublicInactive(t *testing.T, ctx context.Context, w *wcWorld, c *wcCaller) {
	t.Helper()
	pgseed.SetUserState(ctx, t, w.env.Admin, c.user.String(), false, true, 0)
}

// wcRow is one request of a caller's cell set.
type wcRow struct {
	name, method, path, body, xOrg string
	// platformDecided: the row's admin answer is the platform decision alone (no org scope read decides it).
	platformDecided bool
	// action is the platform action a platform admin's on side must be judged allowed for.
	action string
	// orgClaim: the site is reached only with an org claim (acr revoke answers an org-less token before it).
	orgClaim bool
	// superuserBody: the body writes is_superuser; with users owned the refusal of users.go:281/:454 must not answer.
	superuserBody bool
	// liveRowExempt: the Scope serves it while a go-api impersonation row is live (liveRowExemptPatterns).
	liveRowExempt bool
}

// wcSearchTag is the full_name prefix of a caller's targets; the list rows search it.
func wcSearchTag(label string) string { return "wc13728-" + label + "-" }

func wcRows(c *wcCaller, side int) []wcRow {
	org := c.org[side].String()
	var rows []wcRow
	tg := c.targets[side]
	tag := c.spec.label + "-side" + wcSideName[side]
	members := "/api/v1/admin/orgs/" + org
	rows = []wcRow{
		{name: "r21_list", method: http.MethodGet, path: "/api/v1/admin/users?search=" + wcSearchTag(c.spec.label), platformDecided: true, action: "admin.users"},
		{name: "r21_list_xorg", method: http.MethodGet, path: "/api/v1/admin/users?search=" + wcSearchTag(c.spec.label), xOrg: org, action: "admin.users"},
		{name: "r22_get", method: http.MethodGet, path: "/api/v1/admin/users/" + tg["get"].String(), platformDecided: true, action: "admin.users"},
		{name: "r23_create", method: http.MethodPost, path: "/api/v1/admin/users", body: `{"email":"c13728-` + tag + `@example.test"}`, platformDecided: true, action: "admin.users"},
		{name: "r23_create_xorg", method: http.MethodPost, path: "/api/v1/admin/users", xOrg: org,
			body: `{"email":"c13728x-` + tag + `@example.test","role":"member"}`, action: "admin.users"},
		{name: "r24_patch", method: http.MethodPatch, path: "/api/v1/admin/users/" + tg["patch"].String(), body: `{"full_name":"Renamed 13728"}`, platformDecided: true, action: "admin.users"},
		{name: "r25_password", method: http.MethodPost, path: "/api/v1/admin/users/" + tg["password"].String() + "/password",
			body: `{"admin_password":"Admin13728!pw","password":"Target13728!pw"}`, platformDecided: true, action: "admin.users"},
		{name: "r26_delete", method: http.MethodDelete, path: "/api/v1/admin/users/" + tg["delete"].String(), platformDecided: true, action: "admin.users"},
		// The users-owned branches that sit before a HoldsPlatform site (users.go:281, :454, :689), on their shapes.
		{name: "r23_create_superuser", method: http.MethodPost, path: "/api/v1/admin/users",
			body: `{"email":"c13728su-` + tag + `@example.test","is_superuser":true}`, platformDecided: true, action: "admin.users", superuserBody: true},
		{name: "r24_patch_superuser", method: http.MethodPatch, path: "/api/v1/admin/users/" + tg["patch"].String(), body: `{"is_superuser":true}`,
			platformDecided: true, action: "admin.users", superuserBody: true},
		{name: "r26_delete_force", method: http.MethodDelete, path: "/api/v1/admin/users/" + tg["delete"].String() + "?force=true", platformDecided: true, action: "admin.users"},
		{name: "r27_pull_stripe", method: http.MethodPost, path: "/api/v1/billing/plans/pull-stripe", platformDecided: true, action: "admin.billing"},
		{name: "m2_impersonate_status", method: http.MethodGet, path: "/api/v1/admin/impersonate/status", platformDecided: true, action: "admin.impersonate", liveRowExempt: true},
		{name: "site_telemetry_report", method: http.MethodPost, path: "/api/v1/telemetry/report", platformDecided: true, action: "admin.telemetry"},
		{name: "site_acr_revoke", method: http.MethodPost, path: "/api/v1/acr/credentials/cred13728/revoke", action: "admin.orgs", orgClaim: true},
		{name: "site_org_members_list", method: http.MethodGet, path: members + "/members", action: "admin.orgs"},
		{name: "site_org_members_add", method: http.MethodPost, path: members + "/members", body: `{"user_id":"` + tg["add"].String() + `","role":"member"}`, action: "admin.orgs"},
		{name: "site_org_invite", method: http.MethodPost, path: members + "/invites", body: `{"email":"inv13728-` + tag + `@example.test","role":"member"}`, action: "admin.orgs"},
		{name: "site_org_member_role", method: http.MethodPatch, path: members + "/members/" + tg["role"].String(), body: `{"role":"admin"}`, action: "admin.orgs"},
		{name: "site_org_member_remove", method: http.MethodDelete, path: members + "/members/" + tg["remove"].String(), action: "admin.orgs"},
		// Last: a transfer by an owner changes the caller's own role.
		{name: "site_org_transfer", method: http.MethodPost, path: members + "/transfer-ownership", body: `{"new_owner_user_id":"` + tg["transfer"].String() + `"}`, action: "admin.orgs"},
	}
	if c.spec.allRows {
		rows = append(rows, wcAllRows(c, side)...)
	}
	return rows
}

// wcNoSuchID is an id no seeded row has: the rows that name one answer after their guard.
const wcNoSuchID = "99999999-9999-4999-8999-999999999999"

// wcAllRows are the admin-namespace rows outside the weak-cover rows (RISK-F8P2B-admin-1 section 1 rows 1-20 and M1),
// for the impersonation classes. The org rows act on the caller's per-side victim org; stop is last.
func wcAllRows(c *wcCaller, side int) []wcRow {
	victim := "/api/v1/admin/orgs/" + c.victim[side].String()
	tag := c.spec.label + "-side" + wcSideName[side]
	sa := "/api/v1/admin/service-accounts/"
	return []wcRow{
		{name: "row01_orgs_list", method: http.MethodGet, path: "/api/v1/admin/orgs", action: "admin.orgs"},
		{name: "row02_orgs_create", method: http.MethodPost, path: "/api/v1/admin/orgs", body: `{"name":"wc13728 ` + tag + `"}`, action: "admin.orgs"},
		{name: "row03_org_get", method: http.MethodGet, path: victim, action: "admin.orgs"},
		{name: "row04_org_patch", method: http.MethodPatch, path: victim, body: `{"description":"wc13728"}`, action: "admin.orgs"},
		{name: "row06_flags_list", method: http.MethodGet, path: "/api/v1/admin/feature-flags", action: "admin.flags"},
		{name: "row07_flag_patch", method: http.MethodPatch, path: "/api/v1/admin/feature-flags/" + wcNoSuchID, body: `{"is_enabled":true}`, action: "admin.flags"},
		{name: "row08_overrides_list", method: http.MethodGet, path: victim + "/feature-overrides", action: "admin.flags"},
		{name: "row09_override_create", method: http.MethodPost, path: victim + "/feature-overrides", body: `{"feature_id":"` + wcNoSuchID + `"}`, action: "admin.flags"},
		{name: "row10_override_patch", method: http.MethodPatch, path: victim + "/feature-overrides/" + wcNoSuchID, body: `{"is_enabled":false}`, action: "admin.flags"},
		{name: "row11_override_delete", method: http.MethodDelete, path: victim + "/feature-overrides/" + wcNoSuchID, action: "admin.flags"},
		{name: "row12_audit_logs", method: http.MethodGet, path: "/api/v1/admin/platform/audit-logs", action: "admin.audit"},
		{name: "row13_stats", method: http.MethodGet, path: "/api/v1/admin/platform/stats", action: "admin.stats"},
		{name: "row14_impersonate_start", method: http.MethodPost, path: "/api/v1/admin/impersonate", body: `{"target_user_id":"` + c.targets[side]["get"].String() + `"}`, action: "admin.impersonate"},
		{name: "row16_handoff_consume", method: http.MethodPost, path: "/api/v1/admin/impersonate/handoff/consume", body: `{"handoff_id":"wc13728"}`, action: "admin.impersonate", liveRowExempt: true},
		{name: "row17_sa_keys_list", method: http.MethodGet, path: sa + wcNoSuchID + "/keys", action: "admin.users"},
		{name: "row18_sa_key_issue", method: http.MethodPost, path: sa + wcNoSuchID + "/keys", body: `{}`, action: "admin.users"},
		{name: "row19_sa_key_rotate", method: http.MethodPost, path: sa + wcNoSuchID + "/keys/rotate", body: `{}`, action: "admin.users"},
		{name: "row20_sa_key_revoke", method: http.MethodPost, path: sa + "keys/cred13728/revoke", body: `{"reason":"wc13728"}`, action: "admin.users"},
		{name: "m1_sa_create", method: http.MethodPost, path: "/api/v1/admin/service-accounts",
			body: `{"organization_id":"` + c.victim[side].String() + `","owner_principal_id":"` + c.sideUser[side].String() + `","name":"wc13728"}`, action: "admin.users"},
		{name: "m1_sa_disable", method: http.MethodPost, path: sa + wcNoSuchID + "/disable", action: "admin.users"},
		{name: "m1_sa_enable", method: http.MethodPost, path: sa + wcNoSuchID + "/enable", action: "admin.users"},
		{name: "m1_sa_owner", method: http.MethodPost, path: sa + wcNoSuchID + "/owner", body: `{"owner_principal_id":"` + c.sideUser[side].String() + `"}`, action: "admin.users"},
		{name: "row05_org_delete", method: http.MethodDelete, path: victim, action: "admin.orgs"},
		// Last: a stop ends the side's impersonation row.
		{name: "row15_impersonate_stop", method: http.MethodPost, path: "/api/v1/admin/impersonate/stop", action: "admin.impersonate", liveRowExempt: true},
	}
}

type wcAnswer struct {
	status     int
	body       string
	retryAfter string
	decisions  []string
	usersAdmin []string
}

func (w *wcWorld) serve(h http.Handler, plane *authplane.Authorize, admin *wcUsersAdmin, side int, c *wcCaller, r wcRow) wcAnswer {
	var body io.Reader
	if r.body != "" {
		body = strings.NewReader(r.body)
	}
	req := httptest.NewRequest(r.method, r.path, body)
	if r.body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token[side] != "" {
		req.Header.Set("Authorization", "Bearer "+c.token[side])
	}
	if r.xOrg != "" {
		req.Header.Set("X-Org-Id", r.xOrg)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	calls := admin.take()
	for i := range calls {
		calls[i] = w.normalize(calls[i])
	}
	return wcAnswer{status: rec.Code, body: w.normalize(rec.Body.String()), retryAfter: rec.Header().Get("Retry-After"), decisions: plane.Take(),
		usersAdmin: calls}
}

var (
	wcUUID      = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	wcTimestamp = regexp.MustCompile(`\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?`)
	wcToken     = regexp.MustCompile(`"(token|invite_url|accept_url)":"[^"]*"`)
)

// normalize maps every seeded id to its symbolic name (the two sides' twins share one name), any other id and every
// timestamp to a placeholder, and the side tag of a created email to one tag: the two sides' bodies then compare.
func (w *wcWorld) normalize(body string) string {
	body = strings.TrimSpace(body)
	body = wcUUID.ReplaceAllStringFunc(body, func(id string) string {
		if name, ok := w.nameOf(id); ok {
			return "<" + name + ">"
		}
		return "<uuid>"
	})
	body = wcTimestamp.ReplaceAllString(body, "<ts>")
	body = wcToken.ReplaceAllString(body, `"$1":"<token>"`)
	body = strings.ReplaceAll(body, "-sideoff", "-side")
	return strings.ReplaceAll(body, "-sideon", "-side")
}

// wcShort is a body cut for a log line; the cells compare the whole body.
func wcShort(body string) string {
	if len(body) > 600 {
		return body[:600] + "..."
	}
	return body
}

// wcCell is one served cell.
type wcCell struct {
	row   wcRow
	sides [2]wcAnswer
}

// cell serves the row to both sides (off first), logs a CELL line per side and fails on the universal invariants: an
// answer on both sides, and no admin.* question on the off side (the admin cell is off there).
func (w *wcWorld) cell(t *testing.T, c *wcCaller, rowIndex int) wcCell {
	t.Helper()
	return w.cellOn(t, w.main, c, rowIndex)
}

func (w *wcWorld) cellOn(t *testing.T, sides wcSides, c *wcCaller, rowIndex int) wcCell {
	t.Helper()
	var out wcCell
	for side := range 2 {
		r := wcRows(c, side)[rowIndex]
		out.row = r
		out.sides[side] = w.serve(sides.handlers[side], sides.plane, sides.admins[side], side, c, r)
		a := out.sides[side]
		t.Logf("CELL|13728|%s|%s|%s|%s|%d|retry=%q|%s|pdp=%v|users_admin=%v", w.mode.name, c.spec.label, r.name, wcSideName[side], a.status,
			a.retryAfter, wcShort(a.body), a.decisions, a.usersAdmin)
		if a.status == 0 {
			t.Fatalf("%s side: no answer", wcSideName[side])
		}
	}
	// The live members namespace asks admin.orgs as its platform alternative on the site_org_* rows, on both sides.
	if !strings.HasPrefix(out.row.name, "site_org_") {
		for _, d := range out.sides[wcOff].decisions {
			if strings.HasPrefix(d, "admin.") {
				t.Errorf("off side asked %q: the admin cell is off there", d)
			}
		}
	}
	if out.row.superuserBody {
		for side := range 2 {
			if strings.Contains(out.sides[side].body, usersowned.PlatformRolesMessage) {
				t.Errorf("%s side answered the users.go:281/:454 refusal: with users owned it must not fire", wcSideName[side])
			}
		}
	}
	return out
}

func wcHasAllow(decisions []string, action string) bool {
	return slices.ContainsFunc(decisions, func(d string) bool {
		return strings.HasPrefix(d, action+" platform ") && strings.Contains(d, "allowed=true")
	})
}

// wcCellsPerMode is the exact cell count of each mode (21 rows x its classes, the view twin included); a class or a
// row that drops out of the tables fails the mode.
//
// m_off: 10 classes x 21 rows + 3 impersonation classes x 45 rows = 345; m_on: + 4 G3 classes x 21 + the twin x 21 = 450;
// m_split: + the twin x 21 = 366.
var wcCellsPerMode = map[string]int{"m_off": 345, "m_on": 450, "m_split_unreachable_in_prod": 366}

// wcTwin is the view twin of a mode's named-change classes: a reference caller whose decision point answers the
// platform role the named classes get there (a planted role on its principal only). Its go-api view is theirs: in m_on
// no platform role (plain_orgless's), in m_split the public superuser (pa_orgless's). Every row of a named class must
// answer as its twin, on both sides: the answer is the function of the go-api view and the decision point view only.
func wcTwin(mode wcMode) (wcSpec, string, bool) {
	switch mode.name {
	case "m_on":
		return wcSpec{label: "twin_l0p1"}, "platform_admin", true
	case "m_split_unreachable_in_prod":
		return wcSpec{label: "twin_l1p0", publicSuperuser: true, assigned: true}, "", true
	}
	return wcSpec{}, "", false
}

// Every row and site of the weak-cover class, for every caller class, in every platform roles mode, on the real
// decision point: the admin flip keeps the off answer (status, body, user-admin calls), except the two named changes.
func TestAdminWeakCoverRealPDPCells(t *testing.T) {
	ctx := context.Background()
	for _, mode := range wcModes {
		t.Run(mode.name, func(t *testing.T) {
			w := newWCWorld(t, ctx, mode)
			results := map[string][]wcCell{}
			cells := 0
			twinSpec, twinRole, hasTwin := wcTwin(mode)
			if hasTwin {
				tc := w.seed(t, ctx, twinSpec)
				twinSides := w.newSides(t, ctx, w.plantWrap([]wcPlant{{name: "view_twin", target: twinSpec.label,
					snap: func(_ *wcWorld, _ context.Context, _ uuid.UUID, s authorization.GrantSnapshot) authorization.GrantSnapshot {
						return wcRole{s, twinRole}
					}}}))
				for i, r := range wcRows(tc, wcOff) {
					t.Run(twinSpec.label+"/"+r.name, func(t *testing.T) {
						cells++
						results[twinSpec.label] = append(results[twinSpec.label], w.cellOn(t, twinSides, tc, i))
					})
				}
			}
			for _, spec := range wcSpecs {
				if !spec.inMode(mode) {
					continue
				}
				c := w.seed(t, ctx, spec)
				expect := spec.wcExpectIn(mode)
				rows := wcRows(c, wcOff)
				for i := range rows {
					t.Run(spec.label+"/"+rows[i].name, func(t *testing.T) {
						cells++
						sides := w.main
						if spec.impSwitchOn {
							sides = w.impOn
						}
						got := w.cellOn(t, sides, c, i)
						results[spec.label] = append(results[spec.label], got)
						off, on := got.sides[wcOff], got.sides[wcOn]
						if spec.allRows {
							w.impRow(t, c, got)
						}
						if expect == wcImpGate && !got.row.liveRowExempt {
							for side := range 2 {
								a := got.sides[side]
								if a.status != http.StatusForbidden || len(a.decisions) != 0 || len(a.usersAdmin) != 0 {
									t.Errorf("GATE (i) %s side: %d %s, %d PDP calls, %d user-admin calls; want the Scope's 403 with 0 and 0",
										wcSideName[side], a.status, wcShort(a.body), len(a.decisions), len(a.usersAdmin))
								}
							}
						}
						switch expect {
						case wcParity, wcMeasure, wcImpGate:
							if why := wcDiff(off, on, "", ""); why != "" {
								stop := ""
								switch {
								case expect == wcMeasure:
									stop = "STOP (measure cell, P2 d / P6): "
								case spec.allRows:
									stop = "STOP (impersonation GATE (i)/(ii), acp 04:19Z): "
								}
								t.Errorf("%sthe admin flip changed the answer: %s", stop, why)
							}
						}
						judged := (spec.label == "pa_orgless" && !got.row.orgClaim) || (spec.label == "pa_foreign_org" && got.row.orgClaim)
						if judged && !wcHasAllow(on.decisions, got.row.action) {
							t.Errorf("platform admin: on side decisions %v hold no allow of %s: the answer is not the decision point's", on.decisions, got.row.action)
						}
					})
				}
				if expect == wcSplitChange || expect == wcG3Change {
					t.Run(spec.label+"/named_change", func(t *testing.T) {
						wcNamedChange(t, expect, spec.label, results[spec.label], twinSpec.label, results[twinSpec.label],
							results["pa_orgless"], results["plain_orgless"])
					})
				}
			}
			if want := wcCellsPerMode[mode.name]; cells != want {
				t.Errorf("%s ran %d cells, want exactly %d", mode.name, cells, want)
			}
			t.Logf("CELLS|13728|%s|%d", mode.name, cells)
		})
	}
}

// impRow logs one IMPROW line of the impersonation table (row | mode | legacy | enforced | pdp | stub | deny text).
func (w *wcWorld) impRow(t *testing.T, c *wcCaller, got wcCell) {
	t.Helper()
	off, on := got.sides[wcOff], got.sides[wcOn]
	verdict := "SAME"
	if wcDiff(off, on, "", "") != "" {
		verdict = "DIFF"
	}
	deny := "-"
	if on.status >= 400 {
		deny = wcShort(on.body)
	} else if off.status >= 400 {
		deny = "legacy:" + wcShort(off.body)
	}
	t.Logf("IMPROW|13728|%s|%s|%s|%s %s|%d|%d|pdp=%d/%d|stub=%d/%d|%s|%s", c.spec.label, w.mode.name, got.row.name, got.row.method,
		w.normalize(got.row.path), off.status, on.status, len(off.decisions), len(on.decisions), len(off.usersAdmin), len(on.usersAdmin), deny, verdict)
}

// wcDiff is "" when two answers are the same: status, whole normalized body and user-admin calls, with each caller's
// label written CALLER (the bodies of two classes name their own seeded rows).
func wcDiff(a, b wcAnswer, labelA, labelB string) string {
	norm := func(s, label string) string {
		if label == "" {
			return s
		}
		return strings.ReplaceAll(s, label, "CALLER")
	}
	bodyA, bodyB := norm(a.body, labelA), norm(b.body, labelB)
	callsA, callsB := norm(strings.Join(a.usersAdmin, "; "), labelA), norm(strings.Join(b.usersAdmin, "; "), labelB)
	if a.status == b.status && bodyA == bodyB && len(a.usersAdmin) == len(b.usersAdmin) && callsA == callsB {
		return ""
	}
	return fmt.Sprintf("%d %s users_admin [%s] vs %d %s users_admin [%s]", a.status, wcShort(bodyA), callsA, b.status, wcShort(bodyB), callsB)
}

// wcNamedChange asserts a named change (F1) on EVERY row: each side of the class answers as its view twin (status,
// whole body, user-admin calls); on the platform-decided rows the off side also answers as the off reference class and
// the on side as the on reference class, and at least one of them changes.
func wcNamedChange(t *testing.T, expect wcExpect, label string, got []wcCell, twinLabel string, twin, platformAdmin, plain []wcCell) {
	t.Helper()
	offRef, onRef, offRefLabel, onRefLabel := platformAdmin, plain, "pa_orgless", "plain_orgless"
	name := "RISK-F8P2B-admin-1 section 4 / card F2 accepted change (m_split, unreachable in prod)"
	if expect == wcG3Change {
		offRef, onRef, offRefLabel, onRefLabel = plain, platformAdmin, "plain_orgless", "pa_orgless"
		name = "known change the F8p2b G3 START gate keeps at 0 rows"
	}
	if len(got) != len(twin) || len(got) != len(offRef) || len(got) != len(onRef) {
		t.Fatalf("%d cells, twin %d, references %d and %d", len(got), len(twin), len(offRef), len(onRef))
	}
	changed := 0
	for i, cell := range got {
		for side := range 2 {
			if why := wcDiff(cell.sides[side], twin[i].sides[side], label, twinLabel); why != "" {
				t.Errorf("%s %s %s side: the class does not answer as its view twin %s: %s", name, cell.row.name, wcSideName[side], twinLabel, why)
			}
		}
		if !cell.row.platformDecided {
			continue
		}
		if why := wcDiff(cell.sides[wcOff], offRef[i].sides[wcOff], label, offRefLabel); why != "" {
			t.Errorf("%s %s off side: not the %s off answer: %s", name, cell.row.name, offRefLabel, why)
		}
		if why := wcDiff(cell.sides[wcOn], onRef[i].sides[wcOn], label, onRefLabel); why != "" {
			t.Errorf("%s %s on side: not the %s on answer: %s", name, cell.row.name, onRefLabel, why)
		}
		if cell.sides[wcOff].status != cell.sides[wcOn].status {
			changed++
		}
	}
	if changed == 0 {
		t.Errorf("%s: no platform-decided row changed its answer", name)
	}
	t.Logf("NAMED-CHANGE|13728|%s|%s|rows_changed=%d", name, label, changed)
}

// With admin on and the decision point down, a platform admin gets the fail-closed 503 on every row and site; the off
// side (its live namespaces asking a decision point that is up) keeps its answer.
func TestAdminWeakCoverRealPDPDeadDecisionPoint(t *testing.T) {
	ctx := context.Background()
	w := newWCWorld(t, ctx, wcModes[0])
	dead := authplane.StartAuthorize(t, ctx, w.env.URI, w.key, authplane.AuthorizeOptions{UsersOwned: true})
	dead.Stop()
	w.main.handlers[wcOn], w.main.admins[wcOn] = w.goAPI(t, dead, wcLiveEnforce+",admin", AuthProxyConfig{})
	// pa_foreign_org: its org claim lets the acr revoke row reach its HoldsPlatform site.
	c := w.seed(t, ctx, wcSpecs[5])
	for i, r := range wcRows(c, wcOff) {
		t.Run(r.name, func(t *testing.T) {
			got := w.cell(t, c, i)
			on := got.sides[wcOn]
			if on.status != http.StatusServiceUnavailable || on.retryAfter == "" {
				t.Errorf("dead decision point: on %d %s Retry-After %q, want 503 with a retry hint", on.status, on.body, on.retryAfter)
			}
			// The off side may answer 503 from the users-owned post-guard path (the stub); never the decision point's.
			if off := got.sides[wcOff]; off.retryAfter != "" || strings.Contains(off.body, "Authorization service temporarily unavailable") {
				t.Errorf("off side %d %s Retry-After %q: its decision point is up", off.status, off.body, off.retryAfter)
			}
		})
	}
}

// Planted defects in the decision state the production builder reads (P5 S1-S3). Each plant must flip the cell it
// exists for; the unplanted world's answer is the control.
type wcPlantState struct {
	inner authapi.DecisionState
	snap  func(ctx context.Context, principal uuid.UUID, s authorization.GrantSnapshot) authorization.GrantSnapshot
}

func (p wcPlantState) Read(ctx context.Context, fn func(authapi.DecisionTx) error) error {
	return p.inner.Read(ctx, func(tx authapi.DecisionTx) error { return fn(wcPlantTx{DecisionTx: tx, snap: p.snap}) })
}

type wcPlantTx struct {
	authapi.DecisionTx
	snap func(ctx context.Context, principal uuid.UUID, s authorization.GrantSnapshot) authorization.GrantSnapshot
}

func (tx wcPlantTx) Snapshot(ctx context.Context, principal uuid.UUID, org string) (authorization.GrantSnapshot, error) {
	s, err := tx.DecisionTx.Snapshot(ctx, principal, org)
	if err != nil {
		return nil, err
	}
	return tx.snap(ctx, principal, s), nil
}

type wcRole struct {
	authorization.GrantSnapshot
	role string
}

func (s wcRole) PlatformRole() string { return s.role }

type wcNoMembership struct{ authorization.GrantSnapshot }

func (wcNoMembership) Membership(string) (authorization.Membership, bool) {
	return authorization.Membership{}, false
}

// wcPlant is one planted defect of the decision state, on the principal of one caller class only.
type wcPlant struct {
	name string
	// target is the caller class whose principal the plant changes; mode and spec are where it lives (wcModes, wcSpecs).
	target     string
	mode, spec int
	// kill is the main cell whose asserts the plant breaks (F5): the parity assert "the admin flip changed the answer".
	kill   string
	wantOn int
	snap   func(w *wcWorld, ctx context.Context, principal uuid.UUID, s authorization.GrantSnapshot) authorization.GrantSnapshot
}

// wcPlantMainCells wires wcPlants into the main cells' decision point. It is false here; a throwaway D6697 commit sets
// it true, and the --- FAIL lines of its hosted run are the kills of the main cells named in kill.
const wcPlantMainCells = false

var wcPlants = []wcPlant{
	{
		// S1: decisionstate.go:445-446 (is_superuser -> platform_admin) forced off.
		name: "s1_superuser_map_off", target: "pa_orgless", mode: 0, spec: 1, wantOn: http.StatusForbidden,
		kill: "TestAdminWeakCoverRealPDPCells/m_off/pa_orgless/r21_list (parity assert, and the judged-allow assert)",
		snap: func(_ *wcWorld, _ context.Context, _ uuid.UUID, s authorization.GrantSnapshot) authorization.GrantSnapshot {
			return wcRole{s, ""}
		},
	},
	{
		// S2: the owned assignment read (authapi/users.go:82 PublicHoldsSQL) replaced by the public is_superuser column.
		name: "s2_owned_read_is_public_column", target: "su_13209", mode: 1, spec: 11, wantOn: http.StatusOK,
		kill: "TestAdminWeakCoverRealPDPCells/m_on/su_13209/r21_list (parity assert)",
		snap: func(w *wcWorld, ctx context.Context, principal uuid.UUID, s authorization.GrantSnapshot) authorization.GrantSnapshot {
			var public bool
			_ = w.env.Admin.QueryRow(ctx, `SELECT COALESCE(is_superuser, false) FROM public.users WHERE id = $1`, principal).Scan(&public)
			if public {
				return wcRole{s, "platform_admin"}
			}
			return wcRole{s, ""}
		},
	},
	{
		// S3: the membership read (decisionstate membership.EffectiveOn) answers no membership.
		name: "s3_membership_read_none", target: "owner", mode: 0, spec: 2, wantOn: http.StatusForbidden,
		kill: "TestAdminWeakCoverRealPDPCells/m_off/owner/r21_list (parity assert)",
		snap: func(_ *wcWorld, _ context.Context, _ uuid.UUID, s authorization.GrantSnapshot) authorization.GrantSnapshot {
			return wcNoMembership{s}
		},
	},
}

// plantWrap wraps the decision state so each plant changes the snapshot of its target's principal only.
func (w *wcWorld) plantWrap(plants []wcPlant) func(authapi.DecisionState) authapi.DecisionState {
	return func(inner authapi.DecisionState) authapi.DecisionState {
		return wcPlantState{inner: inner, snap: func(ctx context.Context, principal uuid.UUID, s authorization.GrantSnapshot) authorization.GrantSnapshot {
			name, _ := w.nameOf(principal.String())
			for _, p := range plants {
				if name == p.target+".user" {
					s = p.snap(w, ctx, principal, s)
				}
			}
			return s
		}}
	}
}

func TestAdminWeakCoverRealPDPPlantedDefects(t *testing.T) {
	ctx := context.Background()
	for _, p := range wcPlants {
		t.Run(p.name, func(t *testing.T) {
			if wcSpecs[p.spec].label != p.target {
				t.Fatalf("plant %s: spec %d is %s, not its target %s", p.name, p.spec, wcSpecs[p.spec].label, p.target)
			}
			w := newWCWorld(t, ctx, wcModes[p.mode])
			c := w.seed(t, ctx, wcSpecs[p.spec])
			base := w.cell(t, c, 0)
			if why := wcDiff(base.sides[wcOff], base.sides[wcOn], "", ""); why != "" {
				t.Fatalf("control: the unplanted cell must hold: %s", why)
			}
			// The planted decision point: the same production builder over the same database, its state wrapped.
			planted := w.newSides(t, ctx, w.plantWrap([]wcPlant{p}))
			got := w.serve(planted.handlers[wcOn], planted.plane, planted.admins[wcOn], wcOn, c, wcRows(c, wcOn)[0])
			t.Logf("CELL|13728|%s|%s|%s|planted_on|%d|%s|pdp=%v", wcModes[p.mode].name, c.spec.label, p.name, got.status, wcShort(got.body), got.decisions)
			// The main cell's parity assert, on the planted answer: it must report the flip.
			if why := wcDiff(base.sides[wcOff], got, "", ""); why == "" || got.status != p.wantOn {
				t.Errorf("plant %s: on %d %s (control on %d); want %d and a parity diff: %s does not kill it", p.name, got.status,
					wcShort(got.body), base.sides[wcOn].status, p.wantOn, p.kill)
			}
			t.Logf("PLANT|13728|%s|kill=%s|control_on=%d|planted_on=%d", p.name, p.kill, base.sides[wcOn].status, got.status)
		})
	}
}

// P2 a: the schema refuses a live platform_admin assignment whose principal row is missing. The principal of an
// assignment cannot be deleted (ON DELETE RESTRICT: auth.users 0001:42, platform_role_assignments 0034:40-43), and an
// assignment cannot be written without its principal (nor outside audit.Commit: the commit guard, auth migration 0006).
// Each refusal's SQLSTATE is logged (FK line).
func TestAdminWeakCoverRealPDPPrincipalRowCannotBeDroppedUnderAnAssignment(t *testing.T) {
	ctx := context.Background()
	env := platformroleseam.Start(ctx, t, true)
	holder := env.User(ctx, t, true, true)
	_, err := env.Admin.Exec(ctx, `DELETE FROM auth.principals WHERE id = $1`, holder)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("delete of an assignment holder's principal: %v, want a refusal by the database", err)
	}
	var principals, assignments int
	if err := env.Admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM auth.principals WHERE id = $1),
(SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1)`, holder).Scan(&principals, &assignments); err != nil {
		t.Fatal(err)
	}
	if principals != 1 || assignments != 1 {
		t.Fatalf("after the refused delete: %d principals, %d assignments, want 1 and 1", principals, assignments)
	}
	orphan := uuid.New()
	_, err = env.Admin.Exec(ctx, `INSERT INTO auth.platform_role_assignments (principal_id, role_key) VALUES ($1, 'platform_admin')`, orphan)
	if err == nil {
		t.Fatal("an assignment without a principal row was written")
	}
	t.Logf("FK|13728|principal_delete=%s|orphan_assignment=%v", pgErr.Code, err)
}
