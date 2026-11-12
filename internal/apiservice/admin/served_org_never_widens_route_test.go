package admin

import (
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/full-chaos/dev-health-go/pep/pepfake"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
)

// CHAOS-10872 S2: the served-organization form (a path {org_id}) of the "an allow for the token organization never widens what the
// handler admits" pin, through the REAL members route: real guard, real scope, real enforcer over the production rows, the real
// listMembers handler, a membership-row fake for the handler's own check. The caller's token names org A; the path names org B.
var (
	nwUser    = uuid.MustParse("b1000000-0000-4000-8000-0000000000a1")
	nwOrgA    = uuid.MustParse("b1000000-0000-4000-8000-0000000000b1")
	nwOrgB    = uuid.MustParse("b1000000-0000-4000-8000-0000000000b2")
	nwSession = uuid.MustParse("b1000000-0000-4000-8000-0000000000c1")
)

const nwBearer = "served-org-never-widens-token"

type nwStore struct {
	roles     map[uuid.UUID]string
	superuser bool
}

func (s nwStore) UserState(context.Context, uuid.UUID) (policy.UserState, bool, error) {
	return policy.UserState{IsActive: true, IsSuperuser: s.superuser}, true, nil
}

func (s nwStore) Membership(_ context.Context, _, org uuid.UUID) (string, bool, error) {
	role, ok := s.roles[org]
	return role, ok, nil
}

func (s nwStore) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, member, err := s.Membership(ctx, user, org)
	return role, member, true, err
}

func (nwStore) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, nil
}

func (nwStore) ReadsPlatformRoles() bool { return false }

func (s nwStore) AccessUser(context.Context, uuid.UUID) (policy.AccessUserRow, bool, error) {
	return policy.AccessUserRow{State: policy.UserState{IsActive: true, IsSuperuser: s.superuser}, Email: "caller@example.test"}, true, nil
}

func (nwStore) SessionState(context.Context, uuid.UUID) (policy.SessionRow, bool, error) {
	pin := int64(0)
	return policy.SessionRow{PrincipalID: nwUser, LegacyTokenVersion: &pin}, true, nil
}

type nwValidator struct{}

func (nwValidator) Validate(context.Context, string, string, string) (*policy.RoutedResult, error) {
	org := nwOrgA.String()
	return &policy.RoutedResult{Claims: &policy.AccessTokenClaims{
		Subject: "prn_" + base64.RawURLEncoding.EncodeToString(nwUser[:]), SessionID: sessionid.Wire(nwSession), OrgID: &org,
	}}, nil
}

// nwDB answers the handler's own membership read from the roles and records that the handler went on to the member list.
type nwDB struct {
	pgDB
	roles   map[uuid.UUID]string
	reached *bool
}

type nwRoleRow struct {
	role string
	ok   bool
}

func (r nwRoleRow) Scan(dest ...any) error {
	if !r.ok {
		return pgx.ErrNoRows
	}
	*(dest[0].(*string)) = r.role
	return nil
}

func (d nwDB) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	org, _ := args[0].(uuid.UUID)
	role, ok := d.roles[org]
	return nwRoleRow{role, ok}
}

func (d nwDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	*d.reached = true
	return noRows{}, nil
}

func (nwDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, io.ErrClosedPipe
}

// nwPDP allows exactly the members row action on the TOKEN organization A (a question about any other organization has no rule), or
// gives no verdict when dead, and records the organization of each question.
type nwPDP struct {
	dead  bool
	inner pep.Decider
	mu    sync.Mutex
	orgs  []string
}

func (d *nwPDP) Decide(ctx context.Context, req pep.Request) pep.Decision {
	d.mu.Lock()
	d.orgs = append(d.orgs, req.OrganizationID)
	d.mu.Unlock()
	if d.dead {
		return pep.Decision{}
	}
	return d.inner.Decide(ctx, req)
}

func (d *nwPDP) DecideBatch(ctx context.Context, reqs []pep.Request) []pep.Decision {
	out := make([]pep.Decision, len(reqs))
	for i, req := range reqs {
		out[i] = d.Decide(ctx, req)
	}
	return out
}

func (d *nwPDP) asked() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.orgs...)
}

func nwRig(t *testing.T, superuser bool, roles map[uuid.UUID]string, enforce, dead bool) (http.Handler, *nwPDP, *bool) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, err := policy.NewAuthenticator(nwStore{roles: roles, superuser: superuser}, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RouteAccess(nwValidator{}, "dev-health-ops-api"); err != nil {
		t.Fatal(err)
	}
	guard := policy.NewGuard(auth, logger)
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, "dev-health-ops-api")
	if err != nil {
		t.Fatal(err)
	}
	row, ok := rows["GET /api/v1/admin/orgs/{org_id}/members"]
	if !ok {
		t.Fatal("the members row has no action row")
	}
	pdp := &nwPDP{dead: dead, inner: pepfake.New(pepfake.Rule{PrincipalReference: nwBearer, Action: row.Action, ResourceType: string(row.ResourceType),
		ResourceKey: nwOrgA.String(), OrganizationID: nwOrgA.String()})}
	if enforce {
		enforcer, err := policy.NewEnforcer(policy.EnforceConfig{Decider: pdp, Rows: rows, Switch: authzenforce.Parse(authzenforce.GoAPI, "members"),
			Timeout: time.Second, Logger: logger})
		if err != nil {
			t.Fatal(err)
		}
		guard.SetEnforcer(enforcer)
	}
	reached := new(bool)
	h := &handlers{store: pgStore{Pool: nwDB{roles: roles, reached: reached}}, guard: guard, logger: logger}
	mux := http.NewServeMux()
	for _, route := range h.orgRoutes() {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	scope := policy.NewScope(auth, logger)
	return scope.Principal(scope.OrgScope(scope.Impersonation(mux))), pdp, reached
}

func nwServe(h http.Handler, org uuid.UUID) (int, string) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/orgs/"+org.String()+"/members", nil)
	req.Header.Set("Authorization", "Bearer "+nwBearer)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code, strings.TrimSpace(rec.Body.String())
}

// An admin of A only, whose token names A, asks for the members of B by path. The decision point allows A. The handler's own
// check of B refuses, with and without the enforcer: the allow for A does not widen what the handler admits, the question
// is about A only, and the member list is not reached.
func TestServedOrgAnAllowForTheTokenOrganizationNeverWidensTheMembersHandler(t *testing.T) {
	roles := map[uuid.UUID]string{nwOrgA: "admin"}
	legacy, _, _ := nwRig(t, false, roles, false, false)
	wantStatus, wantBody := nwServe(legacy, nwOrgB)
	if wantStatus != http.StatusForbidden || !strings.Contains(wantBody, "Admin access required for organization") {
		t.Fatalf("legacy: %d %s, want the handler's 403 Admin access required for organization", wantStatus, wantBody)
	}
	enforced, pdp, reached := nwRig(t, false, roles, true, false)
	status, body := nwServe(enforced, nwOrgB)
	if status != wantStatus || body != wantBody || *reached {
		t.Errorf("enforced: %d %s, member list reached %v, want the legacy %d %s and not reached", status, body, *reached, wantStatus, wantBody)
	}
	if asked := pdp.asked(); len(asked) != 1 || asked[0] != nwOrgA.String() {
		t.Errorf("the decision point was asked %v, want one question about the token organization %s", asked, nwOrgA)
	}
	// control: the same caller asking for A is admitted by the handler and reaches the member list.
	enforced, pdp, reached = nwRig(t, false, roles, true, false)
	if status, body := nwServe(enforced, nwOrgA); status != http.StatusOK || !*reached {
		t.Errorf("control, own org: %d %s, member list reached %v, want 200 and reached", status, body, *reached)
	}
	if asked := pdp.asked(); len(asked) != 1 || asked[0] != nwOrgA.String() {
		t.Errorf("control: the decision point was asked %v, want one question about %s", asked, nwOrgA)
	}
}

// The outage row for a platform admin (S2-4): serving another organization with no membership row in the token org is a 200
// through the handler today (the platform action admits), and with the decision point dead and the namespace on it is the
// enforced 503; the member list is not reached.
func TestServedOrgAPlatformAdminOutageIsTheEnforced503Not200(t *testing.T) {
	legacy, _, reachedLegacy := nwRig(t, true, map[uuid.UUID]string{}, false, false)
	if status, body := nwServe(legacy, nwOrgB); status != http.StatusOK || !*reachedLegacy {
		t.Fatalf("legacy platform admin serving B: %d %s, reached %v, want 200 and reached", status, body, *reachedLegacy)
	}
	enforced, pdp, reached := nwRig(t, true, map[uuid.UUID]string{}, true, true)
	status, body := nwServe(enforced, nwOrgB)
	if status != http.StatusServiceUnavailable || *reached {
		t.Errorf("enforced, PDP dead: %d %s, member list reached %v, want 503 and not reached", status, body, *reached)
	}
	if asked := pdp.asked(); len(asked) != 1 || asked[0] != nwOrgA.String() {
		t.Errorf("the decision point was asked %v, want one question about the token organization %s", asked, nwOrgA)
	}
}
