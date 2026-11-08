package admin

import (
	"context"
	"encoding/base64"
	"fmt"
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
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
)

// Fixed ids, so an answer body is the same in every run.
var (
	iprUser       = uuid.MustParse("a1000000-0000-4000-8000-0000000000a1")
	iprOrg        = uuid.MustParse("a1000000-0000-4000-8000-0000000000b1")
	iprTargetUser = uuid.MustParse("a1000000-0000-4000-8000-0000000000a2")
	iprTargetOrg  = uuid.MustParse("a1000000-0000-4000-8000-0000000000b2")
	iprSID        = uuid.MustParse("a1000000-0000-4000-8000-0000000000c1")
	iprRowID      = uuid.MustParse("a1000000-0000-4000-8000-0000000000d1")
	iprOtherUser  = uuid.MustParse("a1000000-0000-4000-8000-0000000000a3")
	iprExpires    = time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
)

const iprBearer = "impersonation-read-parity-token"

// iprPDP counts every question and answers admin.orgs with an allow, admin.impersonate with no verdict, and every
// other action with the pepfake answer for an unknown rule.
type iprPDP struct {
	inner pep.Decider
	mu    sync.Mutex
	calls []string
}

func (d *iprPDP) Decide(ctx context.Context, req pep.Request) pep.Decision {
	d.mu.Lock()
	d.calls = append(d.calls, req.Action)
	d.mu.Unlock()
	if req.Action == rule.ActionAdminImpersonate {
		return pep.Decision{}
	}
	return d.inner.Decide(ctx, req)
}

func (d *iprPDP) DecideBatch(ctx context.Context, reqs []pep.Request) []pep.Decision {
	out := make([]pep.Decision, len(reqs))
	for i, req := range reqs {
		out[i] = d.Decide(ctx, req)
	}
	return out
}

func (d *iprPDP) take() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := d.calls
	d.calls = nil
	return out
}

// iprStore is the caller as the authenticator reads it: a live superuser who is an admin of iprOrg; live says whether
// its go-api impersonation row (target iprTargetUser in iprTargetOrg) is in force.
type iprStore struct{ live bool }

func (iprStore) UserState(context.Context, uuid.UUID) (policy.UserState, bool, error) {
	return policy.UserState{IsActive: true, IsSuperuser: true}, true, nil
}

func (iprStore) Membership(_ context.Context, user, org uuid.UUID) (string, bool, error) {
	return "admin", user == iprUser && org == iprOrg, nil
}

func (s iprStore) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, member, err := s.Membership(ctx, user, org)
	return role, member, true, err
}

func (s iprStore) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	if !s.live {
		return nil, nil
	}
	return &policy.Impersonation{ID: iprRowID, AdminUserID: iprUser, TargetUserID: iprTargetUser, TargetOrgID: iprTargetOrg,
		TargetRole: "admin", ExpiresAt: iprExpires}, nil
}

func (iprStore) ReadsPlatformRoles() bool { return false }

func (iprStore) AccessUser(_ context.Context, id uuid.UUID) (policy.AccessUserRow, bool, error) {
	return policy.AccessUserRow{State: policy.UserState{IsActive: true, IsSuperuser: true}, Email: "admin@example.test"}, id == iprUser, nil
}

func (iprStore) SessionState(_ context.Context, id uuid.UUID) (policy.SessionRow, bool, error) {
	pin := int64(0)
	return policy.SessionRow{PrincipalID: iprUser, LegacyTokenVersion: &pin}, id == iprSID, nil
}

type iprValidator struct{}

func (iprValidator) Validate(context.Context, string, string, string) (*policy.RoutedResult, error) {
	org := iprOrg.String()
	return &policy.RoutedResult{Claims: &policy.AccessTokenClaims{
		Subject: "prn_" + base64.RawURLEncoding.EncodeToString(iprUser[:]), SessionID: sessionid.Wire(iprSID), OrgID: &org,
	}}, nil
}

// iprDB answers every list read with no rows, the impersonation row read with the live row (when live), and every
// other single-row read with no row.
type iprDB struct {
	pgDB
	live bool
}

func (iprDB) Query(context.Context, string, ...any) (pgx.Rows, error) { return noRows{}, nil }

func (d iprDB) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	if d.live && strings.Contains(query, "FROM impersonation_sessions") {
		email := "target@example.test"
		return iprValuesRow{values: []any{iprRowID, iprTargetUser, iprTargetOrg, "admin", &email, iprExpires}}
	}
	return iprNoRow{}
}

func (iprDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, fmt.Errorf("iprDB: no writes")
}

func (iprDB) Begin(context.Context) (pgx.Tx, error) { return nil, fmt.Errorf("iprDB: no transactions") }

type iprNoRow struct{}

func (iprNoRow) Scan(...any) error { return pgx.ErrNoRows }

type iprValuesRow struct{ values []any }

func (r iprValuesRow) Scan(dest ...any) error {
	for i, value := range r.values {
		switch out := dest[i].(type) {
		case *uuid.UUID:
			*out = value.(uuid.UUID)
		case *string:
			*out = value.(string)
		case **string:
			*out = value.(*string)
		case *time.Time:
			*out = value.(time.Time)
		default:
			return fmt.Errorf("unexpected scan destination %T", dest[i])
		}
	}
	return nil
}

// iprRig mounts the real users, orgs and impersonation routes behind the real guard and scope middlewares. With
// enforce, the admin and members cells are on over the production rows (the /admin/users rows name admin.users), and
// every PDP question is counted.
func iprRig(t *testing.T, live, enforce bool) (http.Handler, *iprPDP) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, err := policy.NewAuthenticator(iprStore{live: live}, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RouteAccess(iprValidator{}, "dev-health-ops-api"); err != nil {
		t.Fatal(err)
	}
	guard := policy.NewGuard(auth, logger)
	pdp := &iprPDP{inner: pepfake.New(pepfake.Rule{PrincipalReference: iprBearer, Action: rule.ActionAdminOrgs, ResourceType: "platform", ResourceKey: "platform"})}
	if enforce {
		rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, "dev-health-ops-api")
		if err != nil {
			t.Fatal(err)
		}
		enforcer, err := policy.NewEnforcer(policy.EnforceConfig{Decider: pdp, Rows: rows,
			Switch: authzenforce.Parse(authzenforce.GoAPI, "admin,members"), Timeout: time.Second, Logger: logger})
		if err != nil {
			t.Fatal(err)
		}
		guard.SetEnforcer(enforcer)
	}
	h := &handlers{store: pgStore{Pool: iprDB{live: live}}, guard: guard, logger: logger}
	h.cache = newImpersonationCache(nil, h.store, logger)
	mux := http.NewServeMux()
	for _, route := range append(append(h.userRoutes(), h.orgRoutes()...), h.impersonationRoutes()...) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	scope := policy.NewScope(auth, logger)
	return scope.Principal(scope.OrgScope(scope.Impersonation(mux))), pdp
}

type iprAnswer struct {
	status int
	body   string
}

func iprServe(h http.Handler, method, path string) iprAnswer {
	req := httptest.NewRequest(method, path, strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer "+iprBearer)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return iprAnswer{rec.Code, strings.TrimSpace(rec.Body.String())}
}

// iprReads are the four admin reads whose handlers ask platform questions (HoldsPlatform), with the cells-off
// answer of a live-row platform admin as executed on origin/main f496ee5d.
var iprReads = []struct {
	name, path string
	want       iprAnswer
}{
	{"users list", "/api/v1/admin/users", iprAnswer{http.StatusOK, `[]`}},
	{"user get", "/api/v1/admin/users/" + iprOtherUser.String(), iprAnswer{http.StatusNotFound, `{"detail":"User not found"}`}},
	{"org members", "/api/v1/admin/orgs/" + iprTargetOrg.String() + "/members", iprAnswer{http.StatusOK, `[]`}},
	{"impersonation status", "/api/v1/admin/impersonate/status", iprAnswer{http.StatusOK, `{"is_impersonating":true,"target_user_id":"a1000000-0000-4000-8000-0000000000a2","target_email":"target@example.test",` +
		`"target_org_id":"a1000000-0000-4000-8000-0000000000b2","expires_at":"2099-01-01T00:00:00Z"}`}},
}

// A platform admin with a live go-api impersonation row reads an admin route with the admin cell on: the answer is
// the cells-off one, end to end, and no PDP question is asked, the handler's platform questions included.
func TestAnImpersonationReadOnAdminRoutesTakesTheLegacyPathEndToEnd(t *testing.T) {
	legacy, _ := iprRig(t, true, false)
	enforced, pdp := iprRig(t, true, true)
	for _, read := range iprReads {
		t.Run(read.name, func(t *testing.T) {
			off := iprServe(legacy, http.MethodGet, read.path)
			pdp.take()
			on := iprServe(enforced, http.MethodGet, read.path)
			calls := pdp.take()
			t.Logf("CELL|%s|GET %s|enforce-on=%d %s|cells-off=%d %s|pdp_calls=%d %v", read.name, read.path, on.status, on.body, off.status, off.body, len(calls), calls)
			if off != read.want {
				t.Errorf("cells-off %d %s, want the recorded main answer %d %s", off.status, off.body, read.want.status, read.want.body)
			}
			if on != off {
				t.Errorf("enforce-on %d %s, want the cells-off %d %s", on.status, on.body, off.status, off.body)
			}
			if len(calls) != 0 {
				t.Errorf("%d PDP calls %v, want 0", len(calls), calls)
			}
		})
	}
}

// The same platform admin with no live row keeps its PDP questions on the same reads: the impersonation path is no
// wider bypass.
func TestAnAdminReadWithNoImpersonationStillAsksThePDP(t *testing.T) {
	enforced, pdp := iprRig(t, false, true)
	for _, read := range iprReads {
		path := read.path
		if read.name == "org members" {
			// With no session the served organization is the caller's own (another one is the served-org seam's).
			path = "/api/v1/admin/orgs/" + iprOrg.String() + "/members"
		}
		pdp.take()
		iprServe(enforced, http.MethodGet, path)
		if calls := pdp.take(); len(calls) == 0 {
			t.Errorf("%s: no PDP call, want the PDP asked", read.name)
		}
	}
}

// A live-row platform admin's /admin/users writes are refused before any PDP call with the admin cell on: every one of
// the four rows names admin.users and is enforced (D6833: the accepted write narrowing of the read-only impersonation
// flaw). The create and the delete stand for them here; apiservice pins all four.
func TestAnImpersonationWriteOnAdminRoutesKeepsItsAnswer(t *testing.T) {
	enforced, pdp := iprRig(t, true, true)
	pdp.take()
	for _, write := range []struct{ method, path string }{
		{http.MethodPost, "/api/v1/admin/users"},
		{http.MethodDelete, "/api/v1/admin/users/" + iprOtherUser.String()},
	} {
		got := iprServe(enforced, write.method, write.path)
		calls := pdp.take()
		t.Logf("CELL|%s %s|enforce-on=%d %s|pdp_calls=%d %v", write.method, write.path, got.status, got.body, len(calls), calls)
		if got.status != http.StatusForbidden || got.body != `{"detail":"Not allowed in an impersonation session"}` || len(calls) != 0 {
			t.Errorf("%s %s: %d %s, %d PDP calls; want the impersonation 403 and 0", write.method, write.path, got.status, got.body, len(calls))
		}
	}
}
