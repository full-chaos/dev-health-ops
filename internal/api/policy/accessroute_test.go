package policy

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// fakeValidator answers Validate with a fixed result and records the call.
type fakeValidator struct {
	result   *RoutedResult
	err      error
	profiles []string
	legacy   func(ctx context.Context, token string) (*User, error)
}

var errRefusedByValidator = errors.New("validator: refused")

func (f *fakeValidator) Validate(ctx context.Context, service, profile, bearer string) (*RoutedResult, error) {
	f.profiles = append(f.profiles, service+"|"+profile)
	if f.legacy != nil {
		user, err := f.legacy(ctx, bearer)
		if err != nil {
			return nil, errRefusedByValidator
		}
		return &RoutedResult{Legacy: user}, nil
	}
	return f.result, f.err
}

// accessStore is fakeStore with the auth sessions and the EdDSA users read.
type accessStore struct {
	*sessionStore
	profile AccessUserRow
	errUser error
}

func (s *accessStore) AccessUser(_ context.Context, id uuid.UUID) (AccessUserRow, bool, error) {
	if s.errUser != nil {
		return AccessUserRow{}, false, s.errUser
	}
	state, found := s.users[id]
	row := s.profile
	row.State = state
	return row, found, nil
}

var (
	liveSID   = uuid.MustParse("44444444-4444-4444-8444-444444444444")
	ownOrgID  = uuid.MustParse(ownOrg)
	wireOwner = "prn_" + wireID(userID)
)

func wireID(id uuid.UUID) string { return sessionid.Wire(id)[len("ses_"):] }

func newAccessStore() *accessStore {
	base := newStore()
	base.members[[2]uuid.UUID{userID, ownOrgID}] = true
	base.roles = map[[2]uuid.UUID]string{{userID, ownOrgID}: "admin"}
	return &accessStore{
		sessionStore: &sessionStore{fakeStore: base, rows: map[uuid.UUID]SessionRow{liveSID: {PrincipalID: userID}}},
		profile:      AccessUserRow{Email: "user@example.com"},
	}
}

func userClaims(mutate func(*AccessTokenClaims)) *RoutedResult {
	org := ownOrg
	claims := &AccessTokenClaims{Subject: wireOwner, SessionID: sessionid.Wire(liveSID), OrgID: &org}
	if mutate != nil {
		mutate(claims)
	}
	return &RoutedResult{Claims: claims}
}

func routed(t *testing.T, store Store, validator RoutedValidator) *Authenticator {
	t.Helper()
	auth := authenticator(t, store)
	if err := auth.RouteAccess(validator, "dev-health-ops-api"); err != nil {
		t.Fatal(err)
	}
	return auth
}

func TestRouteAccessRefusesAnIncompleteSetup(t *testing.T) {
	auth := authenticator(t, newStore())
	if auth.RouteAccess(nil, "dev-health-ops-api") == nil || auth.RouteAccess(&fakeValidator{}, "") == nil || auth.Routed() {
		t.Fatal("RouteAccess accepted a nil validator or an empty service")
	}
}

// TestRoutedErrorMapping is acp D3/item 5: a refusal is the generic 401, an
// unavailable validator is 503; no reason reaches the body.
func TestRoutedErrorMapping(t *testing.T) {
	cases := map[string]struct {
		err    error
		status int
		body   string
	}{
		"refused":     {errRefusedByValidator, http.StatusUnauthorized, `{"detail":{"message":"Invalid or expired token"}}`},
		"unavailable": {fmt.Errorf("validator: %w", ErrUnavailable), http.StatusServiceUnavailable, `{"detail":{"message":"Database temporarily unavailable"}}`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			auth := routed(t, newAccessStore(), &fakeValidator{err: tc.err})
			handler := RouteProfiles(func(*http.Request) (string, bool) { return "GET /p", true })(NewGuard(auth, quiet()).Wrap(Authenticated, echo))
			got := serve(handler, http.MethodGet, "/p", map[string]string{"Authorization": "Bearer x.y.z"})
			if got.status != tc.status || got.body != tc.body {
				t.Fatalf("%d %s", got.status, got.body)
			}
			if tc.status == http.StatusUnauthorized && got.header.Get("Www-Authenticate") != "Bearer" {
				t.Fatalf("no Bearer challenge: %v", got.header)
			}
		})
	}
}

// TestGuardPassesTheMuxPatternAsTheProfile: inside a route the profile is the
// pattern the mux routed the request to, whatever the pre-route value says.
func TestGuardPassesTheMuxPatternAsTheProfile(t *testing.T) {
	validator := &fakeValidator{err: errRefusedByValidator}
	auth := routed(t, newAccessStore(), validator)
	mux := http.NewServeMux()
	mux.Handle("GET /items/{id}", NewGuard(auth, quiet()).Wrap(Authenticated, echo))
	handler := RouteProfiles(func(*http.Request) (string, bool) { return "GET /elsewhere", true })(mux)
	serve(handler, http.MethodGet, "/items/7", map[string]string{"Authorization": "Bearer x.y.z"})
	if len(validator.profiles) != 1 || validator.profiles[0] != "dev-health-ops-api|GET /items/{id}" {
		t.Fatalf("profiles %v", validator.profiles)
	}
}

// TestRoutedWithoutAProfilePassesAnEmptyProfile: a caller with no profile in
// its context asks for the empty profile, which no row has (accessverify
// refuses it no_profile).
func TestRoutedWithoutAProfilePassesAnEmptyProfile(t *testing.T) {
	validator := &fakeValidator{result: userClaims(nil)}
	auth := routed(t, newAccessStore(), validator)
	if _, err := auth.Authenticate(context.Background(), "x.y.z"); err != nil {
		t.Fatalf("a user result was refused: %v", err)
	}
	if validator.profiles[0] != "dev-health-ops-api|" {
		t.Fatalf("profile %v, want empty", validator.profiles)
	}
}

func TestRoutedUserPath(t *testing.T) {
	ctx := WithRouteProfile(context.Background(), "GET /p")
	cases := map[string]struct {
		result *RoutedResult
		adjust func(*accessStore)
		ok     bool
		err    error
	}{
		"member of the org claim": {result: userClaims(nil), ok: true},
		"no sid":                  {result: userClaims(func(c *AccessTokenClaims) { c.SessionID = "" })},
		"bad sid":                 {result: userClaims(func(c *AccessTokenClaims) { c.SessionID = "ses_bad" })},
		"bad subject":             {result: userClaims(func(c *AccessTokenClaims) { c.Subject = "prn_!!" })},
		"subject not a principal": {result: userClaims(func(c *AccessTokenClaims) { c.Subject = userID.String() })},
		"not a member of the org": {result: userClaims(func(c *AccessTokenClaims) { org := "66666666-6666-4666-8666-666666666666"; c.OrgID = &org })},
		"revoked session":         {result: userClaims(nil), adjust: func(s *accessStore) { s.rows[liveSID] = SessionRow{PrincipalID: userID, Revoked: true} }},
		"inactive user":           {result: userClaims(nil), adjust: func(s *accessStore) { s.users[userID] = UserState{} }},
		"users read unavailable":  {result: userClaims(nil), adjust: func(s *accessStore) { s.errUser = ErrUnavailable }, err: ErrUnavailable},
		"membership read fails":   {result: userClaims(nil), adjust: func(s *accessStore) { s.errMember = ErrUnavailable }, err: ErrUnavailable},
		"empty result":            {result: &RoutedResult{}},
		"no result":               {result: nil},
		"both set":                {result: &RoutedResult{Legacy: &User{}, Claims: &AccessTokenClaims{}}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			store := newAccessStore()
			if tc.adjust != nil {
				tc.adjust(store)
			}
			user, err := routed(t, store, &fakeValidator{result: tc.result}).Authenticate(ctx, "x.y.z")
			switch {
			case tc.ok:
				if err != nil || user.ID != userID || user.OrgID != ownOrg || user.Role != "admin" || user.Email != "user@example.com" || user.UserID != userID.String() {
					t.Fatalf("user %+v err %v", user, err)
				}
			case tc.err != nil:
				if !errors.Is(err, tc.err) || IsRefusal(err) {
					t.Fatalf("err %v, want %v", err, tc.err)
				}
			default:
				if user != nil || !IsRefusal(err) {
					t.Fatalf("user %+v err %v, want a refusal", user, err)
				}
			}
		})
	}
}

// TestRoutedLegacyPathDoesNotRecurse: the legacy verifier the validator calls
// is AuthenticateLegacy, which never goes back through the validator.
func TestRoutedLegacyPathDoesNotRecurse(t *testing.T) {
	validator := &fakeValidator{}
	auth := routed(t, newAccessStore(), validator)
	validator.legacy = auth.AuthenticateLegacy
	user, err := auth.Authenticate(WithRouteProfile(context.Background(), "GET /p"), sign(t, claims(nil)))
	if err != nil || user.ID != userID || len(validator.profiles) != 1 {
		t.Fatalf("user %+v err %v calls %v", user, err, validator.profiles)
	}
}

// TestImpersonationRunsForAUserToken is acp D4: an EdDSA token has no
// is_superuser claim, so the peek alone would skip the impersonation check.
func TestImpersonationRunsForAUserToken(t *testing.T) {
	store := newAccessStore()
	store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
	target := uuid.MustParse("55555555-5555-4555-8555-555555555555")
	store.sessions[userID] = &Impersonation{AdminUserID: userID, TargetUserID: target, TargetOrgID: target}
	auth := routed(t, store, &fakeValidator{result: userClaims(nil)})
	scope := NewScope(auth, quiet())
	header, _ := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{}).SigningString()
	got := serve(RouteProfiles(func(*http.Request) (string, bool) { return "GET /p", true })(scope.Impersonation(echo)), http.MethodGet, "/p",
		map[string]string{"Authorization": "Bearer " + header + ".sig"})
	if !strings.Contains(got.body, "imp="+target.String()) || got.header.Get("X-Impersonating") != "true" {
		t.Fatalf("%d %s %v", got.status, got.body, got.header)
	}
	unrouted := NewScope(authenticator(t, store), quiet())
	if got := serve(unrouted.Impersonation(echo), http.MethodGet, "/p", map[string]string{"Authorization": "Bearer " + header + ".sig"}); strings.Contains(got.body, "imp="+target.String()) {
		t.Fatalf("an unrouted scope ran the check for an EdDSA header: %s", got.body)
	}
}

// TestOnlyGoAPIRoutesAnAuthenticator is acp C1: the policy package is shared
// with query-api; the one non-test RouteAccess call in the module is go-api's
// construction (internal/apiservice), so query-api stays unrouted.
func TestOnlyGoAPIRoutesAnAuthenticator(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	var sites []string
	fset := token.NewFileSet()
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); path != root && (strings.HasPrefix(name, ".") || name == "vendor" || name == "node_modules" || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil || !strings.Contains(string(src), "RouteAccess") {
			return err
		}
		file, err := parser.ParseFile(fset, path, src, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if sel, ok := node.(*ast.SelectorExpr); ok && sel.Sel.Name == "RouteAccess" {
				rel, _ := filepath.Rel(root, path)
				sites = append(sites, filepath.ToSlash(rel))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sites) != 1 || sites[0] != "internal/apiservice/service.go" {
		t.Fatalf("RouteAccess is called from %v; only internal/apiservice/service.go may route an Authenticator", sites)
	}
}

// TestRouteProfileAndBodyCoexistInTheContext: the route profile and the
// decoded body (BodyFirst) are two different context values; setting one in
// either order never hides the other.
func TestRouteProfileAndBodyCoexistInTheContext(t *testing.T) {
	body := pybody.Body{}
	for name, ctx := range map[string]context.Context{
		"profile then body": WithBody(WithRouteProfile(context.Background(), "POST /p"), body),
		"body then profile": WithRouteProfile(WithBody(context.Background(), body), "POST /p"),
	} {
		if got := RouteProfileFrom(ctx); got != "POST /p" {
			t.Errorf("%s: profile %q", name, got)
		}
		if _, ok := BodyFrom(ctx); !ok {
			t.Errorf("%s: body lost", name)
		}
	}
}
