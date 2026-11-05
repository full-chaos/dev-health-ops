package policy

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

var actorID = uuid.MustParse("77777777-7777-4777-8777-777777777777")

const (
	deniedRow  = "POST /denied"
	allowedRow = "POST /allowed"
	// delegatedRefusal is the body of the Guard's refusal of a delegated bearer.
	delegatedRefusal = `{"detail":"Not allowed in an impersonation session"}`
)

// delegatedClaim is a delegated session's HS256 token: auth-service's
// hs256_compat mint writes the actor into impersonating_user_id.
func delegatedClaim(c jwt.MapClaims) { c["impersonating_user_id"] = actorID.String() }

// delegatedWrap is one way a route is guarded, with the store and claims that
// let a plain bearer through it.
type delegatedWrap struct {
	name   string
	wrap   func(g *Guard, next http.Handler) http.Handler
	claims func(jwt.MapClaims)
	store  func(*fakeStore)
}

func asAdmin(c jwt.MapClaims) { c["role"] = "admin" }

func adminRow(s *fakeStore) { s.roles[[2]uuid.UUID{userID, uuid.MustParse(ownOrg)}] = "admin" }

func superRow(s *fakeStore) { s.users[userID] = UserState{IsActive: true, IsSuperuser: true} }

func holderOf(action string) func(*fakeStore) {
	return func(s *fakeStore) { s.users[userID] = UserState{IsActive: true, Platform: only(action)} }
}

func both(fns ...func(jwt.MapClaims)) func(jwt.MapClaims) {
	return func(c jwt.MapClaims) {
		for _, fn := range fns {
			if fn != nil {
				fn(c)
			}
		}
	}
}

var delegatedWraps = []delegatedWrap{
	{"Wrap Authenticated", func(g *Guard, n http.Handler) http.Handler { return g.Wrap(Authenticated, n) }, nil, nil},
	{"Wrap Admin", func(g *Guard, n http.Handler) http.Handler { return g.Wrap(Admin, n) }, asAdmin, adminRow},
	{"Wrap AdminOrg", func(g *Guard, n http.Handler) http.Handler { return g.Wrap(AdminOrg, n) }, asAdmin, adminRow},
	{"Wrap Optional", func(g *Guard, n http.Handler) http.Handler { return g.Wrap(Optional, n) }, nil, nil},
	{"WrapPlatform", func(g *Guard, n http.Handler) http.Handler { return g.WrapPlatform(rule.ActionAdminFlags, n) }, superClaims, superRow},
	{"WrapAdminOr", func(g *Guard, n http.Handler) http.Handler { return g.WrapAdminOr(rule.ActionAdminUsers, n) }, nil, holderOf(rule.ActionAdminUsers)},
	{"BodyFirst", func(g *Guard, n http.Handler) http.Handler { return g.BodyFirst(Authenticated, n) }, nil, nil},
	{"BodyFirstPlatform", func(g *Guard, n http.Handler) http.Handler { return g.BodyFirstPlatform(rule.ActionAdminFlags, n) }, superClaims, superRow},
	{"BodyFirstAdminOr", func(g *Guard, n http.Handler) http.Handler { return g.BodyFirstAdminOr(rule.ActionAdminUsers, n) }, nil, holderOf(rule.ActionAdminUsers)},
}

// atRow serves handler as the mux does, under pattern, so r.Pattern is the
// route template the delegated rows are keyed by.
func atRow(pattern string, handler http.Handler, headers map[string]string) response {
	mux := http.NewServeMux()
	mux.Handle(pattern, handler)
	path := pattern[strings.Index(pattern, " ")+1:]
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	return response{recorder.Code, recorder.Body.String(), recorder.Header()}
}

func delegatedGuard(t *testing.T, store Store, rows func(string) bool) *Guard {
	t.Helper()
	auth := authenticator(t, store)
	if rows != nil {
		auth.DelegatedRows(rows)
	}
	return NewGuard(auth, quiet())
}

func onlyAllowedRow(profile string) bool { return profile == allowedRow }

// TestDelegatedBearerIsRefusedOnEveryWrap: every guard form refuses a
// delegated session's bearer on a row that does not allow it, with go-api's
// 403 body, and so does every row when no table was read. The same caller
// without the claim passes.
func TestDelegatedBearerIsRefusedOnEveryWrap(t *testing.T) {
	for _, w := range delegatedWraps {
		for _, rows := range []struct {
			name string
			fn   func(string) bool
		}{{"denied row", onlyAllowedRow}, {"no table", nil}} {
			store := newStore()
			if w.store != nil {
				w.store(store)
			}
			guard := delegatedGuard(t, store, rows.fn)
			plain := atRow(deniedRow, w.wrap(guard, echo), bearer(t, w.claims))
			if plain.status != http.StatusOK {
				t.Fatalf("%s, %s: the plain bearer answered %d %s, want 200", w.name, rows.name, plain.status, plain.body)
			}
			got := atRow(deniedRow, w.wrap(guard, echo), bearer(t, both(w.claims, delegatedClaim)))
			if got.status != http.StatusForbidden || got.body != delegatedRefusal {
				t.Errorf("%s, %s: the delegated bearer answered %d %s, want 403 %s", w.name, rows.name, got.status, got.body, delegatedRefusal)
			}
		}
	}
}

// TestDelegatedBearerPassesAnAllowedRow: on a row that allows it, the
// delegated bearer meets the guard as any caller does.
func TestDelegatedBearerPassesAnAllowedRow(t *testing.T) {
	for _, w := range delegatedWraps {
		store := newStore()
		if w.store != nil {
			w.store(store)
		}
		guard := delegatedGuard(t, store, onlyAllowedRow)
		got := atRow(allowedRow, w.wrap(guard, echo), bearer(t, both(w.claims, delegatedClaim)))
		if got.status != http.StatusOK {
			t.Errorf("%s: the delegated bearer on an allowed row answered %d %s, want 200", w.name, got.status, got.body)
		}
	}
}

// TestAnAdminTokenBesideAnOpenRowIsNotDenied is today's impersonation: the
// admin's own token, with the admin's open public row, carries no
// impersonating_user_id, so a denied row still serves it as the target.
func TestAnAdminTokenBesideAnOpenRowIsNotDenied(t *testing.T) {
	store := newStore()
	superRow(store)
	store.sessions[userID] = &Impersonation{ID: uuid.New(), AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID,
		TargetRole: "admin", ExpiresAt: time.Now().Add(time.Hour)}
	auth := authenticator(t, store)
	auth.DelegatedRows(onlyAllowedRow)
	scope := NewScope(auth, quiet())
	guard := NewGuard(auth, quiet())
	for _, level := range []Authz{Authenticated, Admin} {
		handler := scope.Principal(scope.Impersonation(guard.Wrap(level, echo)))
		got := atRow(deniedRow, handler, bearer(t, both(superClaims, asAdmin)))
		if got.status != http.StatusOK || !strings.Contains(got.body, "imp="+targetOrgID.String()) {
			t.Errorf("%s: the admin token beside an open row answered %d %s, want 200 as the target", level, got.status, got.body)
		}
	}
}

// TestAccessTokenActorIsImpersonatedBy: an access token's act claim is the
// caller's ImpersonatedBy, the actor's users id; an act that names no
// principal is refused; a token with no act has none.
func TestAccessTokenActorIsImpersonatedBy(t *testing.T) {
	ctx := WithRouteProfile(context.Background(), "GET /p")
	wire := "prn_" + wireID(actorID)
	user, err := routed(t, newAccessStore(), &fakeValidator{result: userClaims(func(c *AccessTokenClaims) { c.Actor = &wire })}).Authenticate(ctx, "x.y.z")
	if err != nil || user.ImpersonatedBy == nil || *user.ImpersonatedBy != actorID.String() || user.ID != userID {
		t.Fatalf("delegated token: user %+v err %v, want ImpersonatedBy %s", user, err, actorID)
	}
	bad := "prn_!!"
	if user, err := routed(t, newAccessStore(), &fakeValidator{result: userClaims(func(c *AccessTokenClaims) { c.Actor = &bad })}).Authenticate(ctx, "x.y.z"); user != nil || !IsRefusal(err) {
		t.Fatalf("an act that names no principal: user %+v err %v, want a refusal", user, err)
	}
	if user, err := routed(t, newAccessStore(), &fakeValidator{result: userClaims(nil)}).Authenticate(ctx, "x.y.z"); err != nil || user.ImpersonatedBy != nil {
		t.Fatalf("no act: user %+v err %v, want no ImpersonatedBy", user, err)
	}
}

// bearerReaders are the calls that read a user bearer outside the Guard.
var bearerReaders = map[string]bool{"Authenticate": true, "AuthenticateLegacy": true, "CheckClaims": true, "BearerToken": true}

// namedBearerReaders are the functions of go-api that may read a user bearer:
// the Guard, the Authenticator itself, and the named sites outside the Guard.
// A named site outside the Guard either takes no bearer to a write or is in
// denyingBearerReaders.
var namedBearerReaders = map[string]bool{
	"internal/api/policy/guard.go:Guard.currentUser":  true,
	"internal/api/policy/guard.go:Guard.optionalUser": true,
	// switch-org under AUTH_PROXY_SESSIONS skips the scope: reads the HS256 bearer only to refuse an impersonating admin; it never serves a handler for it.
	"internal/api/policy/guard.go:Guard.RefuseImpersonatingCaller": true,
	// The shadow PDP's observation, after currentUser (and its deny) admitted the caller; it never changes the answer.
	"internal/api/policy/guard.go:Guard.wrap":                           true,
	"internal/api/policy/principal.go:Authenticator.Authenticate":       true,
	"internal/api/policy/principal.go:Authenticator.AuthenticateLegacy": true,
	"internal/api/policy/principal.go:Authenticator.authenticate":       true,
	// The Principal step: it decides OrgScope and Impersonation and serves no handler.
	"internal/api/policy/principalonce.go:Scope.decide":            true,
	"internal/api/policy/principalonce.go:Scope.authenticateToken": true,
	// POST /api/v1/auth/validate: the token is in the body; it answers validity and writes nothing.
	"internal/api/session/routes.go:handlers.validate": true,
	// POST /api/v1/auth/logout: the user names the audit line; it ends the refresh token in the body.
	// It writes, so it applies the Guard's delegated refusal (denyingBearerReaders).
	"internal/api/session/routes.go:handlers.optionalUser": true,
	// The validator's HS256 leg, reached only from Authenticate.
	"internal/apiservice/accessverify.go:buildAccessVerify": true,
}

// denyingBearerReaders are the named sites outside the Guard that take a
// bearer to a write: each calls Guard.RefuseDelegated on the user it reads,
// so a delegated bearer meets the Guard's refusal there too.
var denyingBearerReaders = map[string]bool{
	"internal/api/session/routes.go:handlers.optionalUser": true,
}

// TestEveryBearerReadOutsideTheGuardIsNamed is the census of the deny: a read
// of a user bearer in go-api (internal/api, internal/apiservice) outside the
// named functions fails, so no route can take a delegated bearer past the
// Guard's deny unseen.
func TestEveryBearerReadOutsideTheGuardIsNamed(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	refuses := map[string]bool{}
	var unnamed []string
	for _, dir := range []string{"internal/api", "internal/apiservice"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			for _, decl := range file.Decls {
				site := rel + ":" + declName(decl)
				ast.Inspect(decl, func(node ast.Node) bool {
					var name string
					switch n := node.(type) {
					case *ast.SelectorExpr:
						name = n.Sel.Name
					case *ast.CallExpr:
						if ident, ok := n.Fun.(*ast.Ident); ok {
							name = ident.Name
						}
					}
					if name == "RefuseDelegated" {
						refuses[site] = true
					}
					if bearerReaders[name] {
						found[site] = true
						if !namedBearerReaders[site] {
							unnamed = append(unnamed, site+" reads a bearer through "+name)
						}
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(unnamed)
	for _, line := range unnamed {
		t.Errorf("UNNAMED BEARER READ %s: route it through the Guard, or name it here with why it takes no delegated bearer to a write", line)
	}
	for site := range denyingBearerReaders {
		if !refuses[site] {
			t.Errorf("DELEGATED BEARER NOT REFUSED %s reads a bearer to a write and does not call Guard.RefuseDelegated", site)
		}
	}
	for _, site := range []string{"internal/api/policy/guard.go:Guard.currentUser", "internal/api/policy/guard.go:Guard.optionalUser", "internal/api/session/routes.go:handlers.validate", "internal/api/session/routes.go:handlers.optionalUser"} {
		if !found[site] {
			t.Errorf("the scan did not see %s: it stopped reading the sources", site)
		}
	}
}

// declName is "Recv.Func" for a method, "Func" for a function, "" otherwise.
func declName(decl ast.Decl) string {
	fn, ok := decl.(*ast.FuncDecl)
	if !ok {
		return ""
	}
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	recv := fn.Recv.List[0].Type
	if star, ok := recv.(*ast.StarExpr); ok {
		recv = star.X
	}
	if ident, ok := recv.(*ast.Ident); ok {
		return ident.Name + "." + fn.Name.Name
	}
	return fn.Name.Name
}
