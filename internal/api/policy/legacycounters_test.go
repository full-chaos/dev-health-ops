package policy

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

type legacyRecorder struct {
	mu   sync.Mutex
	hits map[string]int
}

func recordLegacy(t *testing.T) *legacyRecorder {
	t.Helper()
	rec := &legacyRecorder{hits: map[string]int{}}
	previous := legacyPathAdd
	legacyPathAdd = func(_ context.Context, surface, kind string) {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.hits[surface+"/"+kind]++
	}
	t.Cleanup(func() { legacyPathAdd = previous })
	return rec
}

func (r *legacyRecorder) get(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hits[key]
}

func (r *legacyRecorder) keys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]string, 0, len(r.hits))
	for k := range r.hits {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (r *legacyRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hits = map[string]int{}
}

func validAuth(t *testing.T, mutate func(jwt.MapClaims)) map[string]string {
	t.Helper()
	return map[string]string{"Authorization": "Bearer " + sign(t, claims(mutate))}
}

func guardServe(t *testing.T, level Authz, store *fakeStore, headers map[string]string) response {
	t.Helper()
	return serve(wrapLevel(NewGuard(authenticator(t, store), quiet()), level, echo), http.MethodGet, "/x", headers)
}

func TestLegacyGuardLevelCountsEveryLevelAtEntry(t *testing.T) {
	admin := func(c jwt.MapClaims) { c["role"] = "admin" }
	cases := []struct {
		level   Authz
		kind    string
		headers func(*testing.T) map[string]string
		status  int
	}{
		{Public, "public", func(*testing.T) map[string]string { return nil }, 200},
		{Authenticated, "authenticated", func(t *testing.T) map[string]string { return validAuth(t, nil) }, 200},
		{Admin, "admin", func(t *testing.T) map[string]string { return validAuth(t, admin) }, 200},
		{Superuser, "superuser", func(t *testing.T) map[string]string { return validAuth(t, nil) }, 403},
		{AdminOrg, "admin_org", func(t *testing.T) map[string]string { return validAuth(t, admin) }, 200},
		{Optional, "optional", func(*testing.T) map[string]string { return nil }, 200},
		{Admin, "admin", func(*testing.T) map[string]string { return nil }, 401},
		{Authenticated, "authenticated", func(*testing.T) map[string]string { return map[string]string{"Authorization": "Bearer x.y.z"} }, 401},
	}
	for _, tc := range cases {
		t.Run(tc.kind+"/"+http.StatusText(tc.status), func(t *testing.T) {
			rec := recordLegacy(t)
			store := newStore()
			withClaimRole(store, claims(admin))
			got := guardServe(t, tc.level, store, tc.headers(t))
			if got.status != tc.status {
				t.Fatalf("status %d %s, want %d", got.status, got.body, tc.status)
			}
			if n := rec.get("guard_level/" + tc.kind); n != 1 {
				t.Fatalf("guard_level/%s counted %d times, want 1 (all: %v)", tc.kind, n, rec.keys())
			}
		})
	}
}

func TestLegacyGuardLevelKindsAreTheClosedSet(t *testing.T) {
	want := map[Authz]string{Public: "public", Authenticated: "authenticated", Admin: "admin", Superuser: "superuser", AdminOrg: "admin_org", Optional: "optional"}
	seen := map[string]bool{}
	for level, kind := range want {
		if got := guardLevelKind(level); got != kind || got != level.String() {
			t.Errorf("level %d kind %q, want %q", level, got, kind)
		}
		seen[kind] = true
	}
	if len(seen) != 6 {
		t.Fatalf("kinds %v", seen)
	}
	if got := guardLevelKind(Optional + 1); got != "unknown" {
		t.Errorf("unlisted level kind %q, want unknown", got)
	}
}

func TestLegacyRoleSourceMembershipRowAtAdmin(t *testing.T) {
	rec := recordLegacy(t)
	store := tokenOrgStore(true, "admin")
	got := guardServe(t, Admin, store, validAuth(t, nil))
	if got.status != 200 || rec.get("role_source/membership_row") != 1 || len(rec.keys()) != 2 {
		t.Fatalf("status %d, counts %v", got.status, rec.keys())
	}
}

func TestLegacyRoleSourceClaimFallbackWhenOrgMissing(t *testing.T) {
	rec := recordLegacy(t)
	store := newStore()
	store.absentOrgs = map[uuid.UUID]bool{uuid.MustParse(ownOrg): true}
	store.members = map[[2]uuid.UUID]bool{}
	got := guardServe(t, AdminOrg, store, validAuth(t, func(c jwt.MapClaims) { c["role"] = "admin" }))
	if got.status != 200 || rec.get("role_source/claim_fallback") != 1 || rec.get("role_source/membership_row") != 0 {
		t.Fatalf("status %d, counts %v", got.status, rec.keys())
	}
}

func TestLegacyRoleSourceClaimFallbackWithoutTokenOrg(t *testing.T) {
	rec := recordLegacy(t)
	guardServe(t, Admin, newStore(), validAuth(t, func(c jwt.MapClaims) { c["role"] = "admin"; delete(c, "org_id") }))
	if rec.get("role_source/claim_fallback") != 1 {
		t.Fatalf("counts %v", rec.keys())
	}
}

func TestLegacyRoleSourcePlatformBypassAtAdmin(t *testing.T) {
	rec := recordLegacy(t)
	store := newStore()
	store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
	got := guardServe(t, Admin, store, validAuth(t, nil))
	if got.status != 200 || rec.get("role_source/platform_bypass") != 1 || rec.get("role_source/membership_row") != 0 {
		t.Fatalf("status %d, counts %v", got.status, rec.keys())
	}
}

func TestLegacyRoleSourcePlatformGateAtSuperuser(t *testing.T) {
	for name, super := range map[string]bool{"superuser": true, "refused": false} {
		t.Run(name, func(t *testing.T) {
			rec := recordLegacy(t)
			store := newStore()
			store.users[userID] = UserState{IsActive: true, IsSuperuser: super}
			guardServe(t, Superuser, store, validAuth(t, nil))
			if rec.get("role_source/platform_gate") != 1 {
				t.Fatalf("counts %v", rec.keys())
			}
		})
	}
}

func TestLegacyRoleSourceNotCountedWithoutADecisionSource(t *testing.T) {
	for _, level := range []Authz{Public, Authenticated, Optional} {
		rec := recordLegacy(t)
		guardServe(t, level, newStore(), validAuth(t, nil))
		for _, key := range rec.keys() {
			if strings.HasPrefix(key, "role_source/") {
				t.Errorf("level %s counted %s", level, key)
			}
		}
	}
	rec := recordLegacy(t)
	got := guardServe(t, Admin, tokenOrgStore(false, ""), validAuth(t, nil))
	if got.status != 403 || rec.get("role_source/membership_row")+rec.get("role_source/claim_fallback") != 0 {
		t.Fatalf("a caller without a membership row counted a role source: %d %v", got.status, rec.keys())
	}
}

func TestLegacyDirectReadCountedPerPGStoreMethod(t *testing.T) {
	id := uuid.New()
	calls := []struct {
		kind string
		call func(PGStore)
	}{
		{"users", func(s PGStore) { _, _, _ = s.UserState(context.Background(), id) }},
		{"memberships", func(s PGStore) { _, _, _ = s.Membership(context.Background(), id, id) }},
		{"org_memberships", func(s PGStore) { _, _, _, _ = s.OrgMembership(context.Background(), id, id) }},
		{"impersonation_sessions", func(s PGStore) { _, _ = s.ActiveImpersonation(context.Background(), id) }},
		{"auth_sessions", func(s PGStore) { _, _, _ = s.SessionState(context.Background(), id) }},
		{"users", func(s PGStore) { _, _, _ = s.AccessUser(context.Background(), id) }},
	}
	for _, tc := range calls {
		rec := recordLegacy(t)
		func() {
			defer func() { _ = recover() }() // a nil Pool panics after the count
			tc.call(PGStore{})
		}()
		if got := rec.keys(); len(got) != 1 || got[0] != "direct_read/"+tc.kind {
			t.Errorf("%s: counted %v", tc.kind, got)
		}
	}
}

// Every PGStore method that reads the pool counts first, so a read added
// later cannot skip the counter.
func TestLegacyDirectReadEveryPoolReaderCountsFirst(t *testing.T) {
	for _, name := range []string{"store.go", "session.go", "accessroute_user.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Body == nil || len(fn.Body.List) == 0 {
				continue
			}
			readsPool := false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "Pool" {
					readsPool = true
				}
				return true
			})
			if !readsPool {
				continue
			}
			first, _ := fn.Body.List[0].(*ast.ExprStmt)
			call, _ := exprOf(first).(*ast.CallExpr)
			ident, _ := funOf(call).(*ast.Ident)
			if ident == nil || ident.Name != "countDirectRead" || len(call.Args) != 2 {
				t.Errorf("%s %s: first statement is not countDirectRead(ctx, kind)", name, fn.Name.Name)
				continue
			}
			if arg, ok := call.Args[1].(*ast.Ident); !ok || !strings.HasPrefix(arg.Name, "kindRead") {
				t.Errorf("%s %s: the kind is not a kindRead constant", name, fn.Name.Name)
			}
		}
	}
}

func exprOf(s *ast.ExprStmt) ast.Expr {
	if s == nil {
		return nil
	}
	return s.X
}

func funOf(c *ast.CallExpr) ast.Expr {
	if c == nil {
		return nil
	}
	return c.Fun
}

type optionalStore struct{ *fakeStore }

func (optionalStore) SessionState(context.Context, uuid.UUID) (SessionRow, bool, error) {
	return SessionRow{}, false, nil
}

func (optionalStore) AccessUser(context.Context, uuid.UUID) (AccessUserRow, bool, error) {
	return AccessUserRow{}, false, nil
}

func TestLegacyAuthenticatorKeepsTheOptionalStoreInterfaces(t *testing.T) {
	auth := authenticator(t, optionalStore{newStore()})
	if _, ok := auth.store.(SessionStore); !ok {
		t.Error("the Authenticator's store lost SessionStore")
	}
	if _, ok := auth.store.(AccessUserStore); !ok {
		t.Error("the Authenticator's store lost AccessUserStore")
	}
}

func TestLegacyCountBestEffort(t *testing.T) {
	admin := func(c jwt.MapClaims) { c["role"] = "admin" }
	cases := []struct {
		name    string
		level   Authz
		headers func(*testing.T) map[string]string
		store   func() *fakeStore
	}{
		{"admin allow", Admin, func(t *testing.T) map[string]string { return validAuth(t, admin) }, func() *fakeStore { return tokenOrgStore(true, "admin") }},
		{"admin deny", Admin, func(t *testing.T) map[string]string { return validAuth(t, nil) }, func() *fakeStore { return tokenOrgStore(true, "member") }},
		{"superuser deny", Superuser, func(t *testing.T) map[string]string { return validAuth(t, admin) }, newStore},
		{"optional anonymous", Optional, func(*testing.T) map[string]string { return nil }, newStore},
		{"public", Public, func(*testing.T) map[string]string { return nil }, newStore},
		{"no header", Authenticated, func(*testing.T) map[string]string { return nil }, newStore},
	}
	run := func(t *testing.T, add legacyAdder, set bool, tc int) response {
		previous := legacyPathAdd
		if set {
			legacyPathAdd = add
		}
		t.Cleanup(func() { legacyPathAdd = previous })
		c := cases[tc]
		return guardServe(t, c.level, c.store(), c.headers(t))
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := recordLegacy(t)
			base := run(t, nil, false, i)
			if len(rec.keys()) == 0 {
				t.Fatal("the recording run counted nothing")
			}
			for name, add := range map[string]legacyAdder{
				"panic": func(context.Context, string, string) { panic("meter down") },
				"nil":   nil,
			} {
				got := run(t, add, true, i)
				if got.status != base.status || got.body != base.body {
					t.Errorf("%s adder: %d %q, want %d %q", name, got.status, got.body, base.status, base.body)
				}
			}
		})
	}
}

func TestLegacyNoLabelFromTheRequest(t *testing.T) {
	rec := recordLegacy(t)
	hostile := validAuth(t, func(c jwt.MapClaims) { c["role"] = "admin"; c["email"] = "pii@example.com" })
	hostile["X-Org-Id"] = strangerOrg
	hostile["User-Agent"] = "secret-agent"
	for _, level := range []Authz{Public, Authenticated, Admin, Superuser, AdminOrg, Optional} {
		store := newStore()
		withClaimRole(store, claims(func(c jwt.MapClaims) { c["role"] = "admin" }))
		serve(wrapLevel(NewGuard(authenticator(t, store), quiet()), level, echo), http.MethodGet, "/orgs/"+strangerOrg+"/secret?token=abc", hostile)
	}
	allowed := legacyPinnedKinds()
	for _, key := range rec.keys() {
		if !slices.Contains(allowed, key) {
			t.Errorf("label pair %q is outside the closed set", key)
		}
	}
}

func legacyPinnedKinds() []string {
	return []string{
		"guard_level/public", "guard_level/authenticated", "guard_level/admin", "guard_level/admin_org", "guard_level/superuser", "guard_level/optional",
		"role_source/membership_row", "role_source/claim_fallback", "role_source/platform_bypass", "role_source/platform_gate",
		"direct_read/users", "direct_read/memberships", "direct_read/org_memberships", "direct_read/impersonation_sessions", "direct_read/auth_sessions",
	}
}

// The label set the code can emit, the set this test pins and the row of
// internal/platform/otlpmetrics/labels.tsv are one set.
func TestLegacyLabelSetIsPinnedToTheLabelsTable(t *testing.T) {
	code := []string{
		surfaceGuardLevel + "/" + kindGuardPublic, surfaceGuardLevel + "/" + kindGuardAuthenticated, surfaceGuardLevel + "/" + kindGuardAdmin,
		surfaceGuardLevel + "/" + kindGuardAdminOrg, surfaceGuardLevel + "/" + kindGuardSuperuser, surfaceGuardLevel + "/" + kindGuardOptional,
		surfaceRoleSource + "/" + kindRoleMembershipRow, surfaceRoleSource + "/" + kindRoleClaimFallback,
		surfaceRoleSource + "/" + kindRolePlatformBypass, surfaceRoleSource + "/" + kindRolePlatformGate,
		surfaceDirectRead + "/" + kindReadUsers, surfaceDirectRead + "/" + kindReadMemberships, surfaceDirectRead + "/" + kindReadOrgMemberships,
		surfaceDirectRead + "/" + kindReadImpersonation, surfaceDirectRead + "/" + kindReadAuthSessions,
	}
	pinned := legacyPinnedKinds()
	slices.Sort(code)
	slices.Sort(pinned)
	if !slices.Equal(code, pinned) {
		t.Fatalf("constants %v, pinned %v", code, pinned)
	}
	table, err := os.ReadFile("../../platform/otlpmetrics/labels.tsv")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(table), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) >= 2 && fields[0] == legacyPathMetric {
			if fields[1] != "kind,surface" {
				t.Fatalf("labels.tsv keys %q, want kind,surface", fields[1])
			}
			return
		}
	}
	t.Fatalf("labels.tsv has no row for %s", legacyPathMetric)
}
