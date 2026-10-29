package policy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// countedStore counts every store read the scope can make.
type countedStore struct {
	*accessStore
	queries int
}

func (c *countedStore) UserState(ctx context.Context, id uuid.UUID) (UserState, bool, error) {
	c.queries++
	return c.accessStore.UserState(ctx, id)
}

func (c *countedStore) Membership(ctx context.Context, user, org uuid.UUID) (string, bool, error) {
	c.queries++
	return c.accessStore.Membership(ctx, user, org)
}

func (c *countedStore) ActiveImpersonation(ctx context.Context, admin uuid.UUID) (*Impersonation, error) {
	c.queries++
	return c.accessStore.ActiveImpersonation(ctx, admin)
}

func (c *countedStore) AccessUser(ctx context.Context, id uuid.UUID) (AccessUserRow, bool, error) {
	c.queries++
	return c.accessStore.AccessUser(ctx, id)
}

func (c *countedStore) SessionState(ctx context.Context, id uuid.UUID) (SessionRow, bool, error) {
	c.queries++
	return c.accessStore.SessionState(ctx, id)
}

// scopeRig is a scope over counting doubles: every token the scope
// authenticates, every routed Validate, every store read.
type scopeRig struct {
	store     *countedStore
	validator *fakeValidator
	scope     *Scope
	tokens    []string
}

const rigProfile = "POST /p"

func newScopeRig(t *testing.T, routedMode bool, row *BearerRow) *scopeRig {
	t.Helper()
	rig := &scopeRig{store: &countedStore{accessStore: newAccessStore()}, validator: &fakeValidator{result: userClaims(nil)}}
	auth := authenticator(t, rig.store)
	if routedMode {
		if err := auth.RouteAccess(rig.validator, "dev-health-ops-api"); err != nil {
			t.Fatal(err)
		}
	}
	rig.scope = NewScope(auth, quiet())
	rig.scope.DeclaredRows(func(profile string) (BearerRow, bool) {
		if row == nil || profile != rigProfile {
			return BearerRow{}, false
		}
		return *row, true
	})
	rig.scope.authenticate = func(ctx context.Context, token string) (*User, error) {
		rig.tokens = append(rig.tokens, token)
		return auth.Authenticate(ctx, token)
	}
	return rig
}

func (rig *scopeRig) chain(next http.Handler) http.Handler {
	handler := next
	layers := rig.scope.Layers()
	for index := len(layers) - 1; index >= 0; index-- {
		handler = layers[index](handler)
	}
	return RouteProfiles(func(*http.Request) (string, bool) { return rigProfile, true })(handler)
}

func serveRaw(handler http.Handler, authorization string, headers map[string]string) response {
	request, _ := http.NewRequest(http.MethodPost, "/p", nil)
	request.Header["Authorization"] = []string{authorization}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return response{recorder.Code, recorder.Body.String(), recorder.Header()}
}

func superuserToken(t *testing.T) string {
	return sign(t, claims(func(c jwt.MapClaims) { c["is_superuser"] = true }))
}

// TestEarlyRefusalRowsReachNoValidatorAndNoQuery: every refusal-table row is
// decided before any validator or store read, in both modes. The caller is
// anonymous to the scope: X-Org-Id is not judged, no impersonation runs.
func TestEarlyRefusalRowsReachNoValidatorAndNoQuery(t *testing.T) {
	type rowCase struct {
		name  string
		auth  func(t *testing.T) string
		row   *BearerRow
		super bool
	}
	var cases []rowCase
	for _, refusal := range earlyRefusals {
		prefix := refusal.Prefix
		cases = append(cases,
			rowCase{name: "prefix " + prefix, auth: func(*testing.T) string { return "Bearer " + prefix + strings.Repeat("A", 43) }},
			rowCase{name: "prefix " + prefix + " after U+00A0", auth: func(*testing.T) string { return "Bearer\xa0" + prefix + "x.y.z" }},
			rowCase{name: "prefix " + prefix + " on an access row", row: &BearerRow{AccessToken: true}, auth: func(*testing.T) string { return "bearer " + prefix + "eyJ.eyJ.sig" }},
		)
	}
	cases = append(cases,
		rowCase{name: "webhook row with a valid superuser token", row: &BearerRow{Webhook: true}, auth: func(t *testing.T) string { return "Bearer " + superuserToken(t) }, super: true},
		rowCase{name: "webhook row with an unparseable value", row: &BearerRow{Webhook: true}, auth: func(*testing.T) string { return "Bearer a b c" }},
		rowCase{name: "domain row with a valid superuser token", row: &BearerRow{Domain: true}, auth: func(t *testing.T) string { return "Bearer " + superuserToken(t) }, super: true},
	)
	for _, mode := range []string{"legacy", "routed"} {
		for _, tc := range cases {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				rig := newScopeRig(t, mode == "routed", tc.row)
				if tc.super {
					rig.store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
					rig.store.sessions[userID] = &Impersonation{AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID}
				}
				got := serveRaw(rig.chain(echo), tc.auth(t), map[string]string{"X-Org-Id": strangerOrg})
				if got.status != http.StatusOK || got.body != "org= user=- imp=-" || got.header.Get("X-Impersonating") != "" {
					t.Fatalf("%d %q %v, want an anonymous caller", got.status, got.body, got.header)
				}
				if len(rig.tokens) != 0 || len(rig.validator.profiles) != 0 || rig.store.queries != 0 {
					t.Fatalf("authenticated %d, validator %d, queries %d; want 0 each", len(rig.tokens), len(rig.validator.profiles), rig.store.queries)
				}
			})
		}
	}
}

// TestRefusalTableHasOneRowPerPrefix: one row per prefix, and every prefixed
// Bearer class of credential-classes.json is a row's class with that prefix.
func TestRefusalTableHasOneRowPerPrefix(t *testing.T) {
	seen := map[string]bool{}
	tableClass := map[string]string{}
	for _, refusal := range earlyRefusals {
		if refusal.Prefix == "" || seen[refusal.Prefix] {
			t.Fatalf("prefix %q is empty or repeated", refusal.Prefix)
		}
		seen[refusal.Prefix] = true
		for _, class := range refusal.Classes {
			tableClass[class] = refusal.Prefix
		}
	}
	var prefixes []string
	for _, refusal := range earlyRefusals {
		prefixes = append(prefixes, refusal.Prefix)
	}
	if want := []string{"svc_", "fcpush_", "fcacr_", "whsec_", "fcwlb_"}; !slices.Equal(prefixes, want) {
		t.Fatalf("refusal prefixes %v, want exactly %v", prefixes, want)
	}
	var document struct {
		Classes []struct {
			ClassID string `json:"class_id"`
			Markers []struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"markers"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(authv1.CredentialClasses, &document); err != nil {
		t.Fatal(err)
	}
	contract := map[string]string{}
	for _, class := range document.Classes {
		prefix, bearer := "", false
		for _, marker := range class.Markers {
			switch {
			case marker.Kind == "credential_prefix":
				prefix = marker.Name
			case marker.Kind == "authorization_scheme" && marker.Name == "Bearer":
				bearer = true
			}
		}
		if prefix != "" && bearer {
			contract[class.ClassID] = prefix
		}
	}
	if len(contract) == 0 {
		t.Fatal("no prefixed Bearer class in credential-classes.json")
	}
	for class, prefix := range contract {
		row, ok := tableClass[class]
		if !ok || !strings.HasPrefix(prefix, row) {
			t.Errorf("class %s (prefix %s) has no refusal row with its prefix (row %q)", class, prefix, row)
		}
	}
	for class := range tableClass {
		if _, ok := contract[class]; !ok {
			t.Errorf("refusal row names %s, which is not a prefixed Bearer class", class)
		}
	}
}

// TestRefusalPrefixesNeverVerify is the refusal parity proof: a JWS header is
// a JSON object, so its first byte is JSON whitespace or '{' (base64url "ey"
// for '{"'). The first base64url quantum of every refusal prefix decodes to a
// byte that is neither, so no value with that prefix is a JWS, and the legacy
// verifier refuses every one today.
func TestRefusalPrefixesNeverVerify(t *testing.T) {
	verifier, err := edgetoken.New(testKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	if got := base64.RawURLEncoding.EncodeToString([]byte(`{"`))[:2]; got != "ey" {
		t.Fatalf("base64url of {\" starts %q", got)
	}
	for _, refusal := range earlyRefusals {
		if len(refusal.Prefix) < 4 {
			t.Fatalf("prefix %q is shorter than one base64url quantum", refusal.Prefix)
		}
		raw, err := base64.RawURLEncoding.DecodeString(refusal.Prefix[:4])
		if err != nil {
			t.Fatalf("%s: %v", refusal.Prefix, err)
		}
		if first := raw[0]; bytes.IndexByte([]byte(" \t\n\r{"), first) >= 0 {
			t.Fatalf("prefix %s decodes to first byte %q, which can start a JSON object", refusal.Prefix, first)
		}
		for _, value := range []string{refusal.Prefix + strings.Repeat("A", 43), refusal.Prefix + "eyJhbGciOiJIUzI1NiJ9.e30.sig"} {
			if _, err := verifier.Verify(value); err == nil {
				t.Fatalf("%s verified", value)
			}
			if _, _, err := jwt.NewParser().ParseUnverified(value, jwt.MapClaims{}); err == nil {
				t.Fatalf("%s parsed as a JWS", value)
			}
		}
	}
}

// TestImpersonationGateReadsTheVerifiedClaimOnly: the impersonation check runs
// for a legacy token only when its VERIFIED is_superuser claim is truthy. A
// unverifiable superuser claim (a token that does not verify) gets no impersonation
// path: no session read, and one authenticate for the whole scope.
func TestImpersonationGateReadsTheVerifiedClaimOnly(t *testing.T) {
	unverifiable, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims(func(c jwt.MapClaims) { c["is_superuser"] = true })).SignedString([]byte(strings.Repeat("other-key-", 4)))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name         string
		token        func(t *testing.T) string
		impersonated bool
		sessionReads int
	}{
		{name: "unverifiable superuser claim", token: func(*testing.T) string { return unverifiable }},
		{name: "verified claim false, superuser row", token: func(t *testing.T) string { return sign(t, claims(nil)) }},
		{name: "verified claim true", token: superuserToken, impersonated: true, sessionReads: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newScopeRig(t, false, nil)
			rig.store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
			rig.store.sessions[userID] = &Impersonation{AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID}
			got := serveRaw(rig.chain(echo), "Bearer "+tc.token(t), nil)
			if impersonated := got.header.Get("X-Impersonating") == "true"; impersonated != tc.impersonated || got.status != http.StatusOK {
				t.Fatalf("%d %q impersonated=%v, want %v", got.status, got.body, impersonated, tc.impersonated)
			}
			if rig.store.sessionCalls != tc.sessionReads || len(rig.tokens) != 1 {
				t.Fatalf("session reads %d (want %d), authenticated %d (want 1)", rig.store.sessionCalls, tc.sessionReads, len(rig.tokens))
			}
		})
	}
}

// TestPrincipalIsAuthenticatedOnceForTheScope: OrgScope and Impersonation
// share one authenticate of the request's bearer, in both modes.
func TestPrincipalIsAuthenticatedOnceForTheScope(t *testing.T) {
	t.Run("legacy", func(t *testing.T) {
		rig := newScopeRig(t, false, &BearerRow{AccessToken: true})
		rig.store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
		rig.store.sessions[userID] = &Impersonation{AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID}
		got := serveRaw(rig.chain(echo), "Bearer "+superuserToken(t), map[string]string{"X-Org-Id": strangerOrg})
		if got.header.Get("X-Impersonating") != "true" || len(rig.tokens) != 1 || rig.store.userCalls != 1 {
			t.Fatalf("%d %q authenticated %d users reads %d", got.status, got.body, len(rig.tokens), rig.store.userCalls)
		}
	})
	t.Run("routed user access token", func(t *testing.T) {
		rig := newScopeRig(t, true, &BearerRow{AccessToken: true})
		rig.store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
		rig.store.sessions[userID] = &Impersonation{AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID}
		header, _ := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{}).SigningString()
		got := serveRaw(rig.chain(echo), "Bearer "+header+".sig", nil)
		if got.header.Get("X-Impersonating") != "true" || len(rig.validator.profiles) != 1 {
			t.Fatalf("%d %q validator calls %v", got.status, got.body, rig.validator.profiles)
		}
	})
}

// TestRoutedScopeAsksTheValidatorOnlyForAnAccessRow is acp D6 decided before
// the validator: with the issuer set, no row or a row without an access class
// makes the caller anonymous to the scope without a Validate call.
func TestRoutedScopeAsksTheValidatorOnlyForAnAccessRow(t *testing.T) {
	header, _ := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{}).SigningString()
	for name, tc := range map[string]struct {
		row   *BearerRow
		calls int
		body  string
	}{
		"no row":        {row: nil, body: "org= user=- imp=-"},
		"public row":    {row: &BearerRow{}, body: "org= user=- imp=-"},
		"access row":    {row: &BearerRow{AccessToken: true}, calls: 1, body: "org=" + ownOrg + " user=- imp=-"},
		"legacy no row": {row: nil, body: "org= user=- imp=-"},
	} {
		t.Run(name, func(t *testing.T) {
			rig := newScopeRig(t, name != "legacy no row", tc.row)
			got := serveRaw(rig.chain(echo), "Bearer "+header+".sig", nil)
			if got.body != tc.body || len(rig.validator.profiles) != tc.calls {
				t.Fatalf("%q validator calls %v, want %q and %d", got.body, rig.validator.profiles, tc.body, tc.calls)
			}
		})
	}
}

// TestScopeWithoutPrincipalDecisionFailsClosed is acp D-A: OrgScope and
// Impersonation never read the header themselves; without the Principal
// decision they refuse with the generic 500, logged once with a reason code.
func TestScopeWithoutPrincipalDecisionFailsClosed(t *testing.T) {
	for name, build := range map[string]func(*Scope) http.Handler{
		"org scope":     func(s *Scope) http.Handler { return s.OrgScope(s.Impersonation(echo)) },
		"impersonation": func(s *Scope) http.Handler { return s.Impersonation(echo) },
	} {
		for _, authorization := range []string{"", "Bearer valid"} {
			t.Run(name+"/"+strconv.Quote(authorization), func(t *testing.T) {
				var logs bytes.Buffer
				store := newStore()
				scope := NewScope(authenticator(t, store), slog.New(slog.NewTextHandler(&logs, nil)))
				headers := map[string]string{}
				if authorization != "" {
					headers["Authorization"] = "Bearer " + sign(t, claims(nil))
				}
				got := serve(build(scope), http.MethodGet, "/x", headers)
				if got.status != http.StatusInternalServerError || got.body != `{"detail":"Internal Server Error"}` {
					t.Fatalf("%d %s", got.status, got.body)
				}
				if n := strings.Count(logs.String(), "reason=principal_undecided"); n != 1 || store.userCalls != 0 {
					t.Fatalf("%d reason lines, %d user reads: %s", n, store.userCalls, logs.String())
				}
			})
		}
	}
}

// TestOnlyThePrincipalStepReadsAuthorization: in this package only the
// Principal step and the route guard read the Authorization header or call
// BearerToken, and the scope files decode no token segment.
func TestOnlyThePrincipalStepReadsAuthorization(t *testing.T) {
	allowed := []string{"principalonce.go", "guard.go"}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		if name == "scope.go" || name == "accessroute.go" {
			for _, spec := range file.Imports {
				if path, _ := strconv.Unquote(spec.Path.Value); path == "encoding/base64" || path == "encoding/json" {
					t.Errorf("%s imports %s", name, path)
				}
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.BasicLit:
				if typed.Kind == token.STRING && typed.Value == `"Authorization"` && !slices.Contains(allowed, name) {
					t.Errorf("%s reads the Authorization header", fset.Position(typed.Pos()))
				}
			case *ast.CallExpr:
				if ident, ok := typed.Fun.(*ast.Ident); ok && ident.Name == "BearerToken" && !slices.Contains(allowed, name) {
					t.Errorf("%s calls BearerToken", fset.Position(typed.Pos()))
				}
			}
			return true
		})
	}
}

// TestProductionChainsInstallPrincipalFirst pins the two production scope
// chains: the api listener's scope list starts with Principal, and query-api's
// /graphql wraps OrgScope (which wraps Impersonation) in Principal.
func TestProductionChainsInstallPrincipalFirst(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	parse := func(rel string) *ast.File {
		file, err := parser.ParseFile(fset, filepath.Join(root, rel), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		return file
	}
	selectorName := func(expr ast.Expr) string {
		if sel, ok := expr.(*ast.SelectorExpr); ok {
			return sel.Sel.Name
		}
		return ""
	}
	var apiLists [][]string
	ast.Inspect(parse("internal/apiservice/service.go"), func(node ast.Node) bool {
		if lit, ok := node.(*ast.CompositeLit); ok {
			var names []string
			for _, elt := range lit.Elts {
				names = append(names, selectorName(elt))
			}
			if slices.Contains(names, "OrgScope") {
				apiLists = append(apiLists, names)
			}
		}
		return true
	})
	if len(apiLists) != 1 || !slices.Equal(apiLists[0], []string{"Principal", "OrgScope", "Impersonation"}) {
		t.Fatalf("api listener scope lists %v, want one [Principal OrgScope Impersonation]", apiLists)
	}
	var edge []string
	ast.Inspect(parse("internal/queryapi/server/graphql_edge_route.go"), func(node ast.Node) bool {
		if fn, ok := node.(*ast.FuncDecl); ok && fn.Name.Name != "graphQLEdgeChain" {
			return false
		}
		if assign, ok := node.(*ast.AssignStmt); ok && len(assign.Rhs) == 1 {
			var chain []string
			for expr := assign.Rhs[0]; ; {
				call, ok := expr.(*ast.CallExpr)
				if !ok || len(call.Args) != 1 {
					break
				}
				chain = append(chain, selectorName(call.Fun))
				expr = call.Args[0]
			}
			if slices.Contains(chain, "OrgScope") || slices.Contains(chain, "Impersonation") || slices.Contains(chain, "Principal") {
				edge = append(edge, strings.Join(chain, "("))
			}
		}
		return true
	})
	if !slices.Equal(edge, []string{"Impersonation", "Principal(OrgScope"}) {
		t.Fatalf("/graphql scope wiring %v, want [Impersonation Principal(OrgScope]", edge)
	}
}
