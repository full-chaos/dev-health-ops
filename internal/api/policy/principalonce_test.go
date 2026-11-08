package policy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"go/ast"
	"go/build"
	"go/constant"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
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

func newScopeRig(t *testing.T, routedMode bool, row *BearerRow, configure ...func(*countedStore)) *scopeRig {
	t.Helper()
	rig := &scopeRig{store: &countedStore{accessStore: newAccessStore()}, validator: &fakeValidator{result: userClaims(nil)}}
	for _, apply := range configure {
		apply(rig.store)
	}
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
	if want := []string{"svc_", "fcpush_", "fcacr_", "whsec_", "fcwlb_", "fcsak_", "fcoac_", "fcocs_"}; !slices.Equal(prefixes, want) {
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
// byte that is neither, so no value with that prefix is a JWS.
func TestRefusalPrefixesNeverVerify(t *testing.T) {
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
			if _, _, err := jwt.NewParser().ParseUnverified(value, jwt.MapClaims{}); err == nil {
				t.Fatalf("%s parsed as a JWS", value)
			}
		}
	}
}

// TestPrincipalIsAuthenticatedOnceForTheScope: OrgScope and Impersonation
// share one authenticate of the request's bearer.
func TestPrincipalIsAuthenticatedOnceForTheScope(t *testing.T) {
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
	for _, problem := range authorizationGuardProblems(".", []string{"principalonce.go", "guard.go"}) {
		t.Error(problem)
	}
}

// headerKey returns the key expression of a Header.Get, Header.Values or
// Header[key] read.
func headerKey(node ast.Node, info *types.Info) (ast.Expr, bool) {
	isHeader := func(expr ast.Expr) bool {
		typ := info.Types[expr].Type
		if typ == nil {
			return false
		}
		switch typ.String() {
		case "net/http.Header", "net/textproto.MIMEHeader":
			return true
		}
		return false
	}
	switch typed := node.(type) {
	case *ast.CallExpr:
		if sel, ok := typed.Fun.(*ast.SelectorExpr); ok && (sel.Sel.Name == "Get" || sel.Sel.Name == "Values") && len(typed.Args) == 1 && isHeader(sel.X) {
			return typed.Args[0], true
		}
	case *ast.IndexExpr:
		if isHeader(typed.X) {
			return typed.Index, true
		}
	}
	return nil, false
}

var goRootOnce sync.Once

// sourceImporter returns a source importer. A test binary built with
// -trimpath has no GOROOT, so go/build cannot find the standard library
// until the go command supplies it.
func sourceImporter() types.Importer {
	goRootOnce.Do(func() {
		if build.Default.GOROOT != "" {
			return
		}
		out, err := exec.Command("go", "env", "GOROOT").Output()
		if err != nil || strings.TrimSpace(string(out)) == "" {
			return
		}
		build.Default.GOROOT = strings.TrimSpace(string(out))
	})
	if build.Default.GOROOT == "" {
		panic("guard type check: GOROOT is empty and `go env GOROOT` failed")
	}
	return importer.ForCompiler(token.NewFileSet(), "source", nil)
}

// authorizationGuardProblems scans the production files of dir and returns
// every read of the Authorization header outside allowed, and every reason
// the scan cannot be trusted.
func authorizationGuardProblems(dir string, allowed []string) []string {
	return guardProblemsWith(dir, allowed, sourceImporter())
}

func guardProblemsWith(dir string, allowed []string, fallback types.Importer) []string {
	var problems []string
	entries, err := os.ReadDir(dir)
	if err != nil {
		return []string{err.Error()}
	}
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	var parsed []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			return []string{err.Error()}
		}
		files[name] = file
		parsed = append(parsed, file)
	}
	if len(parsed) == 0 {
		return []string{"no production Go files parsed in " + dir}
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}}
	thirdParty := newLocalImporter(fset, dir, fallback)
	conf := types.Config{
		Importer: thirdParty,
		Error: func(err error) {
			if typeErr, ok := err.(types.Error); ok && thirdParty.unresolved(typeErr.Msg) {
				return
			}
			problems = append(problems, "type check: "+err.Error())
		},
	}
	_, _ = conf.Check("policy", fset, parsed, info)
	for name, file := range files {
		if name == "scope.go" || name == "accessroute.go" {
			for _, spec := range file.Imports {
				if path, _ := strconv.Unquote(spec.Path.Value); path == "encoding/base64" || path == "encoding/json" {
					problems = append(problems, name+" imports "+path)
				}
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case ast.Expr:
				if value := info.Types[typed].Value; value != nil && value.Kind() == constant.String &&
					strings.EqualFold(constant.StringVal(value), "Authorization") && !slices.Contains(allowed, name) {
					problems = append(problems, fset.Position(typed.Pos()).String()+" reads the Authorization header")
				}
			}
			if !slices.Contains(allowed, name) {
				if key, ok := headerKey(node, info); ok && info.Types[key].Value == nil {
					problems = append(problems, fset.Position(key.Pos()).String()+" reads a header with a key that is not a constant")
				}
			}
			if call, ok := node.(*ast.CallExpr); ok {
				if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "BearerToken" && !slices.Contains(allowed, name) {
					problems = append(problems, fset.Position(call.Pos()).String()+" calls BearerToken")
				}
			}
			return true
		})
	}
	return problems
}

// localImporter resolves the imports that belong to the module that holds dir
// from source, the standard library with the fallback, and every other import
// as an empty complete package: the guard needs only this module's constants.
type localImporter struct {
	fset     *token.FileSet
	fallback types.Importer
	root     string
	module   string
	cache    map[string]*types.Package
	outside  map[string]bool
}

func newLocalImporter(fset *token.FileSet, dir string, fallback types.Importer) *localImporter {
	l := &localImporter{fset: fset, fallback: fallback, root: dir, cache: map[string]*types.Package{}, outside: map[string]bool{}}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return l
	}
	for cur := abs; ; cur = filepath.Dir(cur) {
		if mod, err := os.ReadFile(filepath.Join(cur, "go.mod")); err == nil {
			if fields := strings.Fields(strings.SplitN(string(mod), "\n", 2)[0]); len(fields) == 2 && fields[0] == "module" {
				l.module, l.root = fields[1], cur
			}
			break
		}
		if filepath.Dir(cur) == cur {
			break
		}
	}
	return l
}

// unresolved reports whether a type error only names a member of an
// out-of-module package.
func (l *localImporter) unresolved(msg string) bool {
	for name := range l.outside {
		if strings.HasPrefix(msg, "undefined: "+name+".") {
			return true
		}
	}
	return false
}

func (l *localImporter) Import(path string) (*types.Package, error) {
	if l.module == "" || (path != l.module && !strings.HasPrefix(path, l.module+"/")) {
		if !strings.Contains(strings.SplitN(path, "/", 2)[0], ".") {
			return l.fallback.Import(path)
		}
		elems := strings.Split(path, "/")
		name := elems[len(elems)-1]
		if match, _ := regexp.MatchString(`^v[0-9]+$`, name); match && len(elems) > 1 {
			name = elems[len(elems)-2]
		}
		l.outside[name] = true
		pkg := types.NewPackage(path, name)
		pkg.MarkComplete()
		return pkg, nil
	}
	if pkg, ok := l.cache[path]; ok {
		return pkg, nil
	}
	dir := filepath.Join(l.root, strings.TrimPrefix(strings.TrimPrefix(path, l.module), "/"))
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []*ast.File
	for _, entry := range entries {
		if name := entry.Name(); strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go") {
			file, err := parser.ParseFile(l.fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
			if err != nil {
				return nil, err
			}
			files = append(files, file)
		}
	}
	pkg, _ := (&types.Config{Importer: l, Error: func(error) {}}).Check(path, l.fset, files, nil)
	l.cache[path] = pkg
	return pkg, nil
}

// plantModule writes a throwaway module with an imported header-name package
// and the given files, and returns its root.
func plantModule(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	all := map[string]string{
		"go.mod":     "module example.test/p\n\ngo 1.21\n",
		"hdr/hdr.go": "package hdr\n\nconst Authorization = \"Authorization\"\nconst Author = \"Author\"\n",
		"clean.go":   "package p\n\nfunc clean() int { return 1 }\n",
	}
	for name, body := range files {
		all[name] = body
	}
	for name, body := range all {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// TestAuthorizationGuardResolvesImportedConstants plants reads the guard must
// refuse (a key built from an imported constant, a local alias of one, an
// imported prefix, a read inside ReadOnce) and cases it must fail closed on.
func TestAuthorizationGuardResolvesImportedConstants(t *testing.T) {
	allowed := []string{"principalonce.go", "guard.go"}
	const head = "package p\n\nimport (\n\t\"net/http\"\n\n\t\"example.test/p/hdr\"\n)\n\n"
	refused := map[string]map[string]string{
		"imported constant direct":         {"scope.go": head + "func f(r *http.Request) string { return r.Header.Get(hdr.Authorization) }\n"},
		"local alias of imported constant": {"scope.go": head + "const key = hdr.Authorization\n\nfunc f(r *http.Request) string { return r.Header.Get(key) }\n"},
		"imported prefix plus suffix":      {"scope.go": head + "func f(r *http.Request) string { return r.Header.Get(hdr.Author + \"ization\") }\n"},
		"read inside ReadOnce":             {"request_reads.go": head + "type A struct{}\n\nfunc (a *A) ReadOnce(next http.Handler) http.Handler {\n\treturn http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {\n\t\t_ = r.Header.Get(hdr.Authorization)\n\t\tnext.ServeHTTP(w, r)\n\t})\n}\n"},
		"unresolved key in Get":            {"scope.go": head + "func f(r *http.Request, key string) string { return r.Header.Get(key) }\n"},
		"unresolved key in Values":         {"scope.go": head + "func f(r *http.Request, key string) []string { return r.Header.Values(key) }\n"},
		"unresolved key in index":          {"scope.go": head + "func f(r *http.Request, key string) []string { return r.Header[key] }\n"},
		"type error":                       {"scope.go": "package p\n\nfunc f() int { return missing }\n"},
	}
	for name, files := range refused {
		t.Run("refuses "+name, func(t *testing.T) {
			t.Parallel()
			if problems := authorizationGuardProblems(plantModule(t, files), allowed); len(problems) == 0 {
				t.Fatal("guard passed; want it to refuse")
			}
		})
	}
	t.Run("refuses a scan of zero files", func(t *testing.T) {
		if problems := authorizationGuardProblems(t.TempDir(), allowed); len(problems) == 0 {
			t.Fatal("guard passed with no files; want it to refuse")
		}
	})
	accepted := map[string]map[string]string{
		"clean module":           nil,
		"allowed file reads it":  {"principalonce.go": head + "func f(r *http.Request) string { return r.Header.Get(hdr.Authorization) }\n"},
		"other header, constant": {"scope.go": head + "var _ = hdr.Author\n\nfunc f(r *http.Request) string { return r.Header.Get(\"X-Org-Id\") }\n"},
	}
	for name, files := range accepted {
		t.Run("accepts "+name, func(t *testing.T) {
			t.Parallel()
			if problems := authorizationGuardProblems(plantModule(t, files), allowed); len(problems) != 0 {
				t.Fatalf("guard refused: %v", problems)
			}
		})
	}
}

// failingThirdParty resolves the standard library and refuses every other
// import, as a CI leg without the third-party sources does.
type failingThirdParty struct{ std types.Importer }

func (f failingThirdParty) Import(path string) (*types.Package, error) {
	if strings.Contains(strings.SplitN(path, "/", 2)[0], ".") {
		return nil, errors.New("no source for " + path)
	}
	return f.std.Import(path)
}

// TestGuardChecksTheRealPackageWithoutThirdPartyImports: the guard needs only
// this module's constants, so it must pass when no third-party import loads.
func TestGuardChecksTheRealPackageWithoutThirdPartyImports(t *testing.T) {
	fallback := failingThirdParty{std: sourceImporter()}
	for _, problem := range guardProblemsWith(".", []string{"principalonce.go", "guard.go"}, fallback) {
		t.Error(problem)
	}
}

// TestProductionChainsInstallPrincipalFirst pins the two production scope
// chains: the api listener's scope list is ReadOnce (one membership read per
// request for the org scope and the route's guard), then Principal, and
// query-api's /graphql wraps OrgScope (which wraps Impersonation) in Principal.
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
	if len(apiLists) != 1 || !slices.Equal(apiLists[0], []string{"ReadOnce", "Principal", "OrgScope", "Impersonation"}) {
		t.Fatalf("api listener scope lists %v, want one [ReadOnce Principal OrgScope Impersonation]", apiLists)
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
