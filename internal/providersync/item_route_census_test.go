package providersync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// itemDecoderExceptions are item decoders whose callers do not name a route yet, each with its reason.
var itemDecoderExceptions = map[string]string{
	"gitLabRawSize": "decodes a field value of a GraphQL node, not a page item",
}

// TestItemDecoderFailuresCarryTheRoute: a providersync function whose first parameter is `raw json.RawMessage` and that returns the bare
// providerfoundation.ErrNormalizationInvalid is an item decoder. The page walker has dropped the response by the time it
// runs, so every call site must return its error through RouteFailure (or ResponseFailure) naming the route that served
// the page.
func TestItemDecoderFailuresCarryTheRoute(t *testing.T) {
	t.Parallel()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	parsed := map[string]*ast.File{}
	decoders := map[string]bool{}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, name, source, 0)
		if err != nil {
			t.Fatal(err)
		}
		parsed[name] = file
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Recv == nil && fn.Body != nil && takesRawMessage(fn) && returnsBareNormalizationInvalid(fn.Body) {
				decoders[fn.Name.Name] = true
			}
		}
	}
	if len(decoders) < 76 {
		t.Fatalf("found %d item decoders, want at least the 76 of CHAOS-10292, CHAOS-13631, CHAOS-14279 and CHAOS-14280: %v", len(decoders), decoders)
	}
	for name := range itemDecoderExceptions {
		if !decoders[name] {
			t.Errorf("exception %s is no longer an item decoder: drop it", name)
		}
	}
	var bad []string
	calls := 0
	for _, file := range parsed {
		ast.Inspect(file, func(node ast.Node) bool {
			var list []ast.Stmt
			switch n := node.(type) {
			case *ast.BlockStmt:
				list = n.List
			case *ast.CaseClause:
				list = n.Body
			default:
				return true
			}
			for i, stmt := range list {
				callee, errName := itemDecoderCall(stmt, decoders)
				if callee == "" || itemDecoderExceptions[callee] != "" {
					continue
				}
				calls++
				routed := false
				if inIf, ok := strings.CutPrefix(errName, "@if:"); ok {
					routed = blockReturnsRouted(stmt.(*ast.IfStmt).Body, inIf)
				} else if i+1 < len(list) {
					routed = returnsRoutedError(list[i+1], errName)
				}
				if !routed {
					bad = append(bad, fset.Position(stmt.Pos()).String()+" "+callee)
				}
			}
			return true
		})
	}
	sort.Strings(bad)
	if len(bad) > 0 {
		t.Fatalf("item decoder calls whose error does not name a route:\n%s", strings.Join(bad, "\n"))
	}
	if calls < 94 {
		t.Fatalf("checked %d item decoder calls, want the 94 call sites of CHAOS-10292, CHAOS-13631, CHAOS-14279 and CHAOS-14280", calls)
	}
}

// takesRawMessage: the page-item decoder form, a first parameter `raw json.RawMessage` (one item of a page).
func takesRawMessage(fn *ast.FuncDecl) bool {
	params := fn.Type.Params.List
	if len(params) == 0 || len(params[0].Names) == 0 || params[0].Names[0].Name != "raw" {
		return false
	}
	selector, ok := params[0].Type.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "RawMessage"
}

func isNormalizationInvalid(expr ast.Expr) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "ErrNormalizationInvalid" {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == "providerfoundation"
}

func returnsBareNormalizationInvalid(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(node ast.Node) bool {
		if _, ok := node.(*ast.FuncLit); ok {
			return false
		}
		if ret, ok := node.(*ast.ReturnStmt); ok {
			for _, result := range ret.Results {
				if isNormalizationInvalid(result) {
					found = true
				}
				// A wrapper call that passes the sentinel on (return wrap(ErrNormalizationInvalid)) names no route either.
				if call, isCall := result.(*ast.CallExpr); isCall {
					for _, arg := range call.Args {
						if isNormalizationInvalid(arg) {
							found = true
						}
					}
				}
			}
		}
		return true
	})
	return found
}

// itemDecoderCall names the decoder a statement calls and the error variable it binds: `x, err := decode(raw)`,
// `x, y, err := decode(raw)` or `if err := decode(raw, &p); err != nil {` (then the error check is the statement itself).
func itemDecoderCall(stmt ast.Stmt, decoders map[string]bool) (string, string) {
	switch s := stmt.(type) {
	case *ast.AssignStmt:
		return assignedDecoderCall(s, decoders)
	case *ast.IfStmt:
		if init, ok := s.Init.(*ast.AssignStmt); ok {
			if callee, errName := assignedDecoderCall(init, decoders); callee != "" {
				return callee, "@if:" + errName
			}
		}
	}
	return "", ""
}

func assignedDecoderCall(s *ast.AssignStmt, decoders map[string]bool) (string, string) {
	if len(s.Rhs) != 1 || len(s.Lhs) == 0 {
		return "", ""
	}
	call, ok := s.Rhs[0].(*ast.CallExpr)
	if !ok {
		return "", ""
	}
	ident, ok := call.Fun.(*ast.Ident)
	if !ok || !decoders[ident.Name] {
		return "", ""
	}
	last, ok := s.Lhs[len(s.Lhs)-1].(*ast.Ident)
	if !ok {
		return ident.Name, ""
	}
	return ident.Name, last.Name
}

func returnsRoutedError(stmt ast.Stmt, errName string) bool {
	ifStmt, ok := stmt.(*ast.IfStmt)
	return ok && blockReturnsRouted(ifStmt.Body, errName)
}

func blockReturnsRouted(body *ast.BlockStmt, errName string) bool {
	if len(body.List) != 1 {
		return false
	}
	ret, ok := body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) == 0 {
		return false
	}
	call, ok := ret.Results[len(ret.Results)-1].(*ast.CallExpr)
	if !ok {
		return false
	}
	// One wrapper level is allowed (return wrap(RouteFailure(route, reason, err))): the wrapper keeps the cause chain.
	if inner, wrapped := call.Args, len(call.Args) == 1; wrapped {
		if innerCall, isCall := inner[0].(*ast.CallExpr); isCall {
			call = innerCall
		}
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || (selector.Sel.Name != "RouteFailure" && selector.Sel.Name != "ResponseFailure") || len(call.Args) != 3 {
		return false
	}
	arg, ok := call.Args[2].(*ast.Ident)
	return ok && arg.Name == errName
}

// inlineDecodeExceptions are functions of any providersync file that decode inline and return the bare ErrNormalizationInvalid,
// keyed "file:function", each with its reason.
var inlineDecodeExceptions = map[string]string{
	"launchdarkly_references_clickhouse.go:parseLaunchDarklyCodeReferences": "decodes the whole code-references document, not a page item (the caller launchdarkly_route.go:189 wraps RouteFailure with the code-references route; the resolver at launchdarkly_references_clickhouse.go:48 gets the payload that call already parsed)",
	"jira_incidents_route.go:decodeJiraIncidentJSON":                        "shared helper; the body caller jira_incidents_route.go:426 wraps ResponseInvalid(BodyDecode) and the item caller :343 wraps RouteFailure",
	"jira_work_items_route.go:decodeJiraJSON":                               "shared helper; the body callers jira_work_items_route.go:644 and jira_dev_status.go:95 wrap ResponseInvalid(BodyDecode), the graphql body caller jira_atlassian_route.go:1116 wraps it too, and the item callers (jira_work_items_route.go:429, :494, jira_atlassian_route.go, jira_team_catalog_route.go) wrap RouteFailure",
	"native_rest.go:NativeRESTHandler.normalizeGitHubWorkItems":             "cannot fail on decode: filterWorkItemWindow (native_rest.go:691) already dropped every item that does not decode into the same workItemPayload; the check shares one condition with the number and title validation",
	"native_rest.go:NativeRESTHandler.normalizeGitLabWorkItems":             "cannot fail on decode: filterWorkItemWindow (native_rest.go:691) already dropped every item that does not decode into the same workItemPayload; the check shares one condition with the iid and title validation",
	"native_rest.go:NativeRESTHandler.fetchGitHubChildren":                  "cannot fail on decode: the parent came through filterWorkItemWindow (native_rest.go:691); the check shares one condition with the number validation",
	"native_rest.go:NativeRESTHandler.fetchGitLabChildren":                  "cannot fail on decode: the parent came through filterWorkItemWindow (native_rest.go:691); the check shares one condition with the iid validation",
	"repository_shadow.go:ClickHouseRepositoryShadowSource.Load":            "decodes the settings JSON of a ClickHouse row, not a provider response",
	"github_tests_chunked_route.go:GitHubTestsRouteHandler.CollectChunks":   "the job item decode in collectRunDetail stays bare: its error reaches githubTestsOpenRunSkip, which compares it by identity (github_tests_run_listing.go:66)",
	"github_tests_run_listing.go:fetchGitHubTestsRun":                       "decodes the whole single-run body, not a page item; its error reaches githubTestsOpenRunSkip, which compares it by identity (github_tests_run_listing.go:66)",
	"gitlab_feature_flags_route.go:ensureJSONEOF":                           "trailing-data check inside the item decoder decodeGitLabFeatureFlagObject; its callers (gitlab_feature_flags_route.go:217, :255) already name the route",
}

// TestInlineItemDecodesAreListed: an inline decode (json.Unmarshal or a Decoder's Decode in an if statement) whose branch returns the
// bare ErrNormalizationInvalid names no route, and TestItemDecoderFailuresCarryTheRoute cannot see it. Outside an item decoder each
// such function is moved into an item decoder or listed in inlineDecodeExceptions with its reason.
func TestInlineItemDecodesAreListed(t *testing.T) {
	t.Parallel()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	found := map[string]bool{}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || (fn.Recv == nil && takesRawMessage(fn) && returnsBareNormalizationInvalid(fn.Body)) {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				ifStmt, ok := node.(*ast.IfStmt)
				if ok && (containsDecodeCall(ifStmt.Init) || containsDecodeCall(ifStmt.Cond)) && returnsBareNormalizationInvalid(ifStmt.Body) {
					found[name+":"+funcKey(fn)] = true
				}
				return true
			})
		}
	}
	var bad []string
	for key := range found {
		if inlineDecodeExceptions[key] == "" {
			bad = append(bad, key)
		}
	}
	for key := range inlineDecodeExceptions {
		if !found[key] {
			bad = append(bad, key+" (listed, no inline decode left: drop it)")
		}
	}
	sort.Strings(bad)
	if len(bad) > 0 {
		t.Fatalf("inline decodes that return a bare ErrNormalizationInvalid, not in inlineDecodeExceptions:\n%s", strings.Join(bad, "\n"))
	}
}

func funcKey(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	receiver := fn.Recv.List[0].Type
	if star, ok := receiver.(*ast.StarExpr); ok {
		receiver = star.X
	}
	if ident, ok := receiver.(*ast.Ident); ok {
		return ident.Name + "." + fn.Name.Name
	}
	return fn.Name.Name
}

// containsDecodeCall: a json.Unmarshal call or a Decode method call (json.Decoder) inside node.
func containsDecodeCall(node ast.Node) bool {
	if node == nil {
		return false
	}
	found := false
	ast.Inspect(node, func(inner ast.Node) bool {
		call, ok := inner.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if pkg, isIdent := selector.X.(*ast.Ident); isIdent && pkg.Name == "json" && selector.Sel.Name == "Unmarshal" {
			found = true
		}
		if selector.Sel.Name == "Decode" {
			found = true
		}
		return true
	})
	return found
}
