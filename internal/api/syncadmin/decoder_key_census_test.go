package syncadmin

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strconv"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// decodeSyncConfigUpdateKeys is every body key decodeSyncConfigUpdate (update_config.go) reads: the string literals of
// its key list and the key argument of each problems.<Optional|Required...>(object, "<key>") call.
func decodeSyncConfigUpdateKeys(t *testing.T) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "update_config.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	add := func(expr ast.Expr) {
		literal, ok := expr.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return
		}
		key, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(keys, key) {
			keys = append(keys, key)
		}
	}
	for _, decl := range file.Decls {
		function, ok := decl.(*ast.FuncDecl)
		if !ok || function.Name.Name != "decodeSyncConfigUpdate" {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.CompositeLit:
				for _, element := range node.Elts {
					add(element)
				}
			case *ast.CallExpr:
				if selector, ok := node.Fun.(*ast.SelectorExpr); ok && len(node.Args) >= 2 {
					if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == "problems" {
						add(node.Args[1])
					}
				}
			}
			return true
		})
	}
	slices.Sort(keys)
	return keys
}

// Every key decodeSyncConfigUpdate accepts is classified by listOnlyPatch: next to a list-only body, each key other than
// sync_options (sent with a value it decodes without a problem) makes the PATCH not list-only, so a new decoder key
// cannot slip a change past the GitHub runtime-options refresh unclassified.
func TestEveryDecoderKeyIsClassifiedByListOnlyPatch(t *testing.T) {
	keys := decodeSyncConfigUpdateKeys(t)
	for _, want := range []string{"initial_sync_depth", "is_active", "schedule_cron", "sync_options", "sync_targets", "timezone"} {
		if !slices.Contains(keys, want) {
			t.Fatalf("premise: the census of decodeSyncConfigUpdate (%v) lacks %q", keys, want)
		}
	}
	const listOnly = `"sync_options":{"ci_artifact_exclude_prefixes":["ci-x-"]}`
	decode := func(text string) (syncConfigUpdate, bool) {
		value, err := pyjson.DecodeString(text)
		if err != nil {
			t.Fatal(err)
		}
		in, problems := decodeSyncConfigUpdate(pybody.Body{Value: value})
		return in, len(problems) == 0
	}
	if in, ok := decode(`{` + listOnly + `}`); !ok || !listOnlyPatch(in) {
		t.Fatalf("premise: a body with only the list is not list-only")
	}
	for _, key := range keys {
		if key == "sync_options" {
			continue
		}
		classified, decoded := false, false
		for _, value := range []string{`"0 * * * *"`, `true`, `1`, `["tests"]`, `{}`} {
			in, ok := decode(`{` + listOnly + `,` + strconv.Quote(key) + `:` + value + `}`)
			if !ok {
				continue
			}
			decoded = true
			classified = !listOnlyPatch(in)
			break
		}
		if !decoded {
			t.Errorf("decoder key %q: no candidate value decodes without a problem; add one to this cell", key)
		} else if !classified {
			t.Errorf("decoder key %q is not classified by listOnlyPatch: a body carrying it next to the list reads as list-only", key)
		}
	}
	t.Logf("decoder keys: %v", keys)
}
