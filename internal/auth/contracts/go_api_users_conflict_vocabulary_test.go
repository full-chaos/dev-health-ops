package contracts

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// goAPIUsersConflictSites are the reasons go-api's admin users area answers in a 409 detail.reason, each with the
// file:line of its producers: a usersConflict call with a literal reason, and every writeUsersOwned call
// (operation_moved).
func goAPIUsersConflictSites(t *testing.T, dir string) map[string][]string {
	t.Helper()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	sites := map[string][]string{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			fn, ok := call.Fun.(*ast.Ident)
			if !ok {
				return true
			}
			at := fset.Position(call.Pos())
			where := fmt.Sprintf("%s:%d", name, at.Line)
			switch fn.Name {
			case "writeUsersOwned":
				sites["operation_moved"] = append(sites["operation_moved"], where)
			case "usersConflict":
				lit, ok := call.Args[0].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					t.Errorf("%s: usersConflict's reason is not a literal", where)
					return true
				}
				reason, _ := strconv.Unquote(lit.Value)
				sites[reason] = append(sites[reason], where)
			}
			return true
		})
	}
	return sites
}

// The go_api_users_conflict_reason values are exactly the reasons go-api's admin users area answers in a 409: a new
// conflict, or a value no site produces any more, fails here (acp 2026-11-09 Q11).
func TestGoAPIUsersConflictReasonsAreTheProducedSet(t *testing.T) {
	root := testRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "contracts/auth/v1/vocabulary/reason-codes.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vocab struct {
		Conflict struct{ Values []string } `json:"go_api_users_conflict_reason"`
	}
	if err := json.Unmarshal(raw, &vocab); err != nil {
		t.Fatal(err)
	}
	sites := goAPIUsersConflictSites(t, filepath.Join(root, "internal/apiservice/admin"))
	produced := make([]string, 0, len(sites))
	for reason := range sites {
		produced = append(produced, reason)
	}
	sort.Strings(produced)
	listed := append([]string(nil), vocab.Conflict.Values...)
	sort.Strings(listed)
	if len(listed) == 0 || !reflect.DeepEqual(listed, produced) {
		t.Fatalf("go_api_users_conflict_reason %v, produced by the go-api sites %v (sites %v)", listed, produced, sites)
	}
}
