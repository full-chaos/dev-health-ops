package httpguard

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Every Go client that sends a credential and may be sent to another origin uses the ONE origin rule of origin.go.
// Two guards, both over production code:
//
//  1. No package compares the scheme, host or port of two URLs by itself (an `a.Host != b.Host` beside the shared rule is a
//     second definition that drifts: case, default port, IP spelling). Only this package may.
//  2. Every row of redirect_sites.tsv whose class says the client lets a credential-bearing request follow or refuse
//     by origin (guarded, drops-credential, custom-policy) names a file that reaches the shared rule: it calls httpguard
//     or providerfoundation.DropCredentialsOnHostChange (which strips through httpguard). A new such client with a
//     private rule has no such reference and FAILS here; a never-follows row needs no origin rule and is not asked for one.

// notAnOriginRule are the production files that compare a Host, Port or Scheme field and are not a credential origin rule.
var notAnOriginRule = map[string]string{
	"goapicli/restprove/main.go":  "CLI flag sanity check that two operator-given URLs name one service; no request, no credential",
	"platform/config/config.go":   "database endpoint count (host and port of a DSN fallback), not a URL",
	"storage/postgres/runtime.go": "database endpoint equality (pgx config), not a URL",
}

// refusesEveryRedirect are rows of the origin classes whose policy refuses every redirect, so no origin rule is needed.
var refusesEveryRedirect = map[string]string{
	"internal/goapiproof/legclient.go": "CheckRedirect returns errRedirectRefused for every hop",
}

var originFields = map[string]bool{"Host": true, "Scheme": true, "Hostname": true, "Port": true}

func originSelector(e ast.Expr) bool {
	if call, ok := e.(*ast.CallExpr); ok {
		e = call.Fun
	}
	sel, ok := e.(*ast.SelectorExpr)
	return ok && originFields[sel.Sel.Name]
}

func TestNoPackageComparesURLOriginsByItself(t *testing.T) {
	root := filepath.Join(moduleRootRel, "internal")
	fset := token.NewFileSet()
	walked := 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if d.IsDir() {
			if rel == "httpguard" || rel == "testsupport" || d.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		walked++
		ast.Inspect(file, func(n ast.Node) bool {
			bin, ok := n.(*ast.BinaryExpr)
			if _, exempt := notAnOriginRule[filepath.ToSlash(rel)]; exempt {
				return false
			}
			if ok && (bin.Op == token.EQL || bin.Op == token.NEQ) && originSelector(bin.X) && originSelector(bin.Y) {
				t.Errorf("%s: compares URL origin parts itself; use httpguard.SameOrigin", fset.Position(bin.Pos()))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if walked < 500 {
		t.Fatalf("walked only %d production files: the guard measured nothing", walked)
	}
}

func TestEveryOriginRuleClientReachesTheSharedRule(t *testing.T) {
	rows, err := os.ReadFile("redirect_sites.tsv")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, line := range strings.Split(string(rows), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cols := strings.Split(line, "\t")
		if len(cols) < 5 {
			continue
		}
		switch cols[4] {
		case "guarded", "drops-credential", "custom-policy":
		default:
			continue
		}
		source, err := os.ReadFile(filepath.Join(moduleRootRel, cols[0]))
		if err != nil {
			t.Errorf("%s: %v", cols[0], err)
			continue
		}
		if _, exempt := refusesEveryRedirect[cols[0]]; exempt {
			continue
		}
		text := string(source)
		if !strings.Contains(text, "httpguard.") && !strings.Contains(text, "DropCredentialsOnHostChange") {
			t.Errorf("%s (%s, %s): class %s but the file never reaches the shared origin rule", cols[0], cols[1], cols[2], cols[4])
		}
		checked++
	}
	if checked < 8 {
		t.Fatalf("checked only %d rows: the guard measured nothing", checked)
	}
}
