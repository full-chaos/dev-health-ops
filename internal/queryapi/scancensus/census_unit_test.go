package scancensus

import (
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"
)

func TestNeutralizeKeepsSelectListTypes(t *testing.T) {
	got := Neutralize("SELECT sum(x) AS n FROM t WHERE org_id = {org_id:String} AND d >= {d:Date} %s\nLIMIT %d")[0]
	for _, want := range []string{"defaultValueOfTypeName('String')", "defaultValueOfTypeName('Date')", "LIMIT 1"} {
		if !strings.Contains(got, want) {
			t.Errorf("neutralized statement lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "%s") || strings.Contains(got, "{") {
		t.Errorf("neutralized statement keeps a placeholder:\n%s", got)
	}
}

func TestNeutralizeKeepsALiteralPercent(t *testing.T) {
	if got := Neutralize("SELECT 1 FROM t WHERE p LIKE '100%%'")[0]; !strings.Contains(got, "'100%'") {
		t.Errorf("a literal percent was lost: %s", got)
	}
}

func TestSelectListSplices(t *testing.T) {
	cases := []struct {
		statement string
		want      bool
	}{
		{"SELECT a, %s AS value FROM t", true},
		{"SELECT a, coalesce(%s, '') FROM t", true},
		{"SELECT a, sum(b) FROM t WHERE x = 1 %s", false},
		{"SELECT a, (SELECT count() FROM u WHERE k = 1 AND %s) AS n FROM t", false},
		{"SELECT a, (%s) AS n FROM t", true},
		{"SELECT a, 'FROM %s' FROM t", true},
		{"SELECT (SELECT count() FROM u WHERE k = 1) AS c, %s AS n FROM t", true},
		{"SELECT (SELECT count() FROM u WHERE k = 1) AS c, (SELECT 1 FROM v WHERE %s) AS d FROM t", false},
		{"SELECT (SELECT max(x) FROM u WHERE k = 1), coalesce(%s, 0) AS n FROM t", true},
	}
	for _, c := range cases {
		if got := SelectListSplices(c.statement); got != c.want {
			t.Errorf("SelectListSplices(%q) = %v, want %v", c.statement, got, c.want)
		}
	}
}

// A reader that declares its own narrow interface is a reader: the census finds
// a scanner by method set, not by the name of its type.
func TestHasScanMethodFindsAScannerByMethodSet(t *testing.T) {
	const src = `package p
type localRows interface {
	Next() bool
	Scan(dest ...any) error
}
type named struct{}
func (named) Scan(dest ...any) error { return nil }
type wrongArgs struct{}
func (wrongArgs) Scan(dest any) error { return nil }
type wrongResult struct{}
func (wrongResult) Scan(dest ...any) bool { return false }
type typed struct{}
func (typed) Scan(dest ...string) error { return nil }
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "p.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	pkg, err := (&types.Config{Importer: importer.Default()}).Check("p", fset, []*ast.File{file}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]bool{"localRows": true, "named": true, "wrongArgs": false, "wrongResult": false, "typed": false} {
		if got := hasScanMethod(pkg.Scope().Lookup(name).Type(), pkg); got != want {
			t.Errorf("hasScanMethod(%s) = %v, want %v", name, got, want)
		}
	}
}
