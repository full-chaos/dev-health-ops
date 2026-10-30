package platformrole

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// superuserSite is one function that decides on platform power today: it reads
// IsSuperuser or names the policy.Superuser guard level. mix is the number of
// the site (PLAN-10979 1.3) where platform power is used as organization power,
// or "-".
type superuserSite struct {
	file, function string
	reads          int
	mix            string
}

// TestEverySuperuserDecisionSiteIsListed is the census of the decision sites:
// every non-test function that reads IsSuperuser or names policy.Superuser is
// listed in testdata/superuser_sites.tsv with its count of reads, so a new or
// moved site fails here. The rows marked with a mix number are the eight
// places where platform power stands in for organization power; each becomes
// an explicit platform action in a later change.
func TestEverySuperuserDecisionSiteIsListed(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	listed := readSuperuserSites(t)
	found := map[string]int{}
	fset := token.NewFileSet()
	for _, dir := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			rel := filepath.ToSlash(strings.TrimPrefix(path, root+string(filepath.Separator)))
			for function, n := range superuserReads(file) {
				found[rel+"\t"+function] += n
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	mixes := map[string]bool{}
	for key, site := range listed {
		if site.mix != "-" {
			mixes[site.mix] = true
		}
		if got := found[key]; got != site.reads {
			t.Errorf("%s %s: %d superuser reads, listed %d (a site moved, changed or went away: update the census)", site.file, site.function, got, site.reads)
		}
	}
	var unlisted []string
	for key, n := range found {
		if _, ok := listed[key]; !ok {
			unlisted = append(unlisted, fmt.Sprintf("%s\t%d\t-", key, n))
		}
	}
	sort.Strings(unlisted)
	for _, row := range unlisted {
		t.Errorf("unlisted superuser decision site (add it to testdata/superuser_sites.tsv, with its mix number if it grants organization power): %s", row)
	}
	for i := 1; i <= 8; i++ {
		if !mixes[strconv.Itoa(i)] {
			t.Errorf("mix site %d is not marked on any row", i)
		}
	}
}

// superuserReads counts, per function, the reads of an IsSuperuser field and
// the uses of policy.Superuser. Assignments to IsSuperuser are writes, not
// decisions, and are not counted.
func superuserReads(file *ast.File) map[string]int {
	counts := map[string]int{}
	written := map[*ast.SelectorExpr]bool{}
	count := func(scope string, node ast.Node) {
		ast.Inspect(node, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.AssignStmt:
				for _, lhs := range n.Lhs {
					if sel, ok := lhs.(*ast.SelectorExpr); ok {
						written[sel] = true
					}
				}
			case *ast.SelectorExpr:
				if written[n] {
					return true
				}
				if n.Sel.Name == "IsSuperuser" {
					counts[scope]++
				}
				if x, ok := n.X.(*ast.Ident); ok && x.Name == "policy" && n.Sel.Name == "Superuser" {
					counts[scope]++
				}
			}
			return true
		})
	}
	for _, decl := range file.Decls {
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			count(funcName(decl), decl)
		case *ast.GenDecl:
			count("-", decl)
		}
	}
	for scope, n := range counts {
		if n == 0 {
			delete(counts, scope)
		}
	}
	return counts
}

func funcName(decl *ast.FuncDecl) string {
	if decl.Recv == nil || len(decl.Recv.List) == 0 {
		return decl.Name.Name
	}
	recv := decl.Recv.List[0].Type
	if star, ok := recv.(*ast.StarExpr); ok {
		recv = star.X
	}
	if index, ok := recv.(*ast.IndexExpr); ok {
		recv = index.X
	}
	if ident, ok := recv.(*ast.Ident); ok {
		return ident.Name + "." + decl.Name.Name
	}
	return decl.Name.Name
}

func readSuperuserSites(t *testing.T) map[string]superuserSite {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", "superuser_sites.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	sites := map[string]superuserSite{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 4 {
			t.Fatalf("superuser_sites.tsv: %q is not file, function, reads, mix", line)
		}
		reads, err := strconv.Atoi(fields[2])
		if err != nil || reads < 1 {
			t.Fatalf("superuser_sites.tsv: %q: reads must be a positive count", line)
		}
		key := fields[0] + "\t" + fields[1]
		if _, dup := sites[key]; dup {
			t.Fatalf("superuser_sites.tsv: %s listed twice", key)
		}
		sites[key] = superuserSite{file: fields[0], function: fields[1], reads: reads, mix: fields[3]}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return sites
}
