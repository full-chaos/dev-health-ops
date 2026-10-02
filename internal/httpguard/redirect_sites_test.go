package httpguard

import (
	"bufio"
	"flag"
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
)

var updateSites = flag.Bool("update-sites", false, "rewrite redirect_sites.tsv from production code (new rows are UNCLASSIFIED and fail the test until classified)")

// The row set of redirect_sites.tsv is DERIVED from production code, never written by hand: every place a production file
// builds an http.Client (composite literal, new, zero-value var), uses http.DefaultClient, calls http.Get/Post/PostForm/
// Head, or assigns a CheckRedirect is a SITE. A site with no row fails; a row with no site fails; a row must carry a class
// and a cite (what travels, and where the guard or the policy is). Regenerate the row set with
// `go test ./internal/httpguard -run TestEveryHTTPClientSiteIsClassified -update-sites`, then classify the new rows.
//
// Classes:
//
//	guarded               the callee that sends the credential wraps its client with httpguard (cite the line).
//	never-follows         the client is built with its own no-redirect policy (cite it) or refuses redirects.
//	follows-no-credential the client follows redirects and the request carries no credential, or drops it off-origin
//	                      (cite what travels).
type site struct {
	file, symbol, kind string
}

var siteClasses = map[string]bool{"guarded": true, "never-follows": true, "follows-no-credential": true}

func TestEveryHTTPClientSiteIsClassified(t *testing.T) {
	found := scanSites(t)
	rows := readRows(t)
	if *updateSites {
		writeRows(t, found, rows)
		rows = readRows(t)
	}
	var problems []string
	for key, count := range found {
		row, ok := rows[key]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("UNLISTED site: %s %s %s (x%d)", key.file, key.symbol, key.kind, count))
		case row.count != count:
			problems = append(problems, fmt.Sprintf("COUNT differs: %s %s %s: table %d, code %d", key.file, key.symbol, key.kind, row.count, count))
		case !siteClasses[row.class]:
			problems = append(problems, fmt.Sprintf("UNCLASSIFIED: %s %s %s (class %q)", key.file, key.symbol, key.kind, row.class))
		case strings.TrimSpace(row.cite) == "":
			problems = append(problems, fmt.Sprintf("NO CITE: %s %s %s", key.file, key.symbol, key.kind))
		}
	}
	for key := range rows {
		if _, ok := found[key]; !ok {
			problems = append(problems, fmt.Sprintf("STALE row: %s %s %s", key.file, key.symbol, key.kind))
		}
	}
	sort.Strings(problems)
	for _, problem := range problems {
		t.Error(problem)
	}
	t.Logf("%d sites, %d rows, %d problems", len(found), len(rows), len(problems))
}

type row struct {
	count       int
	class, cite string
}

func scanSites(t *testing.T) map[site]int {
	t.Helper()
	root := filepath.Join("..", "..")
	found := map[site]int{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "vendor", "testdata":
				return filepath.SkipDir
			}
			if rel == "internal/testsupport" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			t.Fatalf("%s: %v", rel, parseErr)
		}
		for _, declaration := range file.Decls {
			symbol := ""
			switch d := declaration.(type) {
			case *ast.FuncDecl:
				symbol = d.Name.Name
				if d.Recv != nil && len(d.Recv.List) == 1 {
					symbol = receiverName(d.Recv.List[0].Type) + "." + symbol
				}
			case *ast.GenDecl:
				symbol = "package-level"
				if len(d.Specs) > 0 {
					if value, ok := d.Specs[0].(*ast.ValueSpec); ok && len(value.Names) > 0 {
						symbol = "var " + value.Names[0].Name
					}
				}
			}
			ast.Inspect(declaration, func(node ast.Node) bool {
				for _, kind := range kindsOf(node) {
					found[site{rel, symbol, kind}]++
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func receiverName(expression ast.Expr) string {
	switch e := expression.(type) {
	case *ast.StarExpr:
		return receiverName(e.X)
	case *ast.Ident:
		return e.Name
	case *ast.IndexExpr:
		return receiverName(e.X)
	}
	return "?"
}

func isHTTP(expression ast.Expr, name string) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != name {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == "http"
}

func kindsOf(node ast.Node) []string {
	switch n := node.(type) {
	case *ast.CompositeLit:
		if isHTTP(n.Type, "Client") {
			return []string{"client-literal"}
		}
	case *ast.CallExpr:
		if ident, ok := n.Fun.(*ast.Ident); ok && ident.Name == "new" && len(n.Args) == 1 && isHTTP(n.Args[0], "Client") {
			return []string{"client-new"}
		}
		for _, name := range []string{"Get", "Post", "PostForm", "Head"} {
			if isHTTP(n.Fun, name) {
				return []string{"http." + name}
			}
		}
	case *ast.SelectorExpr:
		if isHTTP(n, "DefaultClient") {
			return []string{"default-client"}
		}
	case *ast.ValueSpec:
		if isHTTP(n.Type, "Client") {
			return []string{"client-zero-value"}
		}
	case *ast.AssignStmt:
		var kinds []string
		for _, left := range n.Lhs {
			if selector, ok := left.(*ast.SelectorExpr); ok && selector.Sel.Name == "CheckRedirect" {
				kinds = append(kinds, "checkredirect-assign")
			}
		}
		return kinds
	}
	return nil
}

func readRows(t *testing.T) map[site]row {
	t.Helper()
	rows := map[site]row{}
	handle, err := os.Open("redirect_sites.tsv")
	if err != nil {
		if os.IsNotExist(err) {
			return rows
		}
		t.Fatal(err)
	}
	defer handle.Close()
	scanner := bufio.NewScanner(handle)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.SplitN(line, "\t", 6)
		if len(fields) != 6 {
			t.Fatalf("redirect_sites.tsv: want 6 tab-separated fields (file, symbol, kind, count, class, cite): %q", line)
		}
		count, convErr := strconv.Atoi(fields[3])
		if convErr != nil {
			t.Fatalf("redirect_sites.tsv: count of %q: %v", line, convErr)
		}
		key := site{fields[0], fields[1], fields[2]}
		if _, dup := rows[key]; dup {
			t.Fatalf("redirect_sites.tsv: duplicate row %v", key)
		}
		rows[key] = row{count, fields[4], fields[5]}
	}
	return rows
}

const sitesHeader = `# Every http.Client construction, http.DefaultClient use, http.Get/Post/PostForm/Head call and CheckRedirect assignment
# of production code (CHAOS-7910). The ROW SET is generated by TestEveryHTTPClientSiteIsClassified -update-sites; only the
# class (guarded | never-follows | follows-no-credential) and the cite are edited by hand.
# file<TAB>symbol<TAB>kind<TAB>count<TAB>class<TAB>cite
`

func writeRows(t *testing.T, found map[site]int, old map[site]row) {
	t.Helper()
	keys := make([]site, 0, len(found))
	for key := range found {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.file != b.file {
			return a.file < b.file
		}
		if a.symbol != b.symbol {
			return a.symbol < b.symbol
		}
		return a.kind < b.kind
	})
	var out strings.Builder
	out.WriteString(sitesHeader)
	for _, key := range keys {
		class, cite := "UNCLASSIFIED", ""
		if previous, ok := old[key]; ok {
			class, cite = previous.class, previous.cite
		}
		fmt.Fprintf(&out, "%s\t%s\t%s\t%d\t%s\t%s\n", key.file, key.symbol, key.kind, found[key], class, cite)
	}
	if err := os.WriteFile("redirect_sites.tsv", []byte(out.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}
