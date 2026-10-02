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
// NOT covered (stated, not claimed): the class cell is checked by nothing here (the probes are the check of behaviour); a
// client made inside a dependency (oauth2.Config.Client, an SDK) is outside the walk; a dot-import or a blank import of
// net/http in a production file FAILS (it would hide sites).
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

type row struct {
	count       int
	class, cite string
}

func TestEveryHTTPClientSiteIsClassified(t *testing.T) {
	found, problems := scanSites(t, filepath.Join("..", ".."), func(rel string) bool { return rel == "internal/testsupport" })
	rows := readRows(t, "redirect_sites.tsv")
	if *updateSites {
		writeRows(t, "redirect_sites.tsv", found, rows)
		rows = readRows(t, "redirect_sites.tsv")
	}
	problems = append(problems, compareSites(found, rows)...)
	sort.Strings(problems)
	for _, problem := range problems {
		t.Error(problem)
	}
	t.Logf("%d sites, %d rows, %d problems", len(found), len(rows), len(problems))
}

// compareSites is the verdict: every defect of the row set against the found sites, as sorted text.
func compareSites(found map[site]int, rows map[site]row) []string {
	var problems []string
	for key, count := range found {
		got, ok := rows[key]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("UNLISTED site: %s %s %s (x%d)", key.file, key.symbol, key.kind, count))
		case got.count != count:
			problems = append(problems, fmt.Sprintf("COUNT differs: %s %s %s: table %d, code %d", key.file, key.symbol, key.kind, got.count, count))
		case !siteClasses[got.class]:
			problems = append(problems, fmt.Sprintf("UNCLASSIFIED: %s %s %s (class %q)", key.file, key.symbol, key.kind, got.class))
		case strings.TrimSpace(got.cite) == "":
			problems = append(problems, fmt.Sprintf("NO CITE: %s %s %s", key.file, key.symbol, key.kind))
		}
	}
	for key := range rows {
		if _, ok := found[key]; !ok {
			problems = append(problems, fmt.Sprintf("STALE row: %s %s %s", key.file, key.symbol, key.kind))
		}
	}
	sort.Strings(problems)
	return problems
}

// scanSites walks every non-test .go file under root (skipping .git, node_modules, vendor, testdata and what skipDir
// names) and returns the sites by (file, symbol, kind) with their count, and the files that cannot be walked soundly.
func scanSites(t *testing.T, root string, skipDir func(rel string) bool) (map[site]int, []string) {
	t.Helper()
	found := map[site]int{}
	var problems []string
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
			if skipDir(rel) {
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
		names := importNames(file)
		if names.blockedImport != "" {
			problems = append(problems, fmt.Sprintf("UNWALKABLE: %s imports net/http as %s: its sites cannot be seen", rel, names.blockedImport))
			return nil
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
					switch spec := d.Specs[0].(type) {
					case *ast.ValueSpec:
						if len(spec.Names) > 0 {
							symbol = "var " + spec.Names[0].Name
						}
					case *ast.TypeSpec:
						symbol = "type " + spec.Name.Name
					}
				}
			}
			ast.Inspect(declaration, func(node ast.Node) bool {
				for _, kind := range names.kindsOf(node) {
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
	return found, problems
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

// fileImports is what a file calls net/http and golang.org/x/oauth2 (an alias is resolved; a dot or blank import of
// net/http is refused: blockedImport).
type fileImports struct {
	http, oauth2  map[string]bool
	blockedImport string
}

func importNames(file *ast.File) fileImports {
	names := fileImports{http: map[string]bool{}, oauth2: map[string]bool{}}
	for _, spec := range file.Imports {
		path, _ := strconv.Unquote(spec.Path.Value)
		local := ""
		if spec.Name != nil {
			local = spec.Name.Name
		}
		switch path {
		case "net/http":
			switch local {
			case ".", "_":
				names.blockedImport = local
			case "":
				names.http["http"] = true
			default:
				names.http[local] = true
			}
		case "golang.org/x/oauth2":
			if local == "" {
				local = "oauth2"
			}
			names.oauth2[local] = true
		}
	}
	return names
}

func selects(expression ast.Expr, pkgs map[string]bool, name string) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != name {
		return false
	}
	ident, ok := selector.X.(*ast.Ident)
	return ok && pkgs[ident.Name]
}

// kindsOf names what a node is, when it is a site: a client built (literal, new, zero-value var, field, embedded field,
// collection element, type alias), http.DefaultClient, a reference to http.Get/Post/PostForm/Head (a call or a method
// value), oauth2.NewClient (its transport sets Authorization on every request, a redirected one too), or a
// CheckRedirect assignment.
func (n fileImports) kindsOf(node ast.Node) []string {
	switch x := node.(type) {
	case *ast.CompositeLit:
		if selects(x.Type, n.http, "Client") {
			return []string{"client-literal"}
		}
		if array, ok := x.Type.(*ast.ArrayType); ok && selects(array.Elt, n.http, "Client") {
			return []string{"client-collection"}
		}
		if mapping, ok := x.Type.(*ast.MapType); ok && selects(mapping.Value, n.http, "Client") {
			return []string{"client-collection"}
		}
	case *ast.CallExpr:
		if ident, ok := x.Fun.(*ast.Ident); ok && ident.Name == "new" && len(x.Args) == 1 && selects(x.Args[0], n.http, "Client") {
			return []string{"client-new"}
		}
		if selects(x.Fun, n.oauth2, "NewClient") {
			return []string{"oauth2.NewClient"}
		}
	case *ast.SelectorExpr:
		if selects(x, n.http, "DefaultClient") {
			return []string{"default-client"}
		}
		for _, name := range []string{"Get", "Post", "PostForm", "Head"} {
			if selects(x, n.http, name) {
				return []string{"http." + name}
			}
		}
	case *ast.ValueSpec:
		if selects(x.Type, n.http, "Client") {
			return []string{"client-zero-value"}
		}
	case *ast.Field:
		if selects(x.Type, n.http, "Client") {
			return []string{"client-field"}
		}
	case *ast.TypeSpec:
		if selects(x.Type, n.http, "Client") {
			return []string{"client-type"}
		}
	case *ast.AssignStmt:
		var kinds []string
		for _, left := range x.Lhs {
			if selector, ok := left.(*ast.SelectorExpr); ok && selector.Sel.Name == "CheckRedirect" {
				kinds = append(kinds, "checkredirect-assign")
			}
		}
		return kinds
	}
	return nil
}

func readRows(t *testing.T, name string) map[site]row {
	t.Helper()
	rows := map[site]row{}
	handle, err := os.Open(name)
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
			t.Fatalf("%s: want 6 tab-separated fields (file, symbol, kind, count, class, cite): %q", name, line)
		}
		count, convErr := strconv.Atoi(fields[3])
		if convErr != nil {
			t.Fatalf("%s: count of %q: %v", name, line, convErr)
		}
		key := site{fields[0], fields[1], fields[2]}
		if _, dup := rows[key]; dup {
			t.Fatalf("%s: duplicate row %v", name, key)
		}
		rows[key] = row{count, fields[4], fields[5]}
	}
	return rows
}

const sitesHeader = `# Every http.Client construction, http.DefaultClient use, http.Get/Post/PostForm/Head reference, oauth2.NewClient call and
# CheckRedirect assignment of production code (CHAOS-7910). The ROW SET is generated by TestEveryHTTPClientSiteIsClassified
# -update-sites; only the class (guarded | never-follows | follows-no-credential) and the cite are edited by hand.
# file<TAB>symbol<TAB>kind<TAB>count<TAB>class<TAB>cite
`

func writeRows(t *testing.T, name string, found map[site]int, old map[site]row) {
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
	if err := os.WriteFile(name, []byte(out.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// The walker and the verdict are themselves tested: a source set with one site of EVERY kind (and the shapes that hid
// sites: an aliased import, a type alias, a field, an embedded field, a method value, a collection) and a row table with
// one of EVERY defect; the exact problem list is asserted, so each matcher or verdict clause removed is RED.
func TestTheSiteWalkerSeesEveryKindAndTheVerdictNamesEveryDefect(t *testing.T) {
	root := t.TempDir()
	write := func(name, source string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), []byte(source), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("kinds.go", `package kinds

import (
	"net/http"
	nethttp "net/http"
	"golang.org/x/oauth2"
)

type alias = http.Client
type named http.Client
type holder struct {
	field    http.Client
	http.Client
}

var zero http.Client

func everyKind(ctx, src any) {
	_ = &http.Client{}
	_ = &nethttp.Client{}
	_ = new(http.Client)
	_ = &alias{}
	_ = http.DefaultClient
	_, _ = http.Get("")
	_, _ = http.Post("", "", nil)
	_, _ = http.PostForm("", nil)
	_, _ = http.Head("")
	get := http.Get
	_ = get
	_ = []http.Client{{}}
	_ = map[string]http.Client{"a": {}}
	_ = oauth2.NewClient(ctx, src)
	var c *http.Client
	c.CheckRedirect = nil
}
`)
	write("dot.go", `package kinds

import . "net/http"

var _ = &Client{}
`)
	write("blank.go", `package kinds

import _ "net/http"
`)
	write("kinds_test.go", `package kinds

import "net/http"

var _ = &http.Client{}
`)
	write("testdata/ignored.go", `package ignored

import "net/http"

var _ = &http.Client{}
`)
	write("skipme/skipped.go", `package skipped

import "net/http"

var _ = &http.Client{}
`)
	found, unwalkable := scanSites(t, root, func(rel string) bool { return rel == "skipme" })
	want := map[site]int{
		{"kinds.go", "everyKind", "client-literal"}:       2,
		{"kinds.go", "everyKind", "client-new"}:           1,
		{"kinds.go", "everyKind", "client-collection"}:    2,
		{"kinds.go", "everyKind", "default-client"}:       1,
		{"kinds.go", "everyKind", "http.Get"}:             2,
		{"kinds.go", "everyKind", "http.Post"}:            1,
		{"kinds.go", "everyKind", "http.PostForm"}:        1,
		{"kinds.go", "everyKind", "http.Head"}:            1,
		{"kinds.go", "everyKind", "oauth2.NewClient"}:     1,
		{"kinds.go", "everyKind", "checkredirect-assign"}: 1,
		{"kinds.go", "type alias", "client-type"}:         1,
		{"kinds.go", "type named", "client-type"}:         1,
		{"kinds.go", "type holder", "client-field"}:       2,
		{"kinds.go", "var zero", "client-zero-value"}:     1,
	}
	// "&alias{}" is a literal of the alias type, not of http.Client by name: it is seen through the alias declaration
	// (client-type), and "var c *http.Client" is a pointer, not a client.
	for key, count := range want {
		if found[key] != count {
			t.Errorf("walker: %v: found %d, want %d", key, found[key], count)
		}
	}
	for key, count := range found {
		if _, ok := want[key]; !ok {
			t.Errorf("walker: unexpected %v x%d", key, count)
		}
	}
	sort.Strings(unwalkable)
	if len(unwalkable) != 2 || !strings.Contains(unwalkable[0], "blank.go") || !strings.Contains(unwalkable[1], "dot.go") {
		t.Errorf("walker: a dot or blank import of net/http must be UNWALKABLE, got %v", unwalkable)
	}
	rows := map[site]row{
		{"kinds.go", "everyKind", "client-literal"}: {2, "guarded", "ok"},
		{"kinds.go", "everyKind", "client-new"}:     {5, "guarded", "ok"},       // COUNT
		{"kinds.go", "everyKind", "default-client"}: {1, "UNCLASSIFIED", "ok"},  // UNCLASSIFIED
		{"kinds.go", "everyKind", "http.Get"}:       {2, "bogus", "ok"},         // unknown class
		{"kinds.go", "everyKind", "http.Post"}:      {1, "never-follows", "  "}, // NO CITE
		{"kinds.go", "everyKind", "http.Head"}:      {1, "never-follows", "ok"},
		{"gone.go", "x", "client-literal"}:          {1, "guarded", "ok"}, // STALE
	}
	got := compareSites(found, rows)
	var wantProblems []string
	for _, key := range []site{
		{"kinds.go", "everyKind", "checkredirect-assign"}, {"kinds.go", "everyKind", "client-collection"},
		{"kinds.go", "everyKind", "http.PostForm"},
		{"kinds.go", "everyKind", "oauth2.NewClient"}, {"kinds.go", "type alias", "client-type"},
		{"kinds.go", "type holder", "client-field"}, {"kinds.go", "type named", "client-type"}, {"kinds.go", "var zero", "client-zero-value"},
	} {
		wantProblems = append(wantProblems, fmt.Sprintf("UNLISTED site: %s %s %s (x%d)", key.file, key.symbol, key.kind, found[key]))
	}
	wantProblems = append(wantProblems,
		"COUNT differs: kinds.go everyKind client-new: table 5, code 1",
		`UNCLASSIFIED: kinds.go everyKind default-client (class "UNCLASSIFIED")`,
		`UNCLASSIFIED: kinds.go everyKind http.Get (class "bogus")`,
		"NO CITE: kinds.go everyKind http.Post",
		"STALE row: gone.go x client-literal")
	sort.Strings(wantProblems)
	if strings.Join(got, "\n") != strings.Join(wantProblems, "\n") {
		t.Errorf("verdict problems differ:\n got:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(wantProblems, "\n"))
	}
}
