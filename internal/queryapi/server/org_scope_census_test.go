package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// CHAOS-2582: every served ClickHouse read of an organization's table is
// scoped by org_id in its own SQL. ClickHouse ignores a bound parameter that
// the statement text never names, so a binding alone does not scope a read: the
// predicate must be in the text. The census reads every non-test function of
// the served read paths (the query API, MCP routes included, and the REST api):
// a function whose SQL text reads a table that carries an org_id column, or a
// table named at run time (FROM %s, FROM ` + x), must carry an org_id
// predicate in that text, in a package-level constant it names, or in a
// same-package function it calls. It must also carry at least as many org_id
// predicates as it has reads of such tables, so a second statement of one
// function cannot go unscoped behind the first.
//
// A function that takes its scope from its caller (a WHERE fragment passed in)
// or from a clause it shares between statements is listed below with the
// function that writes its org_id predicate and that predicate's exact text.
// The census checks both ways: an entry that no longer matches a flagged
// function fails, and an entry whose cited function no longer holds its
// predicate fails, so the list cannot rot.
//
// What the census does not prove: that every call site of a caller-scoped
// function passes the cited fragment, or that a predicate sits on the right
// table of a join. The cross-tenant behaviour of each query family is held by
// its own tests.

var orgScopeCensusRoots = []string{"internal/queryapi", "internal/api"}

// orgScopeSite is the function (file and name) that writes an org_id
// predicate, and the predicate as one line of that function's SQL text.
type orgScopeSite struct {
	file, function, predicate string
}

type orgScopeElsewhere struct {
	reason string
	sites  []orgScopeSite
}

var orgScopeReadsScopedElsewhere = map[string]orgScopeElsewhere{
	"internal/queryapi/cognitiveload/cognitiveload.go:retractedUserDaysQuery": {
		"its innerWhere is fetchUserMetrics's, which starts with the org_id predicate",
		[]orgScopeSite{{"internal/queryapi/cognitiveload/cognitiveload.go", "fetchUserMetrics", "WHERE org_id = {org_id:String}"}},
	},
	"internal/queryapi/cognitiveload/cognitiveload.go:retractedTeamDaysQuery": {
		"its innerWhere is fetchTeamMetrics's, which starts with the org_id predicate",
		[]orgScopeSite{{"internal/queryapi/cognitiveload/cognitiveload.go", "fetchTeamMetrics", "WHERE org_id = {org_id:String}"}},
	},
	"internal/queryapi/security/overview.go:readKpis":      orgScopeSecurityFilter,
	"internal/queryapi/security/overview.go:readBreakdown": orgScopeSecurityFilter,
	"internal/queryapi/security/overview.go:readTopRepos":  orgScopeSecurityFilter,
	"internal/queryapi/security/overview.go:readTrend":     orgScopeSecurityFilter,
	"internal/queryapi/teamscope/teamscope.go:UnitScope": {
		"its latestEffortSource is analytics.LatestWorkUnitRepoEffortSource, which scopes its one read (CHAOS-9231: the source reads the table once)",
		[]orgScopeSite{{"internal/queryapi/analytics/investment.go", "latestWorkUnitRepoEffortSource", "WHERE org_id = {org_id:String}"}},
	},
	"internal/queryapi/newestrow/newestrow.go:PerTeamPredicate": {
		"its where is the caller's scope filter, which starts with the org_id predicate",
		[]orgScopeSite{
			{"internal/queryapi/throughputforecast/clickhouse.go", "scopeFilter", "org_id = {org_id:String}"},
			{"internal/queryapi/capacityforecast/clickhouse.go", "capacityScopeFilters", "org_id = {org_id:String}"},
		},
	},
	"internal/queryapi/analytics/timeseries.go:dedupFromMeasureSource": {
		"its dedup key holds org_id, and the compiled statement filters the alias it returns",
		[]orgScopeSite{{"internal/queryapi/analytics/timeseries.go", "CompileTimeseries", "AND %s.org_id = {org_id:String}"}},
	},
	"internal/queryapi/home/queries_metrics.go:fetchRateDriverDelta": {
		"both FROM sources are fetchMetricDriverDelta's driverDeltaSource, which scopes a table with no natural key itself and takes metricFromClause's scoped source for the others",
		[]orgScopeSite{
			{"internal/queryapi/home/driver_delta_source.go", "driverDeltaSource", "AND org_id = {org_id:String}"},
			{"internal/queryapi/home/queries_metrics.go", "metricFromClause", "AND org_id = {org_id:String}"},
		},
	},
	"internal/queryapi/featureflags/featureflags.go:Resolve": {
		"one flagWhere clause, the org_id predicate first, is the WHERE of both statements",
		[]orgScopeSite{{"internal/queryapi/featureflags/featureflags.go", "flagWhere", "org_id = {org_id:String}"}},
	},
	"internal/queryapi/featureflags/events.go:ResolveEvents": {
		"one eventWhere clause, the org_id predicate first, is the WHERE of both statements",
		[]orgScopeSite{{"internal/queryapi/featureflags/events.go", "eventWhere", "org_id = {org_id:String}"}},
	},
	"internal/queryapi/throughputforecast/clickhouse.go:loadWorkItemOverlay": {
		"one scopeFilter WHERE is spliced into both reads and the newest-row predicate",
		[]orgScopeSite{{"internal/queryapi/throughputforecast/clickhouse.go", "scopeFilter", "org_id = {org_id:String}"}},
	},
}

var orgScopeSecurityFilter = orgScopeElsewhere{
	"its filterClause is buildFilter's, which holds the org_id predicate of both joined tables",
	[]orgScopeSite{{"internal/queryapi/security/filter.go", "buildFilter", "r.org_id = {org_id:String}"},
		{"internal/queryapi/security/filter.go", "buildFilter", "sa.org_id = {org_id:String}"}},
}

// orgScopeSiteLine is the line of the cited predicate in the cited function,
// or 0 when the function or the predicate is gone. The predicate must be a
// whole line of one of the function's string literals.
func orgScopeSiteLine(t *testing.T, root string, site orgScopeSite) int {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filepath.Join(root, filepath.FromSlash(site.file)), nil, 0)
	if err != nil {
		t.Errorf("read %s: %v", site.file, err)
		return 0
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Name.Name != site.function {
			continue
		}
		line := 0
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING || line != 0 {
				return true
			}
			value, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			for offset, text := range strings.Split(value, "\n") {
				if strings.TrimSpace(text) == site.predicate {
					line = fset.Position(lit.Pos()).Line + offset
					return false
				}
			}
			return true
		})
		return line
	}
	return 0
}

var (
	orgScopeTableRead   = regexp.MustCompile(`(?i)\b(?:FROM|JOIN)\s+([a-z_][a-z0-9_]*)`)
	orgScopeDynamicRead = regexp.MustCompile("\\b(?:FROM|JOIN) +(?:%s|%v|\n)")
	orgScopePredicate   = regexp.MustCompile(`(?i)\borg_id\)?\s*(?:=|IN\b)|=\s*\{org_id:`)
	orgScopeTableName   = regexp.MustCompile("(?i)CREATE TABLE(?: IF NOT EXISTS)?\\s+`?([a-z_][a-z0-9_]*)`?")
	orgScopeAddColumn   = regexp.MustCompile("(?i)ALTER TABLE\\s+`?([a-z_][a-z0-9_]*)`?[^;]*ADD COLUMN(?: IF NOT EXISTS)?\\s+`?org_id`?")
	orgScopeColumn      = regexp.MustCompile("(?i)[(,]\\s*`?org_id`?\\s")
	orgScopeViewName    = regexp.MustCompile("(?i)CREATE (?:MATERIALIZED )?VIEW(?: IF NOT EXISTS)?\\s+`?([a-z_][a-z0-9_]*)`?")
	orgScopeWord        = regexp.MustCompile(`(?i)\borg_id\b`)
)

// orgScopedClickHouseTables is every table and view of the ClickHouse head
// (the baseline plus the chain after it) that carries an org_id column. A view
// counts when its statement names org_id at all: its columns are its select
// list, which the column pattern of a table does not read.
func orgScopedClickHouseTables(t *testing.T) map[string]bool {
	t.Helper()
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	tables := map[string]bool{}
	for _, object := range baseline.Objects {
		if orgScopeColumn.MatchString(object.Create) || (object.IsView() && orgScopeWord.MatchString(object.Create)) {
			tables[strings.ToLower(object.Name)] = true
		}
	}
	for _, file := range chain {
		for _, statement := range chmigrate.SplitStatements(chmigrate.StripLineComments(file.SQL)) {
			if m := orgScopeTableName.FindStringSubmatch(statement); m != nil && orgScopeColumn.MatchString(statement) {
				tables[strings.ToLower(m[1])] = true
			}
			if m := orgScopeViewName.FindStringSubmatch(statement); m != nil && orgScopeWord.MatchString(statement) {
				tables[strings.ToLower(m[1])] = true
			}
			if m := orgScopeAddColumn.FindStringSubmatch(statement); m != nil {
				tables[strings.ToLower(m[1])] = true
			}
		}
	}
	return tables
}

const orgScopeModule = "github.com/full-chaos/dev-health-ops/"

type orgScopeFunc struct {
	decl    *ast.FuncDecl
	dir     string
	imports map[string]string // local package name -> directory, this module's packages only
}

type orgScopePackage struct {
	strings map[string]string
	funcs   map[string]orgScopeFunc      // package-level functions
	methods map[string]orgScopeFunc      // methods, by name
	files   map[string]map[string]string // file name -> its imports
}

type orgScopeLoader struct {
	t        *testing.T
	root     string
	packages map[string]*orgScopePackage
	memo     map[string]string
}

func orgScopeLiterals(node ast.Node) string {
	var text strings.Builder
	ast.Inspect(node, func(n ast.Node) bool {
		if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
			if value, err := strconv.Unquote(lit.Value); err == nil {
				text.WriteString(value)
				text.WriteString("\n")
			}
		}
		return true
	})
	return text.String()
}

func (loader *orgScopeLoader) pkg(dir string) *orgScopePackage {
	if pkg, ok := loader.packages[dir]; ok {
		return pkg
	}
	pkg := &orgScopePackage{strings: map[string]string{}, funcs: map[string]orgScopeFunc{}, methods: map[string]orgScopeFunc{}, files: map[string]map[string]string{}}
	loader.packages[dir] = pkg
	entries, err := os.ReadDir(dir)
	if err != nil {
		loader.t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, name), nil, 0)
		if err != nil {
			loader.t.Fatal(err)
		}
		imports := map[string]string{}
		for _, spec := range file.Imports {
			path, _ := strconv.Unquote(spec.Path.Value)
			if !strings.HasPrefix(path, orgScopeModule) {
				continue
			}
			local := filepath.Base(path)
			if spec.Name != nil {
				local = spec.Name.Name
			}
			imports[local] = filepath.Join(loader.root, filepath.FromSlash(strings.TrimPrefix(path, orgScopeModule)))
		}
		pkg.files[name] = imports
		for _, decl := range file.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				if decl.Body == nil {
					continue
				}
				if decl.Recv != nil {
					pkg.methods[decl.Name.Name] = orgScopeFunc{decl: decl, dir: dir, imports: imports}
				} else {
					pkg.funcs[decl.Name.Name] = orgScopeFunc{decl: decl, dir: dir, imports: imports}
				}
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					if values, ok := spec.(*ast.ValueSpec); ok {
						for index, ident := range values.Names {
							if index < len(values.Values) {
								pkg.strings[ident.Name] = orgScopeLiterals(values.Values[index])
							}
						}
					}
				}
			}
		}
	}
	return pkg
}

// text is the SQL text a function holds itself (its literals and the
// package-level strings it names, of its own package or another package of
// this module) and the full text of every function of this module it calls,
// once per call site: a helper that writes one org_id predicate and is called
// twice scopes two statements.
func (loader *orgScopeLoader) text(fn orgScopeFunc) (own, helpers string) {
	var ownText, helperText strings.Builder
	ownText.WriteString(orgScopeLiterals(fn.decl.Body))
	loader.references(fn, func(value string) { ownText.WriteString(value) }, func(callee orgScopeFunc) {
		helperText.WriteString(loader.fullText(callee))
	})
	return ownText.String(), helperText.String()
}

// fullText is a function's own text and its callees' full text, memoized; a
// call cycle contributes nothing on its second visit.
func (loader *orgScopeLoader) fullText(fn orgScopeFunc) string {
	key := fn.dir + "." + orgScopeFuncKey(fn.decl)
	if text, ok := loader.memo[key]; ok {
		return text
	}
	loader.memo[key] = ""
	own, helpers := loader.text(fn)
	loader.memo[key] = own + helpers
	return own + helpers
}

func orgScopeFuncKey(decl *ast.FuncDecl) string {
	if decl.Recv != nil && len(decl.Recv.List) > 0 {
		return "(" + orgScopeRecvType(decl.Recv.List[0].Type) + ")." + decl.Name.Name
	}
	return decl.Name.Name
}

func orgScopeRecvType(expr ast.Expr) string {
	switch expr := expr.(type) {
	case *ast.StarExpr:
		return orgScopeRecvType(expr.X)
	case *ast.Ident:
		return expr.Name
	case *ast.IndexExpr:
		return orgScopeRecvType(expr.X)
	}
	return ""
}

// references calls str for each package-level string the function names and
// call for each function of this module it names. A name the function declares
// itself shadows a package-level one; a field or method selected from a value
// is followed only as a method of the function's own package.
func (loader *orgScopeLoader) references(fn orgScopeFunc, str func(string), call func(orgScopeFunc)) {
	pkg := loader.pkg(fn.dir)
	locals := orgScopeLocalNames(fn.decl)
	selected := map[*ast.Ident]bool{}
	ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.SelectorExpr:
			if x, isIdent := node.X.(*ast.Ident); isIdent && !locals[x.Name] {
				if dir, imported := fn.imports[x.Name]; imported {
					other := loader.pkg(dir)
					if value, ok := other.strings[node.Sel.Name]; ok {
						str(value)
					}
					if callee, ok := other.funcs[node.Sel.Name]; ok {
						call(callee)
					}
					return false
				}
			}
			selected[node.Sel] = true
			if callee, ok := pkg.methods[node.Sel.Name]; ok {
				call(callee)
			}
		case *ast.Ident:
			if selected[node] || locals[node.Name] {
				return true
			}
			if value, ok := pkg.strings[node.Name]; ok {
				str(value)
			}
			if callee, ok := pkg.funcs[node.Name]; ok {
				call(callee)
			}
		}
		return true
	})
}

// orgScopeLocalNames is every name a function declares (parameters, results,
// receiver, := and var), which shadows a package-level name of the same spelling.
func orgScopeLocalNames(fn *ast.FuncDecl) map[string]bool {
	names := map[string]bool{}
	fields := func(list *ast.FieldList) {
		if list == nil {
			return
		}
		for _, field := range list.List {
			for _, name := range field.Names {
				names[name.Name] = true
			}
		}
	}
	fields(fn.Recv)
	fields(fn.Type.Params)
	fields(fn.Type.Results)
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.AssignStmt:
			if node.Tok == token.DEFINE {
				for _, lhs := range node.Lhs {
					if ident, ok := lhs.(*ast.Ident); ok {
						names[ident.Name] = true
					}
				}
			}
		case *ast.ValueSpec:
			for _, name := range node.Names {
				names[name.Name] = true
			}
		case *ast.RangeStmt:
			for _, expr := range []ast.Expr{node.Key, node.Value} {
				if ident, ok := expr.(*ast.Ident); ok {
					names[ident.Name] = true
				}
			}
		case *ast.FuncLit:
			fields(node.Type.Params)
			fields(node.Type.Results)
		}
		return true
	})
	return names
}

type orgScopeFinding struct {
	key    string
	reads  int
	scopes int
}

func censusOrgScope(t *testing.T, root string, roots []string, tables map[string]bool) (checked int, flagged []orgScopeFinding) {
	t.Helper()
	loader := &orgScopeLoader{t: t, root: root, packages: map[string]*orgScopePackage{}, memo: map[string]string{}}
	for _, censusRoot := range roots {
		err := filepath.WalkDir(filepath.Join(root, censusRoot), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			dir := filepath.Dir(path)
			pkg := loader.pkg(dir)
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				own, helpers := loader.text(orgScopeFunc{decl: fn, dir: dir, imports: pkg.files[filepath.Base(path)]})
				reads := 0
				for _, m := range orgScopeTableRead.FindAllStringSubmatch(own, -1) {
					if tables[strings.ToLower(m[1])] {
						reads++
					}
				}
				// A table named at run time may be a source this text already
				// holds (a package-level subquery), so it needs one predicate
				// but adds no count of its own.
				dynamic := orgScopeDynamicRead.MatchString(own)
				if reads == 0 && !dynamic {
					continue
				}
				checked++
				scopes := len(orgScopePredicate.FindAllString(own+helpers, -1))
				if scopes < reads || scopes == 0 {
					flagged = append(flagged, orgScopeFinding{filepath.ToSlash(rel) + ":" + fn.Name.Name, reads, scopes})
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Slice(flagged, func(i, j int) bool { return flagged[i].key < flagged[j].key })
	return checked, flagged
}

func TestEveryServedClickHouseReadOfAnOrganizationTableIsScopedByOrgID(t *testing.T) {
	tables := orgScopedClickHouseTables(t)
	// A census over an empty table set or no reads would pass while measuring
	// nothing.
	if len(tables) < 100 {
		t.Fatalf("the ClickHouse head names %d tables with an org_id column; the census reads the head wrong", len(tables))
	}
	// A table of the baseline, a table of the chain and a view of the chain.
	for _, name := range []string{"repo_metrics_daily", "work_unit_issue_size", "work_item_interactions_current"} {
		if !tables[name] {
			t.Fatalf("the census table set misses %s, which carries org_id", name)
		}
	}
	root := filepath.Join("..", "..", "..")
	checked, flagged := censusOrgScope(t, root, orgScopeCensusRoots, tables)
	if checked < 250 {
		t.Fatalf("the census found %d functions that read an organization table; it reads the source tree wrong", checked)
	}
	matched := map[string]bool{}
	for _, finding := range flagged {
		if _, ok := orgScopeReadsScopedElsewhere[finding.key]; ok {
			matched[finding.key] = true
			continue
		}
		t.Errorf("%s reads an organization's ClickHouse table %d time(s) with %d org_id predicate(s) in its SQL: add org_id = {org_id:String} to each statement", finding.key, finding.reads, finding.scopes)
	}
	keys := make([]string, 0, len(orgScopeReadsScopedElsewhere))
	for key := range orgScopeReadsScopedElsewhere {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		entry := orgScopeReadsScopedElsewhere[key]
		if !matched[key] {
			t.Errorf("%s is listed as scoped elsewhere but the census does not flag it: remove the entry", key)
		}
		if strings.TrimSpace(entry.reason) == "" || len(entry.sites) == 0 {
			t.Errorf("%s is listed as scoped elsewhere with no reason or no cited predicate", key)
		}
		for _, site := range entry.sites {
			if !orgScopePredicate.MatchString(site.predicate) {
				t.Errorf("%s cites %q, which is not an org_id predicate", key, site.predicate)
				continue
			}
			line := orgScopeSiteLine(t, root, site)
			if line == 0 {
				t.Errorf("%s cites %s %s for %q, which no longer holds it: re-read the caller and fix the read or the entry", key, site.file, site.function, site.predicate)
				continue
			}
			t.Logf("scoped elsewhere: %s <- %s:%d (%s) %q", key, site.file, line, site.function, site.predicate)
		}
	}
	t.Logf("org scope census: %d functions read organization tables, %d scoped by their caller or another package", checked, len(matched))
}

// The census itself: each rule fails on the defect it exists to catch, over a
// small tree of fixture packages.
func TestOrgScopeCensusFlagsEachUnscopedReadShape(t *testing.T) {
	root := t.TempDir()
	write := func(rel, src string) {
		t.Helper()
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("internal/queryapi/shared/shared.go", "package shared\n\n"+
		"const Source = `(SELECT * FROM repo_metrics_daily WHERE org_id = {org_id:String})`\n\n"+
		"func Where() string { return \"WHERE org_id = {org_id:String}\" }\n")
	write("internal/queryapi/fixture/fixture.go", `package fixture

import "github.com/full-chaos/dev-health-ops/internal/queryapi/shared"

const scopedWhere = "WHERE org_id = {org_id:String}"

func noPredicate() string {
	return `+"`SELECT day FROM repo_metrics_daily WHERE day = {day:Date}`"+`
}

func scoped() string {
	return `+"`SELECT day FROM repo_metrics_daily WHERE org_id = {org_id:String}`"+`
}

func secondStatementUnscoped() (string, string) {
	return `+"`SELECT day FROM repo_metrics_daily WHERE org_id = {org_id:String}`"+`,
		`+"`SELECT day FROM work_item_metrics_daily WHERE day = {day:Date}`"+`
}

func scopedByPackageConst() string {
	return "SELECT day FROM repo_metrics_daily " + scopedWhere
}

func scopedByHelper() string {
	return "SELECT day FROM repo_metrics_daily " + where()
}

func where() string { return "WHERE org_id = {org_id:String}" }

func scopedByOtherPackage() string {
	return "SELECT day FROM repo_metrics_daily " + shared.Where()
}

func dynamicFromOtherPackage() string {
	return "SELECT day FROM " + shared.Source
}

func dynamicUnscoped(table string) string {
	return "SELECT day FROM " + table
}

func shadowed() string {
	scopedWhere := "WHERE day = {day:Date}"
	return "SELECT day FROM repo_metrics_daily " + scopedWhere
}

func notAnOrganizationTable() string {
	return `+"`SELECT version FROM schema_migrations`"+`
}
`)
	tables := map[string]bool{"repo_metrics_daily": true, "work_item_metrics_daily": true}
	checked, flagged := censusOrgScope(t, root, []string{"internal/queryapi/fixture"}, tables)
	var got []string
	for _, finding := range flagged {
		got = append(got, finding.key)
	}
	want := []string{
		"internal/queryapi/fixture/fixture.go:dynamicUnscoped",
		"internal/queryapi/fixture/fixture.go:noPredicate",
		"internal/queryapi/fixture/fixture.go:secondStatementUnscoped",
		"internal/queryapi/fixture/fixture.go:shadowed",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("flagged:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if checked != 9 {
		t.Errorf("checked %d functions that read an organization table, want 9", checked)
	}
}
