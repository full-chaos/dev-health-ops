package graph

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// The root-class census pins, for every GraphQL root field, which repository
// ceiling a policy decision's allowlist can put on it. A ceiling narrows a read
// only through a repository key, so a root is narrowable (N) only when every
// ClickHouse table its resolver reaches carries one; one table without a key
// makes it mixed (M), and an allowlist decision on it must be refused. Work-item
// tables count as having no key: a work item reaches a repository only through
// its linked pull-request rows (work_graph_issue_pr), never through its own
// repository column. O is a root that reaches no ClickHouse table at all;
// X, P and W are classed by their action (repository_scope none, a platform
// action, a write), whatever they read.
//
// The census follows each resolver through every function of this module it
// names, the methods of the packages it imports included, and reads the table
// names out of the SQL text. A method is matched by name, so the walk reads
// more than a call can reach, never less: an N row can only be wrong toward M.
// A table named at run time (FROM %s) is not seen; a root whose walk holds one
// lists it under dynamicReads.

type rootClass struct {
	class  string
	reason string
}

const (
	classNarrowable = "N"
	classMixed      = "M"
	classOrgOnly    = "O"
	classNoRepoRule = "X"
	classPlatform   = "P"
	classWrite      = "W"
)

// actionClasses are the classes an action decides on its own.
var actionClasses = map[string]string{
	"integrations.read": classNoRepoRule,
	"admin.telemetry":   classPlatform,
	"reports.write":     classWrite,
}

var rootClasses = map[string]rootClass{
	"Mutation.cloneSavedReport":               {classWrite, "reports.write: a write, refused for a delegated session by the operation guard"},
	"Mutation.createSavedReport":              {classWrite, "reports.write: a write, refused for a delegated session by the operation guard"},
	"Mutation.deleteSavedReport":              {classWrite, "reports.write: a write, refused for a delegated session by the operation guard"},
	"Mutation.triggerReport":                  {classWrite, "reports.write: a write, refused for a delegated session by the operation guard"},
	"Mutation.updateSavedReport":              {classWrite, "reports.write: a write, refused for a delegated session by the operation guard"},
	"Query.aiAttributedPrs":                   {classMixed, "the AI scope resolves teams; the attributed rows join work_items"},
	"Query.aiAttributionOverview":             {classMixed, "the AI scope resolves teams"},
	"Query.aiComparison":                      {classMixed, "the AI scope resolves teams"},
	"Query.aiGovernanceSummary":               {classMixed, "the AI scope resolves teams"},
	"Query.aiImpactSummary":                   {classMixed, "the AI scope resolves teams"},
	"Query.aiOpportunities":                   {classMixed, "the AI scope resolves teams; opportunities join work_items"},
	"Query.aiReviewLoad":                      {classMixed, "the AI scope resolves teams"},
	"Query.aiRiskBreakdown":                   {classMixed, "the AI scope resolves teams"},
	"Query.aiWorkflowDrilldown":               {classMixed, "the workflow walk reads operational_incidents"},
	"Query.analytics":                         {classMixed, "flow matrix and timeseries read work-item tables; investment reads unit membership"},
	"Query.busFactor":                         {classMixed, "the scope resolves teams"},
	"Query.capacityForecast":                  {classMixed, "work_item_metrics_daily has no repository key"},
	"Query.capacityForecasts":                 {classMixed, "capacity_forecasts and teams have no repository key"},
	"Query.catalog":                           {classMixed, "dimension values list teams and unit membership"},
	"Query.cognitiveLoad":                     {classMixed, "team_cognitive_load_daily and teams have no repository key"},
	"Query.complexityTimeseries":              {classNarrowable, "file and repository complexity rows, keyed by repository"},
	"Query.compoundingRisk":                   {classMixed, "compounding_risk_daily and teams have no repository key"},
	"Query.coverageBaselines":                 {classMixed, "the team filter resolves teams"},
	"Query.coverageScopeBaseline":             {classMixed, "the team filter resolves teams"},
	"Query.dataHealth":                        {classNoRepoRule, "integrations.read has repository_scope none: the decision carries no repository member"},
	"Query.experiments":                       {classMixed, "opportunities.BuildResponse is the home builder: work-item, recommendation and risk tables"},
	"Query.featureFlagEvents":                 {classNarrowable, "feature_flag_event rows, keyed by repository"},
	"Query.featureFlags":                      {classNarrowable, "feature_flag rows, keyed by repository"},
	"Query.home":                              {classMixed, "home.BuildResponse reads work-item, recommendation and risk tables"},
	"Query.hotspots":                          {classNarrowable, "file_hotspot_daily rows, keyed by repository"},
	"Query.improveOpportunities":              {classMixed, "work_item_metrics_daily and teams have no repository key"},
	"Query.operatingReview":                   {classMixed, "work-item tables, teams and unit membership have no repository key"},
	"Query.pr":                                {classNarrowable, "pull request, review, commit and linked-issue rows, keyed by repository"},
	"Query.productTelemetryDashboard":         {classMixed, "product_telemetry_events has no repository key"},
	"Query.productTelemetryPlatformDashboard": {classPlatform, "admin.telemetry is a platform action: never decided as an organization row"},
	"Query.recommendations":                   {classMixed, "recommendations_daily has no repository key"},
	"Query.reportRuns":                        {classOrgOnly, "Postgres report runs only; a run's stored output is not proved free of repository data"},
	"Query.reviewEdges":                       {classMixed, "identities and teams have no repository key"},
	"Query.savedReport":                       {classOrgOnly, "Postgres saved-report definition only"},
	"Query.savedReports":                      {classOrgOnly, "Postgres saved-report definitions only"},
	"Query.securityAlerts":                    {classNarrowable, "security_alerts rows, keyed by repository"},
	"Query.securityOverview":                  {classNarrowable, "security_alerts rows, keyed by repository"},
	"Query.sourceHealth":                      {classMixed, "sync and job tables and the identity, team and work-item tables the reader methods reach"},
	"Query.testopsJobFailures":                {classMixed, "the scope resolves teams"},
	"Query.testopsTestWindow":                 {classMixed, "the scope resolves teams"},
	"Query.testopsRisk":                       {classNarrowable, "testops stability, drag and confidence rows, keyed by repository"},
	"Query.throughputForecast":                {classMixed, "estimate_coverage_metrics_daily and work_item_metrics_daily have no repository key"},
	"Query.workGraphArtifacts":                {classMixed, "operational_incidents and unit membership have no repository key"},
	"Query.workGraphEdges":                    {classMixed, "work_item_dependencies, operational_incidents and unit membership have no repository key"},
	"Query.workGraphFlow":                     {classMixed, "unit membership has no repository key"},
	"Query.workItemTeamAttributions":          {classMixed, "work_item_team_attributions is a work-item table"},
	"Query.workUnitTeamAttributions":          {classMixed, "work_item_team_attributions and unit membership have no repository key"},
}

// dynamicReads are the roots whose walk names a table at run time, with the
// tables that name can be.
var dynamicReads = map[string]string{}

const rootCensusModule = "github.com/full-chaos/dev-health-ops/"

var (
	rootCensusTableRead   = regexp.MustCompile(`(?i)\b(?:FROM|JOIN)\s+` + "`?" + `([a-z_][a-z0-9_]*)`)
	rootCensusDynamicRead = regexp.MustCompile("\\b(?:FROM|JOIN) +(?:%s|%v)")
	rootCensusCreate      = regexp.MustCompile("(?i)CREATE (?:TABLE|(?:MATERIALIZED )?VIEW)(?: IF NOT EXISTS)?\\s+`?([a-z_][a-z0-9_]*)`?")
	rootCensusAddRepo     = regexp.MustCompile("(?i)ALTER TABLE\\s+`?([a-z_][a-z0-9_]*)`?[^;]*ADD COLUMN(?: IF NOT EXISTS)?\\s+`?repo_id`?")
	rootCensusRepoColumn  = regexp.MustCompile("(?i)[(,]\\s*`?repo_id`?\\s")
	rootCensusRepoWord    = regexp.MustCompile(`(?i)\brepo_id\b`)
)

// clickHouseTables is every table and view of the ClickHouse head, true when it
// carries a repository key. A view carries one when its statement names repo_id.
func clickHouseTables(t *testing.T) map[string]bool {
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
	mark := func(name string, keyed bool) {
		name = strings.ToLower(name)
		tables[name] = tables[name] || keyed
	}
	for _, object := range baseline.Objects {
		keyed := rootCensusRepoColumn.MatchString(object.Create) || (object.IsView() && rootCensusRepoWord.MatchString(object.Create))
		mark(object.Name, keyed)
	}
	for _, file := range chain {
		for _, statement := range chmigrate.SplitStatements(chmigrate.StripLineComments(file.SQL)) {
			if m := rootCensusCreate.FindStringSubmatch(statement); m != nil {
				mark(m[1], rootCensusRepoColumn.MatchString(statement) || (strings.Contains(strings.ToUpper(statement), "VIEW") && rootCensusRepoWord.MatchString(statement)))
			}
			if m := rootCensusAddRepo.FindStringSubmatch(statement); m != nil {
				mark(m[1], true)
			}
		}
	}
	tables["repos"] = true
	for name := range tables {
		if strings.HasPrefix(name, "work_item") {
			tables[name] = false
		}
	}
	return tables
}

type censusFunc struct {
	decl    *ast.FuncDecl
	dir     string
	imports map[string]string
}

type censusPackage struct {
	strings map[string]string
	funcs   map[string]censusFunc
	methods map[string][]censusFunc
}

type rootCensusLoader struct {
	t        *testing.T
	root     string
	packages map[string]*censusPackage
	memo     map[string]map[string]bool
}

func newRootCensusLoader(t *testing.T, root string) *rootCensusLoader {
	return &rootCensusLoader{t: t, root: root, packages: map[string]*censusPackage{}, memo: map[string]map[string]bool{}}
}

func censusLiterals(node ast.Node) string {
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

func (loader *rootCensusLoader) pkg(dir string) *censusPackage {
	if pkg, ok := loader.packages[dir]; ok {
		return pkg
	}
	pkg := &censusPackage{strings: map[string]string{}, funcs: map[string]censusFunc{}, methods: map[string][]censusFunc{}}
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
			if !strings.HasPrefix(path, rootCensusModule) {
				continue
			}
			local := filepath.Base(path)
			if spec.Name != nil {
				local = spec.Name.Name
			}
			imports[local] = filepath.Join(loader.root, filepath.FromSlash(strings.TrimPrefix(path, rootCensusModule)))
		}
		for _, decl := range file.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				if decl.Body == nil {
					continue
				}
				fn := censusFunc{decl: decl, dir: dir, imports: imports}
				if decl.Recv != nil {
					pkg.methods[decl.Name.Name] = append(pkg.methods[decl.Name.Name], fn)
				} else {
					pkg.funcs[decl.Name.Name] = fn
				}
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					if values, ok := spec.(*ast.ValueSpec); ok {
						for index, ident := range values.Names {
							if index < len(values.Values) {
								pkg.strings[ident.Name] = censusLiterals(values.Values[index])
							}
						}
					}
				}
			}
		}
	}
	return pkg
}

// reads is every word a function's SQL text puts after FROM or JOIN, with the
// text of the package-level strings it names and the reads of every function of
// this module it calls, memoized; a call cycle adds nothing on its second visit.
// A method selected from a value is every method of that name in the
// function's own package and in each package its file imports. The marker
// dynamicMarker stands for a table named at run time.
func (loader *rootCensusLoader) reads(fn censusFunc) map[string]bool {
	key := fn.dir + "." + censusFuncKey(fn.decl)
	if reads, ok := loader.memo[key]; ok {
		return reads
	}
	reads := map[string]bool{}
	loader.memo[key] = reads
	addText := func(text string) {
		for _, m := range rootCensusTableRead.FindAllStringSubmatch(text, -1) {
			reads[strings.ToLower(m[1])] = true
		}
		if rootCensusDynamicRead.MatchString(text) {
			reads[dynamicMarker] = true
		}
	}
	addCall := func(callee censusFunc) {
		for name := range loader.reads(callee) {
			reads[name] = true
		}
	}
	addText(censusLiterals(fn.decl.Body))
	pkg := loader.pkg(fn.dir)
	selected := map[*ast.Ident]bool{}
	ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.SelectorExpr:
			if x, isIdent := node.X.(*ast.Ident); isIdent {
				if dir, imported := fn.imports[x.Name]; imported {
					other := loader.pkg(dir)
					addText(other.strings[node.Sel.Name])
					if callee, ok := other.funcs[node.Sel.Name]; ok {
						addCall(callee)
					}
					return false
				}
			}
			selected[node.Sel] = true
			for _, callee := range pkg.methods[node.Sel.Name] {
				addCall(callee)
			}
			for _, dir := range fn.imports {
				for _, callee := range loader.pkg(dir).methods[node.Sel.Name] {
					addCall(callee)
				}
			}
		case *ast.Ident:
			if selected[node] {
				return true
			}
			addText(pkg.strings[node.Name])
			if callee, ok := pkg.funcs[node.Name]; ok {
				addCall(callee)
			}
		}
		return true
	})
	return reads
}

// dynamicMarker is not a ClickHouse identifier, so no table can collide with it.
const dynamicMarker = "%dynamic"

func censusFuncKey(decl *ast.FuncDecl) string {
	if decl.Recv != nil && len(decl.Recv.List) > 0 {
		return "(" + censusRecvType(decl.Recv.List[0].Type) + ")." + decl.Name.Name
	}
	return decl.Name.Name
}

func censusRecvType(expr ast.Expr) string {
	switch expr := expr.(type) {
	case *ast.StarExpr:
		return censusRecvType(expr.X)
	case *ast.Ident:
		return expr.Name
	case *ast.IndexExpr:
		return censusRecvType(expr.X)
	}
	return ""
}

// readsOf is the ClickHouse tables among a function's reads, split by
// repository key, and whether it names a table at run time.
func readsOf(reads map[string]bool, tables map[string]bool) (keyed, unkeyed []string, dynamic bool) {
	for name := range reads {
		has, known := tables[name]
		if !known {
			continue
		}
		if has {
			keyed = append(keyed, name)
		} else {
			unkeyed = append(unkeyed, name)
		}
	}
	sort.Strings(keyed)
	sort.Strings(unkeyed)
	return keyed, unkeyed, reads[dynamicMarker]
}

// readClass is the class the reads alone decide.
func readClass(keyed, unkeyed []string) string {
	switch {
	case len(unkeyed) > 0:
		return classMixed
	case len(keyed) > 0:
		return classNarrowable
	}
	return classOrgOnly
}

// resolverOf is the resolver method of a root ("Query.home" -> queryResolver.Home).
func resolverOf(pkg *censusPackage, root string) (censusFunc, bool) {
	kind, name, _ := strings.Cut(root, ".")
	receiver := map[string]string{"Query": "queryResolver", "Mutation": "mutationResolver"}[kind]
	method := strings.ToUpper(name[:1]) + name[1:]
	for _, fn := range pkg.methods[method] {
		if censusRecvType(fn.decl.Recv.List[0].Type) == receiver {
			return fn, true
		}
	}
	return censusFunc{}, false
}

func TestEveryGraphQLRootHasTheClassItsReadsDecide(t *testing.T) {
	tables := clickHouseTables(t)
	// A census over an empty table set would class every root O.
	for name, keyed := range map[string]bool{"git_pull_requests": true, "repo_metrics_daily": true, "teams": false, "work_items": false} {
		if has, known := tables[name]; !known || has != keyed {
			t.Fatalf("the census reads %s as known=%v keyed=%v; want keyed=%v", name, known, has, keyed)
		}
	}
	loader := newRootCensusLoader(t, filepath.Join("..", "..", ".."))
	pkg := loader.pkg(".")
	roots := make([]string, 0, len(fieldActions))
	for root := range fieldActions {
		roots = append(roots, root)
	}
	sort.Strings(roots)
	for _, root := range roots {
		row, listed := rootClasses[root]
		if !listed {
			t.Errorf("%s has no class row", root)
			continue
		}
		if strings.TrimSpace(row.reason) == "" {
			t.Errorf("%s: the class row gives no reason", root)
		}
		fn, ok := resolverOf(pkg, root)
		if !ok {
			t.Errorf("%s: no resolver method", root)
			continue
		}
		keyed, unkeyed, dynamic := readsOf(loader.reads(fn), tables)
		want := actionClasses[fieldActions[root]]
		if want == "" {
			want = readClass(keyed, unkeyed)
		}
		t.Logf("%s %s keyed=%v unkeyed=%v dynamic=%v", root, want, keyed, unkeyed, dynamic)
		if row.class != want {
			t.Errorf("%s is classed %s; its reads decide %s (keyed %v, no repository key %v)", root, row.class, want, keyed, unkeyed)
		}
		if _, noted := dynamicReads[root]; dynamic != noted && want == classNarrowable {
			t.Errorf("%s: names a table at run time = %v, listed in dynamicReads = %v", root, dynamic, noted)
		}
	}
	for root := range rootClasses {
		if _, served := fieldActions[root]; !served {
			t.Errorf("%s has a class row but is not a served root", root)
		}
	}
	for root := range dynamicReads {
		if rootClasses[root].class != classNarrowable {
			t.Errorf("%s is listed in dynamicReads but is not an N root", root)
		}
	}
}

// The walk itself, over a fixture tree: a package-level string a function
// names, a function of another package, a method selected from a value of
// another package, and a table named at run time all reach the reads.
func TestRootCensusWalkReachesEveryReadShape(t *testing.T) {
	root := t.TempDir()
	write := func(path, text string) {
		t.Helper()
		full := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("internal/reader/reader.go", "package reader\n\n"+
		"const teamsQuery = `SELECT id FROM teams FINAL`\n\n"+
		"func Teams() string { return teamsQuery }\n\n"+
		"type Store struct{}\n\n"+
		"func (Store) PullRequests() string { return `SELECT id FROM git_pull_requests JOIN repos ON 1`}\n\n"+
		"func (Store) Named(table string) string { return \"SELECT 1 FROM %s\" + table }\n")
	write("internal/resolver/resolver.go", "package resolver\n\n"+
		"import \""+rootCensusModule+"internal/reader\"\n\n"+
		"func OnlyKeyed(s reader.Store) string { return s.PullRequests() }\n\n"+
		"func Mixed(s reader.Store) string { return reader.Teams() + s.PullRequests() }\n\n"+
		"func Dynamic(s reader.Store) string { return s.Named(\"x\") }\n\n"+
		"func Nothing() string { return \"SELECT 1\" }\n")
	tables := map[string]bool{"teams": false, "git_pull_requests": true, "repos": true}
	loader := newRootCensusLoader(t, root)
	pkg := loader.pkg(filepath.Join(root, "internal", "resolver"))
	cases := []struct {
		fn      string
		class   string
		keyed   []string
		unkeyed []string
		dynamic bool
	}{
		{"OnlyKeyed", classNarrowable, []string{"git_pull_requests", "repos"}, nil, false},
		{"Mixed", classMixed, []string{"git_pull_requests", "repos"}, []string{"teams"}, false},
		{"Dynamic", classOrgOnly, nil, nil, true},
		{"Nothing", classOrgOnly, nil, nil, false},
	}
	for _, c := range cases {
		fn, ok := pkg.funcs[c.fn]
		if !ok {
			t.Fatalf("fixture function %s not loaded", c.fn)
		}
		keyed, unkeyed, dynamic := readsOf(loader.reads(fn), tables)
		if strings.Join(keyed, ",") != strings.Join(c.keyed, ",") || strings.Join(unkeyed, ",") != strings.Join(c.unkeyed, ",") || dynamic != c.dynamic {
			t.Errorf("%s: keyed %v unkeyed %v dynamic %v; want %v %v %v", c.fn, keyed, unkeyed, dynamic, c.keyed, c.unkeyed, c.dynamic)
		}
		if got := readClass(keyed, unkeyed); got != c.class {
			t.Errorf("%s: class %s; want %s", c.fn, got, c.class)
		}
	}
}
