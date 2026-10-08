package teamattribution

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

var (
	isPrimaryToken = regexp.MustCompile(`\b(?:[a-z_]+\.)?is_primary\b`)
	// What follows the token when it is compared.
	isPrimaryOperator = regexp.MustCompile(`^\s*(?:=|!=|<>|>=|<=|>|<|(?i:not\s+in|in)\b)`)
	// The two allowed predicate forms (AttributionPrimary, and a team-scoped
	// read of AttributionPrimary + AttributionCoOwner).
	isPrimaryAllowed    = regexp.MustCompile(`^\s*(?:=\s*1\b|IN\s*\(\s*1\s*,\s*2\s*\))`)
	isPrimaryTeamScoped = regexp.MustCompile(`^\s*IN\s*\(`)
	// A team filter in SQL text: `team_id = {param:...}` or `team_id IN {param:...}`.
	teamFilterSQL = regexp.MustCompile(`(?i)\bteam_id\s*(?:=|in)\s*\{`)
	// A bare truthy use: the token right after WHERE/AND/OR/NOT/if( with no
	// operator after it.
	isPrimaryTruthyBefore = regexp.MustCompile(`(?i)(?:\bwhere|\band|\bor|\bnot|\bif\s*\()\s*$`)
	// An aggregate over the flag.
	isPrimaryAggregateBefore = regexp.MustCompile(`(?i)\b(?:argmax|argmin|any|anylast|max|min|sum|avg|groupuniqarray)\s*\(\s*$`)
	// Go code that reads a stored flag as "not zero".
	isPrimaryGoTruthy = regexp.MustCompile(`(?i)\bisprimary\s*(?:!=\s*0|>\s*0|>=\s*1)`)
)

// isPrimaryCensusExclusions: lines that use an is_primary column of ANOTHER
// table in a file that also reads work_item_team_attributions. Each must
// still match, so a stale entry fails.
var isPrimaryCensusExclusions = map[string]string{
	"internal/teamattribution/cascade.go\x00argMax(o.is_primary, (o.version_at, o.valid_from)) AS is_primary,": "team_project_ownership / team_repo_ownership / team_memberships fact loaders (newest version of an ownership row)",
	"internal/teamattribution/cascade.go\x00argMax(v.is_primary, v.updated_at) AS is_primary,":                 "the same fact loaders, inner level",
}

// TestWorkItemTeamAttributionIsPrimaryPredicateCensus holds every reader of
// work_item_team_attributions to the two forms of is_primary: `= 1` (the one
// primary row: org totals, rollups, any read without a team filter) and
// `IN (1, 2)` (a team-scoped read, which also takes the co-owner rows). Any
// other form -- `!= 0`, `> 0`, a bare truthy flag, an aggregate over it, a Go
// `!= 0` on a scanned flag -- would read a co-owner row as primary and count
// an item of a project of several teams more than once. Production Go files
// only; the Python tree no longer reads this table at runtime.
func TestWorkItemTeamAttributionIsPrimaryPredicateCensus(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	var violations []string
	allowedSites, filesRead := 0, 0
	excluded := map[string]int{}
	teamScopedLines := map[string][]int{}
	readerDirs := map[string]bool{}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if entry.IsDir() {
			if strings.HasPrefix(entry.Name(), ".") || rel == "third_party" || rel == "node_modules" || entry.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") {
			return nil
		}
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if !strings.Contains(string(content), "work_item_team_attributions") {
			return nil
		}
		filesRead++
		readerDirs[filepath.Dir(path)] = true
		for number, line := range strings.Split(string(content), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			if key := rel + "\x00" + trimmed; isPrimaryCensusExclusions[key] != "" {
				excluded[key]++
				continue
			}
			site := func(reason string) {
				violations = append(violations, rel+":"+strconv.Itoa(number+1)+": "+reason+": "+trimmed)
			}
			if isPrimaryGoTruthy.MatchString(line) {
				site("a stored is_primary read as not-zero")
			}
			for _, match := range isPrimaryToken.FindAllStringIndex(line, -1) {
				before, after := line[:match[0]], line[match[1]:]
				switch {
				case isPrimaryAggregateBefore.MatchString(before):
					site("an aggregate over is_primary")
				case isPrimaryOperator.MatchString(after):
					if isPrimaryAllowed.MatchString(after) {
						allowedSites++
						if isPrimaryTeamScoped.MatchString(after) {
							teamScopedLines[rel] = append(teamScopedLines[rel], number+1)
						}
					} else {
						site("an is_primary predicate other than `= 1` or `IN (1, 2)`")
					}
				case isPrimaryTruthyBefore.MatchString(before):
					site("a bare truthy is_primary")
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for key, reason := range isPrimaryCensusExclusions {
		if excluded[key] == 0 {
			t.Errorf("stale exclusion (%s): %q matches no line", reason, strings.ReplaceAll(key, "\x00", ": "))
		}
	}
	for rel, lines := range teamScopedLines {
		violations = append(violations, isPrimaryOutsideConstViolations(t, filepath.Join(root, filepath.FromSlash(rel)), rel, lines)...)
	}
	teamScopedSources, readerViolations := teamGroupedReaderViolations(t, root, readerDirs)
	violations = append(violations, readerViolations...)
	for _, violation := range violations {
		t.Error(violation)
	}
	if teamScopedSources < 4 {
		t.Errorf("census found %d team-scoped sources (`IN (1, 2)` consts): the team-grouped reader check did not run", teamScopedSources)
	}
	// A census that reads nothing proves nothing.
	if filesRead < 10 || allowedSites < 14 {
		t.Errorf("census read %d files and %d allowed predicate sites: the walk did not reach the readers", filesRead, allowedSites)
	}
}

// isPrimaryOutsideConstViolations: an `is_primary IN (1, 2)` predicate sits
// only in a package-level const (a team-scoped source), so the reader check
// below can follow every use of it.
func isPrimaryOutsideConstViolations(t *testing.T, path, rel string, lines []int) []string {
	t.Helper()
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var violations []string
	for _, target := range lines {
		inConst := false
		for _, decl := range file.Decls {
			general, ok := decl.(*ast.GenDecl)
			if ok && general.Tok == token.CONST &&
				fileSet.Position(general.Pos()).Line <= target && target <= fileSet.Position(general.End()).Line {
				inConst = true
			}
		}
		if !inConst {
			violations = append(violations, rel+":"+strconv.Itoa(target)+": `is_primary IN (1, 2)` outside a package-level const")
		}
	}
	return violations
}

var (
	attributionPrimaryPredicate    = regexp.MustCompile(`(?i)\bis_primary\s*=\s*1\b`)
	attributionTeamScopedPredicate = regexp.MustCompile(`(?i)\bis_primary\s+IN\s*\(\s*1\s*,\s*2\s*\)`)
	// `is_primary = 1 DESC` orders the rows; it is not a predicate.
	attributionPrimaryOrdering = regexp.MustCompile(`(?i)\bis_primary\s*=\s*1\s+(?:DESC|ASC)\b`)
	// The newest-computed_at fence groups by the item key and closes its
	// subquery at once; it is not a grouping of the result.
	attributionFenceGroupBy = regexp.MustCompile(`(?is)GROUP\s+BY\s+[\w\s,]*?\bwork_item_id\s*\)`)
	queryGroupBy            = regexp.MustCompile(`(?i)\bGROUP\s+BY\b`)
	// A team column of a joined row (t.team_id, a.team_id) or a team id made
	// into an output key.
	queryTeamColumn = regexp.MustCompile(`(?i)\b[a-z_][a-z0-9_]*\.team_id\b|toString\(\s*team_id\s*\)`)
)

// teamGroupedReaderAllowlist: readers that read the primary row only although
// their query groups or filters by a team. Each must still be found.
var teamGroupedReaderAllowlist = map[string]string{
	"internal/queryapi/workgraph\x00resolveWorkUnitTeamAttributions": "work-unit team vote: one team per work unit from the items' primary rows (a work unit is not an item; the vote is unchanged)",
	"internal/queryapi/analytics\x00BuildUnitTeamSubquery":           "work-unit team vote of the investment views: one team per work unit from the items' primary rows",
}

// teamGroupedReaderViolations holds every reader of the attribution rows to
// the rule of the value contract: a query that groups by a team or filters by
// a team reads the team-scoped source (`is_primary IN (1, 2)`), so an item of
// a project of several teams counts for each of them; an organization path
// reads the primary source (`is_primary = 1`). Per package:
//   - a template const that reaches a source through other consts is checked
//     as one query: team-scoped needs a team grouping or filter; primary must
//     have neither (or be allowlisted);
//   - a function that names a team-scoped source names it inside the then
//     branch of `if <team> != ""` / `if len(<team>) > 0`, where <team> is a
//     team variable or the result of a function of the package that returns
//     a team filter; or the function's query always groups by team and reads
//     no primary source;
//   - a function that reads a primary source (named or inline) in a query
//     that groups or filters by team must also have that bound team branch
//     (the primary read is then its organization path), or be allowlisted.
func teamGroupedReaderViolations(t *testing.T, root string, dirs map[string]bool) (int, []string) {
	t.Helper()
	var violations []string
	teamScopedSources := 0
	allowed := map[string]int{}
	for dir := range dirs {
		relDir, _ := filepath.Rel(root, dir)
		relDir = filepath.ToSlash(relDir)
		fileSet := token.NewFileSet()
		packages, err := parser.ParseDir(fileSet, dir, func(info fs.FileInfo) bool {
			return !strings.HasSuffix(info.Name(), "_test.go")
		}, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", dir, err)
		}
		for _, pkg := range packages {
			census := newAttributionPackageCensus(fileSet, pkg)
			teamScopedSources += census.count(sourceTeamScoped)
			for _, finding := range census.check() {
				key := relDir + "\x00" + finding.name
				if _, ok := teamGroupedReaderAllowlist[key]; ok && finding.allowable {
					allowed[key]++
					continue
				}
				violations = append(violations, relDir+"/"+finding.file+":"+strconv.Itoa(finding.line)+": "+finding.name+": "+finding.reason)
			}
		}
	}
	for key, reason := range teamGroupedReaderAllowlist {
		if allowed[key] == 0 {
			violations = append(violations, "stale reader allowlist entry ("+reason+"): "+strings.ReplaceAll(key, "\x00", ": ")+" is no longer a team-grouped primary reader")
		}
	}
	return teamScopedSources, violations
}

type attributionSourceKind int

const (
	sourceNone attributionSourceKind = iota
	sourcePrimary
	sourceTeamScoped
)

type attributionFinding struct {
	file, name, reason string
	line               int
	allowable          bool
}

type attributionPackageCensus struct {
	teamFilterFuncs map[string]bool
	fileSet         *token.FileSet
	files           map[string]*ast.File
	consts          map[string]ast.Expr
	sources         map[string]attributionSourceKind
	position        map[string]token.Pos
}

func newAttributionPackageCensus(fileSet *token.FileSet, pkg *ast.Package) *attributionPackageCensus {
	census := &attributionPackageCensus{
		fileSet: fileSet, files: map[string]*ast.File{}, consts: map[string]ast.Expr{},
		sources: map[string]attributionSourceKind{}, position: map[string]token.Pos{},
	}
	for path, file := range pkg.Files {
		census.files[filepath.Base(path)] = file
		for _, decl := range file.Decls {
			general, ok := decl.(*ast.GenDecl)
			if !ok || general.Tok != token.CONST {
				continue
			}
			for _, spec := range general.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok || len(value.Names) != 1 || len(value.Values) != 1 {
					continue
				}
				census.consts[value.Names[0].Name] = value.Values[0]
				census.position[value.Names[0].Name] = value.Names[0].Pos()
			}
		}
	}
	for name, expr := range census.consts {
		census.sources[name] = sourceKindOf(ownLiteralText(expr))
	}
	census.teamFilterFuncs = census.teamFilterFunctions()
	return census
}

func sourceKindOf(text string) attributionSourceKind {
	if !strings.Contains(text, "work_item_team_attributions") {
		return sourceNone
	}
	text = attributionPrimaryOrdering.ReplaceAllString(text, "")
	switch {
	case attributionTeamScopedPredicate.MatchString(text):
		return sourceTeamScoped
	case attributionPrimaryPredicate.MatchString(text):
		return sourcePrimary
	}
	return sourceNone
}

// ownLiteralText is the text of the string literals written in expr itself,
// not of the consts it names.
func ownLiteralText(expr ast.Node) string {
	var builder strings.Builder
	ast.Inspect(expr, func(node ast.Node) bool {
		if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
			if value, err := strconv.Unquote(literal.Value); err == nil {
				builder.WriteString(value)
				builder.WriteString("\n")
			}
		}
		return true
	})
	return builder.String()
}

func (census *attributionPackageCensus) count(kind attributionSourceKind) int {
	total := 0
	for _, source := range census.sources {
		if source == kind {
			total++
		}
	}
	return total
}

// queryText is the text of the literals in node and of the consts it names
// (recursively); a named source is replaced by a placeholder and reported in
// reached.
func (census *attributionPackageCensus) queryText(node ast.Node, reached map[attributionSourceKind]bool, seen map[string]bool) string {
	var builder strings.Builder
	ast.Inspect(node, func(child ast.Node) bool {
		switch value := child.(type) {
		case *ast.BasicLit:
			if value.Kind == token.STRING {
				if text, err := strconv.Unquote(value.Value); err == nil {
					builder.WriteString(text)
					builder.WriteString("\n")
				}
			}
		case *ast.Ident:
			expr, isConst := census.consts[value.Name]
			if !isConst || seen[value.Name] {
				return true
			}
			if kind := census.sources[value.Name]; kind != sourceNone {
				reached[kind] = true
				builder.WriteString(" <attribution source> ")
				return true
			}
			seen[value.Name] = true
			builder.WriteString(census.queryText(expr, reached, seen))
			delete(seen, value.Name)
		}
		return true
	})
	return builder.String()
}

func teamGrouping(text string) (grouped, filtered bool) {
	text = attributionFenceGroupBy.ReplaceAllString(text, ")")
	return queryGroupBy.MatchString(text) && queryTeamColumn.MatchString(text), teamFilterSQL.MatchString(text)
}

func (census *attributionPackageCensus) where(pos token.Pos) (string, int) {
	position := census.fileSet.Position(pos)
	return filepath.Base(position.Filename), position.Line
}

func (census *attributionPackageCensus) check() []attributionFinding {
	var findings []attributionFinding
	referencedByConst := map[string]bool{}
	for _, expr := range census.consts {
		ast.Inspect(expr, func(node ast.Node) bool {
			if ident, ok := node.(*ast.Ident); ok {
				if _, isConst := census.consts[ident.Name]; isConst {
					referencedByConst[ident.Name] = true
				}
			}
			return true
		})
	}
	for name, expr := range census.consts {
		if census.sources[name] != sourceNone || referencedByConst[name] {
			continue
		}
		reached := map[attributionSourceKind]bool{}
		text := census.queryText(expr, reached, map[string]bool{name: true})
		if len(reached) == 0 {
			continue
		}
		grouped, filtered := teamGrouping(text)
		file, line := census.where(census.position[name])
		if reached[sourceTeamScoped] && !grouped && !filtered {
			findings = append(findings, attributionFinding{file: file, line: line, name: name,
				reason: "a team-scoped source (`is_primary IN (1, 2)`) in a query that neither groups nor filters by team: an organization read would count an item of several teams once per team"})
		}
		if reached[sourcePrimary] && (grouped || filtered) {
			findings = append(findings, attributionFinding{file: file, line: line, name: name, allowable: true,
				reason: "a query that groups or filters by team reads the primary source (`is_primary = 1`) only: a co-owner team does not get the item"})
		}
	}
	for _, file := range census.files {
		for _, decl := range file.Decls {
			if general, ok := decl.(*ast.GenDecl); ok && general.Tok == token.VAR {
				ast.Inspect(general, func(node ast.Node) bool {
					if ident, ok := node.(*ast.Ident); ok && census.sources[ident.Name] == sourceTeamScoped {
						fileName, line := census.where(ident.Pos())
						findings = append(findings, attributionFinding{file: fileName, line: line, name: ident.Name,
							reason: "a team-scoped source used at package level, outside a function's team branch"})
					}
					return true
				})
			}
			function, ok := decl.(*ast.FuncDecl)
			if ok && function.Body != nil {
				findings = append(findings, census.checkFunction(function)...)
			}
		}
	}
	return findings
}

func (census *attributionPackageCensus) teamFilterFunctions() map[string]bool {
	result := map[string]bool{}
	for _, file := range census.files {
		for _, decl := range file.Decls {
			if function, ok := decl.(*ast.FuncDecl); ok && function.Body != nil &&
				teamFilterSQL.MatchString(ownLiteralText(function.Body)) {
				result[function.Name.Name] = true
			}
		}
	}
	return result
}

func (census *attributionPackageCensus) checkFunction(function *ast.FuncDecl) []attributionFinding {
	inlinePrimary := sourceKindOf(ownLiteralText(function.Body)) == sourcePrimary
	type use struct {
		ident *ast.Ident
		bound bool
	}
	var teamScopedUses []use
	primaryNamed := false
	teamFilterFuncs := census.teamFilterFuncs
	teamScopeVars := map[string]bool{}
	ast.Inspect(function.Body, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok || len(assign.Rhs) != 1 {
			return true
		}
		if call, ok := assign.Rhs[0].(*ast.CallExpr); ok {
			if callee, ok := call.Fun.(*ast.Ident); ok && teamFilterFuncs[callee.Name] {
				for _, left := range assign.Lhs {
					if ident, ok := left.(*ast.Ident); ok {
						teamScopeVars[ident.Name] = true
					}
				}
			}
		}
		return true
	})
	isTeamVar := func(expr ast.Expr) bool {
		ident, ok := expr.(*ast.Ident)
		return ok && (teamScopeVars[ident.Name] || strings.Contains(strings.ToLower(ident.Name), "team"))
	}
	boundCondition := func(cond ast.Expr) bool {
		binary, ok := cond.(*ast.BinaryExpr)
		if !ok {
			return false
		}
		empty := func(expr ast.Expr) bool {
			if literal, ok := expr.(*ast.BasicLit); ok {
				return literal.Value == `""` || literal.Value == "0"
			}
			ident, ok := expr.(*ast.Ident)
			return ok && ident.Name == "nil"
		}
		subject := binary.X
		if call, ok := subject.(*ast.CallExpr); ok {
			if callee, ok := call.Fun.(*ast.Ident); ok && callee.Name == "len" && len(call.Args) == 1 {
				subject = call.Args[0]
			}
		}
		if star, ok := subject.(*ast.StarExpr); ok {
			subject = star.X
		}
		switch binary.Op {
		case token.NEQ, token.GTR:
			return isTeamVar(subject) && empty(binary.Y)
		}
		return false
	}
	var stack []ast.Node
	ast.Inspect(function.Body, func(node ast.Node) bool {
		if node == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		stack = append(stack, node)
		ident, ok := node.(*ast.Ident)
		if !ok {
			return true
		}
		switch census.sources[ident.Name] {
		case sourcePrimary:
			primaryNamed = true
		case sourceTeamScoped:
			bound := false
			for index := len(stack) - 1; index > 0; index-- {
				guard, ok := stack[index].(*ast.IfStmt)
				if !ok {
					continue
				}
				// Only the then branch of the nearest enclosing if counts.
				bound = stack[index+1] == guard.Body && boundCondition(guard.Cond)
				break
			}
			teamScopedUses = append(teamScopedUses, use{ident, bound})
		}
		return true
	})
	if len(teamScopedUses) == 0 && !primaryNamed && !inlinePrimary {
		return nil
	}
	reached := map[attributionSourceKind]bool{}
	grouped, filtered := teamGrouping(census.queryText(function.Body, reached, map[string]bool{}))
	readsPrimary := primaryNamed || inlinePrimary
	anyBound := false
	var findings []attributionFinding
	for _, use := range teamScopedUses {
		if use.bound {
			anyBound = true
			continue
		}
		if grouped && !readsPrimary {
			continue
		}
		file, line := census.where(use.ident.Pos())
		findings = append(findings, attributionFinding{file: file, line: line, name: function.Name.Name,
			reason: use.ident.Name + " (`is_primary IN (1, 2)`) used outside the then branch of `if <team> != \"\"`, in a function whose query does not always group by team"})
	}
	if readsPrimary && (grouped || filtered) && !anyBound {
		file, line := census.where(function.Name.Pos())
		findings = append(findings, attributionFinding{file: file, line: line, name: function.Name.Name, allowable: true,
			reason: "a query that groups or filters by team reads the primary source (`is_primary = 1`) only: a co-owner team does not get the item"})
	}
	return findings
}
