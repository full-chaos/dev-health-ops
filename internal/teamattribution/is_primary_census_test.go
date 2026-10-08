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
	teamScopedSources := 0
	for rel, lines := range teamScopedLines {
		sources, reasons := teamScopedIsPrimaryViolations(t, filepath.Join(root, filepath.FromSlash(rel)), lines)
		teamScopedSources += sources
		for _, reason := range reasons {
			violations = append(violations, rel+":"+reason)
		}
	}
	for _, violation := range violations {
		t.Error(violation)
	}
	if teamScopedSources < 2 {
		t.Errorf("census found %d team-scoped sources (`IN (1, 2)` in a const used under a team filter): the team-scoped check did not run", teamScopedSources)
	}
	// A census that reads nothing proves nothing.
	if filesRead < 10 || allowedSites < 14 {
		t.Errorf("census read %d files and %d allowed predicate sites: the walk did not reach the readers", filesRead, allowedSites)
	}
}

// teamScopedIsPrimaryViolations holds every `is_primary IN (1, 2)` read to a
// team filter: the predicate sits in a package-level const, and every use of
// that const is inside an if statement that also applies a team filter (a
// `team_id = {..}` / `team_id IN {..}` literal in the if, or a variable set
// from a function of the file that returns such a literal). Without a team
// filter the read would count an item of several teams once per team.
func teamScopedIsPrimaryViolations(t *testing.T, path string, lines []int) (int, []string) {
	t.Helper()
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	line := func(node ast.Node) int { return fileSet.Position(node.Pos()).Line }
	within := func(node ast.Node, target int) bool {
		return line(node) <= target && target <= fileSet.Position(node.End()).Line
	}
	hasTeamFilterLiteral := func(node ast.Node) bool {
		found := false
		ast.Inspect(node, func(child ast.Node) bool {
			if literal, ok := child.(*ast.BasicLit); ok && literal.Kind == token.STRING && teamFilterSQL.MatchString(literal.Value) {
				found = true
			}
			return !found
		})
		return found
	}
	teamFilterFuncs := map[string]bool{}
	for _, decl := range file.Decls {
		if function, ok := decl.(*ast.FuncDecl); ok && function.Body != nil && hasTeamFilterLiteral(function.Body) {
			teamFilterFuncs[function.Name.Name] = true
		}
	}
	consts := map[string]bool{}
	var reasons []string
	for _, target := range lines {
		name := ""
		for _, decl := range file.Decls {
			general, ok := decl.(*ast.GenDecl)
			if !ok || general.Tok != token.CONST || !within(general, target) {
				continue
			}
			for _, spec := range general.Specs {
				if value, ok := spec.(*ast.ValueSpec); ok && within(value, target) && len(value.Names) == 1 {
					name = value.Names[0].Name
				}
			}
		}
		if name == "" {
			reasons = append(reasons, strconv.Itoa(target)+": `is_primary IN (1, 2)` outside a package-level const (a team-scoped source must be a const used under a team filter)")
			continue
		}
		consts[name] = true
	}
	for _, decl := range file.Decls {
		if general, ok := decl.(*ast.GenDecl); ok && general.Tok == token.VAR {
			ast.Inspect(general, func(node ast.Node) bool {
				if ident, ok := node.(*ast.Ident); ok && consts[ident.Name] {
					reasons = append(reasons, strconv.Itoa(line(ident))+": "+ident.Name+" (`is_primary IN (1, 2)`) used at package level, outside a team filter")
				}
				return true
			})
		}
		function, ok := decl.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		// Variables of this function set from a call of a team-filter function.
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
		var stack []ast.Node
		ast.Inspect(function.Body, func(node ast.Node) bool {
			if node == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			stack = append(stack, node)
			ident, ok := node.(*ast.Ident)
			if !ok || !consts[ident.Name] {
				return true
			}
			var guard *ast.IfStmt
			for index := len(stack) - 1; index >= 0; index-- {
				if candidate, ok := stack[index].(*ast.IfStmt); ok {
					guard = candidate
					break
				}
			}
			scoped := false
			if guard != nil {
				scoped = hasTeamFilterLiteral(guard)
				ast.Inspect(guard.Cond, func(child ast.Node) bool {
					if name, ok := child.(*ast.Ident); ok && teamScopeVars[name.Name] {
						scoped = true
					}
					return !scoped
				})
			}
			if !scoped {
				reasons = append(reasons, strconv.Itoa(line(ident))+": "+ident.Name+" (`is_primary IN (1, 2)`) used without a team filter in the same if statement")
			}
			return true
		})
	}
	return len(consts), reasons
}
