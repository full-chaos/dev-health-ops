package aianalytics

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-14101: user_metrics_daily.team_id is a person's membership label (acp, CHAOS-4396 option (a)), never a
// team scope: a team is the repositories it owns (CHAOS-4321, team-attribution.md "The AI analytics team scope uses
// repository ownership"). These pins keep the AI analytics and cognitive load readers off that column.

var teamIDPredicate = regexp.MustCompile(`\b(?:\w+\.)?team_id\s*(?:=|IN)\s*\{`)

// umdTeamScopeSites lists the functions of a file whose SQL reads user_metrics_daily and filters a team_id by a
// bound parameter. The SQL of a function is its own string literals plus those of the package-level values it
// names, so a statement held in a var and a filter appended in the function are read together.
func umdTeamScopeSites(file *ast.File, values map[string][]string) []string {
	var sites []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		var lits []string
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.BasicLit:
				if n.Kind == token.STRING {
					if v, err := strconv.Unquote(n.Value); err == nil {
						lits = append(lits, v)
					}
				}
			case *ast.Ident:
				lits = append(lits, values[n.Name]...)
			}
			return true
		})
		readsUMD, filtersTeam := false, false
		for _, l := range lits {
			readsUMD = readsUMD || strings.Contains(l, "user_metrics_daily")
			filtersTeam = filtersTeam || teamIDPredicate.MatchString(l)
		}
		if readsUMD && filtersTeam {
			sites = append(sites, fn.Name.Name)
		}
	}
	return sites
}

// packageValues maps each package-level var or const name to its string literals.
func packageValues(files []*ast.File) map[string][]string {
	values := map[string][]string{}
	for _, file := range files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || (gen.Tok != token.VAR && gen.Tok != token.CONST) {
				continue
			}
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, name := range vs.Names {
					if i >= len(vs.Values) {
						continue
					}
					ast.Inspect(vs.Values[i], func(node ast.Node) bool {
						if lit, ok := node.(*ast.BasicLit); ok && lit.Kind == token.STRING {
							if v, err := strconv.Unquote(lit.Value); err == nil {
								values[name.Name] = append(values[name.Name], v)
							}
						}
						return true
					})
				}
			}
		}
	}
	return values
}

func parseSources(t *testing.T, sources map[string]string) []*ast.File {
	t.Helper()
	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	var files []*ast.File
	for _, name := range names {
		f, err := parser.ParseFile(token.NewFileSet(), name, sources[name], 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	return files
}

// The red pair: the detector finds the shape main had (a statement var over user_metrics_daily and a
// `umd.team_id = {...}` filter appended in the function) and passes the owned-repository shape.
func TestUMDTeamScopeDetectorRedPair(t *testing.T) {
	const statement = "package p\nvar stmt = `SELECT 1 FROM user_metrics_daily AS umd WHERE %s`\n"
	red := parseSources(t, map[string]string{"red.go": statement + `
func read(team string) string {
	filters := []string{"umd.day >= {start_day:Date}"}
	if team != "" {
		filters = append(filters, "umd.team_id = {team_id:String}")
	}
	return fmt.Sprintf(stmt, strings.Join(filters, " AND "))
}
`})
	if got := umdTeamScopeSites(red[0], packageValues(red)); len(got) != 1 || got[0] != "read" {
		t.Fatalf("red source: sites = %v, want [read]", got)
	}
	unaliased := parseSources(t, map[string]string{"red2.go": "package p\n" + `
func read(team string) string {
	q := "SELECT 1 FROM user_metrics_daily WHERE org_id = {org_id:String}"
	if team != "" {
		q += " AND team_id = {team_id:String}"
	}
	return q
}
`})
	if got := umdTeamScopeSites(unaliased[0], packageValues(unaliased)); len(got) != 1 {
		t.Fatalf("unaliased red source: sites = %v, want one", got)
	}
	green := parseSources(t, map[string]string{"green.go": statement + `
func read(team string, held []string) string {
	filters := []string{"umd.day >= {start_day:Date}"}
	if team != "" {
		filters = append(filters, "toString(umd.repo_id) IN {team_repo_ids:Array(String)}")
	}
	return fmt.Sprintf(stmt, strings.Join(filters, " AND "))
}
func other(team string) string { return "SELECT 1 FROM ai_impact_metrics_daily WHERE team_id = {team_id:String}" }
`})
	if got := umdTeamScopeSites(green[0], packageValues(green)); len(got) != 0 {
		t.Fatalf("green source: sites = %v, want none", got)
	}
}

// The census: no function of the AI analytics or cognitive load readers filters user_metrics_daily by team_id.
func TestNoAIAnalyticsOrCognitiveLoadReaderScopesUserMetricsDailyByTeamID(t *testing.T) {
	for _, dir := range []string{".", "../cognitiveload"} {
		paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		var files []*ast.File
		var names []string
		for _, path := range paths {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			files = append(files, f)
			names = append(names, path)
		}
		if len(files) == 0 {
			t.Fatalf("%s: the census read %d source files: it did not measure the package", dir, len(files))
		}
		values := packageValues(files)
		readers := 0
		for _, f := range files {
			ast.Inspect(f, func(node ast.Node) bool {
				if lit, ok := node.(*ast.BasicLit); ok && lit.Kind == token.STRING && strings.Contains(lit.Value, "user_metrics_daily") {
					readers++
				}
				return true
			})
		}
		for i, f := range files {
			for _, site := range umdTeamScopeSites(f, values) {
				t.Errorf("%s: %s filters user_metrics_daily by team_id, a membership label: scope a team by its owned repositories", names[i], site)
			}
		}
		if readers == 0 {
			t.Fatalf("%s: no SQL literal names user_metrics_daily: the census measured nothing", dir)
		}
	}
}

// statementClient answers the repository catalogue and the ownership reads from an ownerClient and every other
// statement with no rows, and records each statement with its bindings.
type statementClient struct {
	owner      *ownerClient
	statements []string
	bindings   [][]clickhouse.Binding
}

func (c *statementClient) Query(ctx context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.statements = append(c.statements, st)
	c.bindings = append(c.bindings, b)
	if strings.Contains(st, "{held_repo_ids:Array(String)}") || strings.Contains(st, "AS full_name") {
		return c.owner.Query(ctx, st, b)
	}
	return &scriptedRows{}, nil
}

func (c *statementClient) umdStatements() (sts []string, bindings [][]clickhouse.Binding) {
	for i, st := range c.statements {
		if strings.Contains(st, "FROM user_metrics_daily") {
			sts = append(sts, st)
			bindings = append(bindings, c.bindings[i])
		}
	}
	return sts, bindings
}

// The runtime pin: aiReviewLoad with a team scope reads user_metrics_daily over the team's owned repositories,
// never by team_id; a team that owns nothing reads no user_metrics_daily row at all.
func TestReviewLoadTeamScopeReadsUserMetricsDailyByOwnedRepositories(t *testing.T) {
	dr := model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}
	client := &statementClient{owner: newOwnerClient()}
	team := scopedTeamID
	if _, err := ReviewLoad(context.Background(), client, "org-1", dr, &model.AIScopeInput{TeamID: &team}); err != nil {
		t.Fatal(err)
	}
	sts, bindings := client.umdStatements()
	if len(sts) != 1 {
		t.Fatalf("user_metrics_daily statements = %d, want 1", len(sts))
	}
	if strings.Contains(sts[0], "team_id") {
		t.Fatalf("the user_metrics_daily statement filters team_id:\n%s", sts[0])
	}
	if !strings.Contains(sts[0], "toString(umd.repo_id) IN {team_repo_ids:Array(String)}") {
		t.Fatalf("the user_metrics_daily statement has no owned-repository filter:\n%s", sts[0])
	}
	var held []string
	for _, b := range bindings[0] {
		if b.Name == "team_id" {
			t.Fatalf("a team_id binding reached the user_metrics_daily statement: %v", bindings[0])
		}
		if b.Name == "team_repo_ids" {
			held, _ = b.Value.([]string)
		}
	}
	sort.Strings(held)
	want := []string{ownedRepo, coOwnedRepo}
	sort.Strings(want)
	if strings.Join(held, ",") != strings.Join(want, ",") {
		t.Fatalf("team_repo_ids = %v, want the repositories %s owns %v", held, scopedTeamID, want)
	}

	none := &statementClient{owner: newOwnerClient()}
	nobody := "team-none"
	got, err := ReviewLoad(context.Background(), none, "org-1", dr, &model.AIScopeInput{TeamID: &nobody})
	if err != nil {
		t.Fatal(err)
	}
	if sts, _ := none.umdStatements(); len(sts) != 0 {
		t.Fatalf("a team that owns nothing read user_metrics_daily %d times", len(sts))
	}
	if got.ReviewerConcentration.DataAvailable || got.ReviewerConcentration.ReviewerCount != 0 {
		t.Fatalf("a team that owns nothing: concentration = %+v, want no data", got.ReviewerConcentration)
	}
}
