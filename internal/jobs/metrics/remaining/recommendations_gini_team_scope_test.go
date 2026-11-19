package remaining

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// user_metrics_daily.team_id is a person's membership label (CHAOS-4396), never a team scope: the review-concentration
// Gini reads the repositories the team owns at the window end, the set of its review latency (CHAOS-14163).

// umdTeamFilterSites lists the functions of file whose string literals (with those of the package-level values they
// name) read user_metrics_daily and filter a team_id by a bound parameter.
func umdTeamFilterSites(file *ast.File, values map[string][]string) []string {
	teamFilter := regexp.MustCompile(`\b(?:\w+\.)?team_id\s*(?:=|IN)\s*\{`)
	var sites []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		readsUMD, filtersTeam := false, false
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			var lits []string
			switch n := node.(type) {
			case *ast.BasicLit:
				if n.Kind == token.STRING {
					if v, err := strconv.Unquote(n.Value); err == nil {
						lits = append(lits, v)
					}
				}
			case *ast.Ident:
				lits = values[n.Name]
			}
			for _, l := range lits {
				readsUMD = readsUMD || strings.Contains(l, "user_metrics_daily")
				filtersTeam = filtersTeam || teamFilter.MatchString(l)
			}
			return true
		})
		if readsUMD && filtersTeam {
			sites = append(sites, fn.Name.Name)
		}
	}
	return sites
}

// umdPackageValues maps each package-level var or const name to its string literals.
func umdPackageValues(files []*ast.File) map[string][]string {
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

func parseUMDSources(t *testing.T, sources map[string]string) []*ast.File {
	t.Helper()
	var files []*ast.File
	for name, source := range sources {
		f, err := parser.ParseFile(token.NewFileSet(), name, source, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	return files
}

func TestRecommendationsUMDTeamFilterDetectorRedPair(t *testing.T) {
	red := parseUMDSources(t, map[string]string{"red.go": "package p\n" + `
func read() string {
	return "SELECT author_email FROM user_metrics_daily" + "\n WHERE team_id = {team_id:String}"
}
`})
	if got := umdTeamFilterSites(red[0], umdPackageValues(red)); len(got) != 1 || got[0] != "read" {
		t.Fatalf("red source: sites = %v, want [read]", got)
	}
	viaVar := parseUMDSources(t, map[string]string{"red2.go": "package p\nvar stmt = `SELECT 1 FROM user_metrics_daily AS umd WHERE %s`\n" + `
func read() string { return fmt.Sprintf(stmt, "umd.team_id IN {team_ids:Array(String)}") }
`})
	if got := umdTeamFilterSites(viaVar[0], umdPackageValues(viaVar)); len(got) != 1 {
		t.Fatalf("aliased red source through a package var: sites = %v, want one", got)
	}
	green := parseUMDSources(t, map[string]string{"green.go": "package p\n" + `
func read() string {
	return "SELECT author_email FROM user_metrics_daily WHERE repo_id IN {repo_ids:Array(UUID)}"
}
func other() string { return "SELECT 1 FROM work_item_metrics_daily WHERE team_id = {team_id:String}" }
`})
	if got := umdTeamFilterSites(green[0], umdPackageValues(green)); len(got) != 0 {
		t.Fatalf("green source: sites = %v, want none", got)
	}
}

func TestNoRecommendationsReaderScopesUserMetricsDailyByTeamID(t *testing.T) {
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	var names []string
	readers := 0
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(node ast.Node) bool {
			if lit, ok := node.(*ast.BasicLit); ok && lit.Kind == token.STRING && strings.Contains(lit.Value, "FROM user_metrics_daily") {
				readers++
			}
			return true
		})
		files = append(files, f)
		names = append(names, path)
	}
	if readers == 0 {
		t.Fatalf("the census found no read of user_metrics_daily in %d files: it did not measure the package", len(files))
	}
	values := umdPackageValues(files)
	var found []string
	for i, f := range files {
		for _, site := range umdTeamFilterSites(f, values) {
			found = append(found, names[i]+": "+site)
		}
	}
	if len(found) > 0 {
		t.Fatalf("user_metrics_daily is read with a team_id filter (a membership label, not the team's owned repositories; ticket 14163) in: %v", found)
	}
}

type giniStatementConn struct {
	driver.Conn
	statements []string
	arguments  [][]any
}

func (conn *giniStatementConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	conn.statements = append(conn.statements, query)
	conn.arguments = append(conn.arguments, args)
	return &emptyGiniRows{}, nil
}

type emptyGiniRows struct{ driver.Rows }

func (*emptyGiniRows) Next() bool   { return false }
func (*emptyGiniRows) Close() error { return nil }
func (*emptyGiniRows) Err() error   { return nil }

func TestRecommendationsReviewerGiniReadsTheOwnedRepositories(t *testing.T) {
	conn := &giniStatementConn{}
	loader := &RecommendationsLoader{conn: conn, orgID: "org-1"}
	owned := []uuid.UUID{uuid.MustParse("14163000-0000-0000-0000-00000000000a")}
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	if _, _, _, giniKnown, err := loader.loadReviewSignals(context.Background(), "team-a", owned, start, start.AddDate(0, 0, 14)); err != nil || giniKnown {
		t.Fatalf("loadReviewSignals: known=%v err=%v, want absent and no error", giniKnown, err)
	}
	umd := -1
	for i, statement := range conn.statements {
		if strings.Contains(statement, "FROM user_metrics_daily") {
			umd = i
		}
	}
	if umd < 0 {
		t.Fatalf("no user_metrics_daily statement in %d statements", len(conn.statements))
	}
	statement := conn.statements[umd]
	if strings.Contains(statement, "team_id") {
		t.Fatalf("the reviewer Gini statement filters team_id (the membership label):\n%s", statement)
	}
	if !strings.Contains(statement, "repo_id IN {repo_ids:Array(UUID)}") {
		t.Fatalf("the reviewer Gini statement has no owned-repository filter:\n%s", statement)
	}
	bound := false
	for _, argument := range conn.arguments[umd] {
		if named, ok := argument.(driver.NamedValue); ok && named.Name == "repo_ids" {
			ids, _ := named.Value.([]uuid.UUID)
			bound = len(ids) == 1 && ids[0] == owned[0]
		}
	}
	if !bound {
		t.Fatalf("the reviewer Gini statement does not bind repo_ids to the owned repositories: %v", conn.arguments[umd])
	}
}

func TestRecommendationsReviewerGiniOfATeamWithNoRepositorySendsNoQuery(t *testing.T) {
	conn := &giniStatementConn{}
	loader := &RecommendationsLoader{conn: conn, orgID: "org-1"}
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	_, _, _, giniKnown, err := loader.loadReviewSignals(context.Background(), "team-none", nil, start, start.AddDate(0, 0, 14))
	if err != nil || giniKnown {
		t.Fatalf("loadReviewSignals: known=%v err=%v, want absent and no error", giniKnown, err)
	}
	if len(conn.statements) != 0 {
		t.Fatalf("a team that owns no repository sent %d statement(s), want none:\n%s", len(conn.statements), strings.Join(conn.statements, "\n---\n"))
	}
}
