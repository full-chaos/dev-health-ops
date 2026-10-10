package teamactive

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
)

// The `members` column of the teams table is a roster copy of the team sync
// (CHAOS-9087): no validity window, no source, no second team for a person. A
// person's team comes from team_memberships. No Go source may read it or write
// it. The census parses every non-test Go file under internal/ and cmd/, joins
// the string literals of each concatenation, and fails for a statement that
// names the table `teams` and the bare identifier `members`
// (`manual_members`, `members_json` and a table named `members` are other
// things).

var (
	teamsTable = regexp.MustCompile("\\b(from|join|into|update|table)\\s+`?teams\\b")
	// a table named members (the Linear members dimension), not a column.
	membersTable = regexp.MustCompile("\\b(from|join|into|update|table)\\s+`?members\\b")
	membersWord  = regexp.MustCompile("(^|[^a-z0-9_])`?members`?([^a-z0-9_]|$)")
)

// readsOrWritesTeamsRoster says whether one SQL text names the roster column
// of the teams table.
func readsOrWritesTeamsRoster(sql string) bool {
	text := strings.ToLower(strings.Join(strings.Fields(sql), " "))
	text = strings.ReplaceAll(text, "array join", "array_join")
	if !teamsTable.MatchString(text) {
		return false
	}
	text = membersTable.ReplaceAllString(text, "$1 other_table")
	return membersWord.MatchString(text)
}

// statementsOf returns the SQL-like texts of one Go source: each string literal
// that is not an operand of a `+`, and each outermost `+` chain of literals
// joined in order.
func statementsOf(file *ast.File) []string {
	var out []string
	var visit func(node ast.Node) bool
	chainOperands := func(expr ast.Expr) (parts []string, ok bool) {
		var walk func(e ast.Expr) bool
		walk = func(e ast.Expr) bool {
			switch typed := e.(type) {
			case *ast.BasicLit:
				if typed.Kind != token.STRING {
					return false
				}
				value, err := strconv.Unquote(typed.Value)
				if err != nil {
					return false
				}
				parts = append(parts, value)
				return true
			case *ast.BinaryExpr:
				if typed.Op != token.ADD {
					return false
				}
				// A non-literal operand (a constant, a call) is skipped: the
				// literals around it still join in order.
				walk(typed.X)
				walk(typed.Y)
				return true
			case *ast.ParenExpr:
				return walk(typed.X)
			default:
				return false
			}
		}
		walk(expr)
		return parts, len(parts) > 0
	}
	visit = func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.BinaryExpr:
			if typed.Op == token.ADD {
				if parts, ok := chainOperands(typed); ok {
					out = append(out, strings.Join(parts, " "))
				}
				return false
			}
		case *ast.BasicLit:
			if typed.Kind == token.STRING {
				if value, err := strconv.Unquote(typed.Value); err == nil {
					out = append(out, value)
				}
			}
		}
		return true
	}
	ast.Inspect(file, visit)
	return out
}

func TestRosterCensusRecognisesTheColumn(t *testing.T) {
	reads := map[string]string{
		"select list":      "SELECT id, name, members, manual_members FROM teams FINAL WHERE org_id = ?",
		"only column":      "SELECT members FROM teams",
		"multi line":       "SELECT id,\n       members\nFROM teams\nWHERE org_id = ?",
		"insert list":      "INSERT INTO teams (id, team_uuid, name, description, members, manual_members, project_keys)",
		"alias":            "SELECT t.members FROM teams AS t FINAL",
		"quoted":           "SELECT `members` FROM teams",
		"join side":        "SELECT g.team_id FROM g LEFT JOIN (SELECT id, members FROM teams) AS t ON t.id = g.team_id",
		"upper case":       "SELECT ID, MEMBERS FROM TEAMS",
		"not empty":        "SELECT count() FROM teams FINAL WHERE notEmpty(members)",
		"array join":       "SELECT id FROM teams ARRAY JOIN members AS m",
		"insert with form": "INSERT INTO teams (id,team_uuid,name,description,members,manual_members) VALUES (?,?,?,?,?,?)",
	}
	for name, sql := range reads {
		if !readsOrWritesTeamsRoster(sql) {
			t.Errorf("%s: %q is not recognised as a use of the teams roster column", name, sql)
		}
	}
	clean := map[string]string{
		"manual members":       "SELECT id, name, manual_members FROM teams FINAL WHERE org_id = ?",
		"not empty manual":     "SELECT count() FROM teams FINAL WHERE notEmpty(manual_members)",
		"observation json":     "SELECT members_json FROM team_provider_observations FINAL",
		"memberships table":    "SELECT member_id FROM team_memberships FINAL",
		"the members table":    "INSERT INTO members (org_id, member_id, name) VALUES (?, ?, ?)",
		"members table select": "SELECT member_id FROM members FINAL WHERE org_id = ?",
		"teams without roster": "SELECT id, name, repo_patterns FROM teams FINAL",
		"prose":                "no teams here, only members",
		"team memberships col": "SELECT t.id FROM teams AS t INNER JOIN team_memberships AS m ON m.team_id = t.id",
	}
	for name, sql := range clean {
		if readsOrWritesTeamsRoster(sql) {
			t.Errorf("%s: %q is wrongly recognised as a use of the teams roster column", name, sql)
		}
	}
}

func TestStatementsOfJoinsAConcatenation(t *testing.T) {
	source := "package p\n" +
		"const a = \"SELECT id, \" + \"members \" + \"FROM teams\"\n" +
		"const b = `SELECT id FROM teams`\n" +
		"var c = \"SELECT x FROM teams WHERE y IN (\" + other + \")\"\n"
	file, err := parser.ParseFile(token.NewFileSet(), "p.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	statements := statementsOf(file)
	var roster, plain int
	for _, statement := range statements {
		if readsOrWritesTeamsRoster(statement) {
			roster++
		}
		if teamsTable.MatchString(strings.ToLower(statement)) {
			plain++
		}
	}
	if roster != 1 || plain != 3 {
		t.Fatalf("statements = %q: %d name the roster, %d name the teams table, want 1 and 3", statements, roster, plain)
	}
}

// rosterColumnReaders are the files that may name the column, each with the
// reason. A stale entry (a file that no longer names it) fails the census.
var rosterColumnReaders = map[string]string{
	"../providersync/team_roster_move.go": "the one-time step that moves the roster entries of admin-made teams into team_memberships before the column is dropped",
}

func TestNoGoSourceReadsOrWritesTheTeamsRosterColumn(t *testing.T) {
	reached := map[string]bool{}
	roots := []string{"..", filepath.Join("..", "..", "cmd")}
	scannedFiles, teamStatements := 0, 0
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" || entry.Name() == "vendor" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
			if err != nil {
				return err
			}
			scannedFiles++
			for _, statement := range statementsOf(file) {
				if teamsTable.MatchString(strings.ToLower(strings.Join(strings.Fields(statement), " "))) {
					teamStatements++
				}
				if readsOrWritesTeamsRoster(statement) {
					if _, allowed := rosterColumnReaders[filepath.ToSlash(path)]; allowed {
						reached[filepath.ToSlash(path)] = true
						continue
					}
					t.Errorf("%s names the roster column of the teams table (CHAOS-9087): %s", path, strings.Join(strings.Fields(statement), " "))
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	for path, reason := range rosterColumnReaders {
		if !reached[path] {
			t.Errorf("%s is allowed to name the roster column (%s) and does not: remove the entry", path, reason)
		}
	}
	// A census that read nothing must not pass.
	if scannedFiles < 500 || teamStatements < 20 {
		t.Fatalf("the census read %d Go files and %d statements on the teams table: it did not measure the tree", scannedFiles, teamStatements)
	}
}
