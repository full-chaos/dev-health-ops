package daily

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The census of the reads of the teams table in the daily job.
//
// No resolver of the daily metric families may resolve to an inactive team
// (package teamactive). A read of the teams table is where a resolver gets its
// teams, so every function of this package tree whose SQL reads `teams` must
// apply the rule: it calls teamactive.LoadInactive. The census finds the reads
// by parsing the source, not by a list of names, so a NEW read of the table
// with no rule fails here.
//
// The SQL text is read as the code builds it from string literals: a
// statement in one literal, a statement joined from literals with `+`, and
// the table name as a literal of its own (handed to a format or joined to a
// value) all count as a read.
//
// What the census does not hold: a resolver that takes a team id from another
// source (an ownership row, a stored row of an earlier day), and a statement
// whose table name comes from outside this package tree. Each resolver of the
// first kind has its own test.

// teamsReadPattern is a read of the teams table in SQL text.
var teamsReadPattern = regexp.MustCompile(`(?i)\b(from|join)\s+teams\b`)

// teamsReadsWithoutTheRule are the functions that read the teams table and
// need no active-team rule, each with the reason. Key: "file:function".
var teamsReadsWithoutTheRule = map[string]string{}

// readsTeamsText says that a text is SQL that reads the teams table, or is
// the name of the table by itself.
func readsTeamsText(text string) bool {
	return teamsReadPattern.MatchString(text) || strings.TrimSpace(text) == "teams"
}

// foldStringParts joins the string literals of a `+` expression in their
// order. A part that is not a literal is a gap the pattern cannot match over.
func foldStringParts(expression ast.Expr) string {
	switch typed := expression.(type) {
	case *ast.BinaryExpr:
		if typed.Op == token.ADD {
			return foldStringParts(typed.X) + foldStringParts(typed.Y)
		}
	case *ast.ParenExpr:
		return foldStringParts(typed.X)
	case *ast.BasicLit:
		if typed.Kind == token.STRING {
			if text, err := strconv.Unquote(typed.Value); err == nil {
				return text
			}
		}
	}
	return "\x00"
}

func TestTheCensusOfTeamsReadsSeesAStatementBuiltFromParts(t *testing.T) {
	for source, want := range map[string]bool{
		`"SELECT id FROM teams FINAL"`:                       true,
		`"SELECT id FROM " + "teams" + " FINAL WHERE x = ?"`: true,
		`"SELECT id FROM " + ("teams" + " FINAL")`:           true,
		`"SELECT id FROM " + name + " FINAL"`:                false,
		`"SELECT t.id FROM repos r LEFT JOIN teams AS t"`:    true,
		`"SELECT id FROM team_repo_ownership"`:               false,
		`"SELECT id FROM " + "team" + "s_archive"`:           false,
		`"SELECT id FROM teams_archive"`:                     false,
	} {
		expression, err := parser.ParseExpr(source)
		if err != nil {
			t.Fatalf("parse %s: %v", source, err)
		}
		if got := readsTeamsText(foldStringParts(expression)); got != want {
			t.Errorf("%s: read of teams = %v, want %v", source, got, want)
		}
	}
	if !readsTeamsText(" teams ") || readsTeamsText("teams_archive") {
		t.Errorf("the table name by itself must count, a longer name must not")
	}
}

func TestEveryReadOfTeamsInTheDailyJobAppliesTheActiveTeamRuleCensus(t *testing.T) {
	fileSet := token.NewFileSet()
	type function struct {
		key          string
		readsTeams   bool
		appliesRule  bool
		usedConstant []string
	}
	var functions []*function
	sqlConstants := map[string]bool{}
	parsed := 0
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fileSet, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		parsed++
		literalReadsTeams := func(node ast.Node) bool {
			found := false
			ast.Inspect(node, func(inner ast.Node) bool {
				switch typed := inner.(type) {
				case *ast.BinaryExpr:
					// A statement joined from parts: read it as one text.
					if typed.Op == token.ADD {
						if readsTeamsText(foldStringParts(typed)) {
							found = true
						}
					}
				case *ast.BasicLit:
					if typed.Kind != token.STRING {
						return true
					}
					if text, err := strconv.Unquote(typed.Value); err == nil && readsTeamsText(text) {
						found = true
					}
				}
				return true
			})
			return found
		}
		for _, declaration := range file.Decls {
			switch typed := declaration.(type) {
			case *ast.GenDecl:
				// SQL text held in a package-level constant or variable.
				for _, spec := range typed.Specs {
					value, isValue := spec.(*ast.ValueSpec)
					if !isValue || !literalReadsTeams(value) {
						continue
					}
					for _, name := range value.Names {
						sqlConstants[name.Name] = true
					}
				}
			case *ast.FuncDecl:
				if typed.Body == nil {
					continue
				}
				entry := &function{key: filepath.ToSlash(path) + ":" + typed.Name.Name, readsTeams: literalReadsTeams(typed.Body)}
				ast.Inspect(typed.Body, func(inner ast.Node) bool {
					switch node := inner.(type) {
					case *ast.Ident:
						entry.usedConstant = append(entry.usedConstant, node.Name)
					case *ast.CallExpr:
						if selector, isSelector := node.Fun.(*ast.SelectorExpr); isSelector && selector.Sel.Name == "LoadInactive" {
							if pkg, isIdent := selector.X.(*ast.Ident); isIdent && pkg.Name == "teamactive" {
								entry.appliesRule = true
							}
						}
					}
					return true
				})
				functions = append(functions, entry)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("parse the daily job: %v", err)
	}
	var readers []string
	for _, entry := range functions {
		if !entry.readsTeams {
			for _, name := range entry.usedConstant {
				if sqlConstants[name] {
					entry.readsTeams = true
				}
			}
		}
		if !entry.readsTeams {
			continue
		}
		readers = append(readers, entry.key)
		reason, exempt := teamsReadsWithoutTheRule[entry.key]
		switch {
		case exempt && entry.appliesRule:
			t.Errorf("%s applies the active-team rule and is listed as a read that needs none", entry.key)
		case exempt && len(strings.TrimSpace(reason)) < 40:
			t.Errorf("%s is listed as a read of teams that needs no rule, with no written reason", entry.key)
		case !exempt && !entry.appliesRule:
			t.Errorf("%s reads the teams table and does not apply the active-team rule: call teamactive.LoadInactive and drop "+
				"the inactive teams (teamactive.Keep), or list the function in teamsReadsWithoutTheRule with the reason", entry.key)
		}
	}
	sort.Strings(readers)
	// The two readers of today must be found: a census that finds none
	// measured nothing.
	if parsed < 60 || len(readers) < 2 ||
		!contains(readers, "wellbeing_native_clickhouse.go:LoadWellbeingTeams") ||
		!contains(readers, "ai_impact_native_clickhouse.go:LoadAIImpactTeams") {
		t.Fatalf("parsed %d file(s) and found the readers %v: the census did not measure", parsed, readers)
	}
	for key := range teamsReadsWithoutTheRule {
		if !contains(readers, key) {
			t.Errorf("%s is listed as a read of teams and the parse does not find it", key)
		}
	}
}
