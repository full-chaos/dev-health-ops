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
// What the census does not hold: a resolver that takes a team id from another
// source (an ownership row, a stored row of an earlier day). Each of those has
// its own test.

// teamsReadPattern is a read of the teams table in SQL text.
var teamsReadPattern = regexp.MustCompile(`(?i)\b(from|join)\s+teams\b`)

// teamsReadsWithoutTheRule are the functions that read the teams table and
// need no active-team rule, each with the reason. Key: "file:function".
var teamsReadsWithoutTheRule = map[string]string{}

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
				literal, isLiteral := inner.(*ast.BasicLit)
				if !isLiteral || literal.Kind != token.STRING {
					return true
				}
				if text, err := strconv.Unquote(literal.Value); err == nil && teamsReadPattern.MatchString(text) {
					found = true
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
