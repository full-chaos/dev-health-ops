package main

import (
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// CHAOS-4292 rebase-gate finding (codex, 2026-09-01): the two drift checks
// this metrics.daily cutover wave relied on -- families_test.go's
// families.json validation and internal/jobruntime's
// TestDailyMetricsNativeFamiliesCoverEveryPortedFamily -- both read a
// SOURCE OF TRUTH (families.json, or telemetry.go's own allowlist), never
// the ACTUAL dispatch registration in buildDailyWorker
// (cmd/dev-health-worker/daily.go). Codex proved this by deleting each of
// the four newest `nativeFamilies["X"] = ...` /
// `postBridgeFamilies["X"] = ...` assignments in a disposable worktree:
// both existing tests stayed green. For `incident` specifically this is
// data-affecting, not cosmetic: with no native registration, the partition
// falls through to the Python compatibility bridge, whose
// `active_incidents_query` predicate has no NULL-OK guard and is
// PERMANENTLY ZERO-YIELD (CHAOS-4269) -- silently, since construction
// success means no refusal log and no native-family telemetry either.
//
// This test closes that gap by reading the ACTUAL registration: it parses
// buildDailyWorker's source with go/ast (same pattern
// cmd/dev-health-reconciler/pool_composition_test.go already uses for an
// analogous "the pin must match the wiring" check) and asserts SET
// EQUALITY, in both directions, against families.json's own `"port":"go"`
// set. A family registered here but not `"go"` in families.json is caught
// exactly like a family that's `"go"` but never registered here -- neither
// half of this pair can drift without the other noticing.
func TestDailyWorkerNativeFamilyRegistrationMatchesFamiliesJSONPortGo(t *testing.T) {
	registered := parseRegisteredDailyFamilies(t)
	goFamilies := readFamiliesJSONPortGoSet(t)

	var missingFromRegistration []string
	for family := range goFamilies {
		if !registered[family] {
			missingFromRegistration = append(missingFromRegistration, family)
		}
	}
	sort.Strings(missingFromRegistration)
	if len(missingFromRegistration) > 0 {
		t.Errorf(
			"families.json marks %v as port=\"go\" but buildDailyWorker "+
				"(cmd/dev-health-worker/daily.go) never assigns nativeFamilies[...] "+
				"or postBridgeFamilies[...] for them -- every partition for this "+
				"family silently falls through to the Python compatibility bridge "+
				"with no refusal log and no native-family telemetry",
			missingFromRegistration,
		)
	}

	var registeredButNotGo []string
	for family := range registered {
		if !goFamilies[family] {
			registeredButNotGo = append(registeredButNotGo, family)
		}
	}
	sort.Strings(registeredButNotGo)
	if len(registeredButNotGo) > 0 {
		t.Errorf(
			"buildDailyWorker registers %v via nativeFamilies[...]/"+
				"postBridgeFamilies[...] but families.json does not mark them "+
				"port=\"go\" -- either families.json is stale (a family's cutover "+
				"flag was reverted or never flipped) or the registration is dead "+
				"code; both call sites must agree",
			registeredButNotGo,
		)
	}
}

// parseRegisteredDailyFamilies returns the set of family names actually
// assigned via `nativeFamilies["<name>"] = ...` or
// `postBridgeFamilies["<name>"] = ...` inside buildDailyWorker's source, by
// parsing daily.go with go/ast rather than importing/running it (the real
// function dials live ClickHouse/Postgres and cannot run as a fast unit
// test without production-only dependency injection this PR does not add).
func parseRegisteredDailyFamilies(t *testing.T) map[string]bool {
	t.Helper()
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, filepath.Join(".", "daily.go"), nil, 0)
	if err != nil {
		t.Fatalf("parse daily.go: %v", err)
	}

	const targetFunc = "buildDailyWorker"
	registrationMaps := map[string]bool{"nativeFamilies": true, "postBridgeFamilies": true}
	registered := map[string]bool{}
	found := false

	for _, decl := range parsed.Decls {
		function, isFunc := decl.(*ast.FuncDecl)
		if !isFunc || function.Name.Name != targetFunc {
			continue
		}
		found = true
		ast.Inspect(function.Body, func(node ast.Node) bool {
			assign, isAssign := node.(*ast.AssignStmt)
			if !isAssign {
				return true
			}
			for _, lhs := range assign.Lhs {
				index, isIndex := lhs.(*ast.IndexExpr)
				if !isIndex {
					continue
				}
				mapIdent, isIdent := index.X.(*ast.Ident)
				if !isIdent || !registrationMaps[mapIdent.Name] {
					continue
				}
				keyLit, isLit := index.Index.(*ast.BasicLit)
				if !isLit || keyLit.Kind != token.STRING {
					continue
				}
				familyName, unquoteErr := unquoteGoStringLiteral(keyLit.Value)
				if unquoteErr != nil {
					t.Fatalf("daily.go: unparseable string literal key %s: %v", keyLit.Value, unquoteErr)
				}
				registered[familyName] = true
			}
			return true
		})
	}
	if !found {
		t.Fatalf("%s not found in daily.go", targetFunc)
	}
	if len(registered) == 0 {
		t.Fatalf("%s: parsed zero nativeFamilies/postBridgeFamilies assignments -- "+
			"the AST walk itself is broken, not just missing an entry", targetFunc)
	}
	return registered
}

// unquoteGoStringLiteral strips the surrounding double quotes go/ast leaves
// on a *ast.BasicLit's Value for a plain (non-raw) Go string literal. Every
// key in nativeFamilies/postBridgeFamilies is a simple identifier-shaped
// family name (no escapes), so this is deliberately minimal rather than a
// full strconv.Unquote.
func unquoteGoStringLiteral(literal string) (string, error) {
	if len(literal) < 2 || literal[0] != '"' || literal[len(literal)-1] != '"' {
		return "", errNotAPlainStringLiteral
	}
	return literal[1 : len(literal)-1], nil
}

var errNotAPlainStringLiteral = errors.New("not a plain double-quoted string literal")

// readFamiliesJSONPortGoSet reads the drift-gated families.json (the same
// file families_test.go validates) and returns the set of family names
// currently marked "port":"go".
func readFamiliesJSONPortGoSet(t *testing.T) map[string]bool {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "internal", "jobs", "metrics", "daily", "families.json"))
	if err != nil {
		t.Fatalf("read families.json: %v", err)
	}
	var registry struct {
		Families []struct {
			Name string `json:"name"`
			Port string `json:"port"`
		} `json:"families"`
	}
	if err := json.Unmarshal(data, &registry); err != nil {
		t.Fatalf("decode families.json: %v", err)
	}
	goFamilies := make(map[string]bool, len(registry.Families))
	for _, family := range registry.Families {
		if family.Port == "go" {
			goFamilies[family.Name] = true
		}
	}
	return goFamilies
}
