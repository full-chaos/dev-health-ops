package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"testing"
)

// TestNativeTeamCatalogCollectorsRegisterAllThreeProviders and
// TestResolveClientSwitchReachesAllThreeProviders pin the exact wiring
// CHAOS-4431/CHAOS-4434/CHAOS-4432 each independently added: the native
// team-catalog collector map in sync_dispatch.go and the credential-client
// switch in team_catalog_clients.go must both cover linear+github+gitlab,
// with no key silently dropped by a bad rebase/merge (this pair of files
// conflicted three times landing #1985 on top of #1989+#1984 -- see the
// merge history).
//
// Both nativeTeamCatalogCollectors and ResolveClient's switch require a live
// *pgxpool.Pool / ClickHouse connection to exercise end-to-end (resolveTeam-
// CatalogIntegration's very first statement is a real SQL query; there is no
// interface seam to fake pgxpool.Pool through), and this task is explicitly
// container-free. So this pins the wiring statically via go/parser instead
// of dynamically -- a source-level assertion, not a weaker one: it reads the
// exact same AST the Go compiler builds, so a missing/renamed key or case
// fails exactly as loudly as a runtime check would, without needing a
// database. Mirrors the same source-parsing technique
// tests/tooling/test_go_integration_sharding.py already uses on this repo's
// Python side for an analogous "did every provider register" question.

func parseCmdFile(t *testing.T, filename string) *ast.File {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", filename, err)
	}
	return file
}

func TestNativeTeamCatalogCollectorsRegisterAllThreeProviders(t *testing.T) {
	file := parseCmdFile(t, "sync_dispatch.go")

	var keys []string
	ast.Inspect(file, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, lhs := range assign.Lhs {
			ident, ok := lhs.(*ast.Ident)
			if !ok || ident.Name != "nativeTeamCatalogCollectors" {
				continue
			}
			if i >= len(assign.Rhs) {
				continue
			}
			composite, ok := assign.Rhs[i].(*ast.CompositeLit)
			if !ok {
				continue
			}
			for _, elt := range composite.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				lit, ok := kv.Key.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				// Strip the surrounding quotes BasicLit.Value carries.
				keys = append(keys, lit.Value[1:len(lit.Value)-1])
			}
		}
		return true
	})

	if keys == nil {
		t.Fatal("nativeTeamCatalogCollectors literal not found in sync_dispatch.go -- did the variable get renamed?")
	}
	sort.Strings(keys)
	want := []string{"github", "gitlab", "linear"}
	if len(keys) != len(want) {
		t.Fatalf("nativeTeamCatalogCollectors keys = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("nativeTeamCatalogCollectors keys = %v, want %v", keys, want)
		}
	}
}

func TestResolveClientSwitchReachesAllThreeProviders(t *testing.T) {
	file := parseCmdFile(t, "team_catalog_clients.go")

	var receiver string
	var cases []string
	ast.Inspect(file, func(node ast.Node) bool {
		fn, ok := node.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "ResolveClient" || fn.Recv == nil || len(fn.Recv.List) == 0 {
			return true
		}
		if ident, ok := fn.Recv.List[0].Type.(*ast.Ident); ok {
			receiver = ident.Name
		}
		ast.Inspect(fn.Body, func(inner ast.Node) bool {
			sw, ok := inner.(*ast.SwitchStmt)
			if !ok {
				return true
			}
			for _, stmt := range sw.Body.List {
				clause, ok := stmt.(*ast.CaseClause)
				if !ok {
					continue
				}
				for _, expr := range clause.List {
					lit, ok := expr.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					cases = append(cases, lit.Value[1:len(lit.Value)-1])
				}
			}
			return false
		})
		return false
	})

	if receiver != "teamCatalogClientResolver" {
		t.Fatalf("ResolveClient receiver = %q, want teamCatalogClientResolver -- did the method move?", receiver)
	}
	if cases == nil {
		t.Fatal("ResolveClient's provider switch not found in team_catalog_clients.go")
	}
	sort.Strings(cases)
	want := []string{"github", "gitlab", "linear"}
	if len(cases) != len(want) {
		t.Fatalf("ResolveClient switch cases = %v, want %v", cases, want)
	}
	for i := range want {
		if cases[i] != want[i] {
			t.Fatalf("ResolveClient switch cases = %v, want %v", cases, want)
		}
	}
}
