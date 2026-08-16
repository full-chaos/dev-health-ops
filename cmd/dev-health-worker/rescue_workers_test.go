package main

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/riverqueue/river"
)

func TestRegisterRescueCoverageAddsCoordinatorKindsToPartialClient(t *testing.T) {
	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	workers := river.NewWorkers()
	providerWorker := river.WorkFunc[jobruntime.ProviderUnitArgs](
		func(context.Context, *river.Job[jobruntime.ProviderUnitArgs]) error { return nil },
	)
	if err := river.AddWorkerSafely(workers, providerWorker); err != nil {
		t.Fatal(err)
	}
	if err := registerRescueCoverage(
		workers,
		registry,
		[]jobruntime.HandlerSpec{{Kind: jobruntime.ProviderUnitArgs{}.Kind()}},
	); err != nil {
		t.Fatal(err)
	}
	if err := river.AddWorkerSafely(
		workers,
		river.WorkFunc[syncdispatchruntime.DispatchSyncRunArgs](
			func(context.Context, *river.Job[syncdispatchruntime.DispatchSyncRunArgs]) error { return nil },
		),
	); err == nil {
		t.Fatal("dispatch kind was absent from partial client's rescue registry")
	}
}

func TestEveryWorkerFamilyRegistersGlobalRescueCoverageOnSharedWorkers(t *testing.T) {
	for _, path := range []string{
		"daily.go",
		"operational.go",
		"provider_sync.go",
		"reports.go",
		"sync_dispatch.go",
		"workgraph.go",
	} {
		path := path
		t.Run(path, func(t *testing.T) {
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			var clients, rescueRegistrations int
			ast.Inspect(parsed, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch function := call.Fun.(type) {
				case *ast.SelectorExpr:
					packageName, packageOK := function.X.(*ast.Ident)
					if packageOK && packageName.Name == "river" && function.Sel.Name == "NewClient" {
						clients++
					}
				case *ast.Ident:
					if function.Name == "registerRescueCoverage" {
						if len(call.Args) < 2 || identName(call.Args[0]) != "workers" || identName(call.Args[1]) != "registry" {
							t.Fatalf("%s rescue coverage is not attached to its constructed workers and registry", path)
						}
						rescueRegistrations++
					}
				}
				return true
			})
			if clients != 0 || rescueRegistrations != 1 {
				t.Fatalf("%s family clients=%d rescue registrations=%d", path, clients, rescueRegistrations)
			}
		})
	}
}

func TestWorkerProcessConstructsTheOnlyRiverClient(t *testing.T) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "river_process.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	clients := 0
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		function, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		packageName, ok := function.X.(*ast.Ident)
		if ok && packageName.Name == "river" && function.Sel.Name == "NewClient" {
			clients++
		}
		return true
	})
	if clients != 1 {
		t.Fatalf("process-level River clients = %d, want exactly one", clients)
	}
}

func identName(expression ast.Expr) string {
	identifier, _ := expression.(*ast.Ident)
	if identifier == nil {
		return ""
	}
	return identifier.Name
}
