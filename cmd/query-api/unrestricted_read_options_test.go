package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestNewUnrestrictedReadClickHouseOptions_SetsUnrestrictedMaxBytesToRead
// is the direct, no-ClickHouse-container unit proof of
// newUnrestrictedReadClickHouseOptions's one contract: MaxBytesToRead is a
// non-nil pointer to literal 0, never left nil. dev-health-go/clickhouse
// treats nil as "unset, use the 64 MiB default" (CHAOS-4647) -- a
// deleted/zero-valued field lands on nil, not on unlimited, so this test
// checks the pointer itself, not just its dereferenced value.
//
// query_route_integration_test.go's tip_config_sends_unrestricted_max_bytes_to_read
// covers the same contract end-to-end against a real ClickHouse container
// (reading system.settings back through the real driver); this test is
// the fast, container-free half that runs in `go test ./cmd/query-api/...`
// without testcontainers.
func TestNewUnrestrictedReadClickHouseOptions_SetsUnrestrictedMaxBytesToRead(t *testing.T) {
	opts := newUnrestrictedReadClickHouseOptions("clickhouse://irrelevant-dsn")

	if opts.MaxBytesToRead == nil {
		t.Fatalf("MaxBytesToRead is nil -- dev-health-go/clickhouse treats nil as \"unset, use the 64 MiB default\" (CHAOS-4647), not unlimited")
	}
	if *opts.MaxBytesToRead != 0 {
		t.Fatalf("MaxBytesToRead = %d, want 0 (ClickHouse's own \"unrestricted\")", *opts.MaxBytesToRead)
	}
	if opts.DSN != "clickhouse://irrelevant-dsn" {
		t.Fatalf("DSN = %q, want the dsn argument passed through unchanged", opts.DSN)
	}
}

// TestEveryClickHouseReadClientUsesTheSharedUnrestrictedOptions is a
// totality guard, same method as registered_document_field_gate_test.go
// and route_wiring_comment_drift_test.go elsewhere in this package: it
// derives its answer from the actual source on every run -- walking every
// non-test .go file directly under cmd/query-api for a call to
// dhclickhouse.NewClickHouseQueryClientWithOptions, then checking that the
// enclosing function's own source also references
// newUnrestrictedReadClickHouseOptions -- rather than a hand-maintained
// list of the two known call sites (query_route.go's
// newQueryRouteClickHouseClient, investment_explain_route.go's
// buildInvestmentExplainRoute). A THIRD call site added later, that
// builds its own dhclickhouse.Options{DSN: ...} literal instead of
// layering onto the shared helper, fails this gate the moment it is
// written, instead of waiting for its own prod incident the way
// investment/explain's did (CHAOS-5606: the earlier CHAOS-4647
// row-iteration defect recurring in a second route because the fix lived
// on one call site, not the class).
//
// Scope: only cmd/query-api's own package-main files, not its _test.go
// files and not its internal/* subpackages. _test.go files are
// deliberately excluded: several existing subtests (e.g.
// query_route_integration_test.go's parent_defaults_fail_on_real_volume)
// construct a client with the OLD, unrestricted-less options ON PURPOSE,
// to prove what the bug looked like -- a totality gate over test files
// would fail on those by design, not by defect. internal/* subpackages
// are excluded because newUnrestrictedReadClickHouseOptions is unexported
// to package main; grep -rn NewClickHouseQueryClientWithOptions cmd
// internal (see this PR's own investigation) shows zero non-test call
// sites outside package main today.
func TestEveryClickHouseReadClientUsesTheSharedUnrestrictedOptions(t *testing.T) {
	const dir = "."
	const sharedHelperName = "newUnrestrictedReadClickHouseOptions"
	const targetCall = "NewClickHouseQueryClientWithOptions"

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}

	var violations []string
	sawAnyCallSite := false

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		src, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("read %s: %v", path, readErr)
		}

		fset := token.NewFileSet()
		// Deliberately NOT parser.ParseComments: comments must never
		// satisfy this gate (a doc comment that merely NAMES the helper,
		// next to a call site that does not actually use it, must still
		// fail) -- checking identifiers in the parsed AST, rather than
		// grepping the enclosing function's raw source text, is what
		// keeps a comment from vacuously passing this guard.
		file, parseErr := parser.ParseFile(fset, path, src, 0)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", path, parseErr)
		}

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}

			callsTarget := false
			usesSharedHelper := false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch v := n.(type) {
				case *ast.CallExpr:
					if sel, ok := v.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == targetCall {
						callsTarget = true
					}
				case *ast.Ident:
					if v.Name == sharedHelperName {
						usesSharedHelper = true
					}
				}
				return true
			})
			if !callsTarget {
				continue
			}
			sawAnyCallSite = true

			if !usesSharedHelper {
				violations = append(violations, fmt.Sprintf(
					"%s:%d: func %s calls %s without an %s(...) identifier reference in its body",
					path, fset.Position(fn.Pos()).Line, fn.Name.Name, targetCall, sharedHelperName,
				))
			}
		}
	}

	if !sawAnyCallSite {
		t.Fatalf("found zero %s call sites under cmd/query-api's package-main files -- this guard's premise (there are read clients to check) no longer holds; investigate before trusting a green result", targetCall)
	}

	sort.Strings(violations)
	if len(violations) > 0 {
		t.Fatalf("every query-api read client must build its options via %s (CHAOS-4647/CHAOS-5606 unrestricted-read posture); found %d call site(s) that do not:\n%s",
			sharedHelperName, len(violations), strings.Join(violations, "\n"))
	}
}
