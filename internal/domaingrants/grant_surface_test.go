package domaingrants

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// findModuleRoot walks up from the current package directory to the nearest
// go.mod, so this test works whether `go test` is invoked from the repo
// root or the package directory (both are used across this repo's CI and
// local workflows).
func findModuleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find go.mod walking up from %s", dir)
		}
		dir = parent
	}
}

// TestDomainGrantSurfaceMatchesQuerySurface is the CI gate for CHAOS-3033's
// grant-surface-deriver follow-up: it statically derives, from the actual
// Go query surface reachable through the Postgres domain connection pool,
// which (table, privilege) pairs the domain role needs, and fails with a
// precise diff when that derived surface disagrees with EITHER
// runtimeGrantStatements (internal/storage/river/migrate.go) or
// required_table_privileges (internal/storage/postgres/domain_authorization.go)
// -- or when those two hand-maintained lists disagree with each other.
//
// This exists because migrate.go's grants and domain_authorization.go's
// readiness assertion were each written by restating the other, so they
// always agree with each other and had repeatedly drifted from what the Go
// code actually executes (CHAOS-3099, CHAOS-3100, CHAOS-3101). See
// /Users/chris/projects/full-chaos/dev-health/.remember/grant-surface-derivation.md
// for the findings this test produced on origin/main, and the handoff
// README for design rationale and known limitations of the static analysis
// (dynamic SQL, interface dispatch, etc -- ADVISORY findings below cover
// what the tool could NOT prove one way or the other).
//
// Only CRITICAL findings fail this test. ADVISORY findings (a granted
// privilege with no derived evidence -- which may be a legitimate over-grant
// or simply outside this analyzer's static reach) are logged, never
// silently dropped, never fatal: see compare.go's Severity doc comment.
func TestDomainGrantSurfaceMatchesQuerySurface(t *testing.T) {
	root := findModuleRoot(t)

	derived, err := Derive(root)
	if err != nil {
		t.Fatalf("Derive: %v", err)
	}
	gt, err := LoadGroundTruth()
	if err != nil {
		t.Fatalf("LoadGroundTruth: %v", err)
	}
	report := Compare(derived, gt)

	t.Logf("derived %d tables from the domain-pool query surface (%d dynamic SQL sites, %d unresolved call sites, %d devirtualized)",
		report.DerivedTableCount, report.DynamicSiteCount, report.UnresolvedCount, report.DevirtualizedCount)

	var critical, advisory []Finding
	for _, f := range report.Findings {
		if f.Severity == Critical {
			critical = append(critical, f)
		} else {
			advisory = append(advisory, f)
		}
	}

	for _, f := range advisory {
		t.Logf("[ADVISORY] %s", f.Summary)
	}

	if len(critical) == 0 {
		return
	}

	var b strings.Builder
	fmt.Fprintf(&b, "\n%d CRITICAL grant-surface disagreement(s) between the derived query surface and the hand-maintained privilege lists:\n\n", len(critical))
	for i, f := range critical {
		fmt.Fprintf(&b, "%d) %s\n", i+1, f.Summary)
		for _, e := range f.Evidence {
			fmt.Fprintf(&b, "     evidence: %s:%d  %s\n", e.File, e.Line, e.Statement)
		}
	}
	fmt.Fprintf(&b, "\nFix: add matching rows to BOTH internal/storage/river/migrate.go's runtimeGrantStatements "+
		"AND internal/storage/postgres/domain_authorization.go's required_table_privileges in the same commit "+
		"(the Option A posture ruling from CHAOS-3033) -- never one without the other, or CheckDomainAuthorization "+
		"will fail closed for every domain worker, not just the one that needed the new grant.\n")
	t.Fatal(b.String())
}
