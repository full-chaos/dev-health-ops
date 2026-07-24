// Command dev-health-grantcheck statically derives the Postgres domain-pool
// query surface and cross-checks it against the two hand-maintained
// privilege lists (internal/storage/river/migrate.go's
// runtimeGrantStatements and internal/storage/postgres/domain_authorization.go's
// required_table_privileges). It is the human-invocable counterpart to
// internal/domaingrants's TestDomainGrantSurfaceMatchesQuerySurface, which is
// what actually gates CI (via `go test ./...`) -- this binary exists so the
// same analysis can be re-run standalone to regenerate a report, without
// depending on `go test`'s output format.
//
// Usage:
//
//	go run ./cmd/dev-health-grantcheck [-root <module dir>] [-format text|markdown]
//
// Exit status is 1 if any CRITICAL finding is present, 0 otherwise
// (ADVISORY-only or clean). Same pass/fail contract as the test.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/domaingrants"
)

func main() {
	root := flag.String("root", ".", "module root directory (containing go.mod)")
	format := flag.String("format", "text", "output format: text or markdown")
	flag.Parse()

	absRoot, err := absPath(*root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dev-health-grantcheck: %v\n", err)
		os.Exit(2)
	}

	derived, err := domaingrants.Derive(absRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dev-health-grantcheck: deriving query surface: %v\n", err)
		os.Exit(2)
	}
	gt, err := domaingrants.LoadGroundTruth()
	if err != nil {
		fmt.Fprintf(os.Stderr, "dev-health-grantcheck: loading ground truth: %v\n", err)
		os.Exit(2)
	}
	report := domaingrants.Compare(derived, gt)

	switch *format {
	case "markdown":
		printMarkdown(report, derived)
	default:
		printText(report, derived)
	}

	if report.HasCritical() {
		os.Exit(1)
	}
}

func absPath(p string) (string, error) {
	if p == "" {
		p = "."
	}
	return filepath.Abs(p)
}

func printText(report *domaingrants.Report, derived *domaingrants.DerivedSurface) {
	fmt.Printf("Derived %d tables from the domain-pool query surface (%d dynamic SQL sites, %d unresolved call sites, %d devirtualized interface calls)\n\n",
		report.DerivedTableCount, report.DynamicSiteCount, report.UnresolvedCount, report.DevirtualizedCount)

	var critical, advisory int
	for _, f := range report.Findings {
		prefix := "ADVISORY"
		if f.Severity == domaingrants.Critical {
			prefix = "CRITICAL"
			critical++
		} else {
			advisory++
		}
		fmt.Printf("[%s] %s\n", prefix, f.Summary)
		for _, e := range f.Evidence {
			fmt.Printf("    evidence: %s:%d  %s\n", e.File, e.Line, e.Statement)
		}
	}
	fmt.Printf("\n%d critical, %d advisory\n", critical, advisory)

	if len(derived.Dynamic) > 0 {
		fmt.Printf("\nDynamic SQL sites (could not statically resolve which statement runs):\n")
		for _, d := range derived.Dynamic {
			fmt.Printf("  %s:%d  %s\n", d.File, d.Line, d.Reason)
		}
	}
}

func printMarkdown(report *domaingrants.Report, derived *domaingrants.DerivedSurface) {
	var b strings.Builder
	fmt.Fprintf(&b, "# Grant surface derivation report\n\n")
	fmt.Fprintf(&b, "Derived **%d** tables from the domain-pool query surface "+
		"(%d dynamic SQL sites, %d unresolved call sites, %d devirtualized interface calls).\n\n",
		report.DerivedTableCount, report.DynamicSiteCount, report.UnresolvedCount, report.DevirtualizedCount)

	fmt.Fprintf(&b, "## Findings\n\n| Severity | Table | Privilege | Summary |\n| --- | --- | --- | --- |\n")
	for _, f := range report.Findings {
		fmt.Fprintf(&b, "| %s | `%s` | %s | %s |\n", f.Severity, f.Table, privilegeLabel(f), f.Summary)
	}
	fmt.Println(b.String())
}

func privilegeLabel(f domaingrants.Finding) string {
	if f.Table == "" {
		return ""
	}
	return f.Privilege.String()
}
