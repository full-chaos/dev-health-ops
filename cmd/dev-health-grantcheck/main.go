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
// It also emits the COORDINATOR role's ADVISORY report (-roles), which exists
// because that report has no other delivery channel. The coordinator analysis is
// advisory (see internal/domaingrants.AdvisoryReport), so it is delivered through
// t.Log -- and ci/check_go.sh runs `go test -mod=readonly ./...` WITHOUT -v, which
// suppresses a passing test's logs entirely. In CI the coordinator report was
// therefore invisible: consumers saw a zero exit status and a package-level "ok"
// and nothing else, which is precisely the "advisory output read as a pass" failure
// the advisory posture was supposed to make impossible. A report whose only channel
// is suppressed output is not a report.
//
// Usage:
//
//	go run ./cmd/dev-health-grantcheck [-root <module dir>] [-format text|markdown]
//	go run ./cmd/dev-health-grantcheck -roles [-root <module dir>]
//
// Exit status for the DOMAIN check is 1 if any CRITICAL finding is present, 0
// otherwise -- the same pass/fail contract as the gating test.
//
// Exit status for -roles is ALWAYS 0, including when it reports CRITICAL findings.
// That is deliberate and is the whole point: the coordinator surface gates nothing,
// so a nonzero exit would make it a gate by the back door. Read the report; do not
// read the exit code.
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
	roles := flag.Bool("roles", false,
		"emit the per-role ADVISORY report (coordinator + domain) instead of the domain gate check; "+
			"always exits 0, because this surface gates nothing")
	flag.Parse()

	absRoot, err := absPath(*root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dev-health-grantcheck: %v\n", err)
		os.Exit(2)
	}

	if *roles {
		if err := printRoleAdvisoryReport(absRoot); err != nil {
			fmt.Fprintf(os.Stderr, "dev-health-grantcheck: %v\n", err)
			os.Exit(2)
		}
		// Deliberately exit 0 even with CRITICAL findings present: see the package
		// doc comment. A nonzero exit here would silently turn an advisory report
		// into a gate, which is the decision this posture exists to avoid.
		return
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

	if len(derived.UnresolvedTx) > 0 {
		fmt.Printf("\nUnresolved cross-function transactions (co-residency NOT verified -- a pgx.Tx parameter whose origin Begin() call could not be traced unambiguously; any transaction-consistency finding touching these is incomplete, not clean):\n")
		for _, u := range derived.UnresolvedTx {
			fmt.Printf("  %s:%d  in %s\n", u.File, u.Line, u.Function)
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

// printRoleAdvisoryReport writes the full per-role advisory report to stdout.
//
// This is the coordinator report's ONLY non-suppressed delivery channel. The test
// that produces the same content does so through t.Log, which `go test` discards
// for a passing package unless -v is passed -- and CI does not pass it.
func printRoleAdvisoryReport(root string) error {
	inputs := make([]domaingrants.RoleInput, 0, len(domaingrants.AllPoolRoles))
	for _, role := range domaingrants.AllPoolRoles {
		derived, err := domaingrants.DeriveForRole(root, role)
		if err != nil {
			return fmt.Errorf("deriving %s surface: %w", role, err)
		}
		truth, err := domaingrants.LoadGroundTruthForRole(role)
		if err != nil {
			return fmt.Errorf("loading %s ground truth: %w", role, err)
		}
		inputs = append(inputs, domaingrants.RoleInput{Role: role, Derived: derived, Truth: truth})
	}
	report, err := domaingrants.CompareRoles(inputs)
	if err != nil {
		return fmt.Errorf("comparing roles: %w", err)
	}

	const banner = "ADVISORY REPORT -- THIS GATES NOTHING."
	fmt.Println("=====================================================================")
	fmt.Println(banner)
	fmt.Println("Everything below is REPORTED, not asserted. A clean run is NOT evidence")
	fmt.Println("that CoordinatorPosture() is correct, and this command exits 0 even when")
	fmt.Println("it reports CRITICAL findings. The domain role is gated separately by")
	fmt.Println("TestDomainGrantSurfaceMatchesQuerySurface. Promoting this to a gate")
	fmt.Println("requires the blind-spot closure argument tracked in CHAOS-3164.")
	fmt.Println("=====================================================================")

	byCategory := map[domaingrants.AdvisoryCategory][]string{}
	for _, line := range domaingrants.AdvisoryReport(report) {
		byCategory[line.Category] = append(byCategory[line.Category], line.Text)
	}
	for _, category := range domaingrants.AllAdvisoryCategories {
		entries := byCategory[category]
		fmt.Printf("\n=== %s (%d) ===\n", category, len(entries))
		for _, entry := range entries {
			fmt.Printf("    %s\n", entry)
		}
	}
	fmt.Println()
	fmt.Println("=====================================================================")
	fmt.Println(banner + " Exit status is 0 regardless of the above.")
	fmt.Println("=====================================================================")
	return nil
}
