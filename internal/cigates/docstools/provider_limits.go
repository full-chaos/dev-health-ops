package docstools

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// The provider limits page is the catalog the sync budget's route families are documented in; it must exist and be
// reachable from the mkdocs nav. Run from the repository root.
const providerLimitsPage = "docs/reference/limits-and-compatibility/provider-limits.md"

func init() {
	register("check-provider-limits", providerLimitsVerb)
}

// providerLimitsProblems is every way root breaks the contract; none means it holds.
func providerLimitsProblems(root string) []string {
	var bad []string
	if st, err := os.Stat(filepath.Join(root, filepath.FromSlash(providerLimitsPage))); err != nil || st.IsDir() {
		bad = append(bad, "missing canonical provider limits page "+providerLimitsPage)
	}
	nav, err := os.ReadFile(filepath.Join(root, "mkdocs.yml"))
	if err != nil || !strings.Contains(string(nav), strings.TrimPrefix(providerLimitsPage, "docs/")) {
		bad = append(bad, "provider-limits.md is not wired into the mkdocs.yml nav: the page would be unreachable")
	}
	return bad
}

func providerLimitsVerb(args []string, stdout, stderr io.Writer) int {
	if len(args) != 0 {
		fmt.Fprintln(stderr, "usage: cigate docs check-provider-limits")
		return 2
	}
	root, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(stderr, "check-provider-limits:", err)
		return 1
	}
	if bad := providerLimitsProblems(root); len(bad) > 0 {
		for _, b := range bad {
			fmt.Fprintln(stderr, b)
		}
		return 1
	}
	fmt.Fprintln(stdout, "Provider limits page check passed")
	return 0
}
