package docstools

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// check-candidate-facts: every key of the canonical investment taxonomy is named in docs/reference/taxonomies/investment.md
// and no dotted key the page names is outside it. Was scripts/check_docs_candidate_facts.py; the vocabulary now comes from
// the live Go registry (units.SortedThemes, units.SortedSubcategories). The key regex is invTxKeyRe, the same ASCII-only
// pattern as the Python KEY_RE.

func init() {
	register("check-candidate-facts", candFactsVerb)
}

func candFactsVerb(_ []string, stdout, stderr io.Writer) int {
	root, err := docsRoot(invTxDocRel)
	if err != nil {
		fmt.Fprintln(stderr, "check-candidate-facts:", err)
		return 1
	}
	page := filepath.Join(root, invTxDocRel)
	if info, statErr := os.Stat(page); statErr != nil || !info.Mode().IsRegular() {
		fmt.Fprintln(stdout, "ERROR: canonical taxonomy source or page is missing")
		return 1
	}
	text, err := a1ReadText(page)
	if err != nil {
		var de a1DecodeError
		if errors.As(err, &de) {
			return a1ReadFail(stderr, "check-candidate-facts:", err)
		}
		return a1Traceback(stderr, "OSError", err.Error())
	}
	themes := invTxSet(invTxThemes())
	expected := invTxSet(invTxSubcategories())
	for theme := range themes {
		expected[theme] = true
	}
	documented := map[string]bool{}
	for _, match := range invTxKeyRe.FindAllStringSubmatch(text, -1) {
		documented[match[1]] = true
	}
	var missing, unknown []string
	for key := range expected {
		if !documented[key] {
			missing = append(missing, key)
		}
	}
	for key := range documented {
		if !expected[key] && (strings.Contains(key, ".") || themes[key]) {
			unknown = append(unknown, key)
		}
	}
	sort.Strings(missing)
	sort.Strings(unknown)
	var errs []string
	if len(missing) > 0 {
		errs = append(errs, "missing canonical taxonomy keys: "+a1ListRepr(missing))
	}
	if len(unknown) > 0 {
		errs = append(errs, "unknown taxonomy keys: "+a1ListRepr(unknown))
	}
	if len(errs) > 0 {
		for _, e := range errs {
			fmt.Fprintf(stdout, "ERROR: %s\n", e)
		}
		return 1
	}
	fmt.Fprintf(stdout, "Canonical fact drift check passed for %d themes and %d subcategories\n", len(themes), len(invTxSubcategories()))
	return 0
}
