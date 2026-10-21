package docstools

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// check-docs-facts: was scripts/check_docs_candidate_facts.py (CHAOS-10435). Keeps the canonical docs page of the
// investment taxonomy in step with the taxonomy. The vocabulary comes from the live Go registry
// (internal/jobs/workgraph/units), the same source check-investment-drift reads; Python read the set literals of
// src/dev_health_ops/investment_taxonomy.py, so "the registry file is missing" and "THEMES is not a set literal" have no
// Go equivalent (named in testdata/README.md). The Python script took no arguments and ignored any it was given.

func init() { register("check-docs-facts", dcFactsVerb) }

const dcFactsDocRel = "docs/reference/taxonomies/investment.md"

func dcFactsVerb(_ []string, stdout, stderr io.Writer) int {
	root, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(stderr, "check-docs-facts:", err)
		return 1
	}
	docPath := filepath.Join(root, dcFactsDocRel)
	if fi, serr := os.Stat(docPath); serr != nil || !fi.Mode().IsRegular() { // F48-50
		fmt.Fprintln(stdout, "ERROR: canonical taxonomy source or page is missing")
		return 1
	}
	themes, subcategories := invTxThemes(), invTxSubcategories()
	text, err := a1ReadText(docPath)
	if err != nil {
		return dcFail(stderr, err)
	}
	documented := map[string]bool{}
	for _, m := range invTxKeyRe.FindAllStringSubmatch(text, -1) {
		documented[m[1]] = true
	}
	themeSet := invTxSet(themes)
	expected := map[string]bool{}
	for _, k := range append(append([]string(nil), themes...), subcategories...) {
		expected[k] = true
	}
	missing, unknown := map[string]bool{}, map[string]bool{}
	for k := range expected {
		if !documented[k] {
			missing[k] = true
		}
	}
	for k := range documented {
		if !expected[k] && (strings.Contains(k, ".") || themeSet[k]) {
			unknown[k] = true
		}
	}
	var errs []string
	if len(missing) > 0 {
		errs = append(errs, "missing canonical taxonomy keys: "+a1ListRepr(dcSortedKeys(missing)))
	}
	if len(unknown) > 0 {
		errs = append(errs, "unknown taxonomy keys: "+a1ListRepr(dcSortedKeys(unknown)))
	}
	if len(errs) > 0 {
		for _, e := range errs {
			fmt.Fprintf(stdout, "ERROR: %s\n", e)
		}
		return 1
	}
	fmt.Fprintf(stdout, "Canonical fact drift check passed for %d themes and %d subcategories\n", len(themes), len(subcategories))
	return 0
}

func dcSortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
