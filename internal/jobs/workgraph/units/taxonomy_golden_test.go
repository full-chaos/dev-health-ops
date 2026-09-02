package units

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// taxonomyGolden mirrors tests/fixtures/investment_taxonomy_python_golden.json,
// produced by generate_investment_taxonomy_golden.py, which IMPORTS the
// canonical registry rather than restating it.
type taxonomyGolden struct {
	ThemesSorted        []string          `json:"themes_sorted"`
	SubcategoriesSorted []string          `json:"subcategories_sorted"`
	SubcategoryToTheme  map[string]string `json:"subcategory_to_theme"`
	PromptCategoryList  string            `json:"prompt_category_list"`
	ThemeOfUnknown      map[string]string `json:"theme_of_unknown"`
	LeafNamesSorted     []string          `json:"leaf_names_sorted"`
}

func loadTaxonomyGolden(t *testing.T) taxonomyGolden {
	t.Helper()
	path := filepath.Join(
		"..", "..", "..", "..",
		"tests", "fixtures", "investment_taxonomy_python_golden.json",
	)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read taxonomy golden: %v", err)
	}
	var golden taxonomyGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse taxonomy golden: %v", err)
	}
	if len(golden.SubcategoriesSorted) == 0 {
		t.Fatal("taxonomy golden has no subcategories; the corpus would pass vacuously")
	}
	return golden
}

// TestTaxonomyMatchesPython pins the registry itself. The Go lists are
// transcribed rather than generated -- there is no upstream to derive them from
// -- so this is the only thing standing between a typo and two planes that
// silently categorise into different buckets.
func TestTaxonomyMatchesPython(t *testing.T) {
	golden := loadTaxonomyGolden(t)

	if got, want := SortedThemes[:], golden.ThemesSorted; !equalStrings(got, want) {
		t.Errorf("themes differ\n  go:     %v\n  python: %v", got, want)
	}
	if got, want := SortedSubcategories[:], golden.SubcategoriesSorted; !equalStrings(got, want) {
		t.Errorf("subcategories differ\n  go:     %v\n  python: %v", got, want)
	}
}

// TestSortedOrderMatchesPythonSorted checks the ORDER, not just the membership.
//
// Worth its own test because a set-equality assertion passes on a list that is
// correct but misordered, and order is the part that reaches the prompt. Go's
// sort.Strings compares bytes and Python's sorted() compares code points; the
// two agree here only because every key is ASCII, so this also fails loudly if a
// non-ASCII key is ever added.
func TestSortedOrderMatchesPythonSorted(t *testing.T) {
	golden := loadTaxonomyGolden(t)

	for _, key := range golden.SubcategoriesSorted {
		for _, r := range key {
			if r > 0x7F {
				t.Fatalf(
					"subcategory %q contains a non-ASCII rune U+%04X; Go sorts by "+
						"byte and Python by code point, so the two orders can no "+
						"longer be assumed to agree -- recheck before trusting this",
					key, r,
				)
			}
		}
	}
	if !sortedAscending(SortedSubcategories[:]) {
		t.Error("SortedSubcategories is not in ascending order")
	}
	if !sortedAscending(SortedThemes[:]) {
		t.Error("SortedThemes is not in ascending order")
	}
}

// TestSubcategoryToThemeMatchesPython pins the derived map, including that the
// theme is the prefix before the FIRST dot rather than the segment after the
// last one.
func TestSubcategoryToThemeMatchesPython(t *testing.T) {
	golden := loadTaxonomyGolden(t)

	for key, want := range golden.SubcategoryToTheme {
		if got := ThemeOf(key); got != want {
			t.Errorf("ThemeOf(%q) = %q, python = %q", key, got, want)
		}
		if !IsSubcategory(key) {
			t.Errorf("IsSubcategory(%q) = false, but python has it in SUBCATEGORIES", key)
		}
		if !IsTheme(want) {
			t.Errorf("IsTheme(%q) = false, but it is the theme of %q", want, key)
		}
	}
}

// TestThemeOfUnknownReturnsEmptyString pins the silent "" default.
//
// This is the assertion most likely to be "fixed" by a later reader into an
// error return. The cases include a theme name passed as a subcategory, a
// case-differing key, and keys with surrounding whitespace -- none of which are
// normalised, and all of which yield "".
func TestThemeOfUnknownReturnsEmptyString(t *testing.T) {
	golden := loadTaxonomyGolden(t)

	if len(golden.ThemeOfUnknown) == 0 {
		t.Fatal("no unknown-key cases in the corpus")
	}
	for key, want := range golden.ThemeOfUnknown {
		if want != "" {
			t.Fatalf(
				"corpus case %q expects %q; this test only covers keys python "+
					"maps to the empty string", key, want,
			)
		}
		if got := ThemeOf(key); got != want {
			t.Errorf("ThemeOf(%q) = %q, python = %q (the empty default is the contract)", key, got, want)
		}
	}
}

// TestPromptCategoryListMatchesPython pins the prompt fragment verbatim.
//
// The separator is ", " -- comma AND space. Getting it wrong changes every
// input_hash, misses every skip-existing lookup, and silently re-categorises the
// whole corpus at full LLM cost. That is a spend defect with no failing test
// anywhere else, which is why the whole string is compared rather than its
// parts.
func TestPromptCategoryListMatchesPython(t *testing.T) {
	golden := loadTaxonomyGolden(t)

	if got := PromptCategoryList(); got != golden.PromptCategoryList {
		t.Errorf(
			"prompt category list differs\n  go:     %q\n  python: %q",
			got, golden.PromptCategoryList,
		)
	}
}

// TestSubcategoryLeafNamesAreUnique guards an invariant the REFERENCE relies on
// without asserting.
//
// api/queries/metrics.py:265-267 does `mappings.setdefault(leaf, ...)` while
// iterating SUBCATEGORIES. setdefault is first-writer-wins, and Python set
// iteration order varies with PYTHONHASHSEED, so a duplicate leaf would make
// canonical_investment_theme_sql() emit different SQL on different interpreter
// runs. It is safe today only because all 15 leaves happen to be distinct.
//
// This lives in the Go suite because the Go suite is what runs on every change
// to the taxonomy. It asserts a property of the shared registry, not of the
// port.
func TestSubcategoryLeafNamesAreUnique(t *testing.T) {
	seen := make(map[string]string, len(SortedSubcategories))
	for _, key := range SortedSubcategories {
		leaf := key[strings.LastIndex(key, ".")+1:]
		if previous, clash := seen[leaf]; clash {
			t.Errorf(
				"leaf %q is shared by %q and %q. The reference's "+
					"canonical_investment_theme_sql uses setdefault(leaf, ...) over a "+
					"set, so this makes its output depend on PYTHONHASHSEED",
				leaf, previous, key,
			)
		}
		seen[leaf] = key
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sortedAscending(values []string) bool {
	for i := 1; i < len(values); i++ {
		if values[i-1] >= values[i] {
			return false
		}
	}
	return true
}
