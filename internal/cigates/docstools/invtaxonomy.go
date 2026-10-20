package docstools

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// gen-taxonomy and check-investment-drift: the generated key block in docs/reference/taxonomies/investment.md and the
// check that the page and every published JSON example agree with the canonical taxonomy. Were
// scripts/gen_taxonomy_docs.py and scripts/check_investment_docs_drift.py; the vocabulary now comes from the live Go
// registry (units.SortedThemes, units.SortedSubcategories, units.ThemeOf), in sorted order.

const (
	invTxBegin   = "<!-- BEGIN GENERATED TAXONOMY -->"
	invTxEnd     = "<!-- END GENERATED TAXONOMY -->"
	invTxDocRel  = "docs/reference/taxonomies/investment.md"
	invTxDocsRel = "docs"
)

var (
	invTxKeyRe       = regexp.MustCompile("`([a-z][a-z_]*(?:\\.[a-z][a-z_]*)?)`")
	invTxJSONBlockRe = regexp.MustCompile("(?s)```json\\s*(.*?)\\s*```")
)

func init() {
	register("gen-taxonomy", invTxGenVerb)
	register("check-investment-drift", invTxDriftVerb)
}

func invTxThemes() []string        { return append([]string(nil), units.SortedThemes[:]...) }
func invTxSubcategories() []string { return append([]string(nil), units.SortedSubcategories[:]...) }

func invTxSet(items []string) map[string]bool {
	out := make(map[string]bool, len(items))
	for _, s := range items {
		out[s] = true
	}
	return out
}

func invTxSetEqual(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func invTxSorted(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// invTxRender builds the generated block.
func invTxRender(themes, subs []string) string {
	lines := []string{invTxBegin, "```text", "# THEMES"}
	lines = append(lines, themes...)
	lines = append(lines, "", "# SUBCATEGORIES (theme.subcategory)")
	lines = append(lines, subs...)
	lines = append(lines, "", "# SUBCATEGORY_TO_THEME")
	for _, s := range subs {
		lines = append(lines, s+" -> "+units.ThemeOf(s))
	}
	lines = append(lines, "```", invTxEnd)
	return strings.Join(lines, "\n")
}

func invTxMarkers(doc string) (start, stop int, ok bool) {
	start = strings.Index(doc, invTxBegin)
	stop = strings.Index(doc, invTxEnd)
	if start == -1 || stop == -1 || stop < start {
		return 0, 0, false
	}
	return start, stop, true
}

func invTxGenVerb(_ []string, _, stderr io.Writer) int {
	root, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(stderr, "gen-taxonomy:", err)
		return 1
	}
	docPath := filepath.Join(root, invTxDocRel)
	doc, err := a1ReadText(docPath)
	if err != nil {
		return a1Traceback(stderr, "FileNotFoundError", "[Errno 2] No such file or directory: "+a1Repr(docPath))
	}
	start, stop, ok := invTxMarkers(doc)
	if !ok {
		fmt.Fprintf(stderr, "Generated taxonomy markers not found in %s\n", docPath)
		return 1
	}
	stop += len(invTxEnd)
	out := doc[:start] + invTxRender(invTxThemes(), invTxSubcategories()) + doc[stop:]
	if err := os.WriteFile(docPath, []byte(out), 0o644); err != nil {
		fmt.Fprintln(stderr, "gen-taxonomy:", err)
		return 1
	}
	return 0
}

// invTxBlockKeys parses the generated block; mapping keeps first-seen order of keys.
func invTxBlockKeys(block string) (themes, subs map[string]bool, mapKeys []string, mapping map[string]string, err error) {
	themes, subs, mapping = map[string]bool{}, map[string]bool{}, map[string]string{}
	section := ""
	for _, raw := range invTxSplitLines(block) {
		line := strings.TrimSpace(raw)
		if line == "" || line == "```text" || line == "```" {
			continue
		}
		if strings.HasPrefix(line, "# ") {
			section = strings.TrimPrefix(line, "# ")
			continue
		}
		switch section {
		case "THEMES":
			themes[line] = true
		case "SUBCATEGORIES (theme.subcategory)":
			subs[line] = true
		case "SUBCATEGORY_TO_THEME":
			sub, theme, found := strings.Cut(line, " -> ")
			if !found {
				return nil, nil, nil, nil, fmt.Errorf("bad mapping line in generated block: %s", line)
			}
			if _, seen := mapping[sub]; !seen {
				mapKeys = append(mapKeys, sub)
			}
			mapping[sub] = theme
		}
	}
	return themes, subs, mapKeys, mapping, nil
}

// invTxSplitLines is str.splitlines for the separators a docs page can hold.
func invTxSplitLines(s string) []string {
	var out []string
	var cur strings.Builder
	rs := []rune(s)
	for i := 0; i < len(rs); i++ {
		switch r := rs[i]; r {
		case '\r':
			if i+1 < len(rs) && rs[i+1] == '\n' {
				i++
			}
			out = append(out, cur.String())
			cur.Reset()
		case '\n', '\v', '\f', 0x1c, 0x1d, 0x1e, 0x85, 0x2028, 0x2029:
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

func invTxDict(keys []string, m map[string]string) string {
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = a1Repr(k) + ": " + a1Repr(m[k])
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// invTxCheckDoc is check_taxonomy_doc; a non-nil error is a Python ValueError (traceback).
func invTxCheckDoc(docPath string) ([]string, error) {
	var errs []string
	themes, subs := invTxThemes(), invTxSubcategories()
	canonThemes, canonSubs := invTxSet(themes), invTxSet(subs)
	canonKeys := append([]string(nil), subs...)
	canonMap := map[string]string{}
	for _, s := range subs {
		canonMap[s] = units.ThemeOf(s)
	}

	doc, err := a1ReadText(docPath)
	if err != nil {
		return nil, fmt.Errorf("[Errno 2] No such file or directory: %s", a1Repr(docPath))
	}
	start, stop, ok := invTxMarkers(doc)
	if !ok {
		return nil, fmt.Errorf("generated taxonomy markers missing in %s", docPath)
	}
	bThemes, bSubs, bKeys, bMap, err := invTxBlockKeys(doc[start+len(invTxBegin) : stop])
	if err != nil {
		return nil, err
	}
	if !invTxSetEqual(bThemes, canonThemes) {
		errs = append(errs, fmt.Sprintf("generated theme keys drift: expected %s, found %s",
			a1ListRepr(invTxSorted(canonThemes)), a1ListRepr(invTxSorted(bThemes))))
	}
	if !invTxSetEqual(bSubs, canonSubs) {
		errs = append(errs, fmt.Sprintf("generated subcategory keys drift: expected %s, found %s",
			a1ListRepr(invTxSorted(canonSubs)), a1ListRepr(invTxSorted(bSubs))))
	}
	mapEqual := len(bMap) == len(canonMap)
	for k, v := range canonMap {
		if bMap[k] != v {
			mapEqual = false
		}
	}
	if !mapEqual {
		errs = append(errs, fmt.Sprintf("generated subcategory mapping drift: expected %s, found %s",
			invTxDict(canonKeys, canonMap), invTxDict(bKeys, bMap)))
	}

	documented := map[string]bool{}
	for _, m := range invTxKeyRe.FindAllStringSubmatch(doc, -1) {
		documented[m[1]] = true
	}
	docThemes, docSubs := map[string]bool{}, map[string]bool{}
	for key := range documented {
		if !strings.Contains(key, ".") {
			if canonThemes[key] {
				docThemes[key] = true
			}
		} else if !strings.HasSuffix(key, ".py") && key != "theme.subcategory" {
			docSubs[key] = true
		}
	}
	unknown := map[string]bool{}
	for k := range docSubs {
		if !canonSubs[k] {
			unknown[k] = true
		}
	}
	if len(unknown) > 0 {
		errs = append(errs, "documented unknown subcategory keys: "+a1ListRepr(invTxSorted(unknown)))
	}
	if !invTxSetEqual(docThemes, canonThemes) {
		errs = append(errs, fmt.Sprintf("documented theme keys drift: expected %s, found %s",
			a1ListRepr(invTxSorted(canonThemes)), a1ListRepr(invTxSorted(docThemes))))
	}
	missing := map[string]bool{}
	for k := range canonSubs {
		if !docSubs[k] {
			missing[k] = true
		}
	}
	if len(missing) > 0 {
		errs = append(errs, "documented subcategory keys missing: "+a1ListRepr(invTxSorted(missing)))
	}
	return errs, nil
}

// invTxUnknownExampleKeys is _unknown_taxonomy_example_keys: unknown theme/subcategory keys in JSON examples.
func invTxUnknownExampleKeys(document, source string) []string {
	var errs []string
	canonThemes, canonSubs := invTxSet(invTxThemes()), invTxSet(invTxSubcategories())
	for i, m := range invTxJSONBlockRe.FindAllStringSubmatch(document, -1) {
		var payload map[string]json.RawMessage
		if json.Unmarshal([]byte(m[1]), &payload) != nil || payload == nil {
			continue
		}
		taxonomies := []map[string]json.RawMessage{payload}
		if raw, ok := payload["investment"]; ok {
			var inv map[string]json.RawMessage
			if json.Unmarshal(raw, &inv) == nil && inv != nil {
				taxonomies = append(taxonomies, inv)
			}
		}
		for _, tax := range taxonomies {
			for _, f := range []struct {
				name  string
				canon map[string]bool
			}{{"themes", canonThemes}, {"subcategories", canonSubs}} {
				raw, ok := tax[f.name]
				if !ok {
					continue
				}
				var values map[string]json.RawMessage
				if json.Unmarshal(raw, &values) != nil || values == nil {
					continue
				}
				bad := map[string]bool{}
				for k := range values {
					if !f.canon[k] {
						bad[k] = true
					}
				}
				if len(bad) > 0 {
					errs = append(errs, fmt.Sprintf("%s JSON example #%d unknown %s keys: %s",
						source, i+1, f.name, a1ListRepr(invTxSorted(bad))))
				}
			}
		}
	}
	return errs
}

func invTxDriftVerb(_ []string, stdout, stderr io.Writer) int {
	root, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(stderr, "check-investment-drift:", err)
		return 1
	}
	errs, err := invTxCheckDoc(filepath.Join(root, invTxDocRel))
	if err != nil {
		return a1Traceback(stderr, "ValueError", err.Error())
	}
	docsDir := filepath.Join(root, invTxDocsRel)
	var rels []string
	_ = filepath.Walk(docsDir, func(path string, info os.FileInfo, werr error) error {
		if werr == nil && info.Mode().IsRegular() && strings.HasSuffix(path, ".md") {
			rel, _ := filepath.Rel(docsDir, path)
			rels = append(rels, filepath.ToSlash(rel))
		}
		return nil
	})
	sort.Slice(rels, func(i, j int) bool { return a1PartsLess(rels[i], rels[j]) })
	for _, rel := range rels {
		text, rerr := a1ReadText(filepath.Join(docsDir, filepath.FromSlash(rel)))
		if rerr != nil {
			return a1Traceback(stderr, "OSError", rerr.Error())
		}
		errs = append(errs, invTxUnknownExampleKeys(text, rel)...)
	}
	if len(errs) > 0 {
		for _, e := range errs {
			fmt.Fprintf(stdout, "ERROR: %s\n", e)
		}
		return 1
	}
	fmt.Fprintln(stdout, "Investment docs drift check passed")
	return 0
}
