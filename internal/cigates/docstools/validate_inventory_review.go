package docstools

// validate-inventory-review: port of scripts/validate_docs_inventory_review.py (Python 3.12). It regenerates the TSV
// and summary views of the factual JSON inventory, compares them with the committed snapshots, and checks the frozen
// disposition matrix against the locked IA. Python semantics are kept step by step (ivpython.go); a shape the port
// cannot follow exactly is refused (ivStricter), never accepted.

import (
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

func init() { register("validate-inventory-review", ivVerb) }

// The frozen Phase 1 review sizes (REVIEWED_*_ROWS); tests set them.
var (
	ivReviewedDispositionRows = 439
	ivReviewedOpsRows         = 303
	ivReviewedWebRows         = 136
)

const (
	ivUsage = "usage: validate-inventory-review [-h] [--generated-json GENERATED_JSON]\n" +
		"                                 [--inventory-dir INVENTORY_DIR]\n" +
		"                                 [--ia-dir IA_DIR]\n"
	ivHelp = ivUsage + "\noptions:\n" +
		"  -h, --help            show this help message and exit\n" +
		"  --generated-json GENERATED_JSON\n" +
		"  --inventory-dir INVENTORY_DIR\n" +
		"  --ia-dir IA_DIR\n"
	ivWorkersGap = "external://dev-health-docs.fullchaos.workers.dev"
)

var (
	ivFactualFields = []string{
		"source_repo", "source_path", "artifact_type", "publication_classification", "current_url",
		"current_nav_location", "content_type", "primary_audience", "secondary_audiences", "product_area", "owner",
		"last_meaningful_review", "generated", "public_today", "duplicate_group", "known_accuracy_risk",
		"known_usability_risk", "build_dependencies", "links_in", "links_out", "notes",
	}
	ivListFields = map[string]bool{
		"current_nav_location": true, "secondary_audiences": true, "build_dependencies": true, "links_in": true, "links_out": true,
	}
	// A Python set: the order Python visits it in changes per process; this order is fixed.
	ivRequiredDisposition = []string{
		"canonical_owner", "migration_phase", "proposed_disposition", "reason", "reviewer", "source_of_truth",
		"target_page_type", "target_section",
	}
	ivPublic    = map[string]bool{"public-nav": true, "public-reference": true}
	ivNonPublic = map[string]bool{"archive": true, "internal-only": true, "internal-source-evidence": true, "remove": true, "remove-or-replace": true, "retain-internal": true, "archive-or-recapture": true}
)

func ivParseArgs(args []string, stdout, stderr io.Writer) ([]string, int, bool) {
	p := ivArgparse{prog: "validate-inventory-review", usage: ivUsage, help: ivHelp,
		opts: []string{"--generated-json", "--inventory-dir", "--ia-dir"}}
	vals, rc, done := p.parse(args, stdout, stderr)
	if done {
		return nil, rc, true
	}
	pick := func(flag, def string) string {
		if v, ok := vals[flag]; ok {
			return ivPath(v)
		}
		return def
	}
	return []string{pick("--generated-json", ".build/documentation-inventory.json"),
		pick("--inventory-dir", "docs-data/inventory"), pick("--ia-dir", "docs-data/ia")}, 0, false
}

func ivVerb(args []string, stdout, stderr io.Writer) int {
	paths, rc, done := ivParseArgs(args, stdout, stderr)
	if done {
		return rc
	}
	line, err := ivValidate(paths[0], paths[1], paths[2])
	if err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, line)
	return 0
}

// ---- files ----

func ivReadBytes(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, ivOSError(err)
	}
	return raw, nil
}

// ivLoadJSON is json.loads(path.read_text(encoding="utf-8")).
func ivLoadJSON(path string) (any, error) {
	raw, err := ivReadBytes(path)
	if err != nil {
		return nil, err
	}
	text, err := ivDecodeUTF8(raw)
	if err != nil {
		return nil, err
	}
	text = strings.ReplaceAll(strings.ReplaceAll(text, "\r\n", "\n"), "\r", "\n")
	v, err := pythonparity.LoadsJSON(text)
	var de *pythonparity.JSONDecodeError
	var ve *pythonparity.PyValueError
	switch {
	case err == nil:
		return v, nil
	case errors.As(err, &de):
		return nil, ivExc{"json.decoder.JSONDecodeError", de.Error()}
	case errors.As(err, &ve):
		return nil, ivExc{"ValueError", ve.Msg}
	case errors.Is(err, pythonparity.ErrLoneSurrogate):
		return nil, ivStricter(path + " holds a lone surrogate")
	case errors.Is(err, pythonparity.ErrJSONTooDeep):
		return nil, ivStricter(path + " nests deeper than " + fmt.Sprint(pythonparity.MaxJSONDepth))
	}
	return nil, err
}

// ivTSVRow is one csv.DictReader row: a None value is nil; the restkey (None) holds the extra fields.
type ivTSVRow struct {
	vals     map[string]*string
	extra    []string
	hasExtra bool
}

func (r ivTSVRow) item(key string) (*string, error) {
	v, ok := r.vals[key]
	if !ok {
		return nil, ivExc{"KeyError", pythonparity.StrRepr(key)}
	}
	return v, nil
}

// get is row.get(key, "").
func (r ivTSVRow) get(key string) *string {
	if v, ok := r.vals[key]; ok {
		return v
	}
	return strp("")
}

func ivStrPtrEq(a, b *string) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }

func (r ivTSVRow) equal(o ivTSVRow) bool {
	if len(r.vals) != len(o.vals) || r.hasExtra != o.hasExtra || len(r.extra) != len(o.extra) {
		return false
	}
	for k, v := range r.vals {
		w, ok := o.vals[k]
		if !ok || !ivStrPtrEq(v, w) {
			return false
		}
	}
	for i := range r.extra {
		if r.extra[i] != o.extra[i] {
			return false
		}
	}
	return true
}

func ivNoneStrip(v *string) (string, error) {
	if v == nil {
		return "", ivExc{"AttributeError", "'NoneType' object has no attribute 'strip'"}
	}
	return pythonparity.Strip(*v), nil
}

// ivLoadTSV is list(csv.DictReader(path.open(encoding="utf-8", newline=""), delimiter="\t")).
func ivLoadTSV(path string) ([]ivTSVRow, error) {
	raw, err := ivReadBytes(path)
	if err != nil {
		return nil, err
	}
	text, err := ivDecodeUTF8(raw)
	if err != nil {
		return nil, err
	}
	recs, err := pythonparity.ReadCSVTab(text)
	if err != nil {
		return nil, ivExc{"_csv.Error", err.Error()}
	}
	if len(recs) == 0 {
		return nil, nil
	}
	names := recs[0]
	var rows []ivTSVRow
	for _, rec := range recs[1:] {
		if len(rec) == 0 {
			continue
		}
		row := ivTSVRow{vals: map[string]*string{}}
		for i := 0; i < len(names) && i < len(rec); i++ {
			row.vals[names[i]] = strp(rec[i])
		}
		if len(names) < len(rec) {
			row.hasExtra, row.extra = true, rec[len(names):]
		} else {
			for _, k := range names[len(rec):] {
				row.vals[k] = nil
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// ivGlob is sorted(Path(dir).glob(pattern)) for a one-part pattern: every entry whose name matches (files,
// directories and dangling links alike); nothing when dir is not a directory or cannot be listed.
func ivGlob(dir, pattern string) ([]string, error) {
	if st, err := os.Stat(ivPath(dir)); err != nil || !st.IsDir() {
		return nil, nil
	}
	entries, err := os.ReadDir(ivPath(dir))
	if err != nil {
		return nil, nil
	}
	var names []string
	for _, e := range entries {
		if !utf8.ValidString(e.Name()) {
			return nil, ivStricter(fmt.Sprintf("a file name in %s is not UTF-8", ivPath(dir)))
		}
		if pythonparity.FnMatch(e.Name(), pattern) {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = ivJoin(dir, n)
	}
	return out, nil
}

// ---- factual views ----

// ivRowDict is dict(row) for one factual row.
func ivRowDict(v any) (pythonparity.OrderedObject, error) {
	switch x := v.(type) {
	case pythonparity.OrderedObject:
		return x, nil
	case nil, bool, *big.Int, float64:
		return nil, ivExc{"TypeError", fmt.Sprintf("'%s' object is not iterable", ivTypeName(v))}
	}
	return nil, ivStricter("a factual row is a " + ivTypeName(v) + ", not an object")
}

func ivGetOr(o pythonparity.OrderedObject, key string) any {
	v, _ := ivGet(o, key)
	return v
}

// ivFactualRow is the TSV line _write_factual_tsv writes for one row.
func ivFactualRow(row any) (string, error) {
	d, err := ivRowDict(row)
	if err != nil {
		return "", err
	}
	cells := make([]string, len(ivFactualFields))
	for i, f := range ivFactualFields {
		v := ivGetOr(d, f)
		if ivListFields[f] {
			var items []any
			if ivTruthy(v) {
				if items, err = ivIterate(v); err != nil {
					return "", err
				}
			}
			parts := make([]string, len(items))
			for j, e := range items {
				parts[j] = ivStr(e)
			}
			cells[i] = strings.Join(parts, " | ")
		} else if v != nil {
			cells[i] = ivStr(v)
		}
	}
	return pythonparity.WriteCSVTabRow(cells)
}

// ivWriteFactualTSV is _write_factual_tsv: the file is created first; a row that fails leaves the rows before it.
func ivWriteFactualTSV(path string, rows any) error {
	header, err := pythonparity.WriteCSVTabRow(ivFactualFields)
	if err != nil {
		return err
	}
	var b strings.Builder
	b.WriteString(header)
	items, iterErr := ivIterate(rows)
	if iterErr == nil {
		if _, ok := rows.([]any); !ok {
			iterErr = ivStricter("the factual rows are a " + ivTypeName(rows) + ", not a list")
		}
	}
	var rowErr error = iterErr
	if rowErr == nil {
		for _, row := range items {
			line, err := ivFactualRow(row)
			if err != nil {
				rowErr = err
				break
			}
			b.WriteString(line)
		}
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		return ivOSError(err)
	}
	return rowErr
}

// ivCounter is Counter(row[field] for row in rows) with sorted(.items()) and the str() of each key.
func ivCounter(rows []pythonparity.OrderedObject, field string) ([]any, []int, error) {
	var keys []any
	var counts []int
	for _, r := range rows {
		v, err := ivItem(r, field)
		if err != nil {
			return nil, nil, err
		}
		if err := ivHashable(v); err != nil {
			return nil, nil, err
		}
		found := false
		for i, k := range keys {
			if ivEq(k, v) {
				counts[i]++
				found = true
				break
			}
		}
		if !found {
			keys = append(keys, v)
			counts = append(counts, 1)
		}
	}
	return keys, counts, nil
}

func ivSortedCounts(keys []any, counts []int) ([]string, error) {
	if len(keys) > 1 {
		strs := make([]string, len(keys))
		for i, k := range keys {
			s, ok := k.(string)
			if !ok {
				return nil, ivStricter("Counter keys of type " + ivTypeName(k) + " next to others: Python sorts them or raises TypeError")
			}
			strs[i] = s
		}
		idx := make([]int, len(keys))
		for i := range idx {
			idx[i] = i
		}
		sort.Slice(idx, func(a, b int) bool { return strs[idx[a]] < strs[idx[b]] })
		out := make([]string, len(idx))
		for i, j := range idx {
			out[i] = fmt.Sprintf("- `%s`: %d", strs[j], counts[j])
		}
		return out, nil
	}
	var out []string
	for i, k := range keys {
		out = append(out, fmt.Sprintf("- `%s`: %d", ivStr(k), counts[i]))
	}
	return out, nil
}

func ivCountOf(keys []any, counts []int, want string) int {
	for i, k := range keys {
		if ivEq(k, want) {
			return counts[i]
		}
	}
	return 0
}

// ivSummary is _summary(rows).
func ivSummary(rows []pythonparity.OrderedObject) (string, error) {
	classKeys, classCounts, err := ivCounter(rows, "publication_classification")
	if err != nil {
		return "", err
	}
	typeKeys, typeCounts, err := ivCounter(rows, "artifact_type")
	if err != nil {
		return "", err
	}
	var unclassified, public int
	var gaps []pythonparity.OrderedObject
	for _, r := range rows {
		c, _ := ivGet(r, "publication_classification")
		if ivEq(c, "unclassified") {
			unclassified++
		}
		if ivEq(c, "gap-unverified") {
			gaps = append(gaps, r)
		}
	}
	for _, r := range rows {
		if ivTruthy(ivGetOr(r, "public_today")) && ivEq(ivGetOr(r, "artifact_type"), "markdown-page") {
			public++
		}
	}
	lines := []string{
		"# Documentation inventory review summary",
		"",
		"Generated deterministically by `scripts/docs_inventory_review.py`.",
		"",
		fmt.Sprintf("- Inventory rows: **%d**", len(rows)),
		fmt.Sprintf("- Current-site Markdown pages: **%d**", ivCountOf(typeKeys, typeCounts, "markdown-page")),
		fmt.Sprintf("- Prototype pages: **%d**", ivCountOf(typeKeys, typeCounts, "prototype-page")),
		fmt.Sprintf("- Pages treated as public today: **%d**", public),
		fmt.Sprintf("- Unclassified pages: **%d**", unclassified),
		fmt.Sprintf("- Explicit external/runtime gaps: **%d**", len(gaps)),
		"",
		"## Publication classifications",
		"",
	}
	classLines, err := ivSortedCounts(classKeys, classCounts)
	if err != nil {
		return "", err
	}
	lines = append(lines, classLines...)
	lines = append(lines, "", "## Artifact types", "")
	typeLines, err := ivSortedCounts(typeKeys, typeCounts)
	if err != nil {
		return "", err
	}
	lines = append(lines, typeLines...)
	lines = append(lines, "", "## Explicit gaps", "")
	if len(gaps) == 0 {
		lines = append(lines, "- None")
	}
	for _, r := range gaps {
		p, err := ivItem(r, "source_path")
		if err != nil {
			return "", err
		}
		note := ""
		if n := ivGetOr(r, "notes"); ivTruthy(n) {
			note = ivStr(n)
		}
		lines = append(lines, fmt.Sprintf("- `%s` — %s", ivStr(p), note))
	}
	return strings.Join(lines, "\n") + "\n", nil
}

func ivAssertEqualFile(actual, expected string) error {
	a, err := ivReadBytes(actual)
	if err != nil {
		return err
	}
	e, err := ivReadBytes(expected)
	if err != nil {
		return err
	}
	if string(a) != string(e) {
		return ivExc{"ValueError", fmt.Sprintf("Committed snapshot is stale: %s. Regenerate the inventory and review the resulting diff.", expected)}
	}
	return nil
}

// ivPathKey is a dict key that is a str or None.
type ivPathKey struct {
	none bool
	s    string
}

func ivKeyOf(v *string) ivPathKey {
	if v == nil {
		return ivPathKey{none: true}
	}
	return ivPathKey{s: *v}
}

func ivByPath(rows []ivTSVRow) (map[ivPathKey]ivTSVRow, error) {
	out := map[ivPathKey]ivTSVRow{}
	for _, r := range rows {
		p, err := r.item("source_path")
		if err != nil {
			return nil, err
		}
		out[ivKeyOf(p)] = r
	}
	return out, nil
}

func ivNoneStr(v *string) string {
	if v == nil {
		return "None"
	}
	return *v
}

func ivValidate(generatedJSON, inventoryDir, iaDir string) (string, error) {
	factual, err := ivLoadJSON(generatedJSON)
	if err != nil {
		return "", err
	}
	factualRows, err := ivItem(factual, "rows")
	if err != nil {
		return "", err
	}
	committedJSON := ivJoin(inventoryDir, "documentation-inventory.json")
	committed, err := ivLoadJSON(committedJSON)
	if err != nil {
		return "", err
	}
	expected, err := ivItem(committed, "row_count")
	if err != nil {
		return "", err
	}
	committedRows, err := ivItem(committed, "rows")
	if err != nil {
		return "", err
	}
	n, err := ivLen(committedRows)
	if err != nil {
		return "", err
	}
	if !ivEq(n, expected) {
		return "", ivExc{"ValueError", "Committed factual inventory row_count is inconsistent"}
	}
	factualCount, err := ivItem(factual, "row_count")
	if err != nil {
		return "", err
	}
	countMismatch := ivExc{"ValueError", fmt.Sprintf("Expected %s dev-health-ops rows, found %s", ivStr(expected), ivStr(factualCount))}
	if !ivEq(factualCount, expected) {
		return "", countMismatch
	}
	if n, err = ivLen(factualRows); err != nil {
		return "", err
	}
	if !ivEq(n, expected) {
		return "", countMismatch
	}

	genTSV, err := ivWithSuffix(generatedJSON, ".tsv")
	if err != nil {
		return "", err
	}
	genSummary, err := ivWithName(generatedJSON, "documentation-inventory-summary.md")
	if err != nil {
		return "", err
	}
	if err := ivWriteFactualTSV(genTSV, factualRows); err != nil {
		return "", err
	}
	list := factualRows.([]any)
	rows := make([]pythonparity.OrderedObject, len(list))
	for i, r := range list {
		rows[i] = r.(pythonparity.OrderedObject)
	}
	summary, err := ivSummary(rows)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(genSummary, []byte(summary), 0o644); err != nil {
		return "", ivOSError(err)
	}
	for _, pair := range [][2]string{
		{generatedJSON, committedJSON},
		{genTSV, ivJoin(inventoryDir, "documentation-inventory.tsv")},
		{genSummary, ivJoin(inventoryDir, "generated-summary.md")},
	} {
		if err := ivAssertEqualFile(pair[0], pair[1]); err != nil {
			return "", err
		}
	}

	disposition, err := ivLoadTSV(ivJoin(inventoryDir, "disposition-matrix.tsv"))
	if err != nil {
		return "", err
	}
	if len(disposition) != ivReviewedDispositionRows {
		return "", ivExc{"ValueError", fmt.Sprintf("Expected %d disposition rows, found %d", ivReviewedDispositionRows, len(disposition))}
	}
	var opsRows, webRows []ivTSVRow
	for _, repo := range []string{"full-chaos/dev-health-ops", "full-chaos/dev-health-web"} {
		for _, r := range disposition {
			v, err := r.item("source_repo")
			if err != nil {
				return "", err
			}
			if v != nil && *v == repo {
				if repo == "full-chaos/dev-health-ops" {
					opsRows = append(opsRows, r)
				} else {
					webRows = append(webRows, r)
				}
			}
		}
	}
	if len(opsRows) != ivReviewedOpsRows {
		return "", ivExc{"ValueError", fmt.Sprintf("Expected %d ops disposition rows, found %d", ivReviewedOpsRows, len(opsRows))}
	}
	if len(webRows) != ivReviewedWebRows {
		return "", ivExc{"ValueError", fmt.Sprintf("Expected %d web disposition rows, found %d", ivReviewedWebRows, len(webRows))}
	}

	splitFiles, err := ivGlob(inventoryDir, "ops-*.tsv")
	if err != nil {
		return "", err
	}
	var reviewedRows []ivTSVRow
	for _, p := range splitFiles {
		rs, err := ivLoadTSV(p)
		if err != nil {
			return "", err
		}
		reviewedRows = append(reviewedRows, rs...)
	}
	opsByPath, err := ivByPath(opsRows)
	if err != nil {
		return "", err
	}
	reviewedByPath, err := ivByPath(reviewedRows)
	if err != nil {
		return "", err
	}
	same := len(reviewedByPath) == len(opsByPath)
	for k, r := range reviewedByPath {
		if o, ok := opsByPath[k]; !ok || !r.equal(o) {
			same = false
		}
	}
	if len(reviewedByPath) != ivReviewedOpsRows || !same {
		return "", ivExc{"ValueError", "Ops disposition matrix differs from its reviewed split files"}
	}

	snapshot, err := ivLoadTSV(ivJoin(inventoryDir, "dev-health-web-snapshot.tsv"))
	if err != nil {
		return "", err
	}
	same = len(snapshot) == len(webRows)
	for i := 0; same && i < len(snapshot); i++ {
		same = snapshot[i].equal(webRows[i])
	}
	if !same {
		return "", ivExc{"ValueError", "dev-health-web-snapshot.tsv differs from the reviewed matrix"}
	}

	iaFiles, err := ivGlob(iaDir, "*.tsv")
	if err != nil {
		return "", err
	}
	approved := map[string]bool{}
	for _, p := range iaFiles {
		rs, err := ivLoadTSV(p)
		if err != nil {
			return "", err
		}
		for _, r := range rs {
			u, err := ivNoneStrip(r.get("url"))
			if err != nil {
				return "", err
			}
			if u != "" {
				approved[u] = true
			}
		}
	}

	var errs []string
	for _, row := range disposition {
		repo, err := row.item("source_repo")
		if err != nil {
			return "", err
		}
		path, err := row.item("source_path")
		if err != nil {
			return "", err
		}
		key := ivNoneStr(repo) + ":" + ivNoneStr(path)
		for _, f := range ivRequiredDisposition {
			v, err := ivNoneStrip(row.get(f))
			if err != nil {
				return "", err
			}
			if v == "" {
				errs = append(errs, key+" missing "+f)
			}
		}
		target, err := ivNoneStrip(row.get("target_url"))
		if err != nil {
			return "", err
		}
		if target != "" && !approved[target] {
			errs = append(errs, key+" targets non-IA URL "+target)
		}
		class, disp := row.vals["publication_classification"], row.vals["proposed_disposition"]
		if class != nil && ivPublic[*class] && target == "" && (disp == nil || !ivNonPublic[*disp]) {
			errs = append(errs, key+" lacks target or explicit non-public disposition")
		}
	}
	for _, r := range rows {
		if ivEq(ivGetOr(r, "publication_classification"), "unclassified") {
			errs = append(errs, "The factual inventory contains unclassified current sources")
			break
		}
	}
	var gapPaths []any
	for _, r := range rows {
		if ivEq(ivGetOr(r, "publication_classification"), "gap-unverified") {
			p, err := ivItem(r, "source_path")
			if err != nil {
				return "", err
			}
			gapPaths = append(gapPaths, p)
		}
	}
	if !ivEq(gapPaths, []any{ivWorkersGap}) {
		errs = append(errs, "The only deferred runtime gap must be the Workers preview crawl")
	}
	if len(errs) > 0 {
		if len(errs) > 50 {
			errs = errs[:50]
		}
		return "", ivExc{"ValueError", "Inventory review failed:\n" + strings.Join(errs, "\n")}
	}
	return fmt.Sprintf("Validated %s factual ops rows and %d reviewed dispositions against the locked IA.",
		ivStr(expected), ivReviewedDispositionRows), nil
}
