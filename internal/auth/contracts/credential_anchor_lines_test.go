package contracts

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/routescan"
)

// Every file:line anchor in credential-classes.json that cites this repository
// is pinned, in credentialanchors/credential-anchor-lines.json, to the text of
// the line it names and to the symbol that holds it (the top-level declaration
// of a Go file, routescan.Decls), and, when the anchor has a line_end, to the
// text of the range's last real line and its symbol. The check finds the text
// in its symbol, never at the cited line: a line moved by an edit above it
// cannot fail here, and the cited line is a citation (a drift count is logged).
// A symbol that is gone or declared twice, a text that is not in its symbol (a
// range end text: in its symbol after the start), and a text found twice there
// all fail. A text that is on several lines of its place when the pin is cut
// (a non-Go file, or a range end such as "if err != nil {") is pinned as
// occurrence k of count N, and a count that changes fails. Each failure names
// the class, the anchor kind and the file:line. An anchor with no pin, a pin no anchor uses, an unknown repo, a
// path that leaves the checkout (lexically or through a symlink), a blank or
// brace-only pinned text, a line_end before the line, and an anchor on an
// absent file fail too; none is skipped.
//
// The anchors on the deleted Python tree (src/dev_health_ops/*.py) are gone:
// credentialanchors/credential-anchor-retired-evidence.tsv says, for each one,
// the line it cited (path@commit^:line, commit = the one that deleted the file)
// and that line's text, and what became of its slot: re-pointed to the one Go
// declaration whose doc says it is that Python symbol, anchored to the Go code
// that now decides the class (no successor claim), replaced by null with a gaps
// entry of its class that quotes the text, or removed with its entry. A row names
// its slot twice: where it was in the base registry (the key, stable) and where it
// is now (current, "-" once removed); a removal shifts the current index of every
// later row of that array by one. The history test (build tag authhistory, run by
// `ci/check_go.sh auth history` with full history) checks the rows against git: the
// row set equals the base registry's Python anchors, each commit deleted its file,
// each range fits, each text matches, and each array a row names is shorter now by
// exactly its removed rows.
//
// Anchors in the web and acr repositories are counted, not checked: those
// files are not in this tree.
//
// Re-pin after a deliberate line move, in one step, from the repo root (never
// with CI set; never in a gate script):
//
//	CREDENTIAL_ANCHOR_UPDATE=1 go test ./internal/auth/contracts/ -run TestCredentialClassAnchorsPointAtTheirPinnedLineText
//
// That rewrites credentialanchors/credential-anchor-lines.json from the registry
// and the current source, then runs the check against it. Read its diff before
// committing: a pin that changed is a text, a symbol or a citation that moved.

type anchorPin struct {
	Path string `json:"path"`
	Line int    `json:"line"`
	End  int    `json:"end,omitempty"`
	Text string `json:"text"`
	// Symbol is the top-level declaration that holds the line in a Go file (routescan.Decls); empty for other files.
	Symbol string `json:"symbol,omitempty"`
	// Occurrence k of Count N: in a non-Go file whose text is on N lines, the pin is the k-th of them.
	Occurrence int `json:"occurrence,omitempty"`
	Count      int `json:"count,omitempty"`
	// EndBack is how many blank or brace-only lines the end pin walked back from line_end to the range's last real
	// line when it was cut.
	EndBack int    `json:"end_back,omitempty"`
	EndText string `json:"end_text,omitempty"`
	// EndSymbol is the declaration that holds the range's last real line when it is not Symbol.
	EndSymbol string `json:"end_symbol,omitempty"`
	// EndOccurrence k of EndCount N: the end text is on N lines of its symbol after the start; the range ends on the
	// k-th.
	EndOccurrence int `json:"end_occurrence,omitempty"`
	EndCount      int `json:"end_count,omitempty"`
}

type anchorReport struct {
	failures []string
	checked  int
	ranged   int
	foreign  int
	drift    int
}

var foreignAnchorRepos = map[string]bool{"web": true, "acr": true}

type credentialAnchorRef struct {
	classID string
	where   string
	repo    string
	path    string
	line    int
	end     int
}

func pinKey(path string, line, end int) string { return fmt.Sprintf("%s:%d-%d", path, line, end) }

func collectCredentialAnchorRefs(document []byte) ([]credentialAnchorRef, error) {
	var parsed struct {
		Classes []map[string]any `json:"classes"`
	}
	if err := json.Unmarshal(document, &parsed); err != nil {
		return nil, err
	}
	var refs []credentialAnchorRef
	var walk func(classID, where string, node any)
	walk = func(classID, where string, node any) {
		switch typed := node.(type) {
		case map[string]any:
			if path, ok := typed["path"].(string); ok {
				if line, ok := typed["line"].(float64); ok {
					repo, _ := typed["repo"].(string)
					end, _ := typed["line_end"].(float64)
					refs = append(refs, credentialAnchorRef{classID: classID, where: where, repo: repo, path: path, line: int(line), end: int(end)})
					return
				}
			}
			keys := make([]string, 0, len(typed))
			for key := range typed {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				walk(classID, where+"/"+key, typed[key])
			}
		case []any:
			for i, child := range typed {
				walk(classID, fmt.Sprintf("%s[%d]", where, i), child)
			}
		}
	}
	for _, class := range parsed.Classes {
		classID, _ := class["class_id"].(string)
		keys := make([]string, 0, len(class))
		for key := range class {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			walk(classID, key, class[key])
		}
	}
	return refs, nil
}

func checkCredentialAnchorLines(document []byte, pins []anchorPin, root string) anchorReport {
	var report anchorReport
	refs, err := collectCredentialAnchorRefs(document)
	if err != nil {
		report.failures = append(report.failures, fmt.Sprintf("credential-classes.json does not parse: %v", err))
		return report
	}
	pinned := map[string]anchorPin{}
	used := map[string]bool{}
	for _, pin := range pins {
		key := pinKey(pin.Path, pin.Line, pin.End)
		if _, dup := pinned[key]; dup {
			report.failures = append(report.failures, fmt.Sprintf("duplicate pin for %s", key))
		}
		pinned[key] = pin
	}
	lineCache := map[string][]string{}
	for _, ref := range refs {
		checkOneAnchor(&report, root, ref, pinned, used, lineCache)
	}
	for key := range pinned {
		if !used[key] {
			report.failures = append(report.failures, fmt.Sprintf("pin %s is used by no anchor: remove it or restore the anchor", key))
		}
	}
	sort.Strings(report.failures)
	return report
}

func checkOneAnchor(report *anchorReport, root string, ref credentialAnchorRef, pinned map[string]anchorPin, used map[string]bool, lineCache map[string][]string) {
	path, line := ref.path, ref.line
	label := fmt.Sprintf("class %s anchor %s %s:%d", ref.classID, strings.TrimPrefix(ref.where, "/"), path, line)
	if ref.end != 0 {
		label += fmt.Sprintf("-%d", ref.end)
	}
	if foreignAnchorRepos[ref.repo] {
		report.foreign++
		return
	}
	if ref.repo != "ops" {
		report.failures = append(report.failures, fmt.Sprintf("%s: repo %q is not ops, web or acr", label, ref.repo))
		return
	}
	if !filepath.IsLocal(path) {
		report.failures = append(report.failures, fmt.Sprintf("%s: path is not a clean relative path inside the checkout", label))
		return
	}
	lines, err := readAnchorFile(root, path, lineCache)
	if err != nil {
		if os.IsNotExist(err) {
			report.failures = append(report.failures, fmt.Sprintf("%s: file is absent: re-point the anchor to its successor or make it null with a gaps entry", label))
			return
		}
		report.failures = append(report.failures, fmt.Sprintf("%s: file does not resolve: %v", label, err))
		return
	}
	key := pinKey(path, line, ref.end)
	pin, ok := pinned[key]
	if !ok {
		var others []string
		for pinKey, other := range pinned {
			if strings.HasPrefix(pinKey, path+":") {
				others = append(others, fmt.Sprintf("%s=%q", pinKey, other.Text))
			}
		}
		sort.Strings(others)
		report.failures = append(report.failures, fmt.Sprintf("%s: no pin for this anchor in credentialanchors/credential-anchor-lines.json; pins of this file: %s", label, strings.Join(others, ", ")))
		return
	}
	used[key] = true
	report.checked++
	if trivialAnchorText(pin.Text) {
		report.failures = append(report.failures, fmt.Sprintf("%s: pinned text %q is blank or brace-only and cannot detect a change: anchor the first real line", label, pin.Text))
		return
	}
	at, problem := locatePin(lines, path, pin.Symbol, pin.Text, pin.Occurrence, pin.Count, 1)
	if problem != "" {
		report.failures = append(report.failures, fmt.Sprintf("%s: %s", label, problem))
		return
	}
	if at != line {
		report.drift++
	}
	if ref.end == 0 {
		if pin.EndText != "" || pin.EndBack != 0 || pin.EndSymbol != "" || pin.EndOccurrence != 0 || pin.EndCount != 0 {
			report.failures = append(report.failures, fmt.Sprintf("%s: the pin has an end but the anchor has no line_end", label))
		}
		return
	}
	report.ranged++
	switch {
	case ref.end < line:
		report.failures = append(report.failures, fmt.Sprintf("%s: line_end %d is before line %d", label, ref.end, line))
		return
	case pin.EndBack < 0:
		report.failures = append(report.failures, fmt.Sprintf("%s: end_back %d is negative", label, pin.EndBack))
		return
	case trivialAnchorText(pin.EndText):
		report.failures = append(report.failures, fmt.Sprintf("%s: pinned end text %q is blank or brace-only and cannot detect a change", label, pin.EndText))
		return
	}
	if ref.end-pin.EndBack == line {
		if pin.EndText != pin.Text || pin.EndSymbol != "" || pin.EndOccurrence != 0 || pin.EndCount != 0 {
			report.failures = append(report.failures, fmt.Sprintf("%s: the range ends on its first line, so its end text must be the line's text %q, not %q", label, pin.Text, pin.EndText))
		}
		return
	}
	endSymbol := pin.EndSymbol
	if endSymbol == "" {
		endSymbol = pin.Symbol
	}
	if _, problem := locatePin(lines, path, endSymbol, pin.EndText, pin.EndOccurrence, pin.EndCount, at+1); problem != "" {
		report.failures = append(report.failures, fmt.Sprintf("%s: range end: %s", label, problem))
	}
}

// locatePin is the line (1-based) that holds text once in a pin's place, at or after line from: in the one
// declaration called symbol for a Go file, or in the whole file for another file (the occurrence-th of exactly count
// lines when occurrence is set). It never reads the line the registry cites.
func locatePin(lines []string, path, symbol, text string, occurrence, count, from int) (int, string) {
	lo, hi, where := 1, len(lines), path
	if strings.HasSuffix(path, ".go") {
		if symbol == "" {
			return 0, "the pin names no symbol: re-pin (CREDENTIAL_ANCHOR_UPDATE=1)"
		}
		decls, err := routescan.Decls([]byte(strings.Join(lines, "\n")))
		if err != nil {
			return 0, fmt.Sprintf("%s does not parse: %v", path, err)
		}
		named := routescan.Named(decls, symbol)
		switch len(named) {
		case 0:
			return 0, fmt.Sprintf("anchor stale: %s declares no %s", path, symbol)
		case 1:
		default:
			return 0, fmt.Sprintf("anchor ambiguous: %s declares %s %d times", path, symbol, len(named))
		}
		lo, hi, where = named[0].Lo, named[0].Hi, path+"#"+symbol
	} else if symbol != "" {
		return 0, fmt.Sprintf("a pin on %s names a symbol (%s): only a Go file has symbols", path, symbol)
	}
	if from > lo {
		lo = from
	}
	var found []int
	for n := lo; n <= hi && n <= len(lines); n++ {
		if strings.TrimSpace(lines[n-1]) == text {
			found = append(found, n)
		}
	}
	if occurrence > 0 {
		if len(found) != count || occurrence > count {
			return 0, fmt.Sprintf("occurrence changed: %q is on %d lines of %s, the pin says occurrence %d of %d: re-pin", text, len(found), where, occurrence, count)
		}
		return found[occurrence-1], ""
	}
	switch len(found) {
	case 1:
		return found[0], ""
	case 0:
		return 0, fmt.Sprintf("anchor stale: %s holds no line %q", where, text)
	}
	return 0, fmt.Sprintf("anchor ambiguous: %q is on %d lines of %s", text, len(found), where)
}

func trivialAnchorText(text string) bool {
	return strings.Trim(text, "{}()[], \t") == ""
}

func readAnchorFile(root, path string, cache map[string][]string) ([]string, error) {
	if lines, ok := cache[path]; ok {
		return lines, nil
	}
	full, err := filepath.EvalSymlinks(filepath.Join(root, path))
	if err != nil {
		return nil, err
	}
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, err
	}
	if rel, err := filepath.Rel(realRoot, full); err != nil || !filepath.IsLocal(rel) {
		return nil, fmt.Errorf("resolves outside the checkout through a symlink")
	}
	raw, err := os.ReadFile(full)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(raw), "\n")
	cache[path] = lines
	return lines, nil
}

// pinSymbolAt is the symbol of the smallest top-level declaration that holds line in a Go file ("" for other files).
func pinSymbolAt(t *testing.T, path string, lines []string, line int) string {
	t.Helper()
	if !strings.HasSuffix(path, ".go") {
		return ""
	}
	decls, err := routescan.Decls([]byte(strings.Join(lines, "\n")))
	if err != nil {
		t.Fatalf("%s does not parse: %v", path, err)
	}
	decl, ok := routescan.Enclosing(decls, line)
	if !ok {
		t.Fatalf("%s:%d is in no top-level declaration: anchor a line of one", path, line)
	}
	return decl.Name
}

// anchorUpdateRequested reads the re-pin switch. The switch rewrites a checked-in file, so it refuses to run with CI set.
func anchorUpdateRequested(getenv func(string) string) (bool, error) {
	if getenv("CREDENTIAL_ANCHOR_UPDATE") != "1" {
		return false, nil
	}
	if getenv("CI") != "" {
		return false, fmt.Errorf("CREDENTIAL_ANCHOR_UPDATE=1 rewrites credentialanchors/credential-anchor-lines.json; it refuses to run with CI set")
	}
	return true, nil
}

// regenerateAnchorFiles rewrites the pin file from the registry and the current
// source. Only the update switch calls it.
func regenerateAnchorFiles(t *testing.T, root string, document []byte) {
	t.Helper()
	refs, err := collectCredentialAnchorRefs(document)
	if err != nil {
		t.Fatal(err)
	}
	pins := map[string]anchorPin{}
	lineCache := map[string][]string{}
	for _, ref := range refs {
		if ref.repo != "ops" || !filepath.IsLocal(ref.path) {
			continue
		}
		lines, err := readAnchorFile(root, ref.path, lineCache)
		if err != nil {
			t.Fatalf("%s:%d: %v: fix the anchor in the registry", ref.path, ref.line, err)
		}
		if ref.line < 1 || ref.line > len(lines) || trivialAnchorText(strings.TrimSpace(lines[ref.line-1])) {
			t.Fatalf("%s:%d is past the end of the file or a blank or brace-only line: fix the registry first", ref.path, ref.line)
		}
		pin := anchorPin{Path: ref.path, Line: ref.line, End: ref.end, Text: strings.TrimSpace(lines[ref.line-1])}
		symbolAt := func(line int) string { return pinSymbolAt(t, ref.path, lines, line) }
		pin.Symbol = symbolAt(ref.line)
		if !strings.HasSuffix(ref.path, ".go") {
			var at []int
			for n, l := range lines {
				if strings.TrimSpace(l) == pin.Text {
					at = append(at, n+1)
				}
			}
			if len(at) > 1 {
				pin.Count = len(at)
				for k, n := range at {
					if n == ref.line {
						pin.Occurrence = k + 1
					}
				}
			}
		}
		if ref.end != 0 {
			if ref.end < ref.line || ref.end > len(lines) {
				t.Fatalf("%s:%d-%d: line_end is before the line or past the end of the file: fix the registry first", ref.path, ref.line, ref.end)
			}
			for ref.end-pin.EndBack > ref.line && trivialAnchorText(strings.TrimSpace(lines[ref.end-pin.EndBack-1])) {
				pin.EndBack++
			}
			pin.EndText = strings.TrimSpace(lines[ref.end-pin.EndBack-1])
			endLine := ref.end - pin.EndBack
			if endSymbol := symbolAt(endLine); endSymbol != pin.Symbol {
				pin.EndSymbol = endSymbol
			}
			if endLine != ref.line {
				endSymbol := pin.EndSymbol
				if endSymbol == "" {
					endSymbol = pin.Symbol
				}
				lo, hi := ref.line+1, len(lines)
				if strings.HasSuffix(ref.path, ".go") {
					decls, err := routescan.Decls([]byte(strings.Join(lines, "\n")))
					if err != nil {
						t.Fatal(err)
					}
					named := routescan.Named(decls, endSymbol)
					if len(named) != 1 {
						t.Fatalf("%s: symbol %s is declared %d times", ref.path, endSymbol, len(named))
					}
					if named[0].Lo > lo {
						lo = named[0].Lo
					}
					hi = named[0].Hi
				}
				var at []int
				for n := lo; n <= hi; n++ {
					if strings.TrimSpace(lines[n-1]) == pin.EndText {
						at = append(at, n)
					}
				}
				if len(at) > 1 {
					pin.EndCount = len(at)
					for k, n := range at {
						if n == endLine {
							pin.EndOccurrence = k + 1
						}
					}
				}
			}
		}
		if at, problem := locatePin(lines, pin.Path, pin.Symbol, pin.Text, pin.Occurrence, pin.Count, 1); problem != "" || at != ref.line {
			t.Fatalf("%s:%d: the cut pin does not find its own line (%d, %s): give the anchor a line whose text is once in its symbol", ref.path, ref.line, at, problem)
		}
		pins[pinKey(pin.Path, pin.Line, pin.End)] = pin
	}
	list := make([]anchorPin, 0, len(pins))
	for _, pin := range pins {
		list = append(list, pin)
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].Path != list[j].Path {
			return list[i].Path < list[j].Path
		}
		if list[i].Line != list[j].Line {
			return list[i].Line < list[j].Line
		}
		return list[i].End < list[j].End
	})
	encoded, err := json.MarshalIndent(list, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("credentialanchors", "credential-anchor-lines.json"), append(encoded, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func loadAnchorPins(t *testing.T) []anchorPin {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("credentialanchors", "credential-anchor-lines.json"))
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var pins []anchorPin
	if err := dec.Decode(&pins); err != nil {
		t.Fatalf("credentialanchors/credential-anchor-lines.json: %v (a pin carries path, line, end, text, symbol, occurrence, count, end_back, end_text, end_symbol, end_occurrence and end_count only)", err)
	}
	return pins
}

func TestCredentialClassAnchorsPointAtTheirPinnedLineText(t *testing.T) {
	root := testRoot(t)
	raw, err := os.ReadFile(filepath.Join(ContractsDir(root), "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	update, err := anchorUpdateRequested(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	if update {
		regenerateAnchorFiles(t, root, raw)
	}
	report := checkCredentialAnchorLines(raw, loadAnchorPins(t), root)
	for _, failure := range report.failures {
		t.Error(failure)
	}
	if report.checked == 0 || report.ranged == 0 {
		t.Fatalf("no ops anchor (or no range) was measured: %d checked, %d ranges", report.checked, report.ranged)
	}
	t.Logf("anchors: %d checked by symbol (%d with a range end), %d in web/acr (counted, not checked)", report.checked, report.ranged, report.foreign)
	t.Logf("citation note: %d of %d anchors cite a line other than the one their pin resolves to", report.drift, report.checked)
}

func TestTheAnchorUpdateRefusesUnderCI(t *testing.T) {
	env := func(vars map[string]string) func(string) string { return func(k string) string { return vars[k] } }
	if ok, err := anchorUpdateRequested(env(map[string]string{"CREDENTIAL_ANCHOR_UPDATE": "1", "CI": "true"})); ok || err == nil {
		t.Errorf("the re-pin switch under CI = %v, %v; want a refusal", ok, err)
	}
	if ok, err := anchorUpdateRequested(env(map[string]string{"CREDENTIAL_ANCHOR_UPDATE": "1"})); !ok || err != nil {
		t.Errorf("the re-pin switch on a workstation = %v, %v; want it on", ok, err)
	}
	if ok, err := anchorUpdateRequested(env(map[string]string{"CI": "true"})); ok || err != nil {
		t.Errorf("no switch = %v, %v; want off", ok, err)
	}
}

func TestTheRetiredPathListIsGone(t *testing.T) {
	if _, err := os.Stat(filepath.Join("credentialanchors", "credential-anchor-retired-paths.json")); !os.IsNotExist(err) {
		t.Errorf("credentialanchors/credential-anchor-retired-paths.json must not exist (err %v): an anchor on an absent file always fails", err)
	}
}

// retiredEvidence is one row of credentialanchors/credential-anchor-retired-evidence.tsv.
type retiredEvidence struct {
	// where is the slot in the base registry the header names; current is the slot now, "-" for a removed one.
	classID, where, current, old, oldText, action, successor, doc string
}

// loadRetiredEvidence returns the rows and the base registry commit the header names ("# base-registry <sha>").
func loadRetiredEvidence(t *testing.T) ([]retiredEvidence, string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("credentialanchors", "credential-anchor-retired-evidence.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []retiredEvidence
	base := ""
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for scanner.Scan() {
		line := scanner.Text()
		if rest, ok := strings.CutPrefix(line, "# base-registry "); ok {
			base = rest
			continue
		}
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 8 {
			t.Fatalf("evidence row %q has %d fields, want 8 (class_id, where, current, old, old_text, action, successor, successor_doc)", line, len(f))
		}
		rows = append(rows, retiredEvidence{f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7]})
	}
	if !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(base) {
		t.Fatalf("the evidence header names base registry commit %q, want a 40-hex main commit", base)
	}
	return rows, base
}

// retiredGap is the gaps entry a null row adds to its class: it quotes the cited line and names the base slot.
func retiredGap(r retiredEvidence) string {
	return fmt.Sprintf("retired anchor %s: %s (%s) was deleted with the Python api; no single documented Go successor", r.where, r.old, r.oldText)
}

var slotIndex = regexp.MustCompile(`^([^\[]*)\[(\d+)\](.*)$`)

// splitSlot splits a slot at its first index: "validators[2]/anchor" is ("validators", 2, "/anchor").
func splitSlot(where string) (array string, index int, rest string, ok bool) {
	m := slotIndex.FindStringSubmatch(where)
	if m == nil {
		return "", 0, "", false
	}
	index, _ = strconv.Atoi(m[2])
	return m[1], index, m[3], true
}

// currentSlots is where each kept row's base slot is now: a removed row takes its element out of its array, so each
// later index of that array moves down by one per removal before it.
func currentSlots(rows []retiredEvidence) map[string]string {
	removed := map[string][]int{}
	for _, r := range rows {
		if array, index, _, ok := splitSlot(r.where); ok && r.action == "removed" {
			removed[r.classID+"\t"+array] = append(removed[r.classID+"\t"+array], index)
		}
	}
	slots := map[string]string{}
	for _, r := range rows {
		if r.action == "removed" {
			continue
		}
		array, index, rest, ok := splitSlot(r.where)
		if !ok {
			slots[r.classID+"\t"+r.where] = r.where
			continue
		}
		shift := 0
		for _, gone := range removed[r.classID+"\t"+array] {
			if gone < index {
				shift++
			}
		}
		slots[r.classID+"\t"+r.where] = fmt.Sprintf("%s[%d]%s", array, index-shift, rest)
	}
	return slots
}

func classHasGap(class map[string]any, gap string) bool {
	gaps, _ := class["gaps"].([]any)
	for _, g := range gaps {
		if g == gap {
			return true
		}
	}
	return false
}

var (
	wherePart    = regexp.MustCompile(`^([^/\[]+)((?:\[\d+\])*)$`)
	whereIndex   = regexp.MustCompile(`\[(\d+)\]`)
	oldShape     = regexp.MustCompile(`^(src/dev_health_ops/[\w/]+\.py)@[0-9a-f]{12}\^:(\d+)(?:-(\d+))?$`)
	successorFmt = regexp.MustCompile(`^([\w./-]+\.go):(\d+)-(\d+)$`)
	identityDoc  = regexp.MustCompile(`^//\s*(?:Package\s+\w+|\w+)\s+is\s`)
	pythonDecl   = regexp.MustCompile(`^(?:async\s+)?(?:def|class)\s+(\w+)`)
)

// registryNode resolves an anchor slot ("issuer/anchors[0]", "consumers[1]/anchor") of one class.
func registryNode(class map[string]any, where string) (any, bool) {
	var node any = class
	for _, part := range strings.Split(where, "/") {
		m := wherePart.FindStringSubmatch(part)
		if m == nil {
			return nil, false
		}
		obj, ok := node.(map[string]any)
		if !ok {
			return nil, false
		}
		if node, ok = obj[m[1]]; !ok {
			return nil, false
		}
		for _, idx := range whereIndex.FindAllStringSubmatch(m[2], -1) {
			list, ok := node.([]any)
			i, _ := strconv.Atoi(idx[1])
			if !ok || i >= len(list) {
				return nil, false
			}
			node = list[i]
		}
	}
	return node, true
}

func checkRetiredEvidence(document []byte, rows []retiredEvidence, root string) []string {
	var parsed struct {
		Classes []map[string]any `json:"classes"`
	}
	if err := json.Unmarshal(document, &parsed); err != nil {
		return []string{fmt.Sprintf("credential-classes.json does not parse: %v", err)}
	}
	classes := map[string]map[string]any{}
	for _, c := range parsed.Classes {
		id, _ := c["class_id"].(string)
		classes[id] = c
	}
	var failures []string
	seen := map[string]bool{}
	held := map[string]string{}
	slots := currentSlots(rows)
	cache := map[string][]string{}
	for _, r := range rows {
		label := fmt.Sprintf("evidence %s %s", r.classID, r.where)
		if seen[r.classID+"\t"+r.where] {
			failures = append(failures, label+": listed twice")
		}
		seen[r.classID+"\t"+r.where] = true
		m := oldShape.FindStringSubmatch(r.old)
		if m == nil {
			failures = append(failures, fmt.Sprintf("%s: old %q is not src/dev_health_ops/<path>.py@<commit12>^:<line>[-<end>]", label, r.old))
			continue
		}
		if _, err := os.Stat(filepath.Join(root, m[1])); !os.IsNotExist(err) {
			failures = append(failures, fmt.Sprintf("%s: %s exists in the checkout; it is not a retired file", label, m[1]))
		}
		class, ok := classes[r.classID]
		if !ok {
			failures = append(failures, label+": no such class")
			continue
		}
		if r.action == "removed" {
			if r.current != "-" {
				failures = append(failures, fmt.Sprintf("%s: a removed slot is current %q, want -", label, r.current))
			}
			if r.successor != "-" || r.doc != "-" {
				failures = append(failures, fmt.Sprintf("%s: a removed slot names successor %q, doc %q", label, r.successor, r.doc))
			}
			if classHasGap(class, retiredGap(r)) {
				failures = append(failures, fmt.Sprintf("%s: the slot is removed, but the class gaps still hold %q", label, retiredGap(r)))
			}
			continue
		}
		if want := slots[r.classID+"\t"+r.where]; r.current != want {
			failures = append(failures, fmt.Sprintf("%s: current %q, but the removals before it put the slot at %q", label, r.current, want))
			continue
		}
		if other, dup := held[r.classID+"\t"+r.current]; dup {
			failures = append(failures, fmt.Sprintf("%s: current %s is also the slot of %s", label, r.current, other))
		}
		held[r.classID+"\t"+r.current] = r.where
		node, ok := registryNode(class, r.current)
		if !ok {
			failures = append(failures, fmt.Sprintf("%s: no such anchor slot %s in the registry", label, r.current))
			continue
		}
		switch r.action {
		case "repoint", "anchored":
			s := successorFmt.FindStringSubmatch(r.successor)
			anchor, isMap := node.(map[string]any)
			if s == nil || !isMap {
				failures = append(failures, fmt.Sprintf("%s: a %s slot holds %v, successor %q", label, r.action, node, r.successor))
				continue
			}
			line, _ := strconv.Atoi(s[2])
			end, _ := strconv.Atoi(s[3])
			got := fmt.Sprintf("%v:%v:%v-%v", anchor["repo"], anchor["path"], anchor["line"], anchor["line_end"])
			if want := fmt.Sprintf("ops:%s:%d-%d", s[1], line, end); got != want {
				failures = append(failures, fmt.Sprintf("%s: the registry anchor is %s, the evidence says %s", label, got, want))
				continue
			}
			if r.action == "anchored" {
				// The slot now cites the Go code that decides the class; it claims no Python successor, so it has no
				// doc, and the gap the null slot carried is closed.
				if r.doc != "-" {
					failures = append(failures, fmt.Sprintf("%s: an anchored slot claims no successor doc, got %q", label, r.doc))
				}
				if classHasGap(class, retiredGap(r)) {
					failures = append(failures, fmt.Sprintf("%s: the slot is anchored, but the class gaps still hold %q", label, retiredGap(r)))
				}
				continue
			}
			decl := pythonDecl.FindStringSubmatch(r.oldText)
			if decl == nil || r.doc != filepath.Base(m[1])+"'s "+decl[1] {
				failures = append(failures, fmt.Sprintf("%s: successor doc %q does not name the def/class the cited line %q declares", label, r.doc, r.oldText))
				continue
			}
			lines, err := readAnchorFile(root, s[1], cache)
			if err != nil || end > len(lines) || line < 1 || end < line {
				failures = append(failures, fmt.Sprintf("%s: the successor %s does not resolve (%v)", label, r.successor, err))
				continue
			}
			block := strings.Join(lines[line-1:end], "\n")
			if !identityDoc.MatchString(strings.TrimSpace(lines[line-1])) || !strings.Contains(block, r.doc) {
				failures = append(failures, fmt.Sprintf("%s: %s does not open with an identity doc (\"X is ...\") naming %s", label, r.successor, r.doc))
			}
		case "null":
			if node != nil {
				failures = append(failures, fmt.Sprintf("%s: the evidence says null, the registry holds %v", label, node))
				continue
			}
			if !classHasGap(class, retiredGap(r)) {
				failures = append(failures, fmt.Sprintf("%s: the class gaps lack %q", label, retiredGap(r)))
			}
		default:
			failures = append(failures, fmt.Sprintf("%s: action %q is not repoint, anchored, null or removed", label, r.action))
		}
	}
	sort.Strings(failures)
	return failures
}

// removedSlotFailures compares each array a row names between the base registry and the current one: it is shorter
// now by exactly the rows that say "removed", so a removal the evidence does not record, or a "removed" row whose
// element is still there, fails.
func removedSlotFailures(rows []retiredEvidence, baseDoc, currentDoc []byte) []string {
	classesOf := func(document []byte) (map[string]map[string]any, error) {
		var parsed struct {
			Classes []map[string]any `json:"classes"`
		}
		if err := json.Unmarshal(document, &parsed); err != nil {
			return nil, err
		}
		classes := map[string]map[string]any{}
		for _, c := range parsed.Classes {
			id, _ := c["class_id"].(string)
			classes[id] = c
		}
		return classes, nil
	}
	baseClasses, err := classesOf(baseDoc)
	if err != nil {
		return []string{fmt.Sprintf("the base registry does not parse: %v", err)}
	}
	currentClasses, err := classesOf(currentDoc)
	if err != nil {
		return []string{fmt.Sprintf("the current registry does not parse: %v", err)}
	}
	removed := map[string]int{}
	var arrays []string
	for _, r := range rows {
		array, _, _, ok := splitSlot(r.where)
		if !ok {
			continue
		}
		key := r.classID + "\t" + array
		if _, listed := removed[key]; !listed {
			arrays = append(arrays, key)
			removed[key] = 0
		}
		if r.action == "removed" {
			removed[key]++
		}
	}
	var failures []string
	for _, key := range arrays {
		classID, array, _ := strings.Cut(key, "\t")
		length := func(classes map[string]map[string]any) int {
			node, ok := registryNode(classes[classID], array)
			list, isList := node.([]any)
			if !ok || !isList {
				return -1
			}
			return len(list)
		}
		before, after := length(baseClasses), length(currentClasses)
		if before < 0 || after < 0 || after != before-removed[key] {
			failures = append(failures, fmt.Sprintf("class %s %s: %d elements at the base, %d now, but the evidence removes %d", classID, array, before, after, removed[key]))
		}
	}
	return failures
}

func TestEveryRetiredPythonAnchorIsRePointedOrNulledAsItsEvidenceSays(t *testing.T) {
	root := testRoot(t)
	raw, err := os.ReadFile(filepath.Join(ContractsDir(root), "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := loadRetiredEvidence(t)
	if len(rows) == 0 {
		t.Fatal("the retired-anchor evidence is empty")
	}
	for _, failure := range checkRetiredEvidence(raw, rows, root) {
		t.Error(failure)
	}
	refs, err := collectCredentialAnchorRefs(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range refs {
		if ref.repo == "ops" && strings.HasPrefix(ref.path, "src/dev_health_ops/") {
			t.Errorf("class %s anchor %s still cites the deleted Python tree: %s:%d", ref.classID, ref.where, ref.path, ref.line)
		}
	}
}

func TestTheRetiredEvidenceCheckRefusesAWrongSuccessorAndAMissingGap(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "internal", "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	source := "// Verify is check.py's verify_token for the Go api.\nfunc Verify() {}\n// Other is something else.\nfunc Other() {}\n"
	if err := os.WriteFile(filepath.Join(root, "internal", "x", "a.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	repoint := retiredEvidence{"demo", "validators[0]/anchor", "validators[0]/anchor", "src/dev_health_ops/check.py@0123456789ab^:10-20", "def verify_token(token):", "repoint", "internal/x/a.go:1-2", "check.py's verify_token"}
	null := retiredEvidence{"demo", "issuer/anchors[0]", "issuer/anchors[0]", "src/dev_health_ops/check.py@0123456789ab^:5", "TOKEN_TTL = 300", "null", "-", "-"}
	doc := func(anchor string, gaps string) []byte {
		return []byte(fmt.Sprintf(`{"classes":[{"class_id":"demo","issuer":{"anchors":[null]},"validators":[{"anchor":%s}],"gaps":[%s]}]}`, anchor, gaps))
	}
	gap, _ := json.Marshal(retiredGap(null))
	good := `{"repo":"ops","path":"internal/x/a.go","line":1,"line_end":2}`
	if f := checkRetiredEvidence(doc(good, string(gap)), []retiredEvidence{repoint, null}, root); len(f) != 0 {
		t.Fatalf("matching evidence must pass: %v", f)
	}
	wrong := `{"repo":"ops","path":"internal/x/a.go","line":3,"line_end":4}`
	if f := checkRetiredEvidence(doc(wrong, string(gap)), []retiredEvidence{repoint, null}, root); len(f) == 0 {
		t.Error("an anchor re-pointed to another symbol must fail")
	}
	moved := repoint
	moved.successor = "internal/x/a.go:3-4"
	if f := checkRetiredEvidence(doc(wrong, string(gap)), []retiredEvidence{moved, null}, root); len(f) == 0 {
		t.Error("a successor without an identity doc naming the Python symbol must fail")
	}
	if f := checkRetiredEvidence(doc(good, `"other gap"`), []retiredEvidence{repoint, null}, root); len(f) == 0 {
		t.Error("a null anchor without its gaps entry must fail")
	}
	otherText := null
	otherText.oldText = "TOKEN_TTL = 600"
	if f := checkRetiredEvidence(doc(good, string(gap)), []retiredEvidence{repoint, otherText}, root); len(f) == 0 {
		t.Error("a gap that quotes other text than the row's old_text must fail")
	}
	wrongDecl := repoint
	wrongDecl.oldText = "def other(token):"
	if f := checkRetiredEvidence(doc(good, string(gap)), []retiredEvidence{wrongDecl, null}, root); len(f) == 0 {
		t.Error("a re-point whose cited line declares another symbol must fail")
	}
}

// A removal takes its element out of the array: the rows after it name their base slot and the slot one lower now.
func TestTheRetiredEvidenceCheckFollowsARemovedSlot(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "internal", "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "internal", "x", "a.go"), []byte("package x\n\nfunc Decide() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := func(line int) string { return fmt.Sprintf("src/dev_health_ops/check.py@0123456789ab^:%d", line) }
	removed := retiredEvidence{"demo", "validators[0]/anchor", "-", old(1), "middleware()", "removed", "-", "-"}
	anchored := retiredEvidence{"demo", "validators[1]/anchor", "validators[0]/anchor", old(2), "lookup()", "anchored", "internal/x/a.go:3-3", "-"}
	null := retiredEvidence{"demo", "validators[2]/anchor", "validators[1]/anchor", old(3), "rotate()", "null", "-", "-"}
	rows := []retiredEvidence{removed, anchored, null}
	gapOf := func(r retiredEvidence) string { g, _ := json.Marshal(retiredGap(r)); return string(g) }
	doc := func(validators, gaps string) []byte {
		return []byte(fmt.Sprintf(`{"classes":[{"class_id":"demo","validators":[%s],"gaps":[%s]}]}`, validators, gaps))
	}
	kept := `{"anchor":{"repo":"ops","path":"internal/x/a.go","line":3,"line_end":3}},{"anchor":null}`
	if f := checkRetiredEvidence(doc(kept, gapOf(null)), rows, root); len(f) != 0 {
		t.Fatalf("a removal with the later rows moved down must pass: %v", f)
	}
	stale := anchored
	stale.current = "validators[1]/anchor"
	if f := checkRetiredEvidence(doc(kept, gapOf(null)), []retiredEvidence{removed, stale, null}, root); len(f) == 0 {
		t.Error("a row that keeps its base index after a removal before it must fail")
	}
	notRemoved := removed
	notRemoved.action = "null"
	notRemoved.current = "validators[0]/anchor"
	if f := checkRetiredEvidence(doc(kept, gapOf(null)+","+gapOf(notRemoved)), []retiredEvidence{notRemoved, anchored, null}, root); len(f) == 0 {
		t.Error("rows that shift without a removed row must fail")
	}
	if f := checkRetiredEvidence(doc(kept, gapOf(null)+","+gapOf(removed)), rows, root); len(f) == 0 {
		t.Error("a removed slot whose gap is still listed must fail")
	}
	if f := checkRetiredEvidence(doc(kept, gapOf(null)+","+gapOf(anchored)), rows, root); len(f) == 0 {
		t.Error("an anchored slot whose gap is still listed must fail")
	}
	removedHere := removed
	removedHere.current = "validators[0]/anchor"
	if f := checkRetiredEvidence(doc(kept, gapOf(null)), []retiredEvidence{removedHere, anchored, null}, root); len(f) == 0 {
		t.Error("a removed slot with a current slot must fail")
	}
	elsewhere := `{"anchor":{"repo":"ops","path":"internal/x/a.go","line":1,"line_end":1}},{"anchor":null}`
	if f := checkRetiredEvidence(doc(elsewhere, gapOf(null)), rows, root); len(f) == 0 {
		t.Error("an anchored slot that holds another anchor than its row must fail")
	}
	withDoc := anchored
	withDoc.doc = "check.py's lookup"
	if f := checkRetiredEvidence(doc(kept, gapOf(null)), []retiredEvidence{removed, withDoc, null}, root); len(f) == 0 {
		t.Error("an anchored slot that claims a successor doc must fail")
	}
	nulled := `{"anchor":null},{"anchor":null}`
	if f := checkRetiredEvidence(doc(nulled, gapOf(null)), rows, root); len(f) == 0 {
		t.Error("an anchored slot that holds null must fail")
	}
}

func TestTheRemovedSlotCountComparesTheBaseArrayWithTheCurrentOne(t *testing.T) {
	removed := retiredEvidence{"demo", "validators[0]/anchor", "-", "", "", "removed", "-", "-"}
	kept := retiredEvidence{"demo", "validators[1]/anchor", "validators[0]/anchor", "", "", "null", "-", "-"}
	base := []byte(`{"classes":[{"class_id":"demo","validators":[{"anchor":null},{"anchor":null}]}]}`)
	one := []byte(`{"classes":[{"class_id":"demo","validators":[{"anchor":null}]}]}`)
	if f := removedSlotFailures([]retiredEvidence{removed, kept}, base, one); len(f) != 0 {
		t.Fatalf("one removed row and one element fewer must pass: %v", f)
	}
	if f := removedSlotFailures([]retiredEvidence{removed, kept}, base, base); len(f) == 0 {
		t.Error("a removed row whose element is still there must fail")
	}
	notRemoved := removed
	notRemoved.action = "null"
	if f := removedSlotFailures([]retiredEvidence{notRemoved, kept}, base, one); len(f) == 0 {
		t.Error("an element gone without a removed row must fail")
	}
}

func TestCredentialAnchorLineCheckFailsOnAMovedLine(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "internal", "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	source := "package x\n\nfunc Issue() {}\nfunc Validate() {}\n"
	if err := os.WriteFile(filepath.Join(root, "internal", "x", "a.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	fixture := func(issuerLine int) []byte {
		return []byte(fmt.Sprintf(`{"classes":[{"class_id":"demo_class",
		  "issuer":{"anchors":[{"repo":"ops","path":"internal/x/a.go","line":%d}]},
		  "validators":[{"anchor":{"repo":"ops","path":"internal/x/a.go","line":4}}]}]}`, issuerLine))
	}
	pins := []anchorPin{
		{Path: "internal/x/a.go", Line: 3, Text: "func Issue() {}", Symbol: "Issue"},
		{Path: "internal/x/a.go", Line: 4, Text: "func Validate() {}", Symbol: "Validate"},
	}

	if report := checkCredentialAnchorLines(fixture(3), pins, root); len(report.failures) != 0 || report.checked != 2 {
		t.Fatalf("a fixture whose anchors hold their text must pass with 2 checked: %+v", report)
	}

	moved := checkCredentialAnchorLines(fixture(2), pins, root)
	want := []string{"demo_class", "issuer/anchors[0]", "internal/x/a.go:2"}
	if len(moved.failures) == 0 {
		t.Fatal("a moved anchor must fail")
	}
	for _, part := range want {
		if !strings.Contains(strings.Join(moved.failures, "\n"), part) {
			t.Errorf("failure does not name %q: %v", part, moved.failures)
		}
	}

	orphan := checkCredentialAnchorLines(fixture(3), append(pins, anchorPin{Path: "internal/x/a.go", Line: 1, Text: "package x", Symbol: "package"}), root)
	if len(orphan.failures) == 0 {
		t.Error("a pin no anchor uses must fail")
	}

	foreignRepo := []byte(`{"classes":[{"class_id":"demo_class","issuer":{"anchors":[{"repo":"nowhere","path":"a","line":1}]}}]}`)
	if report := checkCredentialAnchorLines(foreignRepo, nil, root); len(report.failures) == 0 {
		t.Error("an unknown repo must fail")
	}
}

func TestCredentialAnchorLineCheckPinsTheEndOfTheRange(t *testing.T) {
	root := t.TempDir()
	source := "package x\n\nfunc Verify() bool {\n\treturn false\n}\n"
	write := func(s string) {
		if err := os.WriteFile(filepath.Join(root, "a.go"), []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(source)
	doc := func(line, end int) []byte {
		return []byte(fmt.Sprintf(`{"classes":[{"class_id":"demo_class","issuer":{"anchors":[{"repo":"ops","path":"a.go","line":%d,"line_end":%d}]}}]}`, line, end))
	}
	pin := anchorPin{Path: "a.go", Line: 3, End: 5, Text: "func Verify() bool {", Symbol: "Verify", EndBack: 1, EndText: "return false"}
	if report := checkCredentialAnchorLines(doc(3, 5), []anchorPin{pin}, root); len(report.failures) != 0 || report.ranged != 1 {
		t.Fatalf("a range pinned at its last real line must pass: %+v", report)
	}
	write("package x\n\nfunc Verify() bool {\n\treturn true\n}\n")
	if report := checkCredentialAnchorLines(doc(3, 5), []anchorPin{pin}, root); len(report.failures) == 0 {
		t.Error("a changed last line of the range must fail although the first line holds")
	}
	write("package x\n\nfunc Verify() bool {\n\tif ok {\n\t\treturn false\n\t}\n\treturn false\n}\n")
	twice := pin
	twice.End, twice.EndBack = 8, 1
	if report := checkCredentialAnchorLines(doc(3, 8), []anchorPin{twice}, root); len(report.failures) == 0 || !strings.Contains(report.failures[0], "anchor ambiguous") {
		t.Errorf("a range end text twice after the start without an occurrence must fail as ambiguous: %v", report.failures)
	}
	twice.EndOccurrence, twice.EndCount = 2, 2
	if report := checkCredentialAnchorLines(doc(3, 8), []anchorPin{twice}, root); len(report.failures) != 0 {
		t.Errorf("occurrence 2 of 2 must pass: %v", report.failures)
	}
	twice.EndOccurrence, twice.EndCount = 2, 3
	if report := checkCredentialAnchorLines(doc(3, 8), []anchorPin{twice}, root); len(report.failures) == 0 || !strings.Contains(report.failures[0], "occurrence changed") {
		t.Errorf("an end text count that changed must fail: %v", report.failures)
	}
	write(source)
	for name, c := range map[string]struct {
		line, end int
		pins      []anchorPin
	}{
		"line_end before the line":         {3, 2, []anchorPin{{Path: "a.go", Line: 3, End: 2, Text: "func Verify() bool {", Symbol: "Verify", EndText: "x"}}},
		"a brace-only end text":            {3, 5, []anchorPin{{Path: "a.go", Line: 3, End: 5, Text: "func Verify() bool {", Symbol: "Verify", EndText: "}"}}},
		"an end text only above the start": {3, 5, []anchorPin{{Path: "a.go", Line: 3, End: 5, Text: "func Verify() bool {", Symbol: "Verify", EndBack: 1, EndText: "package x", EndSymbol: "package"}}},
		"an end text in no symbol":         {3, 5, []anchorPin{{Path: "a.go", Line: 3, End: 5, Text: "func Verify() bool {", Symbol: "Verify", EndBack: 1, EndText: "return false", EndSymbol: "Gone"}}},
	} {
		if report := checkCredentialAnchorLines(doc(c.line, c.end), c.pins, root); len(report.failures) == 0 {
			t.Errorf("%s must fail", name)
		}
	}
}

func TestCredentialAnchorLineCheckRefusesABlankOrBraceOnlyPinnedText(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.go"), []byte("package x\n\nfunc Verify() bool { return false }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"classes":[{"class_id":"demo_class","issuer":{"anchors":[{"repo":"ops","path":"a.go","line":2}]}}]}`)
	for _, pins := range [][]anchorPin{
		{{Path: "a.go", Line: 2, Text: "", Symbol: "Verify"}},
		{{Path: "a.go", Line: 2, Text: "}", Symbol: "Verify"}},
	} {
		if report := checkCredentialAnchorLines(document, pins, root); len(report.failures) == 0 {
			t.Errorf("a blank or brace-only pinned text must fail (pin %+v): it cannot detect a change", pins[0])
		}
	}
}

// The check follows a pin's symbol and text, never the cited line: a line moved by an edit above it holds (and counts
// as drift); a wrong, gone, doubled or moved symbol or text fails, and so does a text count that changed.
func TestCredentialAnchorLineCheckFollowsItsSymbolNotTheLine(t *testing.T) {
	root := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	const issue = "func Issue() string {\n\treturn \"issued\"\n}\n"
	doc := []byte(`{"classes":[{"class_id":"demo_class","issuer":{"anchors":[{"repo":"ops","path":"a.go","line":4}]},` +
		`"validators":[{"anchor":{"repo":"ops","path":"s.json","line":2}}]}]}`)
	goPin := anchorPin{Path: "a.go", Line: 4, Text: "return \"issued\"", Symbol: "Issue"}
	jsonPin := anchorPin{Path: "s.json", Line: 2, Text: "\"aud\": {", Occurrence: 1, Count: 2}
	base := func() {
		write("a.go", "package x\n\n"+issue+"\nfunc Other() {}\n")
		write("s.json", "{\n\"aud\": {\n},\n\"aud\": {\n}\n}\n")
	}
	run := func(pins ...anchorPin) anchorReport { return checkCredentialAnchorLines(doc, pins, root) }
	base()
	if r := run(goPin, jsonPin); len(r.failures) != 0 || r.checked != 2 || r.drift != 0 {
		t.Fatalf("the base fixture must pass with 2 checked and no drift: %+v", r)
	}
	write("a.go", "package x\n\n// one\n// two\n// three\n"+issue+"\nfunc Other() {}\n")
	if r := run(goPin, jsonPin); len(r.failures) != 0 || r.drift != 1 {
		t.Errorf("three lines above the func must pass with one drifted citation: %+v", r)
	}
	for name, c := range map[string]struct {
		goSource, jsonSource string
		pins                 []anchorPin
		want                 string
	}{
		"text changed in its symbol":   {"package x\n\nfunc Issue() string {\n\treturn \"minted\"\n}\n", "", nil, "anchor stale: a.go#Issue holds no line"},
		"symbol renamed":               {"package x\n\nfunc Mint() string {\n\treturn \"issued\"\n}\n", "", nil, "anchor stale: a.go declares no Issue"},
		"text moved into another func": {"package x\n\nfunc Issue() string { return \"\" }\n\nfunc Other() string {\n\treturn \"issued\"\n}\n", "", nil, "anchor stale: a.go#Issue holds no line"},
		"text twice in its symbol":     {"package x\n\nfunc Issue() string {\n\tif true {\n\t\treturn \"issued\"\n\t}\n\treturn \"issued\"\n}\n", "", nil, "anchor ambiguous: \"return \\\"issued\\\"\" is on 2 lines of a.go#Issue"},
		"symbol declared twice":        {"package x\n\n" + issue + "\n" + issue, "", nil, "anchor ambiguous: a.go declares Issue 2 times"},
		"json text count changed":      {"", "{\n\"aud\": {\n},\n\"aud\": {\n},\n\"aud\": {\n}\n}\n", nil, "occurrence changed: \"\\\"aud\\\": {\" is on 3 lines of s.json"},
		"go pin without a symbol":      {"", "", []anchorPin{{Path: "a.go", Line: 4, Text: "return \"issued\""}, jsonPin}, "the pin names no symbol"},
		"json pin with a symbol":       {"", "", []anchorPin{goPin, {Path: "s.json", Line: 2, Text: "\"aud\": {", Symbol: "aud", Occurrence: 1, Count: 2}}, "only a Go file has symbols"},
	} {
		t.Run(name, func(t *testing.T) {
			base()
			if c.goSource != "" {
				write("a.go", c.goSource)
			}
			if c.jsonSource != "" {
				write("s.json", c.jsonSource)
			}
			pins := c.pins
			if pins == nil {
				pins = []anchorPin{goPin, jsonPin}
			}
			r := run(pins...)
			if len(r.failures) != 1 || !strings.Contains(r.failures[0], c.want) {
				t.Fatalf("want one failure with %q, got %v", c.want, r.failures)
			}
		})
	}
}

func TestAnAnchorOnAnAbsentFileAlwaysFails(t *testing.T) {
	root := t.TempDir()
	for _, path := range []string{"src/dev_health_ops/gone.py", "internal/auth/deleted.go"} {
		document := []byte(fmt.Sprintf(`{"classes":[{"class_id":"demo_class","issuer":{"anchors":[{"repo":"ops","path":%q,"line":1}]}}]}`, path))
		if report := checkCredentialAnchorLines(document, nil, root); len(report.failures) == 0 {
			t.Errorf("an anchor on the absent %s must fail", path)
		}
	}
}

func TestCredentialAnchorLineCheckRefusesAPathOutsideTheCheckout(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "checkout")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, "outside.txt"), []byte("SECRET-OUTSIDE\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"../outside.txt", "/etc/hostname", "a/../../outside.txt"} {
		document := []byte(fmt.Sprintf(`{"classes":[{"class_id":"demo_class","issuer":{"anchors":[{"repo":"ops","path":%q,"line":1}]}}]}`, path))
		pins := []anchorPin{{Path: path, Line: 1, Text: "other"}}
		report := checkCredentialAnchorLines(document, pins, root)
		if len(report.failures) == 0 {
			t.Errorf("path %q must fail", path)
		}
		if strings.Contains(strings.Join(report.failures, "\n"), "SECRET-OUTSIDE") {
			t.Errorf("path %q leaked the outside file: %v", path, report.failures)
		}
	}
}

func TestCredentialAnchorLineCheckRefusesASymlinkOutOfTheCheckout(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "checkout")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, "outside.txt"), []byte("SECRET-OUTSIDE\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(parent, "outside.txt"), filepath.Join(root, "link.go")); err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"classes":[{"class_id":"demo_class","issuer":{"anchors":[{"repo":"ops","path":"link.go","line":1}]}}]}`)
	report := checkCredentialAnchorLines(document, []anchorPin{{Path: "link.go", Line: 1, Text: "other"}}, root)
	if len(report.failures) == 0 {
		t.Fatal("a symlink out of the checkout must fail")
	}
	if strings.Contains(strings.Join(report.failures, "\n"), "SECRET-OUTSIDE") {
		t.Errorf("the outside file leaked: %v", report.failures)
	}
}
