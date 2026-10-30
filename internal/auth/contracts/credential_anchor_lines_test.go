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
)

// Every file:line anchor in credential-classes.json that cites this repository
// is pinned to the text of the line it names, and, when the anchor has a
// line_end, to the text of the range's last real line, in
// credentialanchors/credential-anchor-lines.json. A line that moves fails here,
// naming the class, the anchor kind, the file:line and the expected text,
// instead of leaving a stale citation. An anchor with no pin, a pin no anchor
// uses, an unknown repo, a path that leaves the checkout (lexically or through
// a symlink), an anchor on a blank or brace-only line, a line_end before the
// line or past the end of the file, and an anchor on an absent file all fail;
// none is skipped. A pin may not carry an offset: the anchor names its first
// real line.
//
// The anchors on the deleted Python tree (src/dev_health_ops/*.py) are gone:
// credentialanchors/credential-anchor-retired-evidence.tsv says, for each one,
// the line it cited (path@commit^:line, commit = the one that deleted the file)
// and that line's text, and whether it was re-pointed to the one Go declaration
// whose doc says it is that Python symbol, or replaced by null with a gaps entry
// of its class that quotes the text. The history test (build tag authhistory,
// run by `ci/check_go.sh auth history` with full history) checks the rows
// against git: the row set equals the base registry's Python anchors, each
// commit deleted its file, each range fits and each text matches.
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
// committing: a pin that changed is a line that moved.

type anchorPin struct {
	Path string `json:"path"`
	Line int    `json:"line"`
	End  int    `json:"end,omitempty"`
	Text string `json:"text"`
	// EndBack is how many blank or brace-only lines the end pin walks back from line_end to the range's last real line.
	EndBack int    `json:"end_back,omitempty"`
	EndText string `json:"end_text,omitempty"`
}

type anchorReport struct {
	failures []string
	checked  int
	ranged   int
	foreign  int
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
	found := "<past end of file>"
	if line >= 1 && line <= len(lines) {
		found = strings.TrimSpace(lines[line-1])
	}
	if found != pin.Text {
		report.failures = append(report.failures, fmt.Sprintf("%s: expected line text %q at %s:%d, found %q", label, pin.Text, path, line, found))
	}
	if ref.end == 0 {
		if pin.EndText != "" || pin.EndBack != 0 {
			report.failures = append(report.failures, fmt.Sprintf("%s: the pin has an end but the anchor has no line_end", label))
		}
		return
	}
	report.ranged++
	switch {
	case ref.end < line:
		report.failures = append(report.failures, fmt.Sprintf("%s: line_end %d is before line %d", label, ref.end, line))
		return
	case ref.end > len(lines):
		report.failures = append(report.failures, fmt.Sprintf("%s: line_end %d is past the end of the file (%d lines)", label, ref.end, len(lines)))
		return
	case pin.EndBack < 0:
		report.failures = append(report.failures, fmt.Sprintf("%s: end_back %d is negative", label, pin.EndBack))
		return
	case trivialAnchorText(pin.EndText):
		report.failures = append(report.failures, fmt.Sprintf("%s: pinned end text %q is blank or brace-only and cannot detect a change", label, pin.EndText))
		return
	}
	for step := 0; step < pin.EndBack; step++ {
		at := ref.end - step
		if at < line || !trivialAnchorText(strings.TrimSpace(lines[at-1])) {
			report.failures = append(report.failures, fmt.Sprintf("%s: end_back %d crosses line %d, which is not blank or brace-only", label, pin.EndBack, at))
			return
		}
	}
	at := ref.end - pin.EndBack
	if at < line {
		report.failures = append(report.failures, fmt.Sprintf("%s: end_back %d walks before line %d", label, pin.EndBack, line))
		return
	}
	if endFound := strings.TrimSpace(lines[at-1]); endFound != pin.EndText {
		report.failures = append(report.failures, fmt.Sprintf("%s: expected range end text %q at %s:%d, found %q", label, pin.EndText, path, at, endFound))
	}
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
		if ref.end != 0 {
			if ref.end < ref.line || ref.end > len(lines) {
				t.Fatalf("%s:%d-%d: line_end is before the line or past the end of the file: fix the registry first", ref.path, ref.line, ref.end)
			}
			for ref.end-pin.EndBack > ref.line && trivialAnchorText(strings.TrimSpace(lines[ref.end-pin.EndBack-1])) {
				pin.EndBack++
			}
			pin.EndText = strings.TrimSpace(lines[ref.end-pin.EndBack-1])
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
		t.Fatalf("credentialanchors/credential-anchor-lines.json: %v (a pin carries path, line, end, text, end_back and end_text only)", err)
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
	t.Logf("anchors: %d line-checked (%d with a range end), %d in web/acr (counted, not checked)", report.checked, report.ranged, report.foreign)
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
	classID, where, old, oldText, action, successor, doc string
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
		if len(f) != 7 {
			t.Fatalf("evidence row %q has %d fields, want 7", line, len(f))
		}
		rows = append(rows, retiredEvidence{f[0], f[1], f[2], f[3], f[4], f[5], f[6]})
	}
	if !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(base) {
		t.Fatalf("the evidence header names base registry commit %q, want a 40-hex main commit", base)
	}
	return rows, base
}

// retiredGap is the gaps entry a null row adds to its class: it quotes the cited line.
func retiredGap(r retiredEvidence) string {
	return fmt.Sprintf("retired anchor %s: %s (%s) was deleted with the Python api; no single documented Go successor", r.where, r.old, r.oldText)
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
		node, ok := registryNode(class, r.where)
		if !ok {
			failures = append(failures, label+": no such anchor slot in the registry")
			continue
		}
		switch r.action {
		case "repoint":
			s := successorFmt.FindStringSubmatch(r.successor)
			anchor, isMap := node.(map[string]any)
			if s == nil || !isMap {
				failures = append(failures, fmt.Sprintf("%s: a re-pointed slot holds %v, successor %q", label, node, r.successor))
				continue
			}
			line, _ := strconv.Atoi(s[2])
			end, _ := strconv.Atoi(s[3])
			got := fmt.Sprintf("%v:%v:%v-%v", anchor["repo"], anchor["path"], anchor["line"], anchor["line_end"])
			if want := fmt.Sprintf("ops:%s:%d-%d", s[1], line, end); got != want {
				failures = append(failures, fmt.Sprintf("%s: the registry anchor is %s, the evidence says %s", label, got, want))
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
			gaps, _ := class["gaps"].([]any)
			found := false
			for _, g := range gaps {
				if g == retiredGap(r) {
					found = true
				}
			}
			if !found {
				failures = append(failures, fmt.Sprintf("%s: the class gaps lack %q", label, retiredGap(r)))
			}
		default:
			failures = append(failures, fmt.Sprintf("%s: action %q is not repoint or null", label, r.action))
		}
	}
	sort.Strings(failures)
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
	repoint := retiredEvidence{"demo", "validators[0]/anchor", "src/dev_health_ops/check.py@0123456789ab^:10-20", "def verify_token(token):", "repoint", "internal/x/a.go:1-2", "check.py's verify_token"}
	null := retiredEvidence{"demo", "issuer/anchors[0]", "src/dev_health_ops/check.py@0123456789ab^:5", "TOKEN_TTL = 300", "null", "-", "-"}
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
		{Path: "internal/x/a.go", Line: 3, Text: "func Issue() {}"},
		{Path: "internal/x/a.go", Line: 4, Text: "func Validate() {}"},
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

	orphan := checkCredentialAnchorLines(fixture(3), append(pins, anchorPin{Path: "internal/x/a.go", Line: 1, Text: "package x"}), root)
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
	pins := []anchorPin{{Path: "a.go", Line: 3, End: 5, Text: "func Verify() bool {", EndBack: 1, EndText: "return false"}}
	if report := checkCredentialAnchorLines(doc(3, 5), pins, root); len(report.failures) != 0 || report.ranged != 1 {
		t.Fatalf("a range pinned at its last real line must pass: %+v", report)
	}
	write("package x\n\nfunc Verify() bool {\n\treturn true\n}\n")
	if report := checkCredentialAnchorLines(doc(3, 5), pins, root); len(report.failures) == 0 {
		t.Error("a changed last line of the range must fail although the first line holds")
	}
	write(source)
	for name, c := range map[string]struct {
		line, end int
		pins      []anchorPin
	}{
		"line_end past the end of the file": {3, 9, []anchorPin{{Path: "a.go", Line: 3, End: 9, Text: "func Verify() bool {", EndText: "x"}}},
		"line_end before the line":          {3, 2, []anchorPin{{Path: "a.go", Line: 3, End: 2, Text: "func Verify() bool {", EndText: "x"}}},
		"end_back across a real line":       {3, 5, []anchorPin{{Path: "a.go", Line: 3, End: 5, Text: "func Verify() bool {", EndBack: 2, EndText: "func Verify() bool {"}}},
		"a brace-only end text":             {3, 5, []anchorPin{{Path: "a.go", Line: 3, End: 5, Text: "func Verify() bool {", EndText: "}"}}},
	} {
		if report := checkCredentialAnchorLines(doc(c.line, c.end), c.pins, root); len(report.failures) == 0 {
			t.Errorf("%s must fail", name)
		}
	}
}

func TestCredentialAnchorLineCheckRefusesABlankOrBraceOnlyAnchorLine(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.go"), []byte("package x\n\nfunc Verify() bool { return false }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"classes":[{"class_id":"demo_class","issuer":{"anchors":[{"repo":"ops","path":"a.go","line":2}]}}]}`)
	for _, pins := range [][]anchorPin{
		{{Path: "a.go", Line: 2, Text: ""}},
		{{Path: "a.go", Line: 2, Text: "func Verify() bool { return false }"}},
	} {
		if report := checkCredentialAnchorLines(document, pins, root); len(report.failures) == 0 {
			t.Errorf("an anchor on a blank line must fail (pin %+v): it names its first real line instead", pins[0])
		}
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
