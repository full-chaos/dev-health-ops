package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Every file:line anchor in credential-classes.json that cites this repository
// is pinned to the text of the line it names in
// testdata/credential-anchor-lines.json. A line that moves fails here, naming
// the class, the anchor kind, the file:line and the expected text, instead of
// leaving a stale citation. An anchor with no pin, a pin no anchor uses, an
// unknown repo and a path that leaves the checkout (lexically or through a
// symlink) all fail; none is skipped.
//
// An anchor whose file is absent fails unless its path is listed in
// testdata/credential-anchor-retired-paths.txt (one path per line). The list
// may hold only absent paths that some anchor still cites: a listed file that
// exists, or a listed path no anchor cites, fails. Purging those anchors from
// the registry is a separate change.
//
// Anchors in the web and acr repositories are counted, not checked: those
// files are not in this tree.
//
// Re-pin after a deliberate line move, in one step, from the repo root:
//
//	CREDENTIAL_ANCHOR_UPDATE=1 go test ./internal/auth/contracts/ -run TestCredentialClassAnchorsPointAtTheirPinnedLineText
//
// That rewrites both testdata files from the registry and the current source,
// then runs the check against them. Read the diff of the two files before
// committing: a pin that changed is a line that moved.

type anchorPin struct {
	Path   string `json:"path"`
	Line   int    `json:"line"`
	Offset int    `json:"offset,omitempty"`
	Text   string `json:"text"`
}

type anchorReport struct {
	failures []string
	checked  int
	retired  int
	foreign  int

	retiredByKind map[string]int
}

var foreignAnchorRepos = map[string]bool{"web": true, "acr": true}

type credentialAnchorRef struct {
	classID string
	where   string
	repo    string
	path    string
	line    int
}

func (a credentialAnchorRef) kind() string {
	where := strings.TrimPrefix(a.where, "/")
	if i := strings.IndexAny(where, "/["); i > 0 {
		return where[:i]
	}
	return where
}

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
					refs = append(refs, credentialAnchorRef{classID: classID, where: where, repo: repo, path: path, line: int(line)})
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

func checkCredentialAnchorLines(document []byte, pins []anchorPin, retiredPaths []string, root string) anchorReport {
	report := anchorReport{retiredByKind: map[string]int{}}
	refs, err := collectCredentialAnchorRefs(document)
	if err != nil {
		report.failures = append(report.failures, fmt.Sprintf("credential-classes.json does not parse: %v", err))
		return report
	}
	pinned := map[string]anchorPin{}
	used := map[string]bool{}
	for _, pin := range pins {
		key := fmt.Sprintf("%s:%d", pin.Path, pin.Line)
		if _, dup := pinned[key]; dup {
			report.failures = append(report.failures, fmt.Sprintf("duplicate pin for %s", key))
		}
		pinned[key] = pin
	}
	retiredSet := map[string]bool{}
	for _, path := range retiredPaths {
		retiredSet[path] = true
	}
	retiredUsed := map[string]bool{}
	lineCache := map[string][]string{}
	for _, ref := range refs {
		checkOneAnchor(&report, root, ref, pinned, used, retiredSet, retiredUsed, lineCache)
	}
	for key := range pinned {
		if !used[key] {
			report.failures = append(report.failures, fmt.Sprintf("pin %s is used by no anchor: remove it or restore the anchor", key))
		}
	}
	for path := range retiredSet {
		if !retiredUsed[path] {
			report.failures = append(report.failures, fmt.Sprintf("retired path %s is cited by no anchor or the file exists: remove it from the retired list", path))
		}
	}
	sort.Strings(report.failures)
	return report
}

func checkOneAnchor(report *anchorReport, root string, ref credentialAnchorRef, pinned map[string]anchorPin, used, retiredSet, retiredUsed map[string]bool, lineCache map[string][]string) {
	path, line := ref.path, ref.line
	label := fmt.Sprintf("class %s anchor %s %s:%d", ref.classID, strings.TrimPrefix(ref.where, "/"), path, line)
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
			if retiredSet[path] {
				retiredUsed[path] = true
				report.retired++
				report.retiredByKind[ref.kind()]++
				return
			}
			report.failures = append(report.failures, fmt.Sprintf("%s: file is absent and its path is not in testdata/credential-anchor-retired-paths.txt", label))
			return
		}
		report.failures = append(report.failures, fmt.Sprintf("%s: file does not resolve: %v", label, err))
		return
	}
	if retiredSet[path] {
		report.failures = append(report.failures, fmt.Sprintf("%s: path is in the retired list but the file exists; remove it from the list and pin it", label))
	}
	key := fmt.Sprintf("%s:%d", path, line)
	pin, ok := pinned[key]
	if !ok {
		var others []string
		for pinKey, other := range pinned {
			if strings.HasPrefix(pinKey, path+":") {
				others = append(others, fmt.Sprintf("%s=%q", pinKey, other.Text))
			}
		}
		sort.Strings(others)
		report.failures = append(report.failures, fmt.Sprintf("%s: no pin at this line in testdata/credential-anchor-lines.json; pinned lines of this file: %s", label, strings.Join(others, ", ")))
		return
	}
	used[key] = true
	report.checked++
	if trivialAnchorText(pin.Text) {
		report.failures = append(report.failures, fmt.Sprintf("%s: pinned text %q is blank or brace-only and cannot detect a change; pin the first real line with an offset", label, pin.Text))
		return
	}
	if pin.Offset < 0 {
		report.failures = append(report.failures, fmt.Sprintf("%s: pin offset %d is negative", label, pin.Offset))
		return
	}
	for step := 0; step < pin.Offset; step++ {
		at := line + step
		if at < 1 || at > len(lines) || !trivialAnchorText(strings.TrimSpace(lines[at-1])) {
			report.failures = append(report.failures, fmt.Sprintf("%s: pin offset %d crosses line %d, which is not blank or brace-only", label, pin.Offset, at))
			return
		}
	}
	at := line + pin.Offset
	found := "<past end of file>"
	if at >= 1 && at <= len(lines) {
		found = strings.TrimSpace(lines[at-1])
	}
	if found != pin.Text {
		report.failures = append(report.failures, fmt.Sprintf("%s: expected line text %q at %s:%d, found %q", label, pin.Text, path, at, found))
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

func loadRetiredPaths(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "credential-anchor-retired-paths.txt"))
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, line := range strings.Split(string(raw), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			paths = append(paths, line)
		}
	}
	return paths
}

// regenerateAnchorFiles rewrites the pin file and the retired-path list from
// the registry and the current source. Only the update switch calls it.
func regenerateAnchorFiles(t *testing.T, root string, document []byte) {
	t.Helper()
	refs, err := collectCredentialAnchorRefs(document)
	if err != nil {
		t.Fatal(err)
	}
	pins := map[string]anchorPin{}
	retired := map[string]bool{}
	lineCache := map[string][]string{}
	for _, ref := range refs {
		if ref.repo != "ops" || !filepath.IsLocal(ref.path) {
			continue
		}
		lines, err := readAnchorFile(root, ref.path, lineCache)
		if os.IsNotExist(err) {
			retired[ref.path] = true
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", ref.path, err)
		}
		pin := anchorPin{Path: ref.path, Line: ref.line}
		for ref.line+pin.Offset <= len(lines) && trivialAnchorText(strings.TrimSpace(lines[ref.line-1+pin.Offset])) {
			pin.Offset++
		}
		if ref.line+pin.Offset > len(lines) {
			t.Fatalf("%s:%d is past the end of the file: fix the registry first", ref.path, ref.line)
		}
		pin.Text = strings.TrimSpace(lines[ref.line-1+pin.Offset])
		pins[fmt.Sprintf("%s:%d", ref.path, ref.line)] = pin
	}
	list := make([]anchorPin, 0, len(pins))
	for _, pin := range pins {
		list = append(list, pin)
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].Path != list[j].Path {
			return list[i].Path < list[j].Path
		}
		return list[i].Line < list[j].Line
	})
	encoded, err := json.MarshalIndent(list, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("testdata", "credential-anchor-lines.json"), append(encoded, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	paths := make([]string, 0, len(retired))
	for path := range retired {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	if err := os.WriteFile(filepath.Join("testdata", "credential-anchor-retired-paths.txt"), []byte(strings.Join(paths, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func loadAnchorPins(t *testing.T) []anchorPin {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "credential-anchor-lines.json"))
	if err != nil {
		t.Fatal(err)
	}
	var pins []anchorPin
	if err := json.Unmarshal(raw, &pins); err != nil {
		t.Fatal(err)
	}
	return pins
}

func TestCredentialClassAnchorsPointAtTheirPinnedLineText(t *testing.T) {
	root := testRoot(t)
	raw, err := os.ReadFile(filepath.Join(ContractsDir(root), "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("CREDENTIAL_ANCHOR_UPDATE") == "1" {
		regenerateAnchorFiles(t, root, raw)
	}
	report := checkCredentialAnchorLines(raw, loadAnchorPins(t), loadRetiredPaths(t), root)
	for _, failure := range report.failures {
		t.Error(failure)
	}
	if report.checked == 0 {
		t.Fatal("no ops anchor was measured")
	}
	t.Logf("anchors: %d line-checked, %d on retired absent paths, %d in web/acr (counted, not checked)", report.checked, report.retired, report.foreign)
	kinds := make([]string, 0, len(report.retiredByKind))
	for kind := range report.retiredByKind {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		t.Logf("retired anchors by kind: %s=%d", kind, report.retiredByKind[kind])
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

	if report := checkCredentialAnchorLines(fixture(3), pins, nil, root); len(report.failures) != 0 || report.checked != 2 {
		t.Fatalf("a fixture whose anchors hold their text must pass with 2 checked: %+v", report)
	}

	moved := checkCredentialAnchorLines(fixture(2), pins, nil, root)
	want := []string{"demo_class", "issuer/anchors[0]", "internal/x/a.go:2", "func Issue() {}"}
	if len(moved.failures) == 0 {
		t.Fatal("a moved anchor must fail")
	}
	for _, part := range want {
		if !strings.Contains(strings.Join(moved.failures, "\n"), part) {
			t.Errorf("failure does not name %q: %v", part, moved.failures)
		}
	}

	orphan := checkCredentialAnchorLines(fixture(3), append(pins, anchorPin{Path: "internal/x/a.go", Line: 1, Text: "package x"}), nil, root)
	if len(orphan.failures) == 0 {
		t.Error("a pin no anchor uses must fail")
	}

	foreignRepo := []byte(`{"classes":[{"class_id":"demo_class","issuer":{"anchors":[{"repo":"nowhere","path":"a","line":1}]}}]}`)
	if report := checkCredentialAnchorLines(foreignRepo, nil, nil, root); len(report.failures) == 0 {
		t.Error("an unknown repo must fail")
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
		report := checkCredentialAnchorLines(document, pins, nil, root)
		if len(report.failures) == 0 {
			t.Errorf("path %q must fail", path)
		}
		if strings.Contains(strings.Join(report.failures, "\n"), "SECRET-OUTSIDE") {
			t.Errorf("path %q leaked the outside file: %v", path, report.failures)
		}
	}
}

func TestCredentialAnchorLineCheckPinsTheFirstRealLineWhenTheAnchorLineIsBlank(t *testing.T) {
	root := t.TempDir()
	source := "package x\n\nfunc Verify() bool { return false }\n"
	if err := os.WriteFile(filepath.Join(root, "a.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"classes":[{"class_id":"demo_class","issuer":{"anchors":[{"repo":"ops","path":"a.go","line":2}]}}]}`)
	pins := []anchorPin{{Path: "a.go", Line: 2, Offset: 1, Text: "func Verify() bool { return false }"}}
	if report := checkCredentialAnchorLines(document, pins, nil, root); len(report.failures) != 0 {
		t.Fatalf("a blank anchor line pinned at its first real line must pass: %v", report.failures)
	}
	changed := "package x\n\nfunc Verify() bool { return true }\n"
	if err := os.WriteFile(filepath.Join(root, "a.go"), []byte(changed), 0o644); err != nil {
		t.Fatal(err)
	}
	if report := checkCredentialAnchorLines(document, pins, nil, root); len(report.failures) == 0 {
		t.Fatal("a changed first real line must fail even though the anchor line is still blank")
	}
	blankPin := []anchorPin{{Path: "a.go", Line: 2, Text: ""}}
	if report := checkCredentialAnchorLines(document, blankPin, nil, root); len(report.failures) == 0 {
		t.Fatal("a blank or brace-only pin must fail: it cannot detect any change")
	}
	farOffset := []anchorPin{{Path: "a.go", Line: 1, Offset: 2, Text: "func Verify() bool { return true }"}}
	farDoc := []byte(`{"classes":[{"class_id":"demo_class","issuer":{"anchors":[{"repo":"ops","path":"a.go","line":1}]}}]}`)
	if report := checkCredentialAnchorLines(farDoc, farOffset, nil, root); len(report.failures) == 0 {
		t.Fatal("an offset is allowed only across blank or brace-only lines; line 1 is real")
	}
}

func TestCredentialAnchorRetiredListIsAnExplicitAllowlistOfAbsentPaths(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "present.go"), []byte("package x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	doc := func(path string) []byte {
		return []byte(fmt.Sprintf(`{"classes":[{"class_id":"demo_class","issuer":{"anchors":[{"repo":"ops","path":%q,"line":1}]}}]}`, path))
	}
	if report := checkCredentialAnchorLines(doc("gone.py"), nil, nil, root); len(report.failures) == 0 {
		t.Error("an absent file not in the retired list must fail")
	}
	report := checkCredentialAnchorLines(doc("gone.py"), nil, []string{"gone.py"}, root)
	if len(report.failures) != 0 || report.retired != 1 || report.retiredByKind["issuer"] != 1 {
		t.Errorf("an absent listed file must pass and be counted by kind: %+v", report)
	}
	pins := []anchorPin{{Path: "present.go", Line: 1, Text: "package x"}}
	if report := checkCredentialAnchorLines(doc("present.go"), pins, []string{"present.go"}, root); len(report.failures) == 0 {
		t.Error("a listed path whose file exists must fail")
	}
	if report := checkCredentialAnchorLines(doc("present.go"), pins, []string{"never-cited.py"}, root); len(report.failures) == 0 {
		t.Error("a listed path no anchor cites must fail")
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
	report := checkCredentialAnchorLines(document, []anchorPin{{Path: "link.go", Line: 1, Text: "other"}}, nil, root)
	if len(report.failures) == 0 {
		t.Fatal("a symlink out of the checkout must fail")
	}
	if strings.Contains(strings.Join(report.failures, "\n"), "SECRET-OUTSIDE") {
		t.Errorf("the outside file leaked: %v", report.failures)
	}
}
