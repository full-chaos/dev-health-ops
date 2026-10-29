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

// Every file:line anchor in credential-classes.json is pinned to the text of
// the line it names in testdata/credential-anchor-lines.json. A line that
// moves fails here instead of leaving a stale citation. An anchor with no pin,
// a pin no anchor uses, and an unknown repo all fail; none is skipped.

type anchorPin struct {
	Path string `json:"path"`
	Line int    `json:"line"`
	Text string `json:"text"`
}

type anchorReport struct {
	failures []string
	checked  int
	retired  int
	foreign  int
}

const retiredPythonPrefix = "src/dev_health_ops/"

var foreignAnchorRepos = map[string]bool{"web": true, "acr": true}

func checkCredentialAnchorLines(document []byte, pins []anchorPin, root string) anchorReport {
	var report anchorReport
	var parsed struct {
		Classes []map[string]any `json:"classes"`
	}
	if err := json.Unmarshal(document, &parsed); err != nil {
		report.failures = append(report.failures, fmt.Sprintf("credential-classes.json does not parse: %v", err))
		return report
	}
	pinned := map[string]string{}
	used := map[string]bool{}
	for _, pin := range pins {
		key := fmt.Sprintf("%s:%d", pin.Path, pin.Line)
		if _, dup := pinned[key]; dup {
			report.failures = append(report.failures, fmt.Sprintf("duplicate pin for %s", key))
		}
		pinned[key] = pin.Text
	}
	lineCache := map[string][]string{}
	var walk func(classID, where string, node any)
	walk = func(classID, where string, node any) {
		switch typed := node.(type) {
		case map[string]any:
			if path, ok := typed["path"].(string); ok {
				if line, ok := typed["line"].(float64); ok {
					checkOneAnchor(&report, root, classID, where, typed["repo"], path, int(line), pinned, used, lineCache)
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
	for key := range pinned {
		if !used[key] {
			report.failures = append(report.failures, fmt.Sprintf("pin %s is used by no anchor: remove it or restore the anchor", key))
		}
	}
	sort.Strings(report.failures)
	return report
}

func checkOneAnchor(report *anchorReport, root, classID, where string, repo any, path string, line int, pinned map[string]string, used map[string]bool, lineCache map[string][]string) {
	label := fmt.Sprintf("class %s anchor %s %s:%d", classID, strings.TrimPrefix(where, "/"), path, line)
	repoName, _ := repo.(string)
	if foreignAnchorRepos[repoName] {
		report.foreign++
		return
	}
	if repoName != "ops" {
		report.failures = append(report.failures, fmt.Sprintf("%s: repo %q is not ops, web or acr", label, repoName))
		return
	}
	lines, err := readAnchorFile(root, path, lineCache)
	if err != nil {
		if os.IsNotExist(err) && strings.HasPrefix(path, retiredPythonPrefix) {
			report.retired++
			return
		}
		report.failures = append(report.failures, fmt.Sprintf("%s: file does not resolve: %v", label, err))
		return
	}
	if strings.HasPrefix(path, retiredPythonPrefix) {
		report.failures = append(report.failures, fmt.Sprintf("%s: retired python path exists again; pin it", label))
	}
	key := fmt.Sprintf("%s:%d", path, line)
	expected, ok := pinned[key]
	if !ok {
		var others []string
		for pinKey, text := range pinned {
			if strings.HasPrefix(pinKey, path+":") {
				others = append(others, fmt.Sprintf("%s=%q", pinKey, text))
			}
		}
		sort.Strings(others)
		report.failures = append(report.failures, fmt.Sprintf("%s: no pin at this line in testdata/credential-anchor-lines.json; pinned lines of this file: %s", label, strings.Join(others, ", ")))
		return
	}
	used[key] = true
	report.checked++
	found := "<past end of file>"
	if line >= 1 && line <= len(lines) {
		found = strings.TrimSpace(lines[line-1])
	}
	if found != expected {
		report.failures = append(report.failures, fmt.Sprintf("%s: expected line text %q, found %q", label, expected, found))
	}
}

func readAnchorFile(root, path string, cache map[string][]string) ([]string, error) {
	if lines, ok := cache[path]; ok {
		return lines, nil
	}
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(raw), "\n")
	cache[path] = lines
	return lines, nil
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
	report := checkCredentialAnchorLines(raw, loadAnchorPins(t), root)
	for _, failure := range report.failures {
		t.Error(failure)
	}
	if report.checked == 0 {
		t.Fatal("no ops anchor was measured")
	}
	t.Logf("anchors: %d line-checked, %d retired python (file absent), %d in other repos (not in this tree)", report.checked, report.retired, report.foreign)
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
	want := []string{"demo_class", "issuer/anchors[0]", "internal/x/a.go:2", "func Issue() {}"}
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
