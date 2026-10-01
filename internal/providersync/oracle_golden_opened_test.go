package providersync

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// frozenGoldenSet is one directory of goldens and the table that pins them.
type frozenGoldenSet struct {
	dir  string
	pins map[string]string
}

// frozenGoldenSets are the golden sets whose every golden must be opened by
// the test it belongs to whenever that test runs.
var frozenGoldenSets = []frozenGoldenSet{
	{dir: oraclePairGoldenDir, pins: oraclePairGoldenPins},
}

// TestMain fails the package run when a test that ran did not open a golden
// that belongs to it. A golden is the Python half of one comparison. When the
// comparison is removed from a test, or no longer asks for that pair, the test
// still passes and the golden still exists: without this check nothing would
// say that the comparison stopped.
func TestMain(m *testing.M) {
	code := m.Run()
	_, currentFile, _, _ := runtime.Caller(0)
	selected := testSelection(flagText("test.list"), flagText("test.run"), flagText("test.skip"))
	problems := unopenedGoldens(filepath.Dir(currentFile), frozenGoldenSets, selected, func(name string) bool {
		_, opened := oraclePairGoldensOpened.Load(name)
		return opened
	})
	for _, problem := range problems {
		fmt.Fprintln(os.Stderr, "FAIL: "+problem)
	}
	if len(problems) > 0 {
		code = 1
	}
	os.Exit(code)
}

func flagText(name string) string {
	if found := flag.Lookup(name); found != nil {
		return found.Value.String()
	}
	return ""
}

// unopenedGoldens lists every pinned golden whose test was selected by the run
// and did not open it. A golden that cannot be read is not reported here: the
// inventory tests report it.
func unopenedGoldens(packageDir string, sets []frozenGoldenSet, selected func(testName string) bool, opened func(goldenName string) bool) []string {
	var problems []string
	for _, set := range sets {
		names := make([]string, 0, len(set.pins))
		for name := range set.pins {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			raw, err := os.ReadFile(filepath.Join(packageDir, set.dir, name))
			if err != nil {
				continue
			}
			var file struct {
				Header struct {
					Test string `json:"test"`
				} `json:"header"`
			}
			if json.Unmarshal(raw, &file) != nil || file.Header.Test == "" {
				continue
			}
			if selected(file.Header.Test) && !opened(name) {
				problems = append(problems, fmt.Sprintf("golden %s/%s was not opened by its test %s: the test ran and made no comparison with it. "+
					"A frozen answer that no test compares is a comparison that no longer happens: restore the comparison, or delete the golden and its pin with the test",
					set.dir, name, file.Header.Test))
			}
		}
	}
	return problems
}

// testSelection reports whether a test of the given name (as t.Name() gives
// it) ran, from the values of -test.list, -test.run and -test.skip. It follows
// the rules of the testing package: a filter is split at each slash outside
// brackets and parentheses, element n is matched against level n of the name;
// a test runs when every run element it has a level for matches, and is
// skipped when the whole skip filter matches its leading levels.
func testSelection(list, run, skip string) func(testName string) bool {
	runFilter, skipFilter := splitTestFilter(run), splitTestFilter(skip)
	return func(testName string) bool {
		if list != "" {
			return false
		}
		levels := strings.Split(testName, "/")
		for index, element := range runFilter {
			if index >= len(levels) {
				break
			}
			if matched, err := regexp.MatchString(element, levels[index]); err != nil || !matched {
				return false
			}
		}
		if len(skipFilter) > 0 && len(levels) >= len(skipFilter) {
			skipped := true
			for index, element := range skipFilter {
				if matched, err := regexp.MatchString(element, levels[index]); err != nil || !matched {
					skipped = false
					break
				}
			}
			if skipped {
				return false
			}
		}
		return true
	}
}

// splitTestFilter splits a -test.run or -test.skip value at each slash that
// is outside a character class and outside parentheses.
func splitTestFilter(filter string) []string {
	if filter == "" {
		return nil
	}
	var elements []string
	classDepth, groupDepth, start := 0, 0, 0
	for index := 0; index < len(filter); index++ {
		switch filter[index] {
		case '[':
			classDepth++
		case ']':
			if classDepth > 0 {
				classDepth--
			}
		case '(':
			if classDepth == 0 {
				groupDepth++
			}
		case ')':
			if classDepth == 0 {
				groupDepth--
			}
		case '\\':
			index++
		case '/':
			if classDepth == 0 && groupDepth == 0 {
				elements = append(elements, filter[start:index])
				start = index + 1
			}
		}
	}
	return append(elements, filter[start:])
}

// TestTheRunSelectionFollowsTheTestFlags pins testSelection against the cases
// that decide whether a golden must have been opened.
func TestTheRunSelectionFollowsTheTestFlags(t *testing.T) {
	cases := []struct {
		list, run, skip, name string
		want                  bool
	}{
		{"", "", "", "TestA", true},
		{"", "", "", "TestA/sub", true},
		{".", "", "", "TestA", false},
		{"", "^TestA$", "", "TestA", true},
		{"", "^TestA$", "", "TestA/sub", true},
		{"", "^TestA$", "", "TestAB", false},
		{"", "^(TestA|TestB)$", "", "TestB", true},
		{"", "^(TestA|TestB)$", "", "TestC", false},
		{"", "TestA/sub", "", "TestA", true},
		{"", "TestA/sub", "", "TestA/sub", true},
		{"", "TestA/sub", "", "TestA/other", false},
		{"", "TestA/sub", "", "TestB/sub", false},
		{"", "Test[A/]x/sub", "", "TestAx/sub", true},
		{"", "^$", "", "TestA", false},
		{"", "", "^TestA$", "TestA", false},
		{"", "", "^TestA$", "TestA/sub", false},
		{"", "", "^TestA$", "TestB", true},
		{"", "", "TestA/sub", "TestA", true},
		{"", "", "TestA/sub", "TestA/sub", false},
		{"", "", "TestA/sub", "TestA/other", true},
		{"", "^TestA$", "TestA/sub", "TestA/sub", false},
	}
	for _, test := range cases {
		if got := testSelection(test.list, test.run, test.skip)(test.name); got != test.want {
			t.Errorf("list=%q run=%q skip=%q: %s selected=%t, want %t", test.list, test.run, test.skip, test.name, got, test.want)
		}
	}
}

// TestAGoldenItsTestDidNotOpenIsReported pins unopenedGoldens: a golden is
// reported when its test ran and did not open it, and only then.
func TestAGoldenItsTestDidNotOpenIsReported(t *testing.T) {
	packageDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(packageDir, "goldens"), 0o755); err != nil {
		t.Fatal(err)
	}
	pins := map[string]string{}
	for name, test := range map[string]string{"opened.json": "TestOpened", "unopened.json": "TestUnopened/sub", "not-run.json": "TestNotRun"} {
		if err := os.WriteFile(filepath.Join(packageDir, "goldens", name), []byte(`{"header":{"test":"`+test+`"}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		pins[name] = "pinned"
	}
	pins["missing.json"] = "pinned"
	problems := unopenedGoldens(packageDir, []frozenGoldenSet{{dir: "goldens", pins: pins}},
		func(testName string) bool { return testName != "TestNotRun" },
		func(goldenName string) bool { return goldenName == "opened.json" })
	if len(problems) != 1 || !strings.Contains(problems[0], "goldens/unopened.json") || !strings.Contains(problems[0], "TestUnopened/sub") {
		t.Fatalf("problems = %q, want exactly the unopened golden of the test that ran", problems)
	}
}
