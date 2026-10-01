package venueoracle

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
)

// A golden is the Python half of one comparison. When the comparison is taken
// out of a test, or the test stops asking for that golden, the test still
// passes and the file still sits in testdata with its pin: nothing says that
// the comparison stopped. RunTests closes that: after a package's tests ran,
// every golden of the package whose test ran must have been opened by it and
// taken to Finish.

// goldenUse is what a test did with one golden in this process.
type goldenUse struct {
	mu                               sync.Mutex
	finished, ended, skipped, failed bool
}

// goldenUses holds a goldenUse per golden file, by absolute path.
var goldenUses sync.Map

// noteOpened records that t opened the golden at path, and at the test's end
// how the test ended. The use is the same one for every open of that file.
func noteOpened(t *testing.T, path string) *goldenUse {
	t.Helper()
	absolute, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	stored, _ := goldenUses.LoadOrStore(absolute, &goldenUse{})
	use := stored.(*goldenUse)
	t.Cleanup(func() {
		use.mu.Lock()
		defer use.mu.Unlock()
		use.ended = true
		// One test that skipped or failed explains a golden that was not
		// finished; a later test of the same golden that passes must finish it.
		use.skipped = use.skipped || t.Skipped()
		use.failed = use.failed || t.Failed()
	})
	return use
}

func (use *goldenUse) finish() {
	if use == nil {
		return
	}
	use.mu.Lock()
	use.finished = true
	use.mu.Unlock()
}

// RunTests is m.Run for a package that holds goldens: a package's TestMain
// calls os.Exit(venueoracle.RunTests(m)). When the tests passed, it fails the
// run for each golden under the package's testdata whose test ran in this
// binary and did not open it, or opened it and never reached Finish. A test
// that is not compiled into this binary (another build tag), that the -run and
// -skip flags did not select, or that skipped or failed after it opened its
// golden, does not trip it. A test must open its golden before it can skip.
func RunTests(m *testing.M) int { return runTests(m, "testdata") }

// runTests is RunTests for the goldens under dir.
func runTests(m *testing.M, dir string) int {
	code := m.Run()
	if code != 0 {
		return code
	}
	compiled, err := compiledTests(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "FAIL: "+err.Error())
		return 1
	}
	if flagText("test.list") != "" {
		return code
	}
	problems, err := unusedGoldens(dir, compiled, testSelection(flagText("test.run"), flagText("test.skip")), func(path string) (opened, finished, excused bool) {
		stored, ok := goldenUses.Load(path)
		if !ok {
			return false, false, false
		}
		use := stored.(*goldenUse)
		use.mu.Lock()
		defer use.mu.Unlock()
		return true, use.finished, use.skipped || use.failed
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "FAIL: "+err.Error())
		return 1
	}
	for _, problem := range problems {
		fmt.Fprintln(os.Stderr, "FAIL: "+problem)
	}
	if len(problems) > 0 {
		return 1
	}
	return code
}

func flagText(name string) string {
	if found := flag.Lookup(name); found != nil {
		return found.Value.String()
	}
	return ""
}

// compiledTests is the names of the top-level tests of this test binary. The
// testing package does not offer them, so they are read from the M it built;
// a testing package that no longer holds them there is an error, not an empty
// list (an empty list would pass every golden).
func compiledTests(m *testing.M) (map[string]bool, error) {
	changed := errors.New("venueoracle: the testing package no longer holds a binary's tests where RunTests reads them (testing.M.tests[].Name): the check which goldens were opened cannot be made")
	if m == nil {
		return nil, changed
	}
	tests := reflect.ValueOf(m).Elem().FieldByName("tests")
	if !tests.IsValid() || tests.Kind() != reflect.Slice {
		return nil, changed
	}
	names := map[string]bool{}
	for index := 0; index < tests.Len(); index++ {
		name := tests.Index(index).FieldByName("Name")
		if !name.IsValid() || name.Kind() != reflect.String {
			return nil, changed
		}
		names[name.String()] = true
	}
	return names, nil
}

// unusedGoldens lists a problem for each golden file under dir whose test is
// compiled into this binary and was selected, and which that test did not open
// or did not finish. A file that is not a golden of this harness (no header
// with the Python build and the test) is not a golden; one that cannot be read
// is an error.
func unusedGoldens(dir string, compiled map[string]bool, selected func(test string) bool, used func(path string) (opened, finished, excused bool)) ([]string, error) {
	var problems []string
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) && path == dir {
			return filepath.SkipAll
		}
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var file struct {
			Header struct {
				Test        string `json:"test"`
				PythonBuild string `json:"python_build"`
			} `json:"header"`
		}
		if json.Unmarshal(raw, &file) != nil || file.Header.PythonBuild == "" || file.Header.Test == "" {
			return nil
		}
		test := file.Header.Test
		if !compiled[strings.SplitN(test, "/", 2)[0]] || !selected(test) {
			return nil
		}
		absolute, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		opened, finished, excused := used(absolute)
		switch {
		case !opened:
			problems = append(problems, fmt.Sprintf("golden %s was not opened by its test %s: the test ran and made no comparison with it. A frozen answer that no test compares is a comparison that no longer happens: restore the comparison (a test opens its golden before it can skip), or delete the golden and its pin with the test", path, test))
		case !finished && !excused:
			problems = append(problems, fmt.Sprintf("golden %s was opened by its test %s and never finished: the test passed without Golden.Finish, so nothing shows that its answers were compared", path, test))
		}
		return nil
	})
	sort.Strings(problems)
	return problems, err
}

// testSelection reports whether a test of the given name (as t.Name() gives
// it) ran, from the values of -test.run and -test.skip. It follows the rules
// of the testing package: a filter is split at each slash outside brackets and
// parentheses, element n is matched against level n of the name; a test runs
// when every run element it has a level for matches, and is skipped when the
// whole skip filter matches its leading levels.
func testSelection(run, skip string) func(test string) bool {
	runFilter, skipFilter := splitTestFilter(run), splitTestFilter(skip)
	return func(test string) bool {
		levels := strings.Split(test, "/")
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

// splitTestFilter splits a -test.run or -test.skip value at each slash that is
// outside a character class and outside parentheses.
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
