package providersync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
)

var topLevelTestFunction = regexp.MustCompile(`(?m)^func (Test\w+)\(`)

// TestEveryOraclePairHasAFrozenGolden is the inventory of the frozen pairs. No
// test runs a pair's Python any more, so nothing at run time proves a pair is
// still compared; this does, from the files alone:
//
//   - every checked-in pair (testdata/oracle_pairs/<pair>.py) has at least one
//     golden that answers it;
//   - every golden is pinned, holds its pinned bytes, was executed on the pinned
//     build, answers exactly one request of a checked-in pair, is named for
//     that pair and for the test it belongs to, and that test still exists;
//   - every pinned golden is on disk;
//   - every frozen leaf is type-tagged.
//
// A pair without a golden, or a golden without a test, is a comparison that no
// longer happens: it fails here by name.
func TestEveryOraclePairHasAFrozenGolden(t *testing.T) {
	_, currentFile, _, _ := moduleroot.Caller(0)
	packageDir := filepath.Dir(currentFile)

	pairs := map[string]bool{}
	entries, err := fs.ReadDir(embeddedOracleSources, "testdata/oracle_pairs")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		stem, isPython := strings.CutSuffix(entry.Name(), ".py")
		// The runner's own rule (python_generic_row_oracle.py): a pair file is
		// <provider>_<dataset>_<boundary>.py; a leading underscore is a helper.
		if parts := strings.Split(stem, "_"); isPython && !strings.HasPrefix(stem, "_") && len(parts) == 3 {
			pairs[strings.Join(parts, "/")] = true
		}
	}
	if len(pairs) == 0 {
		t.Fatal("no checked-in oracle pair found: the inventory would measure nothing")
	}

	tests := map[string]bool{}
	sources, err := filepath.Glob(filepath.Join(packageDir, "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range sources {
		raw, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range topLevelTestFunction.FindAllSubmatch(raw, -1) {
			tests[string(match[1])] = true
		}
	}

	goldens, err := filepath.Glob(filepath.Join(packageDir, oraclePairGoldenDir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	answered := map[string]int{}
	onDisk := map[string]bool{}
	for _, path := range goldens {
		name := filepath.Base(path)
		onDisk[name] = true
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		pin, pinned := oraclePairGoldenPins[name]
		if !pinned {
			t.Errorf("golden %s is pinned by no entry of oraclePairGoldenPins: no test can open it", name)
			continue
		}
		sum := sha256.Sum256(raw)
		if got := hex.EncodeToString(sum[:]); got != pin {
			t.Errorf("golden %s has sha256 %s, oraclePairGoldenPins pins %s: a golden is recorded by execution, never edited", name, got, pin)
			continue
		}
		var file struct {
			Header struct {
				Test        string `json:"test"`
				PythonBuild string `json:"python_build"`
			} `json:"header"`
			Requests []struct {
				Name string `json:"name"`
				Body string `json:"body"`
			} `json:"requests"`
		}
		if err := json.Unmarshal(raw, &file); err != nil {
			t.Errorf("golden %s: %v", name, err)
			continue
		}
		if file.Header.PythonBuild != oraclePairPythonBuild {
			t.Errorf("golden %s was executed on build %s, the pairs pin %s", name, file.Header.PythonBuild, oraclePairPythonBuild)
		}
		if len(file.Requests) != 1 {
			t.Errorf("golden %s holds %d answers: a pair golden holds the one answer of one comparison", name, len(file.Requests))
			continue
		}
		pairID := file.Requests[0].Name
		if !pairs[pairID] {
			t.Errorf("golden %s answers pair %q, which is no checked-in testdata/oracle_pairs file", name, pairID)
			continue
		}
		if want := oraclePairGoldenName(pairID, file.Header.Test); want != name {
			t.Errorf("golden %s answers pair %q for test %q: its name must be %s", name, pairID, file.Header.Test, want)
		}
		if root := strings.SplitN(file.Header.Test, "/", 2)[0]; !tests[root] {
			t.Errorf("golden %s belongs to test %s, which no longer exists: its comparison no longer happens", name, root)
		}
		if err := untaggedLeafErr([]byte(oraclePairAnswerText(t, file.Requests[0].Body))); err != nil {
			t.Errorf("golden %s: %v", name, err)
		}
		answered[pairID]++
	}

	pinnedNames := make([]string, 0, len(oraclePairGoldenPins))
	for name := range oraclePairGoldenPins {
		pinnedNames = append(pinnedNames, name)
	}
	sort.Strings(pinnedNames)
	for _, name := range pinnedNames {
		if !onDisk[name] {
			t.Errorf("pinned golden %s is missing from %s: record it by execution (see the recipe a test of that pair prints)", name, oraclePairGoldenDir)
		}
	}

	pairIDs := make([]string, 0, len(pairs))
	for pairID := range pairs {
		pairIDs = append(pairIDs, pairID)
	}
	sort.Strings(pairIDs)
	for _, pairID := range pairIDs {
		if answered[pairID] == 0 {
			t.Errorf("pair %q (testdata/oracle_pairs/%s.py) has no frozen golden: no test compares Go with its Python answer",
				pairID, strings.ReplaceAll(pairID, "/", "_"))
		}
	}
	t.Logf("%d pairs, %d goldens", len(pairs), len(goldens))
}

// TestAFrozenPairAnswerRefusesAnUntaggedLeaf pins the tagged-leaf rule of a
// frozen answer: a bare JSON number, string or boolean anywhere in a row is
// refused, and so is an answer with no case.
func TestAFrozenPairAnswerRefusesAnUntaggedLeaf(t *testing.T) {
	row := func(value string) string {
		return `{"cases":[{"id":"c","row":{"field":` + value + `}}],"excluded_fields":{}}`
	}
	accepted := []string{
		`null`,
		`{"t":"int","v":"9007199254740993"}`,
		`[{"t":"str","v":"a"},null]`,
		`{"inner":{"t":"bool","v":"true"},"list":[]}`,
		`{"t":{"t":"str","v":"a"},"v":{"t":"str","v":"b"}}`,
	}
	for _, value := range accepted {
		if err := untaggedLeafErr([]byte(row(value))); err != nil {
			t.Errorf("%s was refused: %v", value, err)
		}
	}
	refused := []string{
		`9007199254740993`,
		`1.5`,
		`"bare"`,
		`true`,
		`[{"t":"str","v":"a"},7]`,
		`{"inner":{"deep":"bare"}}`,
		`{"t":"int","v":7}`,
		`{"t":1,"v":"7"}`,
		`{"t":"int"}`,
		`{"t":"int","v":"7","extra":"x"}`,
	}
	for _, value := range refused {
		if err := untaggedLeafErr([]byte(row(value))); err == nil {
			t.Errorf("%s was accepted as a frozen row value", value)
		}
	}
	if err := untaggedLeafErr([]byte(`{"cases":[],"excluded_fields":{}}`)); err == nil {
		t.Error("an answer with no case was accepted")
	}
	if err := untaggedLeafErr([]byte(`not json`)); err == nil {
		t.Error("an answer that is not JSON was accepted")
	}
}

// TestAPairGoldenAnswersOneComparison pins that one run of a test opens a
// golden once: the second comparison of the same pair in the same run is
// refused; another pair, another test, and the same test executed again
// (go test -count=2) are not.
func TestAPairGoldenAnswersOneComparison(t *testing.T) {
	var opened sync.Map
	firstRun, secondRun := new(int), new(int)
	first := oraclePairGoldenName("github/prs/row", "TestA")
	if err := claimOraclePairGolden(&opened, first, firstRun, "github/prs/row", "TestA"); err != nil {
		t.Fatalf("the first comparison was refused: %v", err)
	}
	if err := claimOraclePairGolden(&opened, first, firstRun, "github/prs/row", "TestA"); err == nil {
		t.Error("the second comparison of the same pair in the same run was accepted")
	}
	if err := claimOraclePairGolden(&opened, first, secondRun, "github/prs/row", "TestA"); err != nil {
		t.Errorf("the same test executed again was refused: %v", err)
	}
	if err := claimOraclePairGolden(&opened, first, secondRun, "github/prs/row", "TestA"); err == nil {
		t.Error("the second comparison in the second run was accepted")
	}
	for _, other := range []string{
		oraclePairGoldenName("github/prs/window", "TestA"),
		oraclePairGoldenName("github/prs/row", "TestA/subtest"),
		oraclePairGoldenName("github/prs/row", "TestB"),
	} {
		if other == first {
			t.Fatalf("golden name %s is not unique to its pair and test", other)
		}
		if err := claimOraclePairGolden(&opened, other, firstRun, "pair", "test"); err != nil {
			t.Errorf("%s was refused: %v", other, err)
		}
	}
}
