package providersync

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/oraclecompare"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// scriptOracle is one frozen run of an oracle that is not a generic pair: the
// name of the oracle and the input of the test's own.
type scriptOracle struct {
	// name identifies the oracle: the request name and the first part of the
	// golden's file name. It must be one of scriptOracleNames.
	name string
	// input is the request's stdin; its digest is part of the golden's key.
	input []byte
}

// scriptOracleNames are the script oracles this package freezes. Each has at
// least one golden (TestEveryScriptOracleHasAFrozenGolden).
var scriptOracleNames = []string{
	"github-prs-normalization",
	"json-dumps-evidence",
	"launchdarkly-normalization",
	"provider-budget",
	"repo-listing",
	"work-item-sink",
}

// scriptOracleGoldenDir holds one golden per run of a script oracle.
const scriptOracleGoldenDir = "testdata/script_golden"

// embeddedScriptOracleSources holds the script oracles' goldens. They are read
// from disk at run time, so they must be part of the test binary's content.
//
//go:embed testdata/script_golden/*.json
var embeddedScriptOracleSources embed.FS

// frozenScriptAnswer returns what the script oracle printed on stdout.
//
// The answer comes from the oracle's golden for this test. No Python runs. A
// golden that is missing, changed or recorded for another input fails the
// test.
//
// The answer is the producer's exact stdout. A caller that decodes it must not
// send a JSON number through float64: decode into exact integer fields, or
// with UseNumber and compare the literal text.
func frozenScriptAnswer(t *testing.T, oracle scriptOracle) []byte {
	t.Helper()
	_, currentFile, _, _ := moduleroot.Caller(0)
	packageDir := filepath.Dir(currentFile)
	oraclecompare.AssertSourcesUnchangedSinceBuild(t, embeddedScriptOracleSources, packageDir)
	assertOracleSourcesUnchangedSinceBuild(t)

	name := scriptOracleGoldenName(oracle.name, t.Name())
	if err := claimOraclePairGolden(&oraclePairGoldensOpened, name, t, oracle.name, t.Name()); err != nil {
		t.Fatal(err)
	}
	pin, pinned := oracleScriptGoldenPins[name]
	if !pinned {
		t.Fatalf("script oracle %q in test %s has no frozen golden: oracleScriptGoldenPins names no %s", oracle.name, t.Name(), name)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        filepath.Join(scriptOracleGoldenDir, name),
		PythonBuild: oraclePairPythonBuild,
		SHA256:      pin,
		Scrub:       scriptPerRunScrub(oracle.name),
	})
	request := venueoracle.ProgramRequest(oracle.name, oracle.input)
	answers := golden.Produce(t, []venueoracle.Request{request})
	golden.Consumed(t, answers...)
	output := []byte(oraclePairAnswerText(t, answers[0].Body))
	if err := scriptPerRunErr(oracle.name, output); err != nil {
		t.Fatalf("golden %s: %v", name, err)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
	return output
}

func knownScriptOracle(name string) bool {
	for _, known := range scriptOracleNames {
		if known == name {
			return true
		}
	}
	return false
}

// scriptOracleGoldenName is the golden file of one run: the oracle and the
// test that asks, so a golden belongs to one test.
func scriptOracleGoldenName(oracleName, testName string) string {
	return "script." + oraclePairGoldenName(oracleName, testName)
}

var scriptGoldenTestFunction = regexp.MustCompile(`(?m)^func (Test\w+)\(`)

// TestEveryScriptOracleHasAFrozenGolden is the inventory of the frozen script
// oracles, from the files alone: every oracle of scriptOracleNames has a
// golden; every golden under testdata/script_golden is pinned, holds its
// pinned bytes, was executed on the pinned build, answers one request of a
// known oracle, is named for that oracle and its test, and that test exists;
// every pinned golden is on disk.
func TestEveryScriptOracleHasAFrozenGolden(t *testing.T) {
	_, currentFile, _, _ := moduleroot.Caller(0)
	packageDir := filepath.Dir(currentFile)

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
		for _, match := range scriptGoldenTestFunction.FindAllSubmatch(raw, -1) {
			tests[string(match[1])] = true
		}
	}

	goldens, err := filepath.Glob(filepath.Join(packageDir, scriptOracleGoldenDir, "*.json"))
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
		pin, pinned := oracleScriptGoldenPins[name]
		if !pinned {
			t.Errorf("golden %s is pinned by no entry of oracleScriptGoldenPins: no test can open it", name)
			continue
		}
		sum := sha256.Sum256(raw)
		if got := hex.EncodeToString(sum[:]); got != pin {
			t.Errorf("golden %s has sha256 %s, oracleScriptGoldenPins pins %s: a golden is recorded by execution, never edited", name, got, pin)
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
			t.Errorf("golden %s was executed on build %s, the script oracles pin %s", name, file.Header.PythonBuild, oraclePairPythonBuild)
		}
		if len(file.Requests) != 1 {
			t.Errorf("golden %s holds %d answers: a script golden holds the one answer of one run", name, len(file.Requests))
			continue
		}
		oracleName := file.Requests[0].Name
		if !knownScriptOracle(oracleName) {
			t.Errorf("golden %s answers %q, which is not in scriptOracleNames", name, oracleName)
			continue
		}
		if want := scriptOracleGoldenName(oracleName, file.Header.Test); want != name {
			t.Errorf("golden %s answers %q for test %q: its name must be %s", name, oracleName, file.Header.Test, want)
		}
		if root := strings.SplitN(file.Header.Test, "/", 2)[0]; !tests[root] {
			t.Errorf("golden %s belongs to test %s, which no longer exists: its comparison no longer happens", name, root)
		}
		if strings.TrimSpace(oraclePairAnswerText(t, file.Requests[0].Body)) == "" {
			t.Errorf("golden %s holds an empty answer", name)
		}
		answered[oracleName]++
	}

	pinnedNames := make([]string, 0, len(oracleScriptGoldenPins))
	for name := range oracleScriptGoldenPins {
		pinnedNames = append(pinnedNames, name)
	}
	sort.Strings(pinnedNames)
	for _, name := range pinnedNames {
		if !onDisk[name] {
			t.Errorf("pinned golden %s is missing from %s: record it by execution (see the recipe its test prints)", name, scriptOracleGoldenDir)
		}
	}
	if len(scriptOracleNames) == 0 {
		t.Fatal("scriptOracleNames is empty: the inventory would measure nothing")
	}
	for _, oracleName := range scriptOracleNames {
		if answered[oracleName] == 0 {
			t.Errorf("script oracle %q has no frozen golden: no test compares Go with its Python answer", oracleName)
		}
	}
	t.Logf("%d script oracles, %d goldens", len(scriptOracleNames), len(goldens))
}
