package providersync

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/oraclecompare"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// scriptOracle is one run of an oracle that is not a generic pair: a Python
// script of this package's testdata (or an inline program) executed over the
// production sources it names, with an input of the test's own.
type scriptOracle struct {
	// name identifies the oracle: the request name and the first part of the
	// golden's file name. It must be one of scriptOracleNames.
	name string
	// script is the script, relative to the package directory. It is run as
	// `python <script> <sources...> [<input file>]`. Empty when program is set.
	script string
	// program is an inline program, run as `python -c <program>`.
	program string
	// sources are the production files handed to the script as arguments,
	// relative to the repository root.
	sources []string
	// input is sent on stdin, or written to a file whose path is the last
	// argument when inputFile is set.
	input     []byte
	inputFile bool
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

// embeddedScriptOracleSources holds the script oracles' own sources and their
// goldens, for the same two reasons embeddedOracleSources holds the pairs': the
// scripts are part of the identity of the producer a golden is keyed on, and
// the goldens are read from disk at run time, so both must be part of the test
// binary's content.
//
//go:embed testdata/python_github_prs_normalization_oracle.py testdata/python_launchdarkly_normalization_oracle.py testdata/python_provider_budget_oracle.py testdata/python_work_item_sink_oracle.py testdata/script_golden/*.json
var embeddedScriptOracleSources embed.FS

// frozenScriptAnswer returns what the script oracle printed on stdout.
//
// Frozen (every run but a recording): the answer comes from the oracle's
// golden for this test. No Python runs. A golden that is missing, changed,
// recorded for another input, from another script or other harness sources, or
// for other production source arguments fails the test.
//
// Recording (the goldenrecord verb): the script of the pinned checkout is
// executed over the pinned checkout's production sources.
//
// The answer is the producer's exact stdout. A caller that decodes it must not
// send a JSON number through float64: decode into exact integer fields, or
// with UseNumber and compare the literal text.
func frozenScriptAnswer(t *testing.T, oracle scriptOracle) []byte {
	t.Helper()
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	repoRoot := filepath.Dir(filepath.Dir(packageDir))
	oraclecompare.AssertSourcesUnchangedSinceBuild(t, embeddedScriptOracleSources, packageDir)
	assertOracleSourcesUnchangedSinceBuild(t)

	if err := oracle.producerErr(); err != nil {
		t.Fatal(err)
	}
	name := scriptOracleGoldenName(oracle.name, t.Name())
	if err := claimOraclePairGolden(&oraclePairGoldensOpened, name, t, oracle.name, t.Name()); err != nil {
		t.Fatal(err)
	}
	recipe := fmt.Sprintf("script oracle %s: git worktree add --detach $DIR %s (with its .venv: uv sync --frozen --no-install-project); "+
		"then from the repository root: go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/providersync/ "+
		"-test '^%s$' -python-root $DIR", oracle.name, oraclePairPythonBuild, strings.SplitN(t.Name(), "/", 2)[0])
	pin, pinned := oracleScriptGoldenPins[name]
	if !pinned {
		t.Fatalf("script oracle %q in test %s has no frozen golden: oracleScriptGoldenPins names no %s. "+
			"A frozen oracle never runs Python and never skips. Add the entry with the value %q, then record: %s",
			oracle.name, t.Name(), name, "PIN:"+strings.TrimSuffix(name, ".json"), recipe)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        filepath.Join(scriptOracleGoldenDir, name),
		PythonBuild: oraclePairPythonBuild,
		SHA256:      pin,
		Recipe:      recipe,
		Scrub:       scriptPerRunScrub(oracle.name),
	})
	root := golden.PythonRoot(t, repoRoot)

	harness := oracleHarnessManifest(t, packageDir)
	program := scriptOracleProgram(t, oracle, harness)
	keyed, passed := oraclePairEnvironment(t, oracle.name)
	request := venueoracle.ProgramRequest(oracle.name, program, oracle.input, keyed)
	answers := golden.Produce(t, root, []venueoracle.Request{request},
		func(root string, _ []venueoracle.Request) []venueoracle.Response {
			output := runPinnedScriptOracle(t, root, harness, oracle, passed)
			if len(output) > oraclePairPackAbove {
				return []venueoracle.Response{{Status: 0, Body: venueoracle.PackBody(output)}}
			}
			return []venueoracle.Response{{Status: 0, Body: string(output)}}
		})
	golden.Consumed(t, answers...)
	output := []byte(oraclePairAnswerText(t, answers[0].Body))
	golden.SkipDiff(t)
	golden.Finish(t)
	return output
}

// producerErr is an error unless the oracle names exactly one producer: a
// script or an inline program. With both set, one of them would run and the
// other would only look like part of the request.
func (oracle scriptOracle) producerErr() error {
	if (oracle.script == "") == (oracle.program == "") {
		return fmt.Errorf("script oracle %q: set exactly one of script and program", oracle.name)
	}
	return nil
}

// TestAScriptOracleNamesExactlyOneProducer pins producerErr.
func TestAScriptOracleNamesExactlyOneProducer(t *testing.T) {
	for _, oracle := range []scriptOracle{
		{name: "none"},
		{name: "both", script: "testdata/python_work_item_sink_oracle.py", program: "print(1)"},
	} {
		if err := oracle.producerErr(); err == nil {
			t.Errorf("oracle %q was accepted", oracle.name)
		}
	}
	for _, oracle := range []scriptOracle{
		{name: "script", script: "testdata/python_work_item_sink_oracle.py"},
		{name: "program", program: "print(1)"},
	} {
		if err := oracle.producerErr(); err != nil {
			t.Errorf("oracle %q was refused: %v", oracle.name, err)
		}
	}
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

// scriptOracleProgram is the identity of the producer of one run: the script
// (or the inline program) by digest, the production source arguments by path
// (their content is part of the pinned build's source digest in the golden's
// header), how the input reaches it, and the harness the scripts import.
func scriptOracleProgram(t *testing.T, oracle scriptOracle, harness string) string {
	t.Helper()
	var text strings.Builder
	if oracle.script != "" {
		raw, err := embeddedScriptOracleSources.ReadFile(oracle.script)
		if err != nil {
			t.Fatalf("script oracle %q: %s is not an embedded script source (add it to embeddedScriptOracleSources): %v", oracle.name, oracle.script, err)
		}
		text.WriteString("script " + manifestLine(oracle.script, raw) + "\n")
	} else {
		text.WriteString("program " + manifestLine("-c", []byte(oracle.program)) + "\n")
	}
	for _, source := range oracle.sources {
		text.WriteString("argument " + source + "\n")
	}
	if oracle.inputFile {
		text.WriteString("input file\n")
	} else {
		text.WriteString("input stdin\n")
	}
	text.WriteString(harness)
	return text.String()
}

// runPinnedScriptOracle executes the oracle from the pinned checkout at root
// and returns its stdout.
func runPinnedScriptOracle(t *testing.T, root, harness string, oracle scriptOracle, passed []string) []byte {
	t.Helper()
	pinnedPackage := filepath.Join(root, "internal", "providersync")
	assertPinnedHarness(t, oracle.name, root, harness)
	python, environment := pinnedInterpreter(t, oracle.name, root, passed)

	var arguments []string
	if oracle.script != "" {
		want, err := embeddedScriptOracleSources.ReadFile(oracle.script)
		if err != nil {
			t.Fatal(err)
		}
		pinnedScript := filepath.Join(pinnedPackage, filepath.FromSlash(oracle.script))
		got, err := os.ReadFile(pinnedScript)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("recording script oracle %q: %s of the pinned checkout %s is not the script this checkout holds (%v)", oracle.name, oracle.script, root, err)
		}
		arguments = append(arguments, pinnedScript)
	} else {
		arguments = append(arguments, "-c", oracle.program)
	}
	for _, source := range oracle.sources {
		path := filepath.Join(root, filepath.FromSlash(source))
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("recording script oracle %q: production source %s: %v", oracle.name, source, err)
		}
		arguments = append(arguments, path)
	}
	command := exec.Command(python, arguments...)
	if oracle.inputFile {
		inputFile := filepath.Join(t.TempDir(), "oracle-input.json")
		if err := os.WriteFile(inputFile, oracle.input, 0o600); err != nil {
			t.Fatal(err)
		}
		command.Args = append(command.Args, inputFile)
	} else {
		command.Stdin = bytes.NewReader(oracle.input)
	}
	command.Env = environment
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("recording script oracle %q: %v", oracle.name, pyoracle.RunError(python, err, stderr.Bytes()))
	}
	return output
}

var scriptGoldenTestFunction = regexp.MustCompile(`(?m)^func (Test\w+)\(`)

// TestEveryScriptOracleHasAFrozenGolden is the inventory of the frozen script
// oracles, from the files alone: every oracle of scriptOracleNames has a
// golden; every golden under testdata/script_golden is pinned, holds its
// pinned bytes, was executed on the pinned build, answers one request of a
// known oracle, is named for that oracle and its test, and that test exists;
// every pinned golden is on disk.
func TestEveryScriptOracleHasAFrozenGolden(t *testing.T) {
	_, currentFile, _, _ := runtime.Caller(0)
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
