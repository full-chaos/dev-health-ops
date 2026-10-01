package providersync

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// oraclePairPythonBuild is the build whose production Python answered every
// frozen pair: python_generic_row_oracle.py executed there, once per
// comparison, against the real functions under src/dev_health_ops.
const oraclePairPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// oraclePairGoldenDir holds one golden per comparison: the pair's Python answer
// for the cases of the one test (or subtest) that asks for it.
const oraclePairGoldenDir = "testdata/oracle_golden"

// oraclePairEnv is the environment that shapes a pair's answer. It is part of
// the request, so an answer recorded under another environment is refused.
var oraclePairEnv = map[string]string{"PYTHONHASHSEED": "0", "TZ": "UTC"}

// oraclePairFileEnv names the environment variables that hand the producer a
// file. Go reads the same file, so its content shapes both sides. The path is a
// new temporary directory in every process, so the request holds the digest of
// the content, and a recording passes the path through.
var oraclePairFileEnv = []string{"IDENTITY_MAPPING_PATH"}

// oraclePairConfigDirs are the testdata directories the pairs read at run time
// besides their own sources. They are part of the producer, so they are part of
// the request too.
var oraclePairConfigDirs = []string{"testdata/investment_configs", "testdata/status_mapping_configs"}

// oraclePairGoldensOpened records each golden a comparison opened in this
// process: one frozen answer stands in for one comparison.
var oraclePairGoldensOpened sync.Map

// frozenPairAnswer returns what python_generic_row_oracle.py printed for pairID
// over encodedCases.
//
// Frozen (every run but a recording): the answer comes from the pair's golden.
// No Python runs. A golden that is missing, changed, recorded for other cases
// or recorded from other harness sources fails the test.
//
// Recording (the goldenrecord verb): the runner is executed from the pinned
// checkout, whose harness sources must equal this checkout's, and the answer is
// written as a candidate.
func frozenPairAnswer(t *testing.T, pairID string, encodedCases []byte) []byte {
	t.Helper()
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	repoRoot := filepath.Dir(filepath.Dir(packageDir))

	name := oraclePairGoldenName(pairID, t.Name())
	if err := claimOraclePairGolden(&oraclePairGoldensOpened, name, pairID, t.Name()); err != nil {
		t.Fatal(err)
	}
	recipe := fmt.Sprintf("pair %s: git worktree add --detach $DIR %s (with its .venv: uv sync --frozen --no-install-project); "+
		"then from the repository root: go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/providersync/ "+
		"-test '^%s$' -python-root $DIR", pairID, oraclePairPythonBuild, strings.SplitN(t.Name(), "/", 2)[0])
	pin, pinned := oraclePairGoldenPins[name]
	if !pinned {
		t.Fatalf("pair %q in test %s has no frozen golden: oraclePairGoldenPins names no %s. "+
			"A frozen oracle never runs Python and never skips. Add the entry with the value %q, then record: %s",
			pairID, t.Name(), name, "PIN:"+strings.TrimSuffix(name, ".json"), recipe)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        filepath.Join(oraclePairGoldenDir, name),
		PythonBuild: oraclePairPythonBuild,
		SHA256:      pin,
		Recipe:      recipe,
	})
	root := golden.PythonRoot(t, repoRoot)

	manifest := oracleHarnessManifest(t, packageDir)
	keyed, passed := oraclePairEnvironment(t, pairID)
	request := venueoracle.ProgramRequest(pairID, manifest, encodedCases, keyed)
	answers := golden.Produce(t, root, []venueoracle.Request{request},
		func(root string, _ []venueoracle.Request) []venueoracle.Response {
			output := runPinnedPairOracle(t, root, manifest, pairID, encodedCases, passed)
			if len(output) > oraclePairPackAbove {
				return []venueoracle.Response{{Status: 0, Body: venueoracle.PackBody(output)}}
			}
			return []venueoracle.Response{{Status: 0, Body: string(output)}}
		})
	golden.Consumed(t, answers...)
	output := []byte(oraclePairAnswerText(t, answers[0].Body))
	if err := untaggedLeafErr(output); err != nil {
		t.Fatalf("pair %q: %v", pairID, err)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
	return output
}

// oraclePairEnvironment is the environment of a pair's producer, twice: keyed
// is what identifies the request (a file-valued variable by the digest of its
// file), passed is what a recording hands the interpreter. Nothing else of the
// test process's environment reaches the producer.
func oraclePairEnvironment(t *testing.T, pairID string) (keyed map[string]string, passed []string) {
	t.Helper()
	keyed = map[string]string{}
	for name, value := range oraclePairEnv {
		keyed[name] = value
		passed = append(passed, name+"="+value)
	}
	for _, name := range oraclePairFileEnv {
		path := os.Getenv(name)
		if path == "" {
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("pair %q: %s names a file the producer would read: %v", pairID, name, err)
		}
		sum := sha256.Sum256(raw)
		keyed[name] = "sha256:" + hex.EncodeToString(sum[:])
		passed = append(passed, name+"="+path)
	}
	sort.Strings(passed)
	return keyed, passed
}

// claimOraclePairGolden is an error when the golden name was already opened in
// this process: a test that compares one pair twice would read the same answer
// for both comparisons.
func claimOraclePairGolden(opened *sync.Map, name, pairID, testName string) error {
	if _, asked := opened.LoadOrStore(name, true); asked {
		return fmt.Errorf("pair %q is asked twice by test %s: one frozen answer stands in for one "+
			"comparison, so give each comparison of a pair its own subtest", pairID, testName)
	}
	return nil
}

// oraclePairPackAbove is the answer size above which a golden stores the
// answer packed (venueoracle.PackBody): the same bytes, compressed.
const oraclePairPackAbove = 64 << 10

// oraclePairAnswerText is the runner's stdout a golden body holds.
func oraclePairAnswerText(t *testing.T, body string) string {
	t.Helper()
	if strings.HasPrefix(body, "gzip+base64:") {
		return venueoracle.UnpackBody(t, body)
	}
	return body
}

var unsafeGoldenNameRune = regexp.MustCompile(`[^A-Za-z0-9_.-]`)

// oraclePairGoldenName is the golden file of one comparison: the pair and the
// test that asks, so a golden belongs to one test.
func oraclePairGoldenName(pairID, testName string) string {
	clean := func(part string) string {
		return unsafeGoldenNameRune.ReplaceAllString(strings.ReplaceAll(part, "/", "."), "_")
	}
	return strings.ReplaceAll(pairID, "/", "_") + "." + clean(testName) + ".golden.json"
}

// oracleHarnessManifest is the identity of the Python harness a pair runs
// through: one "sha256  path" line per harness source (the runner, the
// registry, the loader, the field reflection, every pair and pair helper) and
// per pair configuration file. Pairs import each other and share helpers, so
// the whole set is one producer.
func oracleHarnessManifest(t *testing.T, packageDir string) string {
	t.Helper()
	var lines []string
	err := fs.WalkDir(embeddedOracleSources, ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || !strings.HasSuffix(path, ".py") {
			return walkErr
		}
		raw, err := fs.ReadFile(embeddedOracleSources, path)
		if err != nil {
			return err
		}
		lines = append(lines, manifestLine(path, raw))
		return nil
	})
	if err != nil {
		t.Fatalf("oracle harness manifest: %v", err)
	}
	for _, dir := range oraclePairConfigDirs {
		err := filepath.WalkDir(filepath.Join(packageDir, dir), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil || entry.IsDir() {
				return walkErr
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(packageDir, path)
			if err != nil {
				return err
			}
			lines = append(lines, manifestLine(filepath.ToSlash(relative), raw))
			return nil
		})
		if err != nil {
			t.Fatalf("oracle harness manifest: %v", err)
		}
	}
	if len(lines) == 0 {
		t.Fatal("oracle harness manifest: no harness source found")
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n") + "\n"
}

func manifestLine(path string, raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]) + "  " + path
}

// runPinnedPairOracle executes the generic runner of the pinned checkout at
// root for pairID and returns its stdout. The pinned harness must be the
// harness this checkout holds (manifest): the pairs resolve the production
// sources from their own location, so the runner has to run where the pinned
// production code is.
func runPinnedPairOracle(t *testing.T, root, manifest, pairID string, encodedCases []byte, passed []string) []byte {
	t.Helper()
	pinnedPackage := filepath.Join(root, "internal", "providersync")
	for _, line := range strings.Split(strings.TrimSuffix(manifest, "\n"), "\n") {
		want, path, _ := strings.Cut(line, "  ")
		raw, err := os.ReadFile(filepath.Join(pinnedPackage, filepath.FromSlash(path)))
		if err != nil {
			t.Fatalf("recording pair %q: the pinned checkout lacks the harness file %s: %v", pairID, path, err)
		}
		if got := strings.SplitN(manifestLine(path, raw), "  ", 2)[0]; got != want {
			t.Fatalf("recording pair %q: harness file %s differs between this checkout (sha256 %s) and the pinned checkout %s (sha256 %s): "+
				"the answer would come from a harness the golden does not name", pairID, path, want, root, got)
		}
	}

	python := pyoracle.Resolve(t, root)
	probe, probeErr := exec.Command(python, pyoracle.VersionProbeArgs...).Output()
	pyoracle.RequireDeployed(t, python, probe, probeErr)
	environment := append([]string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"),
		"PYTHONPATH=" + filepath.Join(root, "src"), "PYTHONDONTWRITEBYTECODE=1",
	}, passed...)

	// The production package the runner imports must be the pinned checkout's.
	locate := exec.Command(python, "-c",
		"import importlib.util;s=importlib.util.find_spec('dev_health_ops');print(s.origin if s else '')")
	locate.Env = environment
	origin, err := locate.Output()
	source := filepath.Join(root, "src") + string(filepath.Separator)
	if err != nil || !strings.HasPrefix(strings.TrimSpace(string(origin)), source) {
		t.Fatalf("recording pair %q: %s imports dev_health_ops from %q (%v), not from the pinned checkout %s",
			pairID, python, strings.TrimSpace(string(origin)), err, source)
	}

	casesFile := filepath.Join(t.TempDir(), "oracle-cases.json")
	if err := os.WriteFile(casesFile, encodedCases, 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, filepath.Join(pinnedPackage, "testdata", "python_generic_row_oracle.py"), pairID, casesFile)
	command.Env = environment
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("recording pair %q: %v", pairID, pyoracle.RunError(python, err, stderr.Bytes()))
	}
	return output
}

// untaggedLeafErr is an error when a runner answer holds a row value that is
// not null, a container, or a type-tagged leaf {"t": type, "v": string}. A bare
// JSON number, string or boolean would be compared by its JSON text, which is
// the precision and type collapse the tags exist to stop; a golden must never
// freeze one.
func untaggedLeafErr(output []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(output))
	decoder.UseNumber()
	var decoded struct {
		Cases []struct {
			ID  string         `json:"id"`
			Row map[string]any `json:"row"`
		} `json:"cases"`
	}
	if err := decoder.Decode(&decoded); err != nil {
		return fmt.Errorf("the answer is not the runner's JSON: %w", err)
	}
	if len(decoded.Cases) == 0 {
		return fmt.Errorf("the answer holds no case")
	}
	for _, entry := range decoded.Cases {
		for field, value := range entry.Row {
			if path, found := untaggedLeaf(value, field); found {
				return fmt.Errorf("case %q holds an untagged value at %s: every leaf of a frozen row is {\"t\": type, \"v\": string} or null", entry.ID, path)
			}
		}
	}
	return nil
}

func untaggedLeaf(value any, path string) (string, bool) {
	switch typed := value.(type) {
	case nil:
		return "", false
	case []any:
		for index, item := range typed {
			if found, bad := untaggedLeaf(item, fmt.Sprintf("%s[%d]", path, index)); bad {
				return found, true
			}
		}
		return "", false
	case map[string]any:
		if _, hasTag := typed["t"]; hasTag && len(typed) == 2 {
			if _, hasValue := typed["v"]; hasValue {
				_, tagIsString := typed["t"].(string)
				_, valueIsString := typed["v"].(string)
				if tagIsString && valueIsString {
					return "", false
				}
			}
		}
		for key, item := range typed {
			if found, bad := untaggedLeaf(item, path+"."+key); bad {
				return found, true
			}
		}
		return "", false
	default:
		return path, true
	}
}
