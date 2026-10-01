package cli_test

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// rootFlagsPythonBuild is the build whose dev-hops root parser answered the
// frozen corpus: a build that still carried the Python CLI.
const rootFlagsPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// TestRootFlagsMatchThePythonRootParser compares the root parser with the real
// build_parser().parse_args(...) + main()'s _resolve_org over the corpus: the
// exit code of what refuses, and the value of every root option of what parses.
// The Python answers were executed once on rootFlagsPythonBuild and are frozen
// in testdata/golden/root_flags.json (the recipe regenerates them by
// execution); the corpus and the program are part of the golden's key, so a
// changed corpus or program must be recorded again.
//
// Named limits: a root value is handed to the command, where a flag typed after
// the command wins (asserted by the dispatch tests, not by this oracle); the
// environment defaults of the root parser (LOG_LEVEL, POSTGRES_URI, ...) stay
// each command's own, so the program runs with them unset.
func TestRootFlagsMatchThePythonRootParser(t *testing.T) {
	_, currentFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(currentFile)))
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/root_flags.json",
		PythonBuild: rootFlagsPythonBuild,
		SHA256:      "ec6ba258537d29538603a2cbf0329ab7fc4daf6f0546228ef696c16fb0e2a93b",
		Recipe: "git worktree add --detach $DIR " + rootFlagsPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/cli/ -test '^TestRootFlagsMatchThePythonRootParser$' -python-root $DIR",
	})
	root := golden.PythonRoot(t, repoRoot)

	corpus := cli.RootCorpus()
	input, err := json.Marshal(corpus)
	if err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"PYTHONHASHSEED": "0"}
	request := venueoracle.ProgramRequest("root flags corpus", cli.RootFlagsOracleProgram(), input, env)
	answers := golden.Produce(t, root, []venueoracle.Request{request}, func(root string, _ []venueoracle.Request) []venueoracle.Response {
		python := pyoracle.Resolve(t, root)
		probe, probeErr := exec.Command(python, pyoracle.VersionProbeArgs...).Output()
		pyoracle.RequireDeployed(t, python, probe, probeErr)
		command := exec.Command(python, "-c", cli.RootFlagsOracleProgram())
		command.Stdin = strings.NewReader(string(input))
		// Only what the program needs: the root parser's own environment
		// defaults must be unset (see the named limits).
		command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "PYTHONPATH=" + filepath.Join(root, "src"),
			"PYTHONDONTWRITEBYTECODE=1", "PYTHONHASHSEED=" + env["PYTHONHASHSEED"]}
		output, err := command.Output()
		if err != nil {
			var stderr []byte
			if exitErr, ok := err.(*exec.ExitError); ok {
				stderr = exitErr.Stderr
			}
			t.Fatalf("live python: %v", pyoracle.RunError(python, err, stderr))
		}
		return []venueoracle.Response{{Status: 0, Body: string(output)}}
	})
	golden.Consumed(t, answers...)

	want, err := decodeRootAnswers(answers[0].Body)
	if err != nil {
		t.Fatalf("decode python answer: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python answered %d for %d cases", len(want), len(corpus))
	}
	stages := map[string]int{}
	mismatches := 0
	for index, argv := range corpus {
		got := cli.RootResult(argv)
		if _, compared := got["msg"]; !compared {
			// dho's other refusals are worded by its own dispatcher; only the exit code is compared.
			delete(want[index], "msg")
		}
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(want[index])
		stages[fmt.Sprint(got["stage"])]++
		if cli.CanonicalJSON(gotJSON) != cli.CanonicalJSON(wantJSON) {
			mismatches++
			t.Errorf("case %d %q:\n go     %s\n python %s", index, argv, gotJSON, wantJSON)
		}
	}
	names := make([]string, 0, len(stages))
	for name := range stages {
		names = append(names, name)
	}
	sort.Strings(names)
	t.Logf("%d command lines compared, %d mismatches, stages %v", len(corpus), mismatches, stages)
	// A corpus that only ever parses, or only ever refuses, compares nothing.
	if stages["ok"] < 40 || stages["exit"] < 20 {
		t.Fatalf("the corpus did not reach both outcomes: %v", stages)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

// decodeRootAnswers decodes the producer's stdout: exactly one JSON value, a
// list of answers. Numbers stay the producer's literal text (json.Number): none
// passes through float64 on its way to the comparison. Anything after the value
// is output the comparison would not see, so it is refused.
func decodeRootAnswers(body string) ([]map[string]any, error) {
	var want []map[string]any
	decoder := json.NewDecoder(strings.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&want); err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("the producer's output holds more than one JSON value (next token: %v)", err)
	}
	return want, nil
}

func TestTheRootAnswersAreExactlyOneJSONValue(t *testing.T) {
	if got, err := decodeRootAnswers(`[{"exit": 2, "x": 1.50}]` + "\n"); err != nil || len(got) != 1 || got[0]["x"] != json.Number("1.50") {
		t.Fatalf("one value with a trailing newline: %v %v", got, err)
	}
	for name, body := range map[string]string{
		"trailing text":     `[{"exit": 2}] TRAILING NON-JSON PRODUCER OUTPUT`,
		"a second value":    `[{"exit": 2}][{"exit": 0}]`,
		"a truncated value": `[{"exit": 2}`,
		"nothing":           ``,
	} {
		if got, err := decodeRootAnswers(body); err == nil {
			t.Errorf("%s: decoded %v", name, got)
		}
	}
}
