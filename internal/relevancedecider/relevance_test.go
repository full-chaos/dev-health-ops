// Package relevancedecider pins ci/go_relevance.sh, the decider the always-run go-quality job uses to say whether a change
// set is Go-relevant, to the answers of the decider it replaced (ci/go_relevance.py, CHAOS-4834).
//
// The old decider is Python. Its answers over a fixed set of changed-file lists were recorded once, on the last build
// that carried it, and are frozen in testdata/golden/go_relevance_cases.json (recipe in the golden's spec); a frozen run
// needs no Python. Each case is a list of changed paths in the producer's format (NUL-terminated, as
// `git diff -z --no-renames` writes them, or one per line), judged against a pinned snapshot of go.yml's path filter
// (testdata/go_workflow_fixture.yml) so an answer does not move when go.yml does.
package relevancedecider

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/rotguard"
)

type decisionCase struct {
	Name   string   `json:"name"`
	Format string   `json:"format"` // "nul" or "newline"
	Paths  []string `json:"paths"`
}

// stdin is the bytes the producer would have written for the case.
func (c decisionCase) stdin() []byte {
	var out bytes.Buffer
	for _, path := range c.Paths {
		out.WriteString(path)
		if c.Format == "newline" {
			out.WriteByte('\n')
		} else {
			out.WriteByte(0)
		}
	}
	return out.Bytes()
}

// A workflow whose path filter the decider must refuse (it fails, it does not guess).
type refusalCase struct {
	Name string `json:"name"`
	YAML string `json:"yaml"`
}

var refusals = []refusalCase{
	{"a question mark in a pattern", "on:\n  pull_request:\n    paths:\n      - 'internal/?.go'\n"},
	{"a character class in a pattern", "on:\n  pull_request:\n    paths:\n      - 'internal/[ab].go'\n"},
	{"a negated pattern", "on:\n  pull_request:\n    paths:\n      - '!docs/**'\n"},
	{"no paths declared", "on:\n  pull_request:\n    branches: [main]\n"},
	{"no pull_request block", "on:\n  push:\n    paths:\n      - '**/*.go'\n"},
}

// pythonProgram runs the old decider over the cases and the refusal workflows. It imports the real ci/go_relevance.py
// from the pinned checkout, points it at the pinned workflow snapshot and calls its own main() with the case's stdin.
const pythonProgram = `
import contextlib, importlib.util, io, json, pathlib, sys, tempfile
request = json.loads(sys.stdin.read())
spec = importlib.util.spec_from_file_location("go_relevance", "ci/go_relevance.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
directory = pathlib.Path(tempfile.mkdtemp())

def run(workflow_text, raw):
    path = directory / "workflow.yml"
    path.write_text(workflow_text, encoding="utf-8")
    module.GO_WORKFLOW = path
    stdout, failed = io.StringIO(), False
    sys.stdin = io.StringIO(raw)
    try:
        with contextlib.redirect_stdout(stdout):
            code = module.main()
        failed = code != 0
    except (SystemExit, Exception):
        # A refusal is any non-zero exit: Python's own traceback (a workflow with no
        # pull_request.paths raises KeyError) exits 1 the same as an explicit refusal.
        failed = True
    return {"stdout": stdout.getvalue(), "failed": failed}

answers = {"cases": {}, "refusals": {}}
for case in request["cases"]:
    sep = "\0" if case["format"] == "nul" else "\n"
    answers["cases"][case["name"]] = run(request["workflow"], "".join(path + sep for path in case["paths"]))
for refusal in request["refusals"]:
    answers["refusals"][refusal["name"]] = run(refusal["yaml"], "internal/a.go\n")["failed"]
print(json.dumps(answers, sort_keys=True, ensure_ascii=True))
`

type recorded struct {
	Cases map[string]struct {
		Stdout string
		Failed bool
	} `json:"cases"`
	Refusals map[string]bool `json:"refusals"`
}

func root(t *testing.T) string {
	t.Helper()
	absolute, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return absolute
}

func loadCases(t *testing.T) []decisionCase {
	t.Helper()
	raw, err := os.ReadFile("testdata/cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []decisionCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	return cases
}

// decide runs the new decider over one stdin against one workflow file.
func decide(t *testing.T, workflow string, stdin []byte) (string, bool) {
	t.Helper()
	command := exec.Command("bash", filepath.Join(root(t), "ci", "go_relevance.sh"))
	command.Env = append(os.Environ(), "GO_WORKFLOW="+workflow)
	command.Stdin = bytes.NewReader(stdin)
	var stdout bytes.Buffer
	command.Stdout = &stdout
	err := command.Run()
	return stdout.String(), err != nil
}

func fixturePath(t *testing.T) string {
	t.Helper()
	absolute, err := filepath.Abs("testdata/go_workflow_fixture.yml")
	if err != nil {
		t.Fatal(err)
	}
	return absolute
}

// TestDeciderAnswersAsTheRecordedPythonDid compares, for every case, the new decider's stdout and failure with the old
// decider's, byte for byte.
func TestDeciderAnswersAsTheRecordedPythonDid(t *testing.T) {
	cases := loadCases(t)
	if len(cases) < 12 {
		t.Fatalf("the corpus holds %d cases; the comparison would pass on a thin corpus", len(cases))
	}
	workflow, err := os.ReadFile("testdata/go_workflow_fixture.yml")
	if err != nil {
		t.Fatal(err)
	}
	request, err := json.Marshal(map[string]any{"workflow": string(workflow), "cases": cases, "refusals": refusals})
	if err != nil {
		t.Fatal(err)
	}
	spec := rotguard.Spec("testdata/golden/go_relevance_cases.json", "ec6a25fdf026c7597ff6b8097157cffa7d31f61dadcb62a6274f9344a9bcbe6a",
		"./internal/relevancedecider/", "^TestDeciderAnswersAsTheRecordedPythonDid$")
	answers := programoracle.Run(t, spec, root(t), []programoracle.Program{{Name: "go_relevance.py over the corpus", Text: pythonProgram, Stdin: request}})
	if answers[0].ExitCode != 0 {
		t.Fatalf("the recorded Python run exited %d (stdout %q)", answers[0].ExitCode, answers[0].Stdout)
	}
	var want recorded
	if err := json.Unmarshal([]byte(strings.TrimSpace(answers[0].Stdout)), &want); err != nil {
		t.Fatalf("decode the recorded answers: %v", err)
	}
	if len(want.Cases) != len(cases) || len(want.Refusals) != len(refusals) {
		t.Fatalf("the recording answers %d cases and %d refusals; the corpus has %d and %d", len(want.Cases), len(want.Refusals), len(cases), len(refusals))
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			expected, present := want.Cases[c.Name]
			if !present {
				t.Fatalf("the recording has no answer for %q", c.Name)
			}
			got, failed := decide(t, fixturePath(t), c.stdin())
			if failed != expected.Failed || got != expected.Stdout {
				t.Errorf("the new decider differs from the recorded Python:\n  python (failed=%v):\n%s\n  new    (failed=%v):\n%s", expected.Failed, expected.Stdout, failed, got)
			}
		})
	}
	for _, refusal := range refusals {
		t.Run("refuses "+refusal.Name, func(t *testing.T) {
			if !want.Refusals[refusal.Name] {
				t.Fatalf("the recorded Python did not refuse %q; the case no longer describes the old decider", refusal.Name)
			}
			path := filepath.Join(t.TempDir(), "workflow.yml")
			if err := os.WriteFile(path, []byte(refusal.YAML), 0o600); err != nil {
				t.Fatal(err)
			}
			out, failed := decide(t, path, []byte("internal/a.go\n"))
			if !failed {
				t.Errorf("the new decider accepted a path filter it does not implement and answered:\n%s", out)
			}
		})
	}
}

// TestDeciderRefusesAFlowListNotInTheRecording: go.yml writes the filter as a block list. The old decider read any YAML;
// the new one reads only that shape and refuses another (a guess could silently drop real changes).
func TestDeciderRefusesAFlowListNotInTheRecording(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workflow.yml")
	if err := os.WriteFile(path, []byte("on:\n  pull_request:\n    paths: ['**/*.go']\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, failed := decide(t, path, []byte("internal/a.go\n")); !failed {
		t.Fatalf("a flow-style path list was accepted:\n%s", out)
	}
}

// TestProductionWorkflowPathFilterIsRead runs the decider against the real go.yml (no override): every pattern it declares
// must be readable, and the file that holds the decider's own source must be Go-relevant.
func TestProductionWorkflowPathFilterIsRead(t *testing.T) {
	command := exec.Command("bash", filepath.Join(root(t), "ci", "go_relevance.sh"))
	command.Stdin = strings.NewReader("ci/go_relevance.sh\n")
	out, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "relevant=true") {
		t.Fatalf("the production path filter is not read, or ci/go_relevance.sh is not Go-relevant: err=%v\n%s", err, out)
	}
}
