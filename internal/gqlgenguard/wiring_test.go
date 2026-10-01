package gqlgenguard

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// A guard that is not wired into CI is decoration. These tests read the real
// workflow files and fail if the step is missing, disabled, neutered, or
// unreachable for the very changes it exists to catch.
//
// The workflow files are INPUTS to this test, so they must themselves be able
// to trigger the job that runs it -- otherwise a commit editing only a workflow
// would green-skip the check that reads it. That is asserted below rather than
// assumed.

func readRepoFile(t *testing.T, rel string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot(t), filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(data)
}

// guardStepPattern finds the workflow step that runs the guard.
var guardStepPattern = regexp.MustCompile(`(?m)^\s*run:\s*go run \./cmd/gqlgen-guard\s+(\S+)(.*)$`)

func TestTheDriftCheckIsWiredIntoGoQualityAndIsActive(t *testing.T) {
	// The workflow is PARSED, not scanned as text: a text scan accepts
	// `if: false # if: steps.relevance...`, where the operative condition is
	// false and the gate text survives only in a comment. YAML discards comments, so what is asserted here is what the
	// Actions runner evaluates.
	var wf struct {
		Jobs map[string]struct {
			Steps []map[string]any `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(readRepoFile(t, ".github/workflows/go-quality.yml")), &wf); err != nil {
		t.Fatalf("parse go-quality.yml: %v", err)
	}
	type hit struct {
		job  string
		step map[string]any
	}
	var hits []hit
	for job, j := range wf.Jobs {
		for _, st := range j.Steps {
			if run, ok := st["run"].(string); ok && strings.Contains(run, "gqlgen-guard") {
				hits = append(hits, hit{job, st})
			}
		}
	}
	if len(hits) != 1 {
		t.Fatalf("expected exactly one step running the guard in go-quality.yml, found %d", len(hits))
	}
	st := hits[0].step
	run := strings.TrimSpace(st["run"].(string))
	m := guardStepPattern.FindStringSubmatch("run: " + run)
	if m == nil || strings.Contains(run, "\n") {
		t.Fatalf("the guard step's run is not a single `go run ./cmd/gqlgen-guard <verb>` command: %q", run)
	}
	verb, rest := m[1], strings.TrimSpace(m[2])
	if verb != "check-drift" {
		t.Fatalf("CI runs the guard's %q verb; only check-drift compares without writing", verb)
	}
	if strings.Contains(rest, "-update") {
		t.Fatalf("CI passes -update to the guard (%q), which REWRITES the record instead of comparing against it; the step could then never fail", rest)
	}
	if rest != "" {
		t.Fatalf("CI passes unexpected arguments to the guard: %q", rest)
	}

	// The step's operative condition, exactly -- the relevance gate every
	// step in the job uses, plus the leg that runs it (CHAOS-6690: go-quality
	// runs as parallel legs; the drift check is a `static`-leg step, so it is
	// still executed on every relevant change). A bool `false`, a different
	// expression, or a missing `if:` all fail.
	const gate = "steps.relevance.outputs.relevant == 'true' && matrix.name == 'static'"
	cond, ok := st["if"].(string)
	if !ok || strings.TrimSpace(cond) != gate {
		t.Fatalf("the guard step's `if:` is %#v; it must be exactly %q", st["if"], gate)
	}
	if v, ok := st["continue-on-error"]; ok && v != false {
		t.Fatalf("the guard step sets continue-on-error: %#v, so a failing drift check would not fail the job", v)
	}
}

func TestTheGuardsInputsAreClassifiedRelevantByTheRelevanceScript(t *testing.T) {
	// A path list is only as good as the script that reads it. This asserts the
	// classifier itself, over the real files, rather than the YAML text.
	script := filepath.Join(repoRoot(t), "ci", "go_relevance.py")
	if _, err := os.Stat(script); err != nil {
		// A missing classifier is a failure, never a skip: a skip reads as coverage
		// for a measurement that did not happen (CHAOS-7387).
		t.Fatalf("ci/go_relevance.py is not present: %v", err)
	}

	for _, changed := range []string{
		"internal/queryapi/gqlgen.yml",
		"cmd/gqlgen-guard/main.go",
		"internal/gqlgenguard/guard.go",
		"contracts/gqlgen/v1/expected-drift.record",
		".github/workflows/go-quality.yml",
		"ci/go_relevance.py",
	} {
		t.Run(changed, func(t *testing.T) {
			out, err := runRelevance(t, script, changed)
			if err != nil {
				t.Fatalf("run the relevance classifier: %v", err)
			}
			if !strings.Contains(out, "relevant=true") {
				t.Fatalf("a change to %s is classified NOT Go-relevant, so go-quality -- and the drift check with it -- would be skipped:\n%s",
					changed, out)
			}
		})
	}
}

// TestTheDriftRecordIsCommittedAndReadable is the cheap half of the contract:
// the artefact the CI step compares against exists and says what it is.
func TestTheDriftRecordIsCommittedAndReadable(t *testing.T) {
	record := readRepoFile(t, DefaultDriftPath)
	if !strings.HasPrefix(record, recordHeader) {
		t.Fatalf("%s does not begin with the generated header; it was hand-edited", DefaultDriftPath)
	}
	if !strings.Contains(record, "# config: "+DefaultConfigPath) {
		t.Fatalf("%s does not name the configuration it was produced from", DefaultDriftPath)
	}
	if !strings.Contains(record, "digest ") {
		t.Fatalf("%s carries no digest table", DefaultDriftPath)
	}
}

// runRelevance feeds one changed path to ci/go_relevance.py, which is the
// script go-quality actually uses. Asking the real classifier is the point: a
// second copy of its matching rules here could agree with the YAML and still
// disagree with what CI does.
func runRelevance(t *testing.T, script, changed string) (string, error) {
	t.Helper()
	python, err := relevancePython(exec.LookPath)
	if err != nil {
		// Not a skip (CHAOS-7387): a Python-free environment must FAIL this test,
		// loudly, rather than pass it without ever asking the classifier.
		t.Fatalf("the relevance classifier cannot be asked: %v", err)
	}
	cmd := exec.Command(python, script)
	cmd.Dir = repoRoot(t)
	cmd.Stdin = strings.NewReader(changed + "\n")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// relevancePython picks the interpreter that runs ci/go_relevance.py, or says
// that there is none. lookPath is exec.LookPath in production; the test of the
// missing-interpreter case passes a lookup that finds nothing.
func relevancePython(lookPath func(string) (string, error)) (string, error) {
	for _, candidate := range []string{"python3", "python"} {
		if _, err := lookPath(candidate); err == nil {
			return candidate, nil
		}
	}
	return "", errors.New("neither python3 nor python is on PATH")
}

// TestAMissingPythonIsAFailureNotASkip plants the defect CHAOS-7387 is about: an
// environment with no interpreter. The lookup must report it as an error (the
// callers fail the test on it); before this change the test skipped, so a
// Python-free job passed without ever asking the classifier.
func TestAMissingPythonIsAFailureNotASkip(t *testing.T) {
	if _, err := relevancePython(func(string) (string, error) { return "", exec.ErrNotFound }); err == nil {
		t.Fatal("a lookup that finds no interpreter must be an error")
	}
	// The real lookup over an empty PATH: the same answer through exec.LookPath.
	t.Setenv("PATH", t.TempDir())
	if _, err := relevancePython(exec.LookPath); err == nil {
		t.Fatal("with an empty PATH the real lookup must find no interpreter")
	}
}
