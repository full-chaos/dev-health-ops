package gqlgenguard

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
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
	workflow := readRepoFile(t, ".github/workflows/go-quality.yml")

	matches := guardStepPattern.FindAllStringSubmatch(workflow, -1)
	if len(matches) != 1 {
		t.Fatalf("expected exactly one step running the guard in go-quality.yml, found %d", len(matches))
	}
	verb, rest := matches[0][1], strings.TrimSpace(matches[0][2])

	if verb != "check-drift" {
		t.Fatalf("CI runs the guard's %q verb; only check-drift compares without writing", verb)
	}
	if strings.Contains(rest, "-update") {
		t.Fatalf("CI passes -update to the guard (%q), which REWRITES the record instead of comparing against it; the step could then never fail", rest)
	}
	if rest != "" {
		t.Fatalf("CI passes unexpected arguments to the guard: %q", rest)
	}

	// The step must be gated the same way every other gate step in this job is.
	// A step that always runs would be a different (and slower) contract; a step
	// gated on something else would silently stop running.
	idx := strings.Index(workflow, "run: go run ./cmd/gqlgen-guard")
	head := workflow[:idx]
	stepStart := strings.LastIndex(head, "      - name:")
	if stepStart < 0 {
		t.Fatal("could not find the step that runs the guard")
	}
	step := workflow[stepStart:idx]
	if !strings.Contains(step, "if: steps.relevance.outputs.relevant == 'true'") {
		t.Fatalf("the guard's step is not gated on the job's own relevance check:\n%s", step)
	}
}

func TestTheGuardsOwnInputsCanTriggerTheJobThatRunsIt(t *testing.T) {
	workflow := readRepoFile(t, ".github/workflows/go.yml")

	// go_relevance.py decides whether go-quality runs, from go.yml's own paths
	// lists. Both the push list and the pull_request list must carry every
	// input, or the check is skipped in one venue and not the other.
	required := []struct {
		pattern string
		why     string
	}{
		{"'**/*.go'", "the guard's own Go sources and its tests"},
		{"'cmd/query-api/gqlgen.yml'", "the configuration that decides every output path"},
		{"'contracts/**'", "the expected-drift record and the SDL the generator reads"},
		{"'.github/workflows/go-quality.yml'", "the workflow that hosts the guard's step"},
		{"'.github/workflows/go.yml'", "the file these path lists live in"},
		{"'**/go.mod'", "tools.go's requires, without which the generator cannot start"},
		{"'**/go.sum'", "the same"},
	}
	for _, r := range required {
		if n := strings.Count(workflow, r.pattern); n < 2 {
			t.Errorf("go.yml lists %s only %d time(s); it must appear in BOTH the push and the pull_request paths (%s)",
				r.pattern, n, r.why)
		}
	}
}

func TestTheGuardsInputsAreClassifiedRelevantByTheRelevanceScript(t *testing.T) {
	// A path list is only as good as the script that reads it. This asserts the
	// classifier itself, over the real files, rather than the YAML text.
	script := filepath.Join(repoRoot(t), "ci", "go_relevance.py")
	if _, err := os.Stat(script); err != nil {
		t.Skipf("ci/go_relevance.py is not present: %v", err)
	}

	for _, changed := range []string{
		"cmd/query-api/gqlgen.yml",
		"cmd/gqlgen-guard/main.go",
		"internal/gqlgenguard/guard.go",
		"contracts/gqlgen/v1/expected-drift.record",
		".github/workflows/go-quality.yml",
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
	python := ""
	for _, candidate := range []string{"python3", "python"} {
		if _, err := exec.LookPath(candidate); err == nil {
			python = candidate
			break
		}
	}
	if python == "" {
		t.Skip("neither python3 nor python is on PATH; the relevance classifier cannot be asked")
	}
	cmd := exec.Command(python, script)
	cmd.Dir = repoRoot(t)
	cmd.Stdin = strings.NewReader(changed + "\n")
	out, err := cmd.CombinedOutput()
	return string(out), err
}
