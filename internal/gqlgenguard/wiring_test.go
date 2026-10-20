package gqlgenguard

import (
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
var guardStepPattern = regexp.MustCompile(`(?m)^\s*run:\s*go run (?:-trimpath\s+)?\./cmd/gqlgen-guard\s+(\S+)(.*)$`)

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

func TestTheGuardsInputsAreSelectedByThePlanVerdict(t *testing.T) {
	// A path list is only as good as the program that reads it. This asks the plan verdict go-quality runs
	// (`cigate plan-verdict --go-files-full`), over the real module, rather than the YAML text: a change to any input of
	// the guard must run the whole gate (FULL) or select this package's tests.
	root := repoRoot(t)
	work := t.TempDir()
	run := func(name string, args ...string) []byte {
		t.Helper()
		cmd := exec.Command(name, args...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GOFLAGS=")
		out, err := cmd.Output()
		if err != nil {
			var stderr []byte
			if ee, ok := err.(*exec.ExitError); ok {
				stderr = ee.Stderr
			}
			t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, stderr)
		}
		return out
	}
	golist := filepath.Join(work, "golist.json")
	keys := filepath.Join(work, "keys.txt")
	if err := os.WriteFile(golist, run("go", "list", "-mod=readonly", "-test", "-json", "./..."), 0o644); err != nil {
		t.Fatal(err)
	}
	module := strings.TrimSpace(string(run("go", "list", "-m")))
	var rel []string
	for _, imp := range strings.Fields(string(run("go", "list", "-mod=readonly", "./..."))) {
		if imp == module {
			rel = append(rel, ".")
		} else {
			rel = append(rel, strings.TrimPrefix(imp, module+"/"))
		}
	}
	if err := os.WriteFile(keys, []byte(strings.Join(rel, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cigate := filepath.Join(work, "cigate")
	run("go", "build", "-mod=readonly", "-o", cigate, "./cmd/cigate")

	for _, changed := range []string{
		"internal/queryapi/gqlgen.yml",
		"cmd/gqlgen-guard/main.go",
		"internal/gqlgenguard/guard.go",
		"contracts/gqlgen/v1/expected-drift.record",
		".github/workflows/go-quality.yml",
		"internal/cigates/affected/plan.go",
	} {
		t.Run(changed, func(t *testing.T) {
			list := filepath.Join(t.TempDir(), "changed.txt")
			if err := os.WriteFile(list, []byte(changed+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			out := string(run(cigate, "plan-verdict", "--event", "pull_request", "--go-files-full", "--changed", list,
				"--golist", golist, "--keys", keys, "--root", root))
			lines := strings.Split(strings.TrimSpace(out), "\n")
			if strings.HasPrefix(lines[0], "FULL ") {
				return
			}
			for _, k := range lines[1:] {
				if k == "internal/gqlgenguard" {
					return
				}
			}
			t.Fatalf("a change to %s neither runs the whole gate nor selects internal/gqlgenguard, so its tests would be skipped:\n%s", changed, out)
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
