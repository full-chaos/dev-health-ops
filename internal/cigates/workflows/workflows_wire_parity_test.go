package workflows_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// CHAOS-10177: Go port of tests/tooling/test_go_workflow_wire_parity_strict.py. The go.yml graphql-wire-parity job
// runs the check strictly, paired or not: its step takes no tolerance input and the command line is fixed.

const wireParityStep = "Run graphql-wire-parity check against this repo's HEAD"

var wireParityWant = []string{"graphql:wire-parity:check", "--ops-root", "/ws"}

func wireParityStepOf(root string, t *testing.T) (yamlMap, []string) {
	t.Helper()
	doc := loadWf(t, root, "go.yml")
	var matching []yamlMap
	var names []string
	for _, s := range stepsOf(asMap(jobsOf(doc)["graphql-wire-parity"])) {
		names = append(names, asString(s["name"]))
		if asString(s["name"]) == wireParityStep {
			matching = append(matching, s)
		}
	}
	if len(matching) != 1 {
		return nil, []string{fmt.Sprintf("want exactly one step %q in go.yml graphql-wire-parity, got %d among %v", wireParityStep, len(matching), names)}
	}
	return matching[0], nil
}

// wireParityRun executes the step's own script with a recording pnpm and returns the argv it received.
func wireParityRun(t *testing.T, script string, env ...string) []string {
	t.Helper()
	script = strings.ReplaceAll(script, "${{ github.workspace }}", "/ws")
	if strings.Contains(script, "${{") {
		t.Fatalf("step script still holds an expression: %s", script)
	}
	dir := t.TempDir()
	logPath := filepath.Join(dir, "pnpm.log")
	if err := os.WriteFile(filepath.Join(dir, "pnpm"), []byte("#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > \"$PNPM_LOG\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "-e", "-c", script)
	cmd.Env = append([]string{"PATH=" + dir + ":" + withShims("/usr/bin:/bin"), "PNPM_LOG=" + logPath}, env...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("step script failed: %v\n%s", err, out.String())
	}
	return strings.Split(strings.TrimRight(readFile(t, logPath), "\n"), "\n")
}

func wireParityViolations(t *testing.T, root string) []string {
	t.Helper()
	step, bad := wireParityStepOf(root, t)
	if bad != nil {
		return bad
	}
	var out []string
	if _, has := step["env"]; has {
		out = append(out, fmt.Sprintf("the wire-parity step takes an env input: %v", step["env"]))
	}
	if strings.Contains(strings.ToLower(readFile(t, filepath.Join(root, ".github/workflows/go.yml"))), "tolerate") {
		out = append(out, "go.yml mentions a tolerance switch (tolerate): the wire-parity check must be strict")
	}
	script := asString(step["run"])
	for _, env := range [][]string{nil, {"TOLERATE_MANIFEST_ONLY=1"}, {"TOLERATE_MANIFEST_ONLY=true"}, {"TOLERATE_MANIFEST_ONLY="}} {
		if got := wireParityRun(t, script, env...); !reflect.DeepEqual(got, wireParityWant) {
			out = append(out, fmt.Sprintf("with env %v the step ran pnpm %v, want %v", env, got, wireParityWant))
		}
	}
	return out
}

func TestWireParityStepIsStrict(t *testing.T) {
	expectNoViolations(t, wireParityViolations(t, repoRoot(t)))
}

func TestWireParityPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const f = ".github/workflows/go.yml"
	const runLine = `run: pnpm graphql:wire-parity:check --ops-root "${{ github.workspace }}"`
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"step grows a tolerance env input", []mutation{{f, "      - name: " + wireParityStep + "\n", "      - name: " + wireParityStep + "\n        env:\n          TOLERATE_MANIFEST_ONLY: \"1\"\n"}}, []string{"takes an env input", "tolerate"}},
		{"script honours an ambient tolerance variable", []mutation{{f, runLine, `run: pnpm graphql:wire-parity:check --ops-root "${{ github.workspace }}" ${TOLERATE_MANIFEST_ONLY:+--tolerate-manifest-only}`}}, []string{"the step ran pnpm", "tolerate"}},
		{"command line loses the ops root", []mutation{{f, runLine, `run: pnpm graphql:wire-parity:check`}}, []string{"the step ran pnpm"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, []string{f}, c.muts...)
			expectViolation(t, wireParityViolations(t, tree), c.want...)
		})
	}
}
