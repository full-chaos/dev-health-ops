package affected_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/full-chaos/dev-health-ops/internal/cigates/affected"
)

// CHAOS-10174 PR C: Go port of tests/tooling/test_go_affected_integration.py (CHAOS-9947). Pull requests run only the
// integration shards a change can reach. The selector (affected.PlanVerdict) is called on a
// synthetic `go list` document and on the real module; the planner (`check_go.sh integration-shard-plan`) is executed
// with each selection file.

const (
	affMod  = "example.com/m"
	affRoot = "/work/m"
)

func affPkg(rel string, extra map[string]any) map[string]any {
	m := map[string]any{
		"ImportPath": affMod + "/" + rel,
		"Dir":        affRoot + "/" + rel,
		"Module":     map[string]any{"Path": affMod},
	}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func affBinary(rel string, deps ...string) map[string]any {
	var d []string
	for _, dep := range deps {
		d = append(d, fmt.Sprintf("%s/%s [%s/%s.test]", affMod, dep, affMod, rel))
	}
	return map[string]any{"ImportPath": affMod + "/" + rel + ".test", "Deps": d}
}

// leaf <- mid <- top ; other is unrelated. Integration packages: mid, top, other.
func affGolist() []map[string]any {
	return []map[string]any{
		affPkg("internal/leaf", nil),
		affPkg("internal/mid", map[string]any{"EmbedFiles": []string{"sql/001.sql"}}),
		affPkg("internal/top", nil),
		affPkg("internal/other", nil),
		affBinary("internal/mid", "internal/leaf"),
		affBinary("internal/top", "internal/mid", "internal/leaf"),
		affBinary("internal/other"),
	}
}

var affKeys = []string{"internal/mid", "internal/other", "internal/top"}

type selection struct {
	verdict string
	keys    []string
}

func affSelect(script, work string, golist []map[string]any, keys []string, root string, changed ...string) (selection, error) {
	var lines []string
	for _, e := range golist {
		raw, _ := json.Marshal(e)
		lines = append(lines, string(raw))
	}
	files := map[string][]byte{
		"golist.json": []byte(strings.Join(lines, "\n")),
		"keys.txt":    []byte(strings.Join(keys, "\n") + "\n"),
		"changed.txt": []byte(strings.Join(changed, "\x00") + "\x00"),
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(work, name), body, 0o644); err != nil {
			return selection{}, err
		}
	}
	var stdout, stderr bytes.Buffer
	if code := affected.PlanVerdict([]string{"--event", "pull_request", "--changed", filepath.Join(work, "changed.txt"), "--golist", filepath.Join(work, "golist.json"),
		"--keys", filepath.Join(work, "keys.txt"), "--root", root}, &stdout, &stderr); code != 0 {
		return selection{}, fmt.Errorf("selector exited %d: %s", code, stderr.String())
	}
	out := strings.Split(strings.TrimRight(stdout.String(), "\n"), "\n")
	if strings.HasPrefix(out[0], "AFFECTED ") {
		return selection{verdict: "AFFECTED", keys: out[1:]}, nil
	}
	return selection{verdict: out[0]}, nil
}

func wantSelection(got selection, err error, verdict string, keys ...string) error {
	if err != nil {
		return err
	}
	if got.verdict != verdict || !reflect.DeepEqual(got.keys, keys) {
		return fmt.Errorf("selected %q %q, want %q %q", got.verdict, got.keys, verdict, keys)
	}
	return nil
}

func wantFull(got selection, err error, prefix string) error {
	if err != nil {
		return err
	}
	if !strings.HasPrefix(got.verdict, prefix) || len(got.keys) != 0 {
		return fmt.Errorf("verdict %q keys %q, want a verdict starting %q and no keys", got.verdict, got.keys, prefix)
	}
	return nil
}

func affectedSelectorScenarios() []scenario {
	sel := func(script, work string, changed ...string) (selection, error) {
		return affSelect(script, work, affGolist(), affKeys, affRoot, changed...)
	}
	return []scenario{
		{"leaf_change_selects_its_reverse_closure_only", func(s, w string) error {
			g, err := sel(s, w, "internal/leaf/leaf.go")
			return wantSelection(g, err, "AFFECTED", "internal/mid", "internal/top")
		}},
		{"top_change_selects_only_itself", func(s, w string) error {
			g, err := sel(s, w, "internal/top/x_test.go")
			return wantSelection(g, err, "AFFECTED", "internal/top")
		}},
		{"unrelated_change_selects_the_unrelated_package", func(s, w string) error {
			g, err := sel(s, w, "internal/other/o.go")
			return wantSelection(g, err, "AFFECTED", "internal/other")
		}},
		{"shared_changes_run_the_full_suite", func(s, w string) error {
			for _, path := range []string{"ci/check_go.sh", ".github/workflows/go.yml", "go.mod", "go.sum", "internal/testsupport/containers/harness.go"} {
				g, err := sel(s, w, "internal/leaf/leaf.go", path)
				if err := wantFull(g, err, "FULL shared change"); err != nil {
					return fmt.Errorf("%s: %w", path, err)
				}
			}
			return nil
		}},
		{"go_file_in_an_unlisted_directory_fails_open", func(s, w string) error {
			g, err := sel(s, w, "internal/ghost/g.go")
			return wantFull(g, err, "FULL Go file outside any listed package")
		}},
		{"data_file_counts_only_as_testdata_or_embed", func(s, w string) error {
			g, err := sel(s, w, "internal/mid/sql/001.sql")
			if err := wantSelection(g, err, "AFFECTED", "internal/mid", "internal/top"); err != nil {
				return fmt.Errorf("embedded sql: %w", err)
			}
			g, err = sel(s, w, "internal/leaf/testdata/a.json")
			if err != nil || g.verdict != "AFFECTED" {
				return fmt.Errorf("testdata json: %v %v", g, err)
			}
			// a file the package neither embeds nor keeps under testdata may be read at run time by any test
			for _, path := range []string{"internal/mid/fixtures/a.json", "src/dev_health_ops/x.py"} {
				g, err = sel(s, w, path)
				if err := wantFull(g, err, "FULL non-Go"); err != nil {
					return fmt.Errorf("%s: %w", path, err)
				}
			}
			return nil
		}},
		{"docs_and_markdown_are_not_ignored", func(s, w string) error {
			for _, path := range []string{"docs/guide/a.md", "README.md"} {
				g, err := sel(s, w, path)
				if err := wantFull(g, err, "FULL non-Go"); err != nil {
					return fmt.Errorf("%s: %w", path, err)
				}
			}
			return nil
		}},
		{"embedded_markdown_is_placed_by_its_owner", func(s, w string) error {
			golist := []map[string]any{
				affPkg("internal/mid", map[string]any{"EmbedFiles": []string{"sql/README.md"}}),
				affPkg("internal/top", nil),
				affBinary("internal/mid"),
				affBinary("internal/top", "internal/mid"),
			}
			g, err := affSelect(s, w, golist, []string{"internal/mid", "internal/top"}, affRoot, "internal/mid/sql/README.md")
			return wantSelection(g, err, "AFFECTED", "internal/mid", "internal/top")
		}},
		{"go_file_under_testdata_is_a_fixture_of_its_package", func(s, w string) error {
			g, err := sel(s, w, "internal/leaf/testdata/plant/plant.go")
			return wantSelection(g, err, "AFFECTED", "internal/mid", "internal/top")
		}},
		{"package_without_a_test_binary_fails_open", func(s, w string) error {
			g, err := affSelect(s, w, affGolist(), append(append([]string{}, affKeys...), "internal/nobinary"), affRoot, "internal/leaf/leaf.go")
			return wantFull(g, err, "FULL integration package without a test binary")
		}},
		{"empty_go_list_is_an_error_not_a_pass", func(s, w string) error {
			g, err := affSelect(s, w, nil, affKeys, affRoot, "internal/leaf/leaf.go")
			if err == nil {
				return fmt.Errorf("an empty go list was accepted and selected %q %q", g.verdict, g.keys)
			}
			return nil
		}},
		{"cli_reads_nul_separated_input", func(s, w string) error {
			// two NUL-terminated entries whose names contain a space-free path each: split on NUL, not newline
			g, err := sel(s, w, "internal/leaf/leaf.go", "internal/other/o.go")
			return wantSelection(g, err, "AFFECTED", "internal/mid", "internal/other", "internal/top")
		}},
	}
}

func TestGoAffectedIntegrationSelector(t *testing.T) {
	assertAllPass(t, "", affectedSelectorScenarios())
}

var runLine = regexp.MustCompile(`(?m)^  RUN  (\S+)$`)

// realSelection runs the selector over the real module for the given NUL-joined changed list.
func realSelection(t *testing.T, root string, changed []byte) (lines []string, total int, selectionFile string) {
	t.Helper()
	env := inheritedEnv([]string{"GOFLAGS"}, "GOFLAGS=")
	cov := cmdSpec{dir: root, env: env}.run("bash", filepath.Join(root, "ci", "check_go.sh"), "integration-coverage")
	if cov.code != 0 {
		t.Fatalf("integration-coverage: %s%s", cov.stderr, cov.stdout)
	}
	var keys []string
	for _, m := range runLine.FindAllStringSubmatch(cov.stdout, -1) {
		keys = append(keys, m[1])
	}
	work := t.TempDir()
	gl := cmdSpec{dir: root, env: env}.run("go", "list", "-mod=readonly", "-tags=integration", "-test", "-json", "./...")
	if gl.code != 0 {
		t.Fatalf("go list: %s", gl.stderr)
	}
	for name, body := range map[string][]byte{"golist.json": []byte(gl.stdout), "keys.txt": []byte(strings.Join(keys, "\n") + "\n"), "changed.txt": changed} {
		if err := os.WriteFile(filepath.Join(work, name), body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var selOut, selErr bytes.Buffer
	if code := affected.PlanVerdict([]string{"--event", "pull_request", "--changed", filepath.Join(work, "changed.txt"), "--golist", filepath.Join(work, "golist.json"),
		"--keys", filepath.Join(work, "keys.txt"), "--root", root}, &selOut, &selErr); code != 0 {
		t.Fatalf("selector: %s", selErr.String())
	}
	file := filepath.Join(work, "selection.txt")
	if err := os.WriteFile(file, selOut.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimRight(selOut.String(), "\n"), "\n"), len(keys), file
}

var planOutLine = regexp.MustCompile(`(?m)^([a-z]+)=(.*)$`)

func planOutputs(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, m := range planOutLine.FindAllStringSubmatch(string(raw), -1) {
		out[m[1]] = m[2]
	}
	return out, nil
}

// plan runs the real planner from the repository that holds `script`, with an optional selection file.
func plan(script, work, selection string) (cmdResult, map[string]string, error) {
	root := filepath.Dir(filepath.Dir(script))
	out := filepath.Join(work, "github_output")
	env := inheritedEnv([]string{"INTEGRATION_AFFECTED_FILE", "GITHUB_OUTPUT"}, "GITHUB_OUTPUT="+out)
	if selection != "" {
		env = append(env, "INTEGRATION_AFFECTED_FILE="+selection)
	}
	res := cmdSpec{dir: root, env: env}.run("bash", script, "integration-shard-plan")
	outputs, err := planOutputs(out)
	if err != nil && res.code == 0 {
		return res, nil, err
	}
	return res, outputs, nil
}

func malformedSelectionScenario(body string) func(script, work string) error {
	return func(script, work string) error {
		bad := filepath.Join(work, "bad.txt")
		if err := os.WriteFile(bad, []byte(body), 0o644); err != nil {
			return err
		}
		res, _, err := plan(script, work, bad)
		if err != nil {
			return err
		}
		if res.code == 0 {
			return fmt.Errorf("the planner accepted the malformed selection %q", body)
		}
		return nil
	}
}

func affectedPlannerRejectScenarios() []scenario {
	return []scenario{
		{"planner_rejects_an_unplanned_package", malformedSelectionScenario("AFFECTED 1\ninternal/not-a-package\n")},
		{"planner_rejects_a_count_mismatch", malformedSelectionScenario("AFFECTED 2\ninternal/admincli\n")},
		{"planner_rejects_a_duplicate_key", malformedSelectionScenario("AFFECTED 2\ninternal/admincli\ninternal/admincli\n")},
		{"planner_rejects_an_unknown_first_line", malformedSelectionScenario("oops\n")},
	}
}

func TestGoWorkflowGatesShardsOnLegsAndAggregatorAcceptsOnlyExplicitZero(t *testing.T) {
	var wf struct {
		Jobs map[string]struct {
			Outputs map[string]string `yaml:"outputs"`
			Steps   []struct {
				Name string `yaml:"name"`
				If   string `yaml:"if"`
				Run  string `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(readText(t, scriptPath(t, ".github/workflows/go.yml"))), &wf); err != nil {
		t.Fatal(err)
	}
	planJob, ok := wf.Jobs["go-storage-integration-plan"]
	if !ok || planJob.Outputs["legs"] != "${{ steps.plan.outputs.legs }}" {
		t.Fatalf("go-storage-integration-plan must expose legs from steps.plan, got %+v", planJob.Outputs)
	}
	selected := false
	for _, s := range planJob.Steps {
		if strings.HasPrefix(s.Name, "Select the integration") {
			selected = true
			if s.If != "github.event_name == 'pull_request'" {
				t.Fatalf("the selection step must run on pull_request only, got if: %q", s.If)
			}
		}
	}
	if !selected {
		t.Fatal("no step named 'Select the integration...' in go-storage-integration-plan")
	}
	storage, ok := wf.Jobs["go-storage-integration"]
	if !ok || len(storage.Steps) == 0 {
		t.Fatal("go-storage-integration has no steps")
	}
	run := storage.Steps[0].Run
	if !strings.Contains(run, `"${PLAN_LEGS}" = "0"`) || !strings.Contains(run, `"${SHARD_RESULT}" = "skipped"`) {
		t.Fatalf("the aggregator's first step must accept only an explicit PLAN_LEGS=0 with a skipped shard, got:\n%s", run)
	}
}
