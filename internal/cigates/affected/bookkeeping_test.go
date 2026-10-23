package affected_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const sharedChange = "FULL shared change*"

// A pure distribution table moves packages between shards and changes no test: it plans the readers of its class, not
// every package. The set is exact; any other ci/ path, a new one included, and any path that is not canonical as written
// stays a shared change.
func TestBookkeepingTablesAreNotSharedChanges(t *testing.T) {
	x := []string{"x"}
	runRows(t, []verdictRow{
		{name: "test_weights", changed: []string{"ci/go_test_weights.tsv"}, want: "AFFECTED 1", keys: x},
		{name: "providersync_race_weights", changed: []string{"ci/go_providersync_race_weights.tsv"}, want: "AFFECTED 1", keys: x},
		{name: "race_weights_dir_file", changed: []string{"ci/go_race_weights.d/internal__a.tsv"}, want: "AFFECTED 1", keys: x},
		{name: "venue_weights_dir_file", changed: []string{"ci/venue_oracle_weights.d/internal__a.tsv"}, want: "AFFECTED 1", keys: x},
		{name: "two_tables_and_a_leaf_go_file", changed: []string{"ci/go_test_weights.tsv", "ci/go_race_weights.d/a.tsv", "a/a.go"}, want: "AFFECTED 2", keys: []string{"a", "x"}},
		{name: "go_quality_plan_is_not_full_either", extra: []string{"--go-files-full"}, changed: []string{"ci/go_test_weights.tsv"}, want: "AFFECTED 1", keys: x},
		// still shared changes
		{name: "table_with_check_go_is_full", changed: []string{"ci/go_test_weights.tsv", "ci/check_go.sh"}, want: sharedChange},
		{name: "race_excluded_tests_is_full", changed: []string{"ci/race_excluded_tests.tsv"}, want: sharedChange},
		{name: "race_solo_is_full", changed: []string{"ci/go_race_solo.tsv"}, want: sharedChange},
		{name: "new_ci_table_is_full", changed: []string{"ci/foo.tsv"}, want: sharedChange},
		{name: "nested_race_weights_file_is_full", changed: []string{"ci/go_race_weights.d/a/b.tsv"}, want: sharedChange},
		{name: "non_tsv_in_race_weights_dir_is_full", changed: []string{"ci/go_race_weights.d/x.txt"}, want: sharedChange},
		{name: "race_weights_readme_is_full", changed: []string{"ci/go_race_weights.d/README.md"}, want: sharedChange},
		{name: "ci_readme_is_full", changed: []string{"ci/README.md"}, want: sharedChange},
		{name: "dotdot_into_the_set_is_full", changed: []string{"ci/foo/../go_test_weights.tsv"}, want: sharedChange},
		{name: "dotdot_out_of_the_set_is_full", changed: []string{"ci/go_race_weights.d/../check_go.sh"}, want: sharedChange},
		{name: "dot_segment_is_full", changed: []string{"ci/./go_test_weights.tsv"}, want: sharedChange},
		{name: "double_slash_is_full", changed: []string{"ci//go_test_weights.tsv"}, want: sharedChange},
		{name: "check_go_is_still_full", changed: []string{"ci/check_go.sh"}, want: sharedChange},
		{name: "go_mod_with_a_table_is_full", changed: []string{"ci/go_test_weights.tsv", "go.mod"}, want: sharedChange},
		{name: "planner_table_with_a_table_is_full", changed: []string{"ci/go_test_weights.tsv", "internal/cigates/affected/reads.tsv"}, want: "FULL planner input*"},
	})
}

// The same, over the real module with go-quality's keys (every package): each table kind selects the packages that read
// ci/, among them the shard planners' tests and the weight-file census.
func TestBookkeepingTablesSelectTheirReadersInTheRealModule(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	env := inheritedEnv([]string{"GOFLAGS"}, "GOFLAGS=")
	dir := t.TempDir()
	gl := cmdSpec{dir: root, env: env}.run("go", "list", "-mod=readonly", "-test", "-json", "./...")
	if gl.code != 0 {
		t.Fatal("go list: " + gl.stderr)
	}
	pk := cmdSpec{dir: root, env: env}.run("go", "list", "-mod=readonly", "./...")
	if pk.code != 0 {
		t.Fatal("go list: " + pk.stderr)
	}
	mod := strings.TrimSpace(cmdSpec{dir: root, env: env}.run("go", "list", "-m").stdout)
	var keys []string
	for _, imp := range strings.Fields(pk.stdout) {
		if imp == mod {
			keys = append(keys, ".")
		} else {
			keys = append(keys, strings.TrimPrefix(imp, mod+"/"))
		}
	}
	golist, keyFile := filepath.Join(dir, "golist.json"), filepath.Join(dir, "keys.txt")
	if err := os.WriteFile(golist, []byte(gl.stdout), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, []byte(strings.Join(keys, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	bin := cigate(t)
	for _, c := range []struct {
		changed string
		want    []string
	}{
		{"ci/go_test_weights.tsv", []string{"internal/cigates/testshard", "internal/cigates/weightsfiles"}},
		{"ci/go_providersync_race_weights.tsv", []string{"internal/cigates/testshard", "internal/cigates/weightsfiles"}},
		{"ci/go_race_weights.d/internal__providersync.tsv", []string{"internal/cigates/testshard", "internal/cigates/weightsfiles"}},
		{"ci/venue_oracle_weights.d/internal__scheduler__sync.tsv", []string{"internal/cigates/testshard", "internal/cigates/weightsfiles", "internal/cigates/workflows"}},
	} {
		c := c
		t.Run(c.changed, func(t *testing.T) {
			t.Parallel()
			list := filepath.Join(t.TempDir(), "changed.txt")
			if err := os.WriteFile(list, []byte(c.changed+"\x00"), 0o644); err != nil {
				t.Fatal(err)
			}
			res := runRealPlanVerdict(root, env, bin, "plan-verdict", "--event", "pull_request", "--go-files-full",
				"--changed", list, "--golist", golist, "--keys", keyFile, "--root", root)
			if res.code != 0 {
				t.Fatalf("plan-verdict exited %d: %s", res.code, res.stderr)
			}
			lines := strings.Split(strings.TrimRight(res.stdout, "\n"), "\n")
			if !strings.HasPrefix(lines[0], "AFFECTED ") {
				t.Fatalf("a change to %s plans %q, want AFFECTED", c.changed, lines[0])
			}
			have := map[string]bool{}
			for _, k := range lines[1:] {
				have[k] = true
			}
			for _, w := range c.want {
				if !have[w] {
					t.Errorf("a change to %s plans %q without %s", c.changed, lines[0], w)
				}
			}
		})
	}
}
