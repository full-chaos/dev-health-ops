//go:build !race

package integrationshards_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestProvidersyncPlanStaysUnderTheBudgetAndFailsEachPlantedDefect(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	original := readText(t, filepath.Join(root, "ci", "go_providersync_test_weights.tsv"))
	rows := providersyncWeightRows(t)
	first := rows[0]
	firstName := strings.SplitN(first, "\t", 2)[0]
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	withWeights := func(path string, more ...string) shardEnv {
		return shardEnv{extra: append([]string{"DEV_HEALTH_GO_PROVIDER_TEST_WEIGHTS=" + path}, more...)}
	}

	t.Run("checked-in weights plan every shard under the budget", func(t *testing.T) {
		t.Parallel()
		r := runCheckGoSharding(t, shardEnv{}, "integration-shard-plan")
		if r.code != 0 {
			t.Fatalf("exit %d: %s%s", r.code, r.stdout, r.stderr)
		}
		re := regexp.MustCompile(`(?m)^providersync test shard (\d+): relative weight (\d+),`)
		found := re.FindAllStringSubmatch(r.stdout, -1)
		if len(found) != shardCount(t, filepath.Join(root, "ci", "go_providersync_test_shards.tsv")) {
			t.Fatalf("plan printed %d providersync shard line(s)", len(found))
		}
		for _, f := range found {
			if w, _ := strconv.Atoi(f[2]); w > 99000 {
				t.Errorf("shard %s plans %d (10 ms units), over the 99000 budget", f[1], w)
			}
		}
	})

	cases := []struct {
		name string
		env  shardEnv
		want string
	}{
		{"a row naming a test that does not exist", withWeights(write("unknown.tsv", original+"TestZZZNoSuchProvidersyncTest\t6\n")), "which is not a discovered top-level test"},
		{"a row naming an untagged test", withWeights(write("untagged.tsv", original+"TestEveryLogSiteLogsOnlyScalars\t47.43\n")), "ordinary tests do not run in shards"},
		{"one test over 75% of the timeout", withWeights(write("huge.tsv", strings.Replace(original, first, firstName+"\t991", 1))), "over the shard budget"},
		{"a duplicate row", withWeights(write("dup.tsv", original+first+"\n")), "more than once"},
		{"a non-numeric seconds value", withWeights(write("nan.tsv", original+"TestZZZ\tabc\n")), "must be a positive number"},
		{"a row with three fields", withWeights(write("three.tsv", original+"TestZZZ\t6\textra\n")), "exactly two tab-separated fields"},
		{"an empty middle field hiding the seconds", withWeights(write("emptymid.tsv", strings.Replace(original, first, firstName+"\t\t1", 1))), "exactly two tab-separated fields"},
		{"an empty seconds field", withWeights(write("emptysec.tsv", original+"TestZZZ\t\n")), "must be a positive number"},
		{"an empty name field", withWeights(write("emptyname.tsv", original+"\t6\n")), "not a top-level Test function"},
		{"a row with no tab", withWeights(write("notab.tsv", original+"TestZZZ 6\n")), "exactly two tab-separated fields"},
		{"a missing weights file", withWeights(filepath.Join(dir, "absent.tsv")), "weights file not found"},
		{"a planned shard over the budget", withWeights(write("ok.tsv", original), "DEV_HEALTH_GO_PROVIDER_SHARD_BUDGET=85000"), "over the shard budget"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			r := runCheckGoSharding(t, c.env, "integration-shard-plan")
			if r.code != 2 || !strings.Contains(r.stderr, c.want) {
				t.Errorf("exit %d, want 2 with %q: %s", r.code, c.want, r.stderr)
			}
		})
	}
}
