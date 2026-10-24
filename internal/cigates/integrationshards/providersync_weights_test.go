package integrationshards_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func providersyncWeightRows(t *testing.T) []string {
	t.Helper()
	var rows []string
	for _, l := range strings.Split(readText(t, filepath.Join(rootOf(t), "ci", "go_providersync_test_weights.tsv")), "\n") {
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		rows = append(rows, l)
	}
	return rows
}

func TestProvidersyncWeightsFileIsSortedUniqueAndPositive(t *testing.T) {
	t.Parallel()
	rows := providersyncWeightRows(t)
	if len(rows) < 100 {
		t.Fatalf("weights file has %d row(s); a truncated file would send every slow test back to its class weight", len(rows))
	}
	seconds := regexp.MustCompile(`^[0-9]+(\.[0-9]{1,2})?$`)
	prev := ""
	for i, row := range rows {
		fields := strings.Split(row, "\t")
		if len(fields) != 2 || !strings.HasPrefix(fields[0], "Test") || !seconds.MatchString(fields[1]) {
			t.Fatalf("row %d %q must be TestName<TAB>seconds", i+1, row)
		}
		if v, _ := strconv.ParseFloat(fields[1], 64); v <= 0 {
			t.Fatalf("row %d %q must be positive", i+1, row)
		}
		if i > 0 && fields[0] <= prev {
			t.Fatalf("row %d %q is not strictly after %q in byte order (sorted, no duplicates)", i+1, fields[0], prev)
		}
		prev = fields[0]
	}
}

func TestProvidersyncShardBudgetIsThreeQuartersOfTheGoTestTimeout(t *testing.T) {
	t.Parallel()
	script := readText(t, filepath.Join(rootOf(t), "ci", "check_go.sh"))
	var timeout int
	for _, line := range strings.Split(script, "\n") {
		if strings.Contains(line, "go test") && strings.Contains(line, "-run \"${test_regex}\"") && strings.Contains(line, "./internal/providersync") {
			m := regexp.MustCompile(`-timeout=(\d+)m\b`).FindStringSubmatch(line)
			if m == nil {
				t.Fatalf("providersync shard go test has no -timeout=<N>m: %s", line)
			}
			timeout, _ = strconv.Atoi(m[1])
		}
	}
	m := regexp.MustCompile(`DEV_HEALTH_GO_PROVIDER_SHARD_BUDGET:-(\d+)\}`).FindStringSubmatch(script)
	if timeout == 0 || m == nil {
		t.Fatalf("timeout %d or budget default (%v) not found in ci/check_go.sh", timeout, m)
	}
	budget, _ := strconv.Atoi(m[1])
	if want := timeout * 60 * 100 * 3 / 4; budget != want {
		t.Fatalf("shard budget default %d (10 ms units) must be 75%% of the %d min go test timeout = %d", budget, timeout, want)
	}
}

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
		{"one test over 75% of the timeout", withWeights(write("huge.tsv", strings.Replace(original, first, firstName+"\t991", 1))), "over the shard budget"},
		{"a duplicate row", withWeights(write("dup.tsv", original+first+"\n")), "more than once"},
		{"a non-numeric seconds value", withWeights(write("nan.tsv", original+"TestZZZ\tabc\n")), "must be a positive number"},
		{"a row with three fields", withWeights(write("three.tsv", original+"TestZZZ\t6\textra\n")), "exactly two tab-separated fields"},
		{"an empty middle field hiding the seconds", withWeights(write("emptymid.tsv", strings.Replace(original, first, firstName+"\t\t1", 1))), "exactly two tab-separated fields"},
		{"an empty seconds field", withWeights(write("emptysec.tsv", original+"TestZZZ\t\n")), "must be a positive number"},
		{"an empty name field", withWeights(write("emptyname.tsv", original+"\t6\n")), "not a top-level Test function"},
		{"a row with no tab", withWeights(write("notab.tsv", original+"TestZZZ 6\n")), "exactly two tab-separated fields"},
		{"a missing weights file", withWeights(filepath.Join(dir, "absent.tsv")), "weights file not found"},
		{"a planned shard over the budget", withWeights(write("ok.tsv", original), "DEV_HEALTH_GO_PROVIDER_SHARD_BUDGET=90000"), "over the shard budget"},
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
