//go:build !race

package checkgo_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-11787 (interim, removed by CHAOS-11768): a go test call of a test shard gets -timeout 20m when its package list holds a
// package under internal/cigates/, else 10m. The timeout is a helper argument; no environment value picks it.

const cigatesPrefix = receiptsModulePath + "/internal/cigates/"

func timeoutHelperBody(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(scriptPath(t, "ci/check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	start := strings.Index(src, "\ncigates_go_test_timeout() {\n")
	if start < 0 {
		t.Fatal("function cigates_go_test_timeout not found in ci/check_go.sh")
	}
	rest := src[start+1:]
	end := strings.Index(rest, "\n}\n")
	if end < 0 {
		t.Fatal("function cigates_go_test_timeout has no closing brace")
	}
	return rest[:end+2]
}

func TestTheCigatesTimeoutHelperMatchesTheExactPrefix(t *testing.T) {
	t.Parallel()
	body := timeoutHelperBody(t)
	other := receiptsModulePath + "/internal/httpguard"
	for _, c := range []struct {
		name string
		args []string
		want string
	}{
		{"one cigates package", []string{cigatesPrefix + "affected"}, "20m"},
		{"cigates package among others", []string{other, cigatesPrefix + "checkgo", other + "x"}, "20m"},
		{"cigates package last", []string{other, receiptsModulePath + "/internal/api", cigatesPrefix + "workflows"}, "20m"},
		{"no cigates package", []string{other}, "10m"},
		{"no argument", nil, "10m"},
		{"the bare cigates package", []string{receiptsModulePath + "/internal/cigates"}, "10m"},
		{"a sibling with the same stem", []string{receiptsModulePath + "/internal/cigatesx"}, "10m"},
		{"a sibling with the same stem and a slash", []string{receiptsModulePath + "/internal/cigatesx/y"}, "10m"},
		{"another module path", []string{"example.com/other/internal/cigates/x"}, "10m"},
		{"a relative path", []string{"./internal/cigates/x"}, "10m"},
	} {
		cmd := exec.Command("bash", "-c", body+"\ncigates_go_test_timeout \"$@\"\n", "_")
		cmd.Args = append(cmd.Args, c.args...)
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "GO_TEST_JSON_TIMEOUT=1s", "DEV_HEALTH_GO_RACE_TEST_TIMEOUT=1s"}
		out, err := cmd.Output()
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if got := strings.TrimSpace(string(out)); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

func TestEveryGoTestCallOfAShardTakes20mOnlyWhenItHoldsACigatesPackage(t *testing.T) {
	t.Parallel()
	heavy := fullPkg("internal/gqlgenguard")
	// One ambient set: an inherited timeout value changes nothing, and the merge_group security batch (rest_sec) runs too.
	for _, ambient := range [][]string{{"GO_TEST_JSON_TIMEOUT=1s", "DEV_HEALTH_GO_FRESH_SECURITY=1"}} {
		twenty := 0
		for shard := 1; shard <= 3; shard++ {
			res, lines := runShardWithFakeGo(t, shard, "", ambient...)
			if res.code != 0 {
				t.Fatalf("test shard %d/3 (%v) exited %d: %s", shard, ambient, res.code, failureTail(res))
			}
			for _, l := range lines {
				if strings.Contains(l, "-tags=integration") || !strings.Contains(l, " -timeout ") {
					continue
				}
				want := " -timeout 10m "
				for _, f := range strings.Fields(l) {
					if strings.HasPrefix(f, cigatesPrefix) {
						want = " -timeout 20m "
					}
				}
				if hasField(l, heavy) && want != " -timeout 10m " {
					t.Errorf("shard %d (%v): the heavy package alone was given 20m: %s", shard, ambient, l)
				}
				if want == " -timeout 20m " {
					twenty++
				}
				if !strings.Contains(l, want) {
					t.Errorf("shard %d (%v): want%sin: %s", shard, ambient, want, l)
				}
			}
		}
		if twenty < 3 {
			t.Errorf("(%v): %d call(s) took 20m over the 3 shards, want at least one per shard", ambient, twenty)
		}
	}
}

func TestTheCheckGoScriptHasNoCheckgoOnlyTimeoutRule(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "CHECKGO_PACKAGE") {
		t.Error("ci/check_go.sh still carries the checkgo-only CHECKGO_PACKAGE timeout rule")
	}
}
