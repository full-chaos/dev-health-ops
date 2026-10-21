//go:build !race

package splitshards_test

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The packages too big for one integration job run as name-partitioned test shards (ci/go_*_test_shards.tsv). Every test
// the compiled package registers (`go test -list`) must run in exactly one shard of `check_go.sh integration-shard TARGET
// SHARD --dry-run`: none in two shards, none in no shard, none named in a shard that does not compile.

func shardsOf(t *testing.T, manifest string) int {
	t.Helper()
	for _, l := range strings.Split(readText(t, manifest), "\n") {
		if rest, ok := strings.CutPrefix(l, "shards\t"); ok {
			n, err := strconv.Atoi(strings.TrimSpace(rest))
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
	}
	t.Fatalf("%s declares no shard count", manifest)
	return 0
}

func dryRun(t *testing.T, root, target string, shard int) cmdResult {
	t.Helper()
	return freeSpec{dir: root, env: inheritedEnv(nil)}.run("bash", "ci/check_go.sh", "integration-shard", target, strconv.Itoa(shard), "--dry-run")
}

func goTestList(t *testing.T, root, pkg string) map[string]bool {
	t.Helper()
	res := freeSpec{dir: root, env: inheritedEnv(nil, "GOTOOLCHAIN="+pinnedGoToolchain(t), "GOWORK=off")}.run("go", "test", "-mod=readonly", "-tags=integration", "-list", ".*", "./"+pkg)
	if res.code != 0 {
		t.Fatalf("go test -list %s: %s%s", pkg, res.stdout, res.stderr)
	}
	out := map[string]bool{}
	for _, l := range strings.Split(res.stdout, "\n") {
		if strings.HasPrefix(l, "Test") {
			out[l] = true
		}
	}
	return out
}

func runLines(stdout, prefix string) []string {
	var out []string
	for _, l := range strings.Split(stdout, "\n") {
		if rest, ok := strings.CutPrefix(l, prefix); ok {
			out = append(out, rest)
		}
	}
	return out
}

func TestEveryTestRunsInExactlyOneSplitPackageShard(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	for _, c := range []struct{ name, manifest, prefix, pkg string }{
		{"daily", "ci/go_daily_test_shards.tsv", "  DAILY-TEST-RUN ", "internal/jobs/metrics/daily"},
		{"workerservice", "ci/go_workerservice_test_shards.tsv", "  WORKERSERVICE-TEST-RUN ", "internal/workerservice"},
		{"queryapiserver", "ci/go_queryapi_server_test_shards.tsv", "  QAPISERVER-TEST-RUN ", "internal/queryapi/server"},
		{"remaining", "ci/go_remaining_test_shards.tsv", "  REMAINING-TEST-RUN ", "internal/jobs/metrics/remaining"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			var selected []string
			for shard := 1; shard <= shardsOf(t, filepath.Join(root, c.manifest)); shard++ {
				r := dryRun(t, root, c.name, shard)
				if r.code != 0 || !strings.Contains(r.stdout, fmt.Sprintf("%s test shard %d: DRY RUN", c.name, shard)) {
					t.Fatalf("shard %d: exit %d: %s%s", shard, r.code, r.stdout, r.stderr)
				}
				chosen := runLines(r.stdout, c.prefix)
				if len(chosen) == 0 {
					t.Fatalf("%s shard %d selected no tests", c.name, shard)
				}
				selected = append(selected, chosen...)
			}
			got := map[string]bool{}
			for _, s := range selected {
				got[s] = true
			}
			if len(selected) != len(got) {
				t.Errorf("a %s test is in two shards", c.name)
			}
			listed := goTestList(t, root, c.pkg)
			var missing, extra []string
			for k := range listed {
				if !got[k] {
					missing = append(missing, k)
				}
			}
			for k := range got {
				if !listed[k] {
					extra = append(extra, k)
				}
			}
			sort.Strings(missing)
			sort.Strings(extra)
			if len(missing) > 0 || len(extra) > 0 {
				t.Errorf("in no shard: %v; in a shard but not compiled: %v", missing[:min(5, len(missing))], extra[:min(5, len(extra))])
			}
		})
	}
}
