//go:build !race

package splitshards_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// A package file of ci/go_integration_shards.d with a `split-by-test N` row runs as N parts of its top-level tests inside
// the `packages` shards (CHAOS-13520). Every name the compiled package registers under the integration tag that a shard
// runs (the integration-only names, every name under a tag-only hook) must run in exactly one packages shard of the
// emitted matrix, and the package must sit in at most N shards.

var packagesShardRE = regexp.MustCompile(`\{"target":"packages","shard":([0-9]+)\}`)

type splitPackage struct {
	key   string
	parts int
}

func splitPackages(t *testing.T, dir string) []splitPackage {
	t.Helper()
	var out []splitPackage
	for _, f := range rowFiles(t, dir) {
		key, parts := "", 0
		for _, line := range strings.Split(readText(t, f), "\n") {
			fields := strings.Split(line, "\t")
			if len(fields) != 2 || strings.HasPrefix(line, "#") {
				continue
			}
			switch {
			case fields[0] == "split-by-test":
				n, err := strconv.Atoi(fields[1])
				if err != nil {
					t.Fatalf("%s: split-by-test %q: %v", f, fields[1], err)
				}
				parts = n
			case strings.Contains(fields[0], "/"):
				key = fields[0]
			}
		}
		if parts > 0 {
			out = append(out, splitPackage{key: key, parts: parts})
		}
	}
	return out
}

func planCmd(root string, extra ...string) freeSpec {
	return freeSpec{dir: root, env: inheritedEnv([]string{"GITHUB_OUTPUT", "INTEGRATION_AFFECTED_FILE"}, extra...)}
}

func packagesShards(t *testing.T, root string) []int {
	t.Helper()
	r := planCmd(root).run("bash", "ci/check_go.sh", "integration-shard-plan")
	if r.code != 0 {
		t.Fatalf("integration-shard-plan: exit %d: %s%s", r.code, r.stdout, r.stderr)
	}
	var shards []int
	for _, m := range packagesShardRE.FindAllStringSubmatch(r.stdout, -1) {
		n, _ := strconv.Atoi(m[1])
		shards = append(shards, n)
	}
	if len(shards) == 0 {
		t.Fatal("the integration matrix emits no packages shard")
	}
	return shards
}

func TestEverySplitByTestNameRunsInExactlyOnePackagesShard(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	split := splitPackages(t, filepath.Join(root, "ci/go_integration_shards.d"))
	if len(split) == 0 {
		t.Skip("no package of ci/go_integration_shards.d declares split-by-test")
	}
	shards := packagesShards(t, root)
	var mu sync.Mutex
	runs := map[string][]string{}  // key -> every name a shard's dry run selected (with repeats)
	inShards := map[string][]int{} // key -> the shards that ran it
	var wg sync.WaitGroup
	for _, shard := range shards {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			r := dryRun(t, root, "packages", shard)
			if r.code != 0 {
				t.Errorf("packages shard %d dry run: exit %d: %s%s", shard, r.code, r.stdout, r.stderr)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			seen := map[string]bool{}
			for _, l := range runLines(r.stdout, "  SHARD-TEST ") {
				key, name, ok := strings.Cut(l, " ")
				if !ok {
					t.Errorf("shard %d: malformed SHARD-TEST line %q", shard, l)
					continue
				}
				runs[key] = append(runs[key], name)
				if !seen[key] {
					seen[key] = true
					inShards[key] = append(inShards[key], shard)
				}
			}
		}(shard)
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	for _, sp := range split {
		t.Run(sp.key, func(t *testing.T) {
			t.Parallel()
			listed := goTestList(t, root, sp.key)
			want, hook := taggedOnly(t, root, sp.key)
			if hook != "" {
				want = listed
			}
			got := map[string]int{}
			for _, n := range runs[sp.key] {
				got[n]++
			}
			var missing, twice, extra []string
			for n := range want {
				if !listed[n] {
					t.Errorf("%s: the oracle names %s, which go test -tags=integration -list does not list", sp.key, n)
				}
				if got[n] == 0 {
					missing = append(missing, n)
				}
			}
			for n, c := range got {
				if c > 1 {
					twice = append(twice, n)
				}
				if !want[n] {
					extra = append(extra, n)
				}
			}
			sort.Strings(missing)
			sort.Strings(twice)
			sort.Strings(extra)
			if len(missing) > 0 || len(twice) > 0 || len(extra) > 0 {
				t.Errorf("in no shard (%d): %v; in two shards (%d): %v; not a name its shards run (%d): %v",
					len(missing), missing[:min(5, len(missing))], len(twice), twice[:min(5, len(twice))], len(extra), extra[:min(5, len(extra))])
			}
			if n := len(inShards[sp.key]); n == 0 || n > sp.parts {
				t.Errorf("%s runs in %d packages shard(s) %v, want 1..%d (its split-by-test parts)", sp.key, n, inShards[sp.key], sp.parts)
			}
		})
	}
}

// The manifest rules of a split-by-test package: each case writes a copy of ci/go_integration_shards.d with one change and
// wants integration-shard-plan to refuse it with the named text.
func TestSplitByTestManifestRefusals(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	src := filepath.Join(root, "ci/go_integration_shards.d")
	const pkgFile = "internal__syncreconciler.tsv"
	pkgRow := ""
	for _, l := range strings.Split(readText(t, filepath.Join(src, pkgFile)), "\n") {
		if strings.HasPrefix(l, "internal/syncreconciler\t") {
			pkgRow = l + "\n"
		}
	}
	if pkgRow == "" {
		t.Fatalf("%s holds no internal/syncreconciler row", pkgFile)
	}
	names, _ := taggedOnly(t, root, "internal/syncreconciler")
	firstName := ""
	for n := range names {
		if firstName == "" || n < firstName {
			firstName = n
		}
	}
	cases := []struct{ name, content, want string }{
		{"one part", pkgRow + "split-by-test\t1\n", "split-by-test needs at least 2 parts"},
		{"test row without split", pkgRow + firstName + "\t30\n", "test rows need a split-by-test row"},
		{"stale test row", pkgRow + "split-by-test\t2\nTestNoSuchTestInThisPackage\t30\n", "which its shards do not run"},
		{"more parts than tests", pkgRow + "split-by-test\t100000\n", "parts but its shards run"},
		{"split twice", pkgRow + "split-by-test\t2\nsplit-by-test\t3\n", "split-by-test more than once"},
		{"test row twice", pkgRow + "split-by-test\t2\n" + firstName + "\t30\n" + firstName + "\t31\n", "more than once"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			dir := copyManifest(t, src)
			if err := os.WriteFile(filepath.Join(dir, pkgFile), []byte(c.content), 0o644); err != nil {
				t.Fatal(err)
			}
			r := planCmd(root, "DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST="+dir).run("bash", "ci/check_go.sh", "integration-shard-plan")
			if r.code == 0 || !strings.Contains(r.stdout+r.stderr, c.want) {
				t.Errorf("want a refusal naming %q, got exit %d: %s", c.want, r.code, tail(r.stdout+r.stderr))
			}
		})
	}
	t.Run("legacy split package", func(t *testing.T) {
		t.Parallel()
		dir := copyManifest(t, src)
		f := filepath.Join(dir, "internal__jobs__metrics__daily.tsv")
		if err := os.WriteFile(f, []byte(readText(t, f)+"split-by-test\t2\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		r := planCmd(root, "DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST="+dir).run("bash", "ci/check_go.sh", "integration-shard-plan")
		if r.code == 0 || !strings.Contains(r.stdout+r.stderr, "already runs as its own test shards") {
			t.Errorf("want a refusal of a split of a test-sharded package, got exit %d: %s", r.code, tail(r.stdout+r.stderr))
		}
	})
}

func copyManifest(t *testing.T, src string) string {
	t.Helper()
	dir := t.TempDir()
	for _, f := range rowFiles(t, src) {
		if err := os.WriteFile(filepath.Join(dir, filepath.Base(f)), []byte(readText(t, f)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func tail(s string) string {
	if len(s) > 2000 {
		return "..." + s[len(s)-2000:]
	}
	return s
}
