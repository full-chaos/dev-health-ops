//go:build !race

package splitshards_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// splitDry is what the all-shards dry run gives each split-by-test package: the names every packages shard selects for it
// (SHARD-TEST) and the -run regex of the go test group that holds it in that shard (SHARD-GROUP, then SHARD-REGEX).
type splitDry struct {
	sel   map[int]map[string]map[string]bool // shard -> key -> selected names
	regex map[int]map[string]string          // shard -> key -> -run regex of its go test group
}

func dryRunSplit(t *testing.T, root string, env ...string) splitDry {
	t.Helper()
	all := planCmd(root, env...).run("bash", "ci/check_go.sh", "integration-shard", "packages", "all", "--dry-run")
	if all.code != 0 {
		t.Fatalf("integration-shard packages all --dry-run: exit %d: %s%s", all.code, all.stdout, all.stderr)
	}
	d := splitDry{sel: map[int]map[string]map[string]bool{}, regex: map[int]map[string]string{}}
	loc := allShardsHeader.FindAllStringSubmatchIndex(all.stdout, -1)
	for i, m := range loc {
		shard, _ := strconv.Atoi(all.stdout[m[2]:m[3]])
		end := len(all.stdout)
		if i+1 < len(loc) {
			end = loc[i+1][0]
		}
		d.sel[shard], d.regex[shard] = map[string]map[string]bool{}, map[string]string{}
		var groupKeys []string
		for _, l := range strings.Split(all.stdout[m[1]:end], "\n") {
			switch {
			case strings.HasPrefix(l, "  SHARD-TEST "):
				f := strings.Fields(l)
				if d.sel[shard][f[1]] == nil {
					d.sel[shard][f[1]] = map[string]bool{}
				}
				d.sel[shard][f[1]][f[2]] = true
			case strings.HasPrefix(l, "  SHARD-GROUP "):
				groupKeys = strings.Fields(l)[3:]
			case strings.HasPrefix(l, "  SHARD-REGEX "):
				f := strings.SplitN(l, " ", 5)
				for _, k := range groupKeys {
					d.regex[shard][k] = f[4]
				}
				groupKeys = nil
			}
		}
	}
	if len(d.sel) == 0 {
		t.Fatal("the all-shards dry run prints no packages shard")
	}
	return d
}

func (d splitDry) clone() splitDry {
	c := splitDry{sel: map[int]map[string]map[string]bool{}, regex: map[int]map[string]string{}}
	for s, keys := range d.sel {
		c.sel[s] = map[string]map[string]bool{}
		for k, names := range keys {
			c.sel[s][k] = map[string]bool{}
			for n := range names {
				c.sel[s][k][n] = true
			}
		}
	}
	for s, keys := range d.regex {
		c.regex[s] = map[string]string{}
		for k, r := range keys {
			c.regex[s][k] = r
		}
	}
	return c
}

// splitDefects: every name of want runs in exactly one shard, no shard runs a name outside want, the package sits in at
// most parts shards, and in every shard that runs it the -run regex of its group matches exactly the names that shard
// selects for it among listed (an unanchored or wrong pattern captures a name of another part, or loses one).
func splitDefects(d splitDry, key string, parts int, want, listed map[string]bool) []string {
	var out []string
	count := map[string]int{}
	var shards []int
	for s, keys := range d.sel {
		names, ok := keys[key]
		if !ok {
			continue
		}
		shards = append(shards, s)
		for n := range names {
			count[n]++
		}
		re, err := regexp.Compile(d.regex[s][key])
		if err != nil || d.regex[s][key] == "" {
			out = append(out, "shard "+strconv.Itoa(s)+": no usable -run regex for "+key)
			continue
		}
		for n := range listed {
			if re.MatchString(n) != names[n] {
				out = append(out, "shard "+strconv.Itoa(s)+": -run "+strconv.FormatBool(re.MatchString(n))+" for "+key+" "+n+", selected "+strconv.FormatBool(names[n]))
			}
		}
	}
	for n := range want {
		if count[n] == 0 {
			out = append(out, "in no shard: "+n)
		}
	}
	for n, c := range count {
		if c > 1 {
			out = append(out, "in "+strconv.Itoa(c)+" shards: "+n)
		}
		if !want[n] {
			out = append(out, "not a name its shards run: "+n)
		}
	}
	if len(shards) == 0 || len(shards) > parts {
		out = append(out, key+" runs in "+strconv.Itoa(len(shards))+" shard(s), want 1.."+strconv.Itoa(parts))
	}
	sort.Strings(out)
	return out
}

// parserMisses: the names go test -list gives a split package's integration run (every kind) that no shard runs.
func parserMisses(integrationOnly map[string]bool, d splitDry, key string) []string {
	ran := map[string]bool{}
	for _, keys := range d.sel {
		for n := range keys[key] {
			ran[n] = true
		}
	}
	var out []string
	for n := range integrationOnly {
		if !ran[n] {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

type splitCase struct {
	key           string
	parts         int
	want, listed  map[string]bool
	integrationOn map[string]bool
}

// splitCases reads `go test -list .*` of every split package with and without the integration tag (every kind, through
// the shared listing cache: one compile per package and build, at most listingSlots at once).
func splitCases(t *testing.T, root string) []splitCase {
	t.Helper()
	var out []splitCase
	tc := pinnedGoToolchain(t)
	split := splitPackages(t, filepath.Join(root, "ci/go_integration_shards.d"))
	for _, sp := range split {
		startListing(root, tc, listingKey{pkg: sp.key, tags: "integration"})
		startListing(root, tc, listingKey{pkg: sp.key})
	}
	for _, sp := range split {
		tagged := listedKindsOrFail(t, root, tc, listingKey{pkg: sp.key, tags: "integration"})
		plain := listedKindsOrFail(t, root, tc, listingKey{pkg: sp.key})
		want, hook := taggedOnly(t, root, sp.key)
		integrationOnly := map[string]bool{}
		for n := range tagged {
			if hook != "" || !plain[n] {
				integrationOnly[n] = true
			}
		}
		if hook != "" {
			want = tagged
		}
		out = append(out, splitCase{key: sp.key, parts: sp.parts, want: want, listed: tagged, integrationOn: integrationOnly})
	}
	if len(out) == 0 {
		t.Skip("no package of ci/go_integration_shards.d declares split-by-test")
	}
	return out
}

// Every split package: each name exactly once, the -run regex of every shard selects exactly that shard's part, and every
// name of every kind go test -list gives its integration run (tests, examples with output, fuzz targets) runs in a shard,
// so a kind the planner's parser does not collect fails here. Then four planted defects, each must be caught.
func TestSplitByTestPartsAreExactAndPlantedDefectsAreCaught(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	// The listings compile while the all-shards dry run runs; splitCases then waits for them (one cache, no second compile).
	tc := pinnedGoToolchain(t)
	for _, sp := range splitPackages(t, filepath.Join(root, "ci/go_integration_shards.d")) {
		startListing(root, tc, listingKey{pkg: sp.key, tags: "integration"})
		startListing(root, tc, listingKey{pkg: sp.key})
	}
	d := dryRunSplit(t, root)
	cases := splitCases(t, root)
	for _, c := range cases {
		if bad := splitDefects(d, c.key, c.parts, c.want, c.listed); len(bad) > 0 {
			t.Errorf("%s: %d defect(s): %v", c.key, len(bad), bad[:min(5, len(bad))])
		}
		if miss := parserMisses(c.integrationOn, d, c.key); len(miss) > 0 {
			t.Errorf("%s: go test -list names %v for the integration run, which no packages shard runs", c.key, miss[:min(5, len(miss))])
		}
	}
	if t.Failed() {
		return
	}
	shardOf := func(d splitDry, key, name string) int {
		for s, keys := range d.sel {
			if keys[key][name] {
				return s
			}
		}
		return 0
	}
	c := cases[0]
	var anyName string
	for n := range c.want {
		if anyName == "" || n < anyName {
			anyName = n
		}
	}
	t.Run("plant: a dropped name", func(t *testing.T) {
		p := d.clone()
		delete(p.sel[shardOf(d, c.key, anyName)][c.key], anyName)
		if len(splitDefects(p, c.key, c.parts, c.want, c.listed)) == 0 {
			t.Errorf("planted defect not caught: %s %s dropped", c.key, anyName)
		}
	})
	t.Run("plant: a name in both parts", func(t *testing.T) {
		p := d.clone()
		home := shardOf(d, c.key, anyName)
		for s, keys := range p.sel {
			if s != home && keys[c.key] != nil {
				keys[c.key][anyName] = true
				break
			}
		}
		if len(splitDefects(p, c.key, c.parts, c.want, c.listed)) == 0 {
			t.Errorf("planted defect not caught: %s %s in two shards", c.key, anyName)
		}
	})
	t.Run("plant: an unanchored -run pattern captures a name of the other part", func(t *testing.T) {
		for _, c := range cases {
			for a := range c.want {
				for b := range c.want {
					sa, sb := shardOf(d, c.key, a), shardOf(d, c.key, b)
					if a == b || !strings.HasPrefix(b, a) || sa == sb {
						continue
					}
					p := d.clone()
					r := p.regex[sa][c.key]
					p.regex[sa][c.key] = strings.TrimSuffix(strings.TrimPrefix(r, "^"), "$")
					if len(splitDefects(p, c.key, c.parts, c.want, c.listed)) == 0 {
						t.Errorf("planted defect not caught: unanchored -run of shard %d captures %s (part of shard %d) by %s", sa, b, sb, a)
					}
					return
				}
			}
		}
		t.Fatal("no split package holds two names, one a prefix of the other, in different parts: the plant measures nothing")
	})
	t.Run("plant: a test with no manifest row lands in exactly one part", func(t *testing.T) {
		// Every other name of the package gets a measured row; the one left out stands for a new test nobody wrote a row for.
		dir := copyManifest(t, filepath.Join(root, "ci/go_integration_shards.d"))
		file := filepath.Join(dir, strings.ReplaceAll(c.key, "/", "__")+".tsv")
		body := readText(t, file)
		for n := range c.want {
			if n != anyName {
				body += n + "\t4\n"
			}
		}
		if err := os.WriteFile(file, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		p := dryRunSplit(t, root, "DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST="+dir)
		if bad := splitDefects(p, c.key, c.parts, c.want, c.listed); len(bad) > 0 {
			t.Errorf("with every name but %s measured: %v", anyName, bad[:min(5, len(bad))])
		}
		if n := shardOf(p, c.key, anyName); n == 0 {
			t.Errorf("the unmeasured name %s runs in no shard", anyName)
		}
	})
	t.Run("plant: a listed name of another kind that no shard runs", func(t *testing.T) {
		only := map[string]bool{"ExamplePlantedWithOutput": true, "FuzzPlanted": true}
		if got := parserMisses(only, d, c.key); len(got) != 2 {
			t.Errorf("planted defect not caught: parserMisses = %v, want both planted names", got)
		}
	})
}
