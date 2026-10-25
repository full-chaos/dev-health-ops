//go:build !race

package integrationshards_test

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"unicode"
	"unicode/utf8"
)

// Contracts for deterministic Go storage-integration CI sharding (ci/check_go.sh integration-shard-plan,
// integration-shard, integration-prepull). The expected package set is DERIVED from the live discovery
// (`check_go.sh integration-coverage`), not restated here: the planner dies when ci/go_integration_shards.d/ omits or
// duplicates a discovered package, so a new package needs exactly one manifest row, in ONE file.

const (
	providerPackage      = "internal/providersync"
	dailyPackage         = "internal/jobs/metrics/daily"
	workerservicePackage = "internal/workerservice"
	qapiserverPackage    = "internal/queryapi/server"
	remainingPackage     = "internal/jobs/metrics/remaining"
)

var splitPackages = map[string]bool{
	providerPackage: true, dailyPackage: true, workerservicePackage: true, qapiserverPackage: true, remainingPackage: true,
}

func shardManifestDir(root string) string {
	return filepath.Join(root, "ci", "go_integration_shards.d")
}

func manifestText(t *testing.T, root string) string {
	t.Helper()
	var b strings.Builder
	for _, f := range shardManifestFiles(t, root) {
		b.WriteString(readText(t, f))
	}
	return b.String()
}

type shardEnv struct {
	manifest, providerManifest, githubOutput string
	extra                                    []string
}

func runCheckGoSharding(t *testing.T, e shardEnv, args ...string) cmdResult {
	t.Helper()
	root := rootOf(t)
	env := inheritedEnv(nil, e.extra...)
	if e.manifest != "" {
		env = append(env, "DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST="+e.manifest)
	} else {
		env = append(env, "DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST="+shardManifestDir(root))
	}
	if e.providerManifest != "" {
		env = append(env, "DEV_HEALTH_GO_PROVIDER_TEST_SHARD_MANIFEST="+e.providerManifest)
	} else {
		env = append(env, "DEV_HEALTH_GO_PROVIDER_TEST_SHARD_MANIFEST="+filepath.Join(root, "ci", "go_providersync_test_shards.tsv"))
	}
	if e.githubOutput != "" {
		env = append(env, "GITHUB_OUTPUT="+e.githubOutput)
	}
	return freeSpec{dir: root, env: env}.run("bash", append([]string{"ci/check_go.sh"}, args...)...)
}

var (
	discoveredOnce sync.Once
	discovered     map[string]bool
)

func expectedPackages(t *testing.T) map[string]bool {
	t.Helper()
	discoveredOnce.Do(func() {
		res := freeSpec{dir: rootOf(t), env: inheritedEnv(nil)}.run("bash", "ci/check_go.sh", "integration-coverage")
		if res.code != 0 {
			return
		}
		discovered = map[string]bool{}
		for _, l := range strings.Split(res.stdout, "\n") {
			if rest, ok := strings.CutPrefix(l, "  RUN  "); ok {
				discovered[strings.TrimSpace(rest)] = true
			}
		}
	})
	if len(discovered) < 100 {
		t.Fatalf("integration discovery found only %d package(s): a truncated discovery cannot make every comparison vacuously true", len(discovered))
	}
	if !discovered[providerPackage] {
		t.Fatal("discovery does not list the providersync package")
	}
	return discovered
}

func shardCount(t *testing.T, manifest string) int {
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

func manifestRows(t *testing.T) (int, map[string]int) {
	t.Helper()
	shards, weights := -1, map[string]int{}
	for _, l := range strings.Split(manifestText(t, rootOf(t)), "\n") {
		if strings.TrimSpace(l) == "" || strings.HasPrefix(l, "#") {
			continue
		}
		k, v, _ := strings.Cut(l, "\t")
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("manifest row %q", l)
		}
		if k == "shards" {
			shards = n
		} else {
			weights[k] = n
		}
	}
	if shards < 0 {
		t.Fatal("the manifest declares no shard count")
	}
	return shards, weights
}

var (
	planHeader = regexp.MustCompile(`^integration shard plan: ([0-9]+) shard\(s\), ([0-9]+) package\(s\)$`)
	shardRow   = regexp.MustCompile(`^  SHARD ([0-9]+) ([A-Za-z0-9_./-]+) weight=([0-9]+)s$`)
	shardTotal = regexp.MustCompile(`^integration shard ([0-9]+): estimated ([0-9]+)s, ([0-9]+) package\(s\)$`)
)

type planFacts struct {
	expected                                         map[string]bool
	manifestShards                                   int
	manifestWeights                                  map[string]int
	providerShards, daily, workerservice, qapi, rest int
}

// checkShardPlan: every property the plan must have, whichever way the weights place the packages. The first broken
// one comes back as an error; it returns {shard: {package: weight}}.
func checkShardPlan(stdout, githubOutput string, f planFacts) (map[int]map[string]int, error) {
	lines := strings.Split(stdout, "\n")
	var headers [][]string
	for _, l := range lines {
		if m := planHeader.FindStringSubmatch(l); m != nil {
			headers = append(headers, m)
		}
	}
	if len(headers) != 1 {
		return nil, fmt.Errorf("want one plan header line, got %d", len(headers))
	}
	declared, _ := strconv.Atoi(headers[0][1])
	if declared != f.manifestShards || declared < 2 {
		return nil, fmt.Errorf("the plan says %d shard(s), the manifest declares %d", declared, f.manifestShards)
	}
	if n, _ := strconv.Atoi(headers[0][2]); n != len(f.expected) {
		return nil, fmt.Errorf("the plan header counts %d package(s), discovery found %d", n, len(f.expected))
	}
	assignments := map[int]map[string]int{}
	for _, l := range lines {
		if !strings.HasPrefix(l, "  SHARD ") {
			continue
		}
		m := shardRow.FindStringSubmatch(l)
		if m == nil {
			return nil, fmt.Errorf("malformed shard row: %q", l)
		}
		shard, _ := strconv.Atoi(m[1])
		if shard < 1 || shard > declared {
			return nil, fmt.Errorf("shard %d is outside 1..%d: %q", shard, declared, l)
		}
		w, _ := strconv.Atoi(m[3])
		if assignments[shard] == nil {
			assignments[shard] = map[string]int{}
		}
		assignments[shard][m[2]] = w
	}
	count := map[string]int{}
	for _, rows := range assignments {
		for p := range rows {
			count[p]++
		}
	}
	for p, n := range count {
		if n > 1 {
			return nil, fmt.Errorf("a package is assigned to more than one shard (or twice): %s", p)
		}
		if !f.expected[p] {
			return nil, fmt.Errorf("the plan adds %s", p)
		}
	}
	for p := range f.expected {
		if count[p] == 0 {
			return nil, fmt.Errorf("the plan misses %s", p)
		}
	}
	for _, rows := range assignments {
		for p, w := range rows {
			if mw := f.manifestWeights[p]; w != mw || w <= 0 {
				return nil, fmt.Errorf("%s is planned at %ds, the manifest says %ds", p, w, mw)
			}
		}
	}
	for _, p := range []string{providerPackage, dailyPackage, workerservicePackage} {
		if count[p] == 0 {
			return nil, fmt.Errorf("the plan lacks %s", p)
		}
	}
	type tot struct{ seconds, count int }
	totals := map[int]tot{}
	for _, l := range lines {
		if m := shardTotal.FindStringSubmatch(l); m != nil {
			s, _ := strconv.Atoi(m[1])
			sec, _ := strconv.Atoi(m[2])
			c, _ := strconv.Atoi(m[3])
			totals[s] = tot{sec, c}
		}
	}
	if len(totals) != declared {
		return nil, fmt.Errorf("shard totals for %d shard(s), want every shard 1..%d", len(totals), declared)
	}
	for s := 1; s <= declared; s++ {
		if _, ok := totals[s]; !ok {
			return nil, fmt.Errorf("shard totals for %v, want every shard 1..%d", totals, declared)
		}
	}
	for s, tt := range totals {
		sum := 0
		for _, w := range assignments[s] {
			sum += w
		}
		if tt.count != len(assignments[s]) || tt.seconds != sum {
			return nil, fmt.Errorf("shard %d says %ds over %d package(s); its rows sum to %ds over %d", s, tt.seconds, tt.count, sum, len(assignments[s]))
		}
	}
	heaviest, max, min := 0, -1, 1<<30
	for s, tt := range totals {
		if tt.seconds > max {
			heaviest, max = s, tt.seconds
		}
		if tt.seconds < min {
			min = tt.seconds
		}
	}
	smallest := 1 << 30
	for _, w := range assignments[heaviest] {
		if w < smallest {
			smallest = w
		}
	}
	if max-min > smallest {
		return nil, fmt.Errorf("shard %d is %ds ahead of the lightest shard but its smallest package weighs %ds: the planner is not balancing", heaviest, max-min, smallest)
	}
	out := map[string]string{}
	for _, l := range strings.Split(githubOutput, "\n") {
		if k, v, ok := strings.Cut(l, "="); ok {
			out[k] = v
		}
	}
	rawMatrix, ok := out["matrix"]
	if !ok {
		return nil, fmt.Errorf("the plan wrote no matrix")
	}
	var matrix struct {
		Include []struct {
			Target string `json:"target"`
			Shard  int    `json:"shard"`
		} `json:"include"`
	}
	if err := json.Unmarshal([]byte(rawMatrix), &matrix); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	byTarget := map[string]map[int]bool{}
	for _, e := range matrix.Include {
		key := fmt.Sprintf("%s/%d", e.Target, e.Shard)
		if seen[key] {
			return nil, fmt.Errorf("a matrix entry is repeated: %s", key)
		}
		seen[key] = true
		if byTarget[e.Target] == nil {
			byTarget[e.Target] = map[int]bool{}
		}
		byTarget[e.Target][e.Shard] = true
	}
	oneToN := func(target string, n int) error {
		got := byTarget[target]
		if len(got) != n {
			return fmt.Errorf("the %s shards are not exactly 1..%d", target, n)
		}
		for i := 1; i <= n; i++ {
			if !got[i] {
				return fmt.Errorf("the %s shards are not exactly 1..%d", target, n)
			}
		}
		return nil
	}
	for _, c := range []struct {
		target string
		n      int
	}{{"providersync", f.providerShards}, {"daily", f.daily}, {"workerservice", f.workerservice}, {"queryapiserver", f.qapi}, {"remaining", f.rest}} {
		if err := oneToN(c.target, c.n); err != nil {
			return nil, err
		}
	}
	runsPackages := map[int]bool{}
	for s, rows := range assignments {
		for p := range rows {
			if !splitPackages[p] {
				runsPackages[s] = true
			}
		}
	}
	got := byTarget["packages"]
	if len(got) != len(runsPackages) {
		return nil, fmt.Errorf("the matrix runs packages shards %v; shards holding a package other than the split ones are %v", got, runsPackages)
	}
	for s := range runsPackages {
		if !got[s] {
			return nil, fmt.Errorf("the matrix runs packages shards %v; shards holding a package other than the split ones are %v", got, runsPackages)
		}
	}
	if len(matrix.Include) != f.providerShards+f.daily+f.workerservice+f.qapi+f.rest+len(runsPackages) {
		return nil, fmt.Errorf("the matrix has %d entries, want one per shard of every target", len(matrix.Include))
	}
	return assignments, nil
}

func realPlanFacts(t *testing.T) planFacts {
	t.Helper()
	root := rootOf(t)
	shards, weights := manifestRows(t)
	ci := filepath.Join(root, "ci")
	return planFacts{
		expected: expectedPackages(t), manifestShards: shards, manifestWeights: weights,
		providerShards: shardCount(t, filepath.Join(ci, "go_providersync_test_shards.tsv")),
		daily:          shardCount(t, filepath.Join(ci, "go_daily_test_shards.tsv")),
		workerservice:  shardCount(t, filepath.Join(ci, "go_workerservice_test_shards.tsv")),
		qapi:           shardCount(t, filepath.Join(ci, "go_queryapi_server_test_shards.tsv")),
		rest:           shardCount(t, filepath.Join(ci, "go_remaining_test_shards.tsv")),
	}
}

var (
	planOnce             sync.Once
	planStdout, planGHO  string
	planCode             int
	planStderr           string
	providerTestsOnce    sync.Once
	providerTopLevel     map[string]bool
	providerIntegrationT map[string]bool
)

func realPlan(t *testing.T) (string, string) {
	t.Helper()
	planOnce.Do(func() {
		out := filepath.Join(os.TempDir(), fmt.Sprintf("plan-github-output-%d", os.Getpid()))
		_ = os.WriteFile(out, nil, 0o644)
		res := runCheckGoSharding(t, shardEnv{githubOutput: out}, "integration-shard-plan")
		planCode, planStdout, planStderr = res.code, res.stdout, res.stderr
		b, _ := os.ReadFile(out)
		planGHO = string(b)
		_ = os.Remove(out)
	})
	if planCode != 0 {
		t.Fatalf("integration-shard-plan exited %d: %s%s", planCode, planStdout, planStderr)
	}
	return planStdout, planGHO
}

func providerTests(t *testing.T) (map[string]bool, map[string]bool) {
	t.Helper()
	providerTestsOnce.Do(func() {
		root := rootOf(t)
		res := freeSpec{dir: root, env: inheritedEnv(nil, "GOTOOLCHAIN="+pinnedGoToolchain(t), "GOWORK=off")}.run("go", "list", "-mod=readonly", "-tags=integration",
			"-f={{range .TestGoFiles}}{{println .}}{{end}}{{range .XTestGoFiles}}{{println .}}{{end}}", "./"+providerPackage)
		if res.code != 0 {
			return
		}
		plain := freeSpec{dir: root, env: inheritedEnv(nil, "GOTOOLCHAIN="+pinnedGoToolchain(t), "GOWORK=off")}.run("go", "list", "-e", "-mod=readonly",
			"-f={{range .TestGoFiles}}{{println .}}{{end}}{{range .XTestGoFiles}}{{println .}}{{end}}", "./"+providerPackage)
		if plain.code != 0 {
			return
		}
		inPlain := map[string]bool{}
		for _, f := range strings.Fields(plain.stdout) {
			inPlain[f] = true
		}
		providerTopLevel, providerIntegrationT = map[string]bool{}, map[string]bool{}
		for _, f := range strings.Fields(res.stdout) {
			tests, _ := parsedTestFuncs(t, filepath.Join(root, providerPackage, f))
			for _, n := range tests {
				providerTopLevel[n] = true
				if !inPlain[f] {
					providerIntegrationT[n] = true
				}
			}
		}
	})
	if len(providerTopLevel) == 0 || len(providerIntegrationT) == 0 {
		t.Fatal("providersync test discovery found nothing")
	}
	return providerTopLevel, providerIntegrationT
}

// goTestSignature is the signature rule of cmd/go for a test: one parameter of type *T or *x.T, no results, no type
// parameters (go test refuses to build a package whose Test-named function breaks it).
func goTestSignature(fn *ast.FuncDecl) bool {
	if fn.Type.TypeParams.NumFields() > 0 || fn.Type.Results.NumFields() > 0 || fn.Type.Params == nil ||
		len(fn.Type.Params.List) != 1 || len(fn.Type.Params.List[0].Names) > 1 {
		return false
	}
	ptr, ok := fn.Type.Params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	switch x := ptr.X.(type) {
	case *ast.Ident:
		return x.Name == "T"
	case *ast.SelectorExpr:
		return x.Sel.Name == "T"
	}
	return false
}

// parsedTestFuncs parses one Go test file and returns its top-level tests (cmd/go's rule: "Test" or "Test" plus a rune
// that is not lower case, no receiver, goTestSignature; TestMain aside) and the first top-level init or TestMain.
func parsedTestFuncs(t *testing.T, path string) ([]string, string) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var tests []string
	hook := ""
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil {
			continue
		}
		name := fn.Name.Name
		if name == "init" || name == "TestMain" {
			if hook == "" {
				hook = name
			}
			continue
		}
		if rest, ok := strings.CutPrefix(name, "Test"); ok && goTestSignature(fn) {
			if r, _ := utf8.DecodeRuneInString(rest); rest == "" || !unicode.IsLower(r) {
				tests = append(tests, name)
			}
		}
	}
	return tests, hook
}

// taggedFacts is the oracle for the tag filter, built apart from check_go.sh: per package key, the top-level tests of
// the test files that only the integration build lists (go list -tags=integration minus plain go list), the top-level
// tests of every other test file, and the first `func init`/`func TestMain` of a tag-only file ("<file> <func>"; such a
// package runs unfiltered, because that hook changes the process its untagged tests run in).
type taggedFacts struct {
	tagged, untagged map[string]map[string]bool
	hook             map[string]string
}

var (
	taggedOnce sync.Once
	taggedVal  taggedFacts
	taggedErr  string
)

func integrationTaggedFacts(t *testing.T) taggedFacts {
	t.Helper()
	keys := sortedKeys(expectedPackages(t))
	taggedOnce.Do(func() {
		root := rootOf(t)
		env := inheritedEnv(nil, "GOTOOLCHAIN="+pinnedGoToolchain(t), "GOWORK=off")
		mod := freeSpec{dir: root, env: env}.run("go", "list", "-mod=readonly", "-m")
		if mod.code != 0 {
			taggedErr = "go list -m: " + mod.stdout + mod.stderr
			return
		}
		modpath := strings.TrimSpace(mod.stdout)
		list := func(tags ...string) map[string][]string {
			args := append([]string{"list", "-e", "-mod=readonly"}, tags...)
			args = append(args, "-f={{.ImportPath}}{{range .TestGoFiles}}\t{{.}}{{end}}{{range .XTestGoFiles}}\t{{.}}{{end}}")
			for _, k := range keys {
				args = append(args, "./"+k)
			}
			res := freeSpec{dir: root, env: env}.run("go", args...)
			if res.code != 0 {
				taggedErr += "go list " + strings.Join(tags, " ") + ": " + res.stdout + res.stderr
				return nil
			}
			out := map[string][]string{}
			for _, l := range strings.Split(strings.TrimSpace(res.stdout), "\n") {
				f := strings.Split(l, "\t")
				out[strings.TrimPrefix(f[0], modpath+"/")] = f[1:]
			}
			return out
		}
		withTag, plain := list("-tags=integration"), list()
		if withTag == nil || plain == nil {
			return
		}
		taggedVal = taggedFacts{tagged: map[string]map[string]bool{}, untagged: map[string]map[string]bool{}, hook: map[string]string{}}
		for _, k := range keys {
			inPlain := map[string]bool{}
			for _, f := range plain[k] {
				inPlain[f] = true
			}
			taggedVal.tagged[k], taggedVal.untagged[k] = map[string]bool{}, map[string]bool{}
			for _, f := range withTag[k] {
				tests, hook := parsedTestFuncs(t, filepath.Join(root, k, f))
				into := taggedVal.untagged[k]
				if !inPlain[f] {
					into = taggedVal.tagged[k]
					if hook != "" && taggedVal.hook[k] == "" {
						taggedVal.hook[k] = k + "/" + f + " " + hook
					}
				}
				for _, n := range tests {
					into[n] = true
				}
			}
		}
	})
	if taggedErr != "" || taggedVal.tagged == nil {
		t.Fatalf("tagged-test discovery failed: %s", taggedErr)
	}
	return taggedVal
}

// checkTaggedSelection checks one packages shard's dry run against the oracle: every package of the shard either runs
// exactly its integration-tagged tests (one `SHARD-TEST <key> <Test>` line each, and the same names in the
// `SHARD-REGEX <module> ^(...)$` that go test receives) or, when a tag-only file declares a hook, is named by one
// `SHARD-UNFILTERED <key> <file> <func>` line and runs whole. An untagged test, a missing tagged test, a duplicate,
// a package of another shard or an unjustified unfiltered package is an error.
func checkTaggedSelection(stdout string, shardKeys map[string]bool, f taggedFacts) error {
	selected := map[string]map[string]bool{}
	union := map[string]bool{}
	for _, v := range lineValues(stdout, "  SHARD-TEST ") {
		fs := strings.Fields(v)
		if len(fs) != 2 {
			return fmt.Errorf("malformed SHARD-TEST line %q", v)
		}
		k, n := fs[0], fs[1]
		switch {
		case !shardKeys[k]:
			return fmt.Errorf("SHARD-TEST names %s, a package this shard does not run", k)
		case f.hook[k] != "":
			return fmt.Errorf("SHARD-TEST names %s %s, but %s runs unfiltered (%s)", k, n, k, f.hook[k])
		case f.untagged[k][n] && !f.tagged[k][n]:
			return fmt.Errorf("SHARD-TEST selects the untagged test %s %s: untagged tests run in the test and race legs, not in a shard", k, n)
		case !f.tagged[k][n]:
			return fmt.Errorf("SHARD-TEST names %s %s, which is no integration-tagged top-level test", k, n)
		case selected[k][n]:
			return fmt.Errorf("SHARD-TEST selects %s %s twice", k, n)
		}
		if selected[k] == nil {
			selected[k] = map[string]bool{}
		}
		selected[k][n] = true
		union[n] = true
	}
	if c := lineValues(stdout, "  SHARD-COLLISION "); len(c) > 0 {
		return fmt.Errorf("a go test group regex selects an untagged test of another package: %v", c)
	}
	unfiltered := map[string]bool{}
	for _, v := range lineValues(stdout, "  SHARD-UNFILTERED ") {
		fs := strings.Fields(v)
		if len(fs) != 3 {
			return fmt.Errorf("malformed SHARD-UNFILTERED line %q", v)
		}
		k := fs[0]
		if !shardKeys[k] || f.hook[k] != fs[1]+" "+fs[2] || unfiltered[k] {
			return fmt.Errorf("SHARD-UNFILTERED %q has no matching tag-only hook in this shard (oracle: %q)", v, f.hook[k])
		}
		unfiltered[k] = true
	}
	for k := range shardKeys {
		if f.hook[k] != "" {
			if !unfiltered[k] {
				return fmt.Errorf("%s has a tag-only hook (%s) but no SHARD-UNFILTERED line", k, f.hook[k])
			}
			continue
		}
		if len(f.tagged[k]) == 0 {
			return fmt.Errorf("%s runs in a shard but has no integration-tagged top-level test", k)
		}
		if !sameSet(selected[k], f.tagged[k]) {
			var missing []string
			for n := range f.tagged[k] {
				if !selected[k][n] {
					missing = append(missing, n)
				}
			}
			sort.Strings(missing)
			return fmt.Errorf("%s: %d integration-tagged test(s) in no SHARD-TEST line, e.g. %v", k, len(missing), missing[:min(3, len(missing))])
		}
	}
	regexNames := map[string]bool{}
	regexes := 0
	for _, v := range lineValues(stdout, "  SHARD-REGEX ") {
		fs := strings.Fields(v)
		if len(fs) != 2 || !strings.HasPrefix(fs[1], "^(") || !strings.HasSuffix(fs[1], ")$") {
			return fmt.Errorf("malformed SHARD-REGEX line %q", v)
		}
		regexes++
		for _, n := range strings.Split(strings.TrimSuffix(strings.TrimPrefix(fs[1], "^("), ")$"), "|") {
			regexNames[n] = true
		}
	}
	if len(union) > 0 && regexes == 0 {
		return fmt.Errorf("the shard selects tagged tests but prints no SHARD-REGEX line")
	}
	if !sameSet(regexNames, union) {
		return fmt.Errorf("the -run regex selects %d name(s), the SHARD-TEST lines %d", len(regexNames), len(union))
	}
	return nil
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sameSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

// discoveryFunctions are the ci/check_go.sh functions the discovery harness lifts into bash. A name the script does not
// define is skipped, so the same test runs against an older script.
var discoveryFunctions = []string{
	"die", "check_listed_test_file", "scan_listed_go_tests", "scan_go_test_paths", "gotestfuncs_bin", "go_test_name_ok",
	"integration_test_files", "split_plain_test_files", "split_package_hook", "discover_integration_tagged_tests",
	"run_integration_packages", "discover_daily_tests",
}

// discoveryHarness runs body in bash with the discovery functions of ci/check_go.sh. With files, ROOT is a temporary
// module holding them (plus the repo's ci/tools/gotestfuncs, or tool when it is not empty); without, ROOT is the repo.
func discoveryHarness(t *testing.T, files map[string]string, tool, body string) cmdResult {
	t.Helper()
	root := rootOf(t)
	src := readText(t, filepath.Join(root, "ci", "check_go.sh"))
	dir := root
	if files != nil {
		dir = t.TempDir()
		write := func(rel, text string) {
			p := filepath.Join(dir, rel)
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		write("go.mod", "module fixture\n\ngo 1.22\n")
		for rel, text := range files {
			write(rel, text)
		}
		if tool == "" {
			if raw, err := os.ReadFile(filepath.Join(root, "ci", "tools", "gotestfuncs", "main.go")); err == nil {
				tool = string(raw)
			}
		}
		if tool != "" {
			write("ci/tools/gotestfuncs/main.go", tool)
		}
	}
	var b strings.Builder
	b.WriteString("set -euo pipefail\nROOT=" + strconv.Quote(dir) + "\nGO_ENV_OFF=(env -u GOFLAGS -u GOEXPERIMENT)\nINTEGRATION_TEST_ENV=()\n")
	b.WriteString("declare -A DAILY_INTEGRATION_TEST_NAMES=() DAILY_TEST_CLASS=()\ndeclare -a DAILY_TEST_NAMES=()\n")
	for _, name := range discoveryFunctions {
		if m := regexp.MustCompile(`(?ms)^` + name + `\(\) \{\n.*?^\}\n`).FindString(src); m != "" {
			b.WriteString(m)
		}
	}
	b.WriteString(body)
	return freeSpec{dir: dir, env: inheritedEnv(nil, "GOTOOLCHAIN="+pinnedGoToolchain(t), "GOWORK=off", "TMPDIR="+t.TempDir())}.run("bash", "-c", b.String())
}

// runDiscoveryFixture is discoveryHarness on a fixture module that must succeed; it returns stdout.
func runDiscoveryFixture(t *testing.T, files map[string]string, body string) string {
	t.Helper()
	res := discoveryHarness(t, files, "", body)
	if res.code != 0 {
		t.Fatalf("fixture harness exit %d:\n%s%s", res.code, res.stdout, res.stderr)
	}
	return res.stdout
}

// listerAgreesWithGoTestList compares, per package, the tests discover_integration_tagged_tests found (tagged and
// untagged, from LISTER <pkg> <name> lines) with the compiled `go test -tags=integration -list` of the same package
// (COMPILED <pkg> <name> lines).
func listerAgreesWithGoTestList(out string, pkgs []string) error {
	lister, compiled := map[string]map[string]bool{}, map[string]map[string]bool{}
	for _, p := range pkgs {
		lister[p], compiled[p] = map[string]bool{}, map[string]bool{}
	}
	for _, l := range strings.Split(out, "\n") {
		f := strings.Fields(l)
		if len(f) != 3 || (f[0] != "LISTER" && f[0] != "COMPILED") || lister[f[1]] == nil {
			continue
		}
		if f[0] == "LISTER" {
			lister[f[1]][f[2]] = true
		} else {
			compiled[f[1]][f[2]] = true
		}
	}
	for _, p := range pkgs {
		if len(compiled[p]) == 0 {
			return fmt.Errorf("%s: go test -list printed no test: the comparison measured nothing", p)
		}
		if !sameSet(lister[p], compiled[p]) {
			return fmt.Errorf("%s: the lister found %v, go test -list compiled %v", p, sortedKeys(lister[p]), sortedKeys(compiled[p]))
		}
	}
	return nil
}

// listerVsCompiledBody prints LISTER and COMPILED lines for the packages (module-relative "./dir" paths).
func listerVsCompiledBody(pkgs []string) string {
	var b strings.Builder
	b.WriteString("discover_integration_tagged_tests . " + strings.Join(pkgs, " ") + "\n")
	for _, p := range pkgs {
		b.WriteString(fmt.Sprintf("printf '%%s' \"${INTEGRATION_TAGGED_TESTS[%s]-}${INTEGRATION_UNTAGGED_TESTS[%s]-}\" | sed -n 's|^Test|LISTER %s Test|p'\n", p, p, p))
		b.WriteString(fmt.Sprintf("GOWORK=off go test -mod=readonly -tags=integration -list '^Test' %s | sed -n 's|^Test|COMPILED %s Test|p'\n", p, p))
	}
	return b.String()
}

const mergeGroupReceipt = "CHAOS-11407 retired the receipt \"unit test re-run under -tags=integration\": the integration shards skip " +
	"untagged tests because the go-quality test and race legs run every package on merge_group (plan-verdict FULL). " +
	"merge_group must stay FULL, or the shards must run the untagged tests of the packages a merge_group plan leaves out"

// mergeGroupFullDefects checks internal/cigates/affected/plan.go: the judged events (the only ones planned narrower
// than FULL) exclude merge_group, and plan returns FULL for every other event before it reads the change set.
func mergeGroupFullDefects(src string) []string {
	var d []string
	m := regexp.MustCompile(`(?m)^\s*judgedEvents\s*=\s*map\[string\]bool\{([^}]*)\}`).FindStringSubmatch(src)
	if m == nil {
		return append(d, "plan.go has no judgedEvents table to check")
	}
	keys := map[string]bool{}
	for _, k := range regexp.MustCompile(`"([^"]+)"\s*:\s*true`).FindAllStringSubmatch(m[1], -1) {
		keys[k[1]] = true
	}
	if len(keys) == 0 {
		d = append(d, "judgedEvents holds no event")
	}
	if keys["merge_group"] {
		d = append(d, "judgedEvents holds merge_group: a merge group is planned narrower than FULL")
	}
	if !regexp.MustCompile(`(?m)^\s*if !judgedEvents\[in\.event\] \{\n\s*return full\(`).MatchString(src) {
		d = append(d, "plan no longer returns FULL first for an event outside judgedEvents")
	}
	return d
}

// runIntegrationPackagesDefects checks the body of run_integration_packages: after the dry-run return, exactly two
// go test lines run, the filtered packages with -run of the printed SHARD-REGEX and the hook packages whole.
func runIntegrationPackagesDefects(body string) []string {
	var d []string
	lines := strings.Split(body, "\n")
	dryReturn, goTests := -1, 0
	for i, l := range lines {
		l = strings.TrimSpace(l)
		if l == `[ "${mode}" != "--dry-run" ] || return 0` {
			dryReturn = i
		}
		if !strings.Contains(l, "go test -mod=readonly -trimpath -tags=integration -count=1") {
			continue
		}
		goTests++
		prev := ""
		if i > 0 {
			prev = strings.TrimSpace(lines[i-1])
		}
		switch {
		case dryReturn < 0 || i < dryReturn:
			d = append(d, "a go test runs before the dry-run return")
		case strings.HasSuffix(l, `-run "${group_regex[${group}]}" "${run_pkgs[@]}"`):
			if prev != `mapfile -t run_pkgs <<<"${group_pkgs[${group}]%$'\n'}"` {
				d = append(d, "the -run go test does not run the packages of its group")
			}
		case strings.HasSuffix(l, `-timeout=30m "${run_pkgs[@]}"`) && !strings.Contains(l, "-run"):
			if prev != `run_pkgs=("${unfiltered[@]}")` {
				d = append(d, "the go test without -run runs packages other than the hook packages")
			}
		default:
			d = append(d, "unexpected go test line: "+l)
		}
	}
	if goTests != 2 {
		d = append(d, fmt.Sprintf("%d go test lines, want 2 (each group with -run, hook packages whole)", goTests))
	}
	if !strings.Contains(body, `printf '  SHARD-REGEX %s %s\n' "${module_dir}" "${group_regex[${group}]}"`) {
		d = append(d, "the SHARD-REGEX receipt does not print the regex go test receives")
	}
	return d
}

func TestIntegrationShardScriptShape(t *testing.T) {
	t.Parallel()
	src := readText(t, filepath.Join(rootOf(t), "ci", "check_go.sh"))
	t.Run("provider shard discovery does not compile the test binary", func(t *testing.T) {
		t.Parallel()
		m := regexp.MustCompile(`(?ms)^discover_providersync_tests\(\) \{\n(.*?)^\}`).FindStringSubmatch(src)
		if m == nil {
			t.Fatal("discover_providersync_tests moved")
		}
		body := m[1]
		if !strings.Contains(body, `integration_test_files . "./${PROVIDER_INTEGRATION_PACKAGE_KEY}"`) || strings.Contains(body, "go test -mod=readonly") ||
			strings.Contains(body, `done <<< "${files_output}"`) || !strings.Contains(body, `done < <(printf '%s\n' "${files_output}")`) {
			t.Errorf("provider test discovery is not list-based and here-string free:\n%s", body)
		}
	})
	t.Run("one helper classes test files for every shard path", func(t *testing.T) {
		t.Parallel()
		helper := regexp.MustCompile(`(?ms)^integration_test_files\(\) \{\n(.*?)^\}`).FindStringSubmatch(src)
		if helper == nil {
			t.Fatal("integration_test_files is missing")
		}
		if !strings.Contains(helper[1], "go list -e -mod=readonly -f") || !strings.Contains(helper[1], "go list -mod=readonly -tags=integration -f") ||
			!strings.Contains(helper[1], "{{range .TestGoFiles}}") || !strings.Contains(helper[1], "{{range .XTestGoFiles}}") {
			t.Errorf("integration_test_files does not take both file lists from go list:\n%s", helper[1])
		}
		for _, fn := range []string{"discover_integration_tagged_tests", "discover_providersync_tests", "discover_daily_tests",
			"discover_workerservice_tests", "discover_queryapiserver_tests", "discover_remaining_tests", "split_package_hook"} {
			m := regexp.MustCompile(`(?ms)^` + fn + `\(\) \{\n(.*?)^\}`).FindStringSubmatch(src)
			if m == nil {
				t.Errorf("%s is missing", fn)
				continue
			}
			if strings.Contains(m[1], "go list") || strings.Contains(m[1], "*_test.go") || strings.Contains(m[1], "go:build") {
				t.Errorf("%s lists, globs or reads build lines itself instead of using integration_test_files", fn)
			}
			if fn != "split_package_hook" && !strings.Contains(m[1], "integration_test_files ") {
				t.Errorf("%s does not call integration_test_files", fn)
			}
			if !strings.Contains(m[1], "TEST_FILE_TAG_ONLY[") {
				t.Errorf("%s does not take the class from TEST_FILE_TAG_ONLY", fn)
			}
		}
	})
	t.Run("go test groups: no cross-package name collision, bounded regex", func(t *testing.T) {
		t.Parallel()
		files := map[string]string{
			"a/plain_test.go": "package a\n\nimport \"testing\"\n\nfunc TestA(t *testing.T) {}\n",
			"a/tag_test.go":   "//go:build integration\n\npackage a\n\nimport \"testing\"\n\nfunc TestShared(t *testing.T) {}\n",
			"b/plain_test.go": "package b\n\nimport \"testing\"\n\nfunc TestShared(t *testing.T) {}\n",
			"b/tag_test.go":   "//go:build integration\n\npackage b\n\nimport \"testing\"\n\nfunc TestB(t *testing.T) {}\n",
		}
		// Two packages whose tagged names alone make a regex over the 60000-byte group budget.
		for _, p := range []string{"c1", "c2"} {
			var b strings.Builder
			b.WriteString("//go:build integration\n\npackage " + p + "\n\nimport \"testing\"\n\n")
			for i := 0; i < 400; i++ {
				fmt.Fprintf(&b, "func Test%sAVeryLongIntegrationTestNameThatPadsTheRunRegexTowardsTheArgumentLimit%04d(t *testing.T) {}\n", strings.ToUpper(p), i)
			}
			files[p+"/long_test.go"] = b.String()
			files[p+"/plain_test.go"] = "package " + p + "\n\nimport \"testing\"\n\nfunc TestPlain" + p + "(t *testing.T) {}\n"
		}
		got := runDiscoveryFixture(t, files, `
run_integration_packages --dry-run . fixture ./a ./b ./c1 ./c2 > receipts.txt
cat receipts.txt
mapfile -t regexes < <(sed -n 's/^  SHARD-REGEX \. //p' receipts.txt)
mapfile -t groups < <(sed -n 's/^  SHARD-GROUP \. [0-9]* //p' receipts.txt)
[ "${#groups[@]}" -gt 0 ] || groups=("a b c1 c2")
for i in "${!regexes[@]}"; do
  read -r -a members <<<"${groups[${i}]}"
  for m in "${members[@]}"; do
    GOWORK=off go test -mod=readonly -tags=integration -list "${regexes[${i}]}" "./${m}" | sed -n "s|^Test|LISTEDG ${m} Test|p"
  done
done
`)
		if strings.Contains(got, "SHARD-COLLISION") {
			t.Error("a cross-package name collision is still receipted instead of avoided")
		}
		want := map[string]bool{}
		for _, v := range lineValues(got, "  SHARD-TEST ") {
			want[v] = true
		}
		listed := map[string]int{}
		for _, v := range lineValues(got, "LISTEDG ") {
			listed[v]++
		}
		for v, n := range listed {
			if !want[v] || n != 1 {
				t.Errorf("a group regex runs %q %d time(s); it is no SHARD-TEST of that package", v, n)
			}
		}
		for v := range want {
			if listed[v] != 1 {
				t.Errorf("SHARD-TEST %q is run by no group regex", v)
			}
		}
		if listed["b TestShared"] > 0 || listed["a TestA"] > 0 {
			t.Error("a plain test is selected by a group regex")
		}
		regexes := lineValues(got, "  SHARD-REGEX . ")
		for _, r := range regexes {
			if len(r) > 60000 {
				t.Errorf("a go test group regex is %d bytes, over the 60000-byte budget", len(r))
			}
		}
		if len(regexes) < 3 {
			t.Errorf("%d go test group(s); want a and b apart and c1 and c2 apart (budget)", len(regexes))
		}
		if t.Failed() {
			t.Logf("fixture output:\n%.4000s", got)
		}
	})
	t.Run("the integration verb plans bounded, collision-free groups on the real tree", func(t *testing.T) {
		t.Parallel()
		keys := sortedKeys(expectedPackages(t))
		var pkgs []string
		for _, k := range keys {
			pkgs = append(pkgs, "./"+k)
		}
		res := discoveryHarness(t, nil, "", "run_integration_packages --dry-run . integration "+strings.Join(pkgs, " ")+"\n")
		if res.code != 0 {
			t.Fatalf("exit %d: %.2000s%.2000s", res.code, res.stdout, res.stderr)
		}
		if strings.Contains(res.stdout, "SHARD-COLLISION") {
			t.Error("the integration verb receipts a collision instead of avoiding it")
		}
		regexes := lineValues(res.stdout, "  SHARD-REGEX . ")
		if len(regexes) == 0 {
			t.Fatal("no SHARD-REGEX line")
		}
		for _, r := range regexes {
			if len(r) > 60000 {
				t.Errorf("an integration-verb group regex is %d bytes, over the 60000-byte budget (Linux refuses one argument over 128 KiB)", len(r))
			}
		}
	})
	t.Run("the lister agrees with go test -list on two real packages", func(t *testing.T) {
		t.Parallel()
		pkgs := []string{"./internal/llmorgsettings", "./internal/auth/org"}
		res := discoveryHarness(t, nil, "", listerVsCompiledBody(pkgs))
		if res.code != 0 {
			t.Fatalf("exit %d: %s%s", res.code, res.stdout, res.stderr)
		}
		if err := listerAgreesWithGoTestList(res.stdout, pkgs); err != nil {
			t.Error(err)
		}
	})
	t.Run("a lister failure plans nothing", func(t *testing.T) {
		t.Parallel()
		good := map[string]string{
			"x/plain_test.go": "package x\n\nimport \"testing\"\n\nfunc TestPlain(t *testing.T) {}\n",
			"x/tag_test.go":   "//go:build integration\n\npackage x\n\nimport \"testing\"\n\nfunc TestTagged(t *testing.T) {}\n",
		}
		broken := map[string]string{
			"x/plain_test.go": good["x/plain_test.go"],
			"x/tag_test.go":   "//go:build integration\n\npackage x\n\nimport \"testing\"\n\nfunc TestTagged(t *testing.T) {\n",
		}
		body := "run_integration_packages --dry-run . fixture ./x\nDAILY_INTEGRATION_PACKAGE_KEY=x DAILY_TEST_SHARD_COUNT=1\ndiscover_daily_tests\nprintf 'CLASS %s\\n' \"${DAILY_TEST_NAMES[@]}\"\n"
		if res := discoveryHarness(t, good, "", body); res.code != 0 || !strings.Contains(res.stdout, "  SHARD-TEST x TestTagged\n") {
			t.Fatalf("control: exit %d: %s%s", res.code, res.stdout, res.stderr)
		}
		for name, c := range map[string]struct {
			files map[string]string
			tool  string
		}{
			"a test file that does not parse":   {broken, ""},
			"a lister that does not build":      {good, "//go:build ignore\n\npackage main\n\nfunc main() {\n"},
			"a lister that exits non-zero":      {good, "//go:build ignore\n\npackage main\n\nimport \"os\"\n\nfunc main() { os.Exit(3) }\n"},
			"a lister that prints a bad record": {good, "//go:build ignore\n\npackage main\n\nimport \"fmt\"\n\nfunc main() { fmt.Print(\"Z\\x1fx\\x1fy\\n\") }\n"},
		} {
			for _, step := range []string{"run_integration_packages --dry-run . fixture ./x\n", "DAILY_INTEGRATION_PACKAGE_KEY=x DAILY_TEST_SHARD_COUNT=1\ndiscover_daily_tests\nprintf 'CLASS %s\\n' \"${DAILY_TEST_NAMES[@]}\"\n"} {
				res := discoveryHarness(t, c.files, c.tool, step)
				if res.code == 0 || strings.Contains(res.stdout, "SHARD-") || strings.Contains(res.stdout, "CLASS") {
					t.Errorf("%s: %q exit %d, output %q: a lister failure must stop the plan", name, strings.SplitN(step, "\n", 2)[0], res.code, res.stdout)
				}
			}
		}
	})
	t.Run("package discovery does not spawn grep for every tracked test", func(t *testing.T) {
		t.Parallel()
		m := regexp.MustCompile(`(?ms)^discover_integration_packages\(\) \{\n(.*?)^\}`).FindStringSubmatch(src)
		if m == nil {
			t.Fatal("discover_integration_packages moved")
		}
		body := m[1]
		if !strings.Contains(body, `git -C "${ROOT}/${module_dir}" grep -l -E`) || strings.Contains(body, "ls-files --cached --others") || !strings.Contains(body, "ls-files --others --exclude-standard") {
			t.Errorf("package discovery spawns per-file greps:\n%s", body)
		}
	})
	t.Run("the packages go test runs the selection the receipts print", func(t *testing.T) {
		t.Parallel()
		m := regexp.MustCompile(`(?ms)^run_integration_packages\(\) \{\n(.*?)^\}`).FindStringSubmatch(src)
		if m == nil {
			t.Fatal("run_integration_packages moved")
		}
		if d := runIntegrationPackagesDefects(m[1]); len(d) > 0 {
			t.Fatalf("run_integration_packages: %v", d)
		}
		for name, plant := range map[string][2]string{
			"the filtered go test drops -run":         {`-run "${group_regex[${group}]}" "${run_pkgs[@]}"`, `"${run_pkgs[@]}"`},
			"the filtered go test runs the hook list": {`mapfile -t run_pkgs <<<"${group_pkgs[${group}]%$'\n'}"`, `run_pkgs=("${unfiltered[@]}")`},
			"the receipt prints another regex":        {`printf '  SHARD-REGEX %s %s\n' "${module_dir}" "${group_regex[${group}]}"`, `printf '  SHARD-REGEX %s %s\n' "${module_dir}" '^(.*)$'`},
			"a dry run executes tests":                {`[ "${mode}" != "--dry-run" ] || return 0`, `:`},
		} {
			if !strings.Contains(m[1], plant[0]) {
				t.Fatalf("plant %q: anchor %q is absent", name, plant[0])
			}
			if len(runIntegrationPackagesDefects(strings.Replace(m[1], plant[0], plant[1], 1))) == 0 {
				t.Errorf("planted defect not caught: %s", name)
			}
		}
	})
	t.Run("discovery parses Go: build constraints, Unicode names and hooks", func(t *testing.T) {
		t.Parallel()
		files := map[string]string{
			"p/plain_test.go":     "package p\n\nimport \"testing\"\n\nfunc TestPlain(t *testing.T) {}\n",
			"p/either_test.go":    "//go:build linux || integration\n\npackage p\n\nimport \"testing\"\n\nfunc TestPlainMention(t *testing.T) {}\n",
			"p/legacy_test.go":    "// +build integration\n\npackage p\n\nimport \"testing\"\n\nfunc TestLegacyTagged(t *testing.T) {}\n",
			"p/unicode_test.go":   "//go:build integration\n\npackage p\n\nimport \"testing\"\n\nfunc TestTaggedASCII(t *testing.T) {}\n\nfunc TestÖ(t *testing.T) {}\n",
			"h/plain_test.go":     "package h\n\nimport \"testing\"\n\nfunc TestOrdinary(t *testing.T) {}\n",
			"h/hook_test.go":      "//go:build integration\n\npackage h\n\nimport \"testing\"\n\nfunc /* hook comment */ init() {}\n\nfunc TestHookTagged(t *testing.T) {}\n",
			"q/plain_test.go":     "package q\n\nimport \"testing\"\n\nfunc TestQ(t *testing.T) {}\n",
			"q/commented_test.go": "//go:build integration\n\npackage q\n\nimport \"testing\"\n\n/*\nfunc init() {}\n*/\n\nconst src = `\nfunc TestFake(t *testing.T) {}\n`\n\nfunc TestQTagged(t *testing.T) { _ = src }\n",
			"v/lowercase_test.go": "//go:build integration\n\npackage v\n\nimport \"testing\"\n\nfunc Testable(t *testing.T) {}\n",
			"v/good_test.go":      "//go:build integration\n\npackage v\n\nimport \"testing\"\n\nfunc TestVGood(t *testing.T) {}\n",
			"q/method_test.go":    "//go:build integration\n\npackage q\n\nimport \"testing\"\n\ntype s struct{}\n\nfunc (s) TestMethod(t *testing.T) {}\n",
			"w/sig_test.go":       "//go:build integration\n\npackage w\n\nimport tt \"testing\"\n\nfunc TestGood(t *tt.T) {}\n\nfunc TestNoArgs() {}\n\nfunc TestWrong(x int) {}\n\nfunc TestResult(t *tt.T) error { return nil }\n\nfunc TestTwo(a, b *tt.T) {}\n\nfunc TestGeneric[P any](t *tt.T) {}\n",
		}
		got := runDiscoveryFixture(t, files, `
run_integration_packages --dry-run . fixture ./p ./h ./q ./w ./v
GOWORK=off go test -mod=readonly -tags=integration -list . ./w >/dev/null 2>wrong.txt || printf "WSIG %s\n" "$(grep -c "wrong signature" wrong.txt)"
GOWORK=off go test -mod=readonly -tags=integration -list . ./v >/dev/null 2>lower.txt || printf "VNAME %s\n" "$(grep -c "malformed name" lower.txt)"
regex="$(run_integration_packages --dry-run . fixture ./p | sed -n 's/^  SHARD-REGEX \. //p')"
GOWORK=off go test -mod=readonly -tags=integration -list "${regex}" ./p | sed -n 's/^Test/LISTED Test/p'
DAILY_INTEGRATION_PACKAGE_KEY=p DAILY_TEST_SHARD_COUNT=1
discover_daily_tests
for n in "${DAILY_TEST_NAMES[@]}"; do printf 'CLASS %s %s\n' "${n}" "${DAILY_TEST_CLASS[${n}]}"; done
printf 'SPLITHOOK p [%s]\n' "${DAILY_UNFILTERED}"
DAILY_INTEGRATION_PACKAGE_KEY=h
discover_daily_tests
printf 'SPLITHOOK h [%s]\n' "${DAILY_UNFILTERED}"
`)
		want := []string{
			"  SHARD-TEST p TestLegacyTagged", "  SHARD-TEST p TestTaggedASCII", "  SHARD-TEST p TestÖ",
			"  SHARD-UNFILTERED h h/hook_test.go init",
			"  SHARD-TEST q TestQTagged",
			"CLASS TestLegacyTagged integration", "CLASS TestPlain ordinary", "CLASS TestPlainMention ordinary",
			"CLASS TestTaggedASCII integration", "CLASS TestÖ integration",
			"SPLITHOOK p []", "SPLITHOOK h [h/hook_test.go init]",
			"LISTED TestLegacyTagged", "LISTED TestTaggedASCII", "LISTED TestÖ",
		}
		for _, w := range want {
			if !strings.Contains(got, w+"\n") {
				t.Errorf("missing %q", w)
			}
		}
		for _, l := range strings.Split(got, "\n") {
			switch {
			case l == "LISTED TestPlain" || l == "LISTED TestPlainMention":
				t.Errorf("the shard regex selects a plain test: %q", l)
			case l == "  SHARD-TEST p TestPlain" || l == "  SHARD-TEST p TestPlainMention":
				t.Errorf("a test file the plain build lists runs in the shard: %q", l)
			case strings.HasPrefix(l, "  SHARD-UNFILTERED q "):
				t.Errorf("a hook inside a comment made q unfiltered: %q", l)
			case strings.HasPrefix(l, "  SHARD-TEST h "):
				t.Errorf("a package with a real hook was filtered: %q", l)
			case strings.Contains(l, "TestFake") || strings.Contains(l, "Testable") || strings.Contains(l, "TestMethod"):
				t.Errorf("a string, a lower-case name or a method was taken for a test: %q", l)
			}
		}
		for _, l := range strings.Split(got, "\n") {
			if strings.HasPrefix(l, "  SHARD-TEST w ") && l != "  SHARD-TEST w TestGood" {
				t.Errorf("a Test-named function with a signature cmd/go refuses was taken for a test: %q", l)
			}
		}
		if !strings.Contains(got, "  SHARD-TEST w TestGood\n") {
			t.Error("TestGood(t *tt.T) with testing imported as tt is not selected")
		}
		// cmd/go agrees: go test refuses to build w ("wrong signature", reported for the first bad function) and v (vet:
		// "malformed name" for Testable), so none of them is a runnable test either way; a shard holding one fails loudly.
		if !regexp.MustCompile(`(?m)^WSIG [1-9]`).MatchString(got) {
			t.Errorf("go test -list on w did not fail with wrong signature: %q", got)
		}
		if !regexp.MustCompile(`(?m)^VNAME [1-9]`).MatchString(got) {
			t.Errorf("go test -list on v did not fail with malformed name: %q", got)
		}
		if !strings.Contains(got, "  SHARD-TEST v TestVGood\n") || strings.Contains(got, "SHARD-TEST v Testable") {
			t.Error("package v: want TestVGood selected and Testable not")
		}
		listed := runDiscoveryFixture(t, files, listerVsCompiledBody([]string{"./p", "./h", "./q"}))
		if err := listerAgreesWithGoTestList(listed, []string{"./p", "./h", "./q"}); err != nil {
			t.Error(err)
		}
		if t.Failed() {
			t.Logf("fixture output:\n%s", got)
		}
	})
	t.Run("merge_group stays a FULL go-quality plan", func(t *testing.T) {
		t.Parallel()
		planSrc := readText(t, filepath.Join(rootOf(t), "internal", "cigates", "affected", "plan.go"))
		if d := mergeGroupFullDefects(planSrc); len(d) > 0 {
			t.Fatalf("%s:\n  %s", mergeGroupReceipt, strings.Join(d, "\n  "))
		}
		for name, plant := range map[string][2]string{
			"merge_group becomes a judged event":     {`judgedEvents = map[string]bool{"pull_request": true, "range": true}`, `judgedEvents = map[string]bool{"pull_request": true, "range": true, "merge_group": true}`},
			"an unjudged event no longer plans FULL": {"if !judgedEvents[in.event] {\n\t\treturn full(", "if !judgedEvents[in.event] {\n\t\treturn affected("},
			"the judged-event table moved":           {"judgedEvents = map[string]bool{", "judged = map[string]bool{"},
		} {
			if !strings.Contains(planSrc, plant[0]) {
				t.Fatalf("plant %q: anchor %q is absent from plan.go", name, plant[0])
			}
			if len(mergeGroupFullDefects(strings.Replace(planSrc, plant[0], plant[1], 1))) == 0 {
				t.Errorf("planted defect not caught: %s", name)
			}
		}
	})
	t.Run("the shard arity guard uses an explicit conditional", func(t *testing.T) {
		t.Parallel()
		m := regexp.MustCompile(`(?ms)^  integration-shard\)\n(.*?^    ;;)$`).FindStringSubmatch(src)
		if m == nil {
			t.Fatal("the integration-shard case moved")
		}
		want := "if [ \"$#\" -lt 3 ] || [ \"$#\" -gt 4 ]; then\n      die \"integration-shard requires TARGET SHARD and accepts only optional --dry-run\"\n    fi"
		if !strings.Contains(m[1], want) {
			t.Errorf("the arity guard is not an explicit conditional:\n%s", m[1])
		}
		for _, args := range [][]string{{"packages"}, {"packages", "2", "--dry-run", "extra"}} {
			r := runCheckGoSharding(t, shardEnv{}, append([]string{"integration-shard"}, args...)...)
			if r.code != 2 || !strings.Contains(r.stderr, "integration-shard requires TARGET SHARD and accepts only optional --dry-run") {
				t.Errorf("%v: exit %d: %s", args, r.code, r.stderr)
			}
		}
	})
}

func TestIntegrationShardPlanIsExhaustiveNonemptyAndMachineReadable(t *testing.T) {
	t.Parallel()
	stdout, gho := realPlan(t)
	facts := realPlanFacts(t)
	total := len(facts.expected)
	if !strings.Contains(stdout, fmt.Sprintf("%d package(s) discovered, 0 denylisted, %d will run", total, total)) {
		t.Fatalf("the plan does not report %d discovered packages", total)
	}
	assignments, err := checkShardPlan(stdout, gho, facts)
	if err != nil {
		t.Fatal(err)
	}
	if len(assignments) != facts.manifestShards {
		t.Fatal("a shard holds no package")
	}
	all, integration := providerTests(t)
	for n := range integration {
		if !all[n] {
			t.Fatalf("integration-tagged test %s is not a top-level test", n)
		}
	}
	if len(integration) >= len(all) {
		t.Fatal("every provider test is integration-tagged")
	}
	assigned := map[int]map[string]bool{}
	class := map[string]string{}
	var flat []string
	for _, l := range strings.Split(stdout, "\n") {
		if !strings.HasPrefix(l, "  PROVIDER-SHARD ") {
			continue
		}
		f := strings.Fields(l)
		s, _ := strconv.Atoi(f[1])
		if assigned[s] == nil {
			assigned[s] = map[string]bool{}
		}
		assigned[s][f[2]] = true
		class[f[2]] = strings.TrimPrefix(f[4], "class=")
		flat = append(flat, f[2])
	}
	if len(assigned) != facts.providerShards {
		t.Fatalf("provider shards %d, want %d", len(assigned), facts.providerShards)
	}
	seen := map[string]bool{}
	for _, n := range flat {
		if seen[n] {
			t.Fatalf("provider test %s is in two shards", n)
		}
		seen[n] = true
	}
	// Untagged tests run in the go-quality test and race legs (merge_group plans FULL there), not a third time here.
	if !sameSet(seen, integration) {
		t.Fatalf("the provider shards do not hold exactly the integration-tagged tests: %d vs %d", len(seen), len(integration))
	}
	kinds := map[string]bool{}
	for _, c := range class {
		kinds[c] = true
	}
	if len(kinds) != 1 || !kinds["integration"] {
		t.Fatalf("the provider shards hold a class other than integration: %v", kinds)
	}
	weights, ints := map[int]int{}, map[int]int{}
	re := regexp.MustCompile(`^providersync test shard (\d+): relative weight (\d+), (\d+) test\(s\), (\d+) integration-tagged$`)
	for _, l := range strings.Split(stdout, "\n") {
		if m := re.FindStringSubmatch(l); m != nil {
			s, _ := strconv.Atoi(m[1])
			weights[s], _ = strconv.Atoi(m[2])
			ints[s], _ = strconv.Atoi(m[4])
		}
	}
	spread := func(m map[int]int) int {
		lo, hi := 1<<30, -1
		for _, v := range m {
			if v < lo {
				lo = v
			}
			if v > hi {
				hi = v
			}
		}
		return hi - lo
	}
	// Balanced by measured time, not test count (CHAOS-11105): the spread is bounded by the heaviest unlisted test (the 200 class weight).
	if len(weights) != facts.providerShards || spread(weights) > 200 {
		t.Fatalf("provider shard weights %v / integration counts %v are not balanced to within the heaviest unlisted test", weights, ints)
	}
}

func TestIntegrationShardPlanCheckerFailsEachPlantedDefect(t *testing.T) {
	t.Parallel()
	stdout, gho := realPlan(t)
	facts := realPlanFacts(t)
	if _, err := checkShardPlan(stdout, gho, facts); err != nil {
		t.Fatalf("the real plan fails its own checker: %v", err)
	}
	lines := strings.Split(stdout, "\n")
	byShard := map[int]map[string]int{}
	for _, l := range lines {
		if m := shardRow.FindStringSubmatch(l); m != nil {
			s, _ := strconv.Atoi(m[1])
			w, _ := strconv.Atoi(m[3])
			if byShard[s] == nil {
				byShard[s] = map[string]int{}
			}
			byShard[s][m[2]] = w
		}
	}
	// the plan with some shards' rows replaced and their total lines made consistent
	withRows := func(changes map[int]map[string]int) string {
		assigned := map[int]map[string]int{}
		for s, rows := range byShard {
			assigned[s] = map[string]int{}
			for k, v := range rows {
				assigned[s][k] = v
			}
		}
		for s, rows := range changes {
			assigned[s] = rows
		}
		var out []string
		for _, l := range lines {
			if strings.HasPrefix(l, "  SHARD ") {
				continue
			}
			if m := shardTotal.FindStringSubmatch(l); m != nil {
				s, _ := strconv.Atoi(m[1])
				sum := 0
				var names []string
				for n, w := range assigned[s] {
					sum += w
					names = append(names, n)
				}
				sort.Strings(names)
				out = append(out, fmt.Sprintf("integration shard %d: estimated %ds, %d package(s)", s, sum, len(assigned[s])))
				for _, n := range names {
					out = append(out, fmt.Sprintf("  SHARD %d %s weight=%ds", s, n, assigned[s][n]))
				}
			} else {
				out = append(out, l)
			}
		}
		return strings.Join(out, "\n")
	}
	plant := func(name, plan, output string) {
		t.Helper()
		if _, err := checkShardPlan(plan, output, facts); err == nil {
			t.Errorf("planted defect not caught: %s", name)
		}
	}
	var splitShards = map[int]bool{}
	for s, rows := range byShard {
		for p := range rows {
			if splitPackages[p] {
				splitShards[s] = true
			}
		}
	}
	var plain []int
	for s := range byShard {
		if !splitShards[s] {
			plain = append(plain, s)
		}
	}
	sort.Ints(plain)
	shardA, shardB := plain[0], plain[1]
	lightName, lightWeight := "", 1<<30
	for n, w := range byShard[shardA] {
		if w < lightWeight || (w == lightWeight && n < lightName) {
			lightName, lightWeight = n, w
		}
	}
	copyWith := func(m map[string]int, k string, v int) map[string]int {
		o := map[string]int{}
		for a, b := range m {
			o[a] = b
		}
		if v < 0 {
			delete(o, k)
		} else {
			o[k] = v
		}
		return o
	}
	plant("a package assigned twice (totals consistent)", withRows(map[int]map[string]int{shardB: copyWith(byShard[shardB], lightName, lightWeight)}), gho)
	plant("a package missing (totals consistent)", withRows(map[int]map[string]int{shardA: copyWith(byShard[shardA], lightName, -1)}), gho)
	plant("a package planned at the wrong weight (totals consistent)", withRows(map[int]map[string]int{shardA: copyWith(byShard[shardA], lightName, lightWeight+1)}), gho)
	for i, l := range lines {
		if m := shardTotal.FindStringSubmatch(l); m != nil {
			sec, _ := strconv.Atoi(m[2])
			bad := append([]string(nil), lines...)
			bad[i] = fmt.Sprintf("integration shard %s: estimated %ds, %s package(s)", m[1], sec+1, m[3])
			plant("a shard total that does not add up", strings.Join(bad, "\n"), gho)
			break
		}
	}
	for i, l := range lines {
		if strings.HasPrefix(l, "  SHARD ") {
			bad := append([]string(nil), lines...)
			bad[i] = "  SHARD 1 " + strings.Fields(l)[2] + " weight=fast"
			plant("a malformed row", strings.Join(bad, "\n"), gho)
			break
		}
	}
	for i, l := range lines {
		if m := planHeader.FindStringSubmatch(l); m != nil {
			n, _ := strconv.Atoi(m[1])
			bad := append([]string(nil), lines...)
			bad[i] = fmt.Sprintf("integration shard plan: %d shard(s), %s package(s)", n+1, m[2])
			plant("a shard count over the manifest's", strings.Join(bad, "\n"), gho)
			break
		}
	}
	// an unbalanced plan whose rows and totals are consistent: only the balance property can refuse it
	sizes := map[int]int{}
	for s, rows := range byShard {
		sizes[s] = len(rows)
	}
	totals := map[int]int{}
	for _, l := range lines {
		if m := shardTotal.FindStringSubmatch(l); m != nil {
			s, _ := strconv.Atoi(m[1])
			totals[s], _ = strconv.Atoi(m[2])
		}
	}
	bestW, bestPkg, bestShard := -1, "", 0
	for s, rows := range byShard {
		for p, w := range rows {
			if !splitPackages[p] && sizes[s] > 1 && w > bestW {
				bestW, bestPkg, bestShard = w, p, s
			}
		}
	}
	var others []int
	for s := range totals {
		if s != bestShard {
			others = append(others, s)
		}
	}
	sort.Slice(others, func(i, j int) bool { return totals[others[i]] > totals[others[j]] })
	target := 0
	for _, s := range others {
		if !splitShards[s] {
			target = s
			break
		}
	}
	srcRows, dstRows := copyWith(byShard[bestShard], bestPkg, -1), copyWith(byShard[target], bestPkg, bestW)
	unbalanced := withRows(map[int]map[string]int{bestShard: srcRows, target: dstRows})
	if _, err := checkShardPlan(unbalanced, gho, facts); err == nil || !strings.Contains(err.Error(), "not balancing") {
		t.Errorf("an unbalanced but consistent plan must be refused as not balancing, got %v", err)
	}
	var matrix map[string]any
	for _, l := range strings.Split(gho, "\n") {
		if v, ok := strings.CutPrefix(l, "matrix="); ok {
			_ = json.Unmarshal([]byte(v), &matrix)
		}
	}
	inc, _ := matrix["include"].([]any)
	matrix["include"] = append(inc, map[string]any{"target": "packages", "shard": 99})
	raw, _ := json.Marshal(matrix)
	plant("a matrix entry for a shard that holds nothing", stdout, "matrix="+string(raw))
}

func lineValues(stdout, prefix string) []string {
	var out []string
	for _, l := range strings.Split(stdout, "\n") {
		if rest, ok := strings.CutPrefix(l, prefix); ok {
			out = append(out, rest)
		}
	}
	return out
}

func TestIntegrationManifestDriftAndDuplicatePackagesFailLoudly(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	original := manifestText(t, root)
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	var kept []string
	for _, l := range strings.Split(strings.TrimRight(original, "\n"), "\n") {
		if !strings.HasPrefix(l, "internal/providersync\t") {
			kept = append(kept, l)
		}
	}
	r := runCheckGoSharding(t, shardEnv{manifest: write("missing.tsv", strings.Join(kept, "\n")+"\n")}, "integration-shard-plan")
	if r.code != 2 || !strings.Contains(r.stderr, "manifest is missing discovered package 'internal/providersync'") {
		t.Errorf("a missing row: exit %d: %s", r.code, r.stderr)
	}
	r = runCheckGoSharding(t, shardEnv{manifest: write("duplicate.tsv", original+"internal/providersync\t1166\n")}, "integration-shard-plan")
	if r.code != 2 || !strings.Contains(r.stderr, "manifest lists 'internal/providersync' more than once") {
		t.Errorf("a duplicate row: exit %d: %s", r.code, r.stderr)
	}
	r = runCheckGoSharding(t, shardEnv{providerManifest: write("invalid-provider.tsv", "shards\t1\nintegration-test-weight\t100\nordinary-test-weight\t1\n")}, "integration-shard-plan")
	if r.code != 2 || !strings.Contains(r.stderr, "provider test shard manifest must declare at least two shards") {
		t.Errorf("a one-shard provider manifest: exit %d: %s", r.code, r.stderr)
	}
}

func TestAnIsolatedPackageKeyMatchingNothingFailsLoudlyAndWorkerserviceIsASingleton(t *testing.T) {
	t.Parallel()
	r := runCheckGoSharding(t, shardEnv{extra: []string{"DEV_HEALTH_GO_INTEGRATION_ISOLATED_KEYS=internal/workerservice internal/queryapi/server internal/nosuchpackage"}}, "integration-shard-plan")
	if r.code == 0 || !strings.Contains(r.stderr, "isolated integration package 'internal/nosuchpackage' matches no discovered package") {
		t.Fatalf("an unmatched isolated key: exit %d: %s", r.code, r.stderr)
	}
	stdout, _ := realPlan(t)
	byShard := map[string][]string{}
	for _, l := range strings.Split(stdout, "\n") {
		if m := shardRow.FindStringSubmatch(l); m != nil {
			byShard[m[1]] = append(byShard[m[1]], m[2])
		}
	}
	for _, p := range []string{workerservicePackage, qapiserverPackage, remainingPackage} {
		var holders []string
		for s, pkgs := range byShard {
			if containsStr(pkgs, p) {
				holders = append(holders, s)
			}
		}
		if len(holders) != 1 || len(byShard[holders[0]]) != 1 {
			t.Errorf("%s does not hold a shard row of its own: %v", p, byShard)
		}
	}
}

func declaredImage(t *testing.T, constant string) string {
	t.Helper()
	m := regexp.MustCompile(`(?m)^` + pyS + `*` + constant + pyS + `*=` + pyS + `*"([^"]+)"`).FindStringSubmatch(readText(t, filepath.Join(rootOf(t), "internal", "testsupport", "containers", "harness.go")))
	if m == nil {
		t.Fatalf("%s is not declared in the container harness", constant)
	}
	return m[1]
}

func pinnedClickHouseImage(t *testing.T) string {
	t.Helper()
	image := declaredImage(t, "ClickHouseImage")
	if !regexp.MustCompile(`^clickhouse/clickhouse-server(:26(\.[0-9]+)*|@sha256:[0-9a-f]{64})$`).MatchString(image) {
		t.Fatalf("ClickHouseImage must be clickhouse/clickhouse-server pinned to a 26.x tag or a sha256 digest, got %q", image)
	}
	return image
}

func mirroredImage(t *testing.T, image, prefix string) string {
	t.Helper()
	if prefix == "" {
		return image
	}
	first, _, found := strings.Cut(image, "/")
	if found && strings.EqualFold(first, "docker.io") {
		t.Fatalf("test dependency image %q names docker.io explicitly", image)
	}
	if found && (strings.ContainsAny(first, ".:")) {
		return image
	}
	return strings.TrimRight(prefix, "/") + "/" + image
}

func prepullStubs(t *testing.T) (dir string) {
	t.Helper()
	dir = t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	docker := "#!/usr/bin/env bash\nset -euo pipefail\nattempt=0\nif [ -f \"${DOCKER_ATTEMPTS_FILE}\" ]; then\n  attempt=\"$(<\"${DOCKER_ATTEMPTS_FILE}\")\"\nfi\nattempt=$((attempt + 1))\nprintf '%s\\n' \"${attempt}\" > \"${DOCKER_ATTEMPTS_FILE}\"\nprintf '%s\\n' \"$*\" >> \"${DOCKER_ARGS_FILE}\"\n[ \"${attempt}\" -ge \"${DOCKER_SUCCEED_ON}\" ]\n"
	sleep := "#!/usr/bin/env bash\nset -euo pipefail\nprintf '%s\\n' \"$*\" >> \"${SLEEP_ARGS_FILE}\"\n"
	for name, body := range map[string]string{"docker": docker, "sleep": sleep} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func runPrepull(t *testing.T, dir, succeedOn string, args ...string) cmdResult {
	t.Helper()
	env := inheritedEnv(nil,
		"PATH="+filepath.Join(dir, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"),
		"DOCKER_ATTEMPTS_FILE="+filepath.Join(dir, "attempts"), "DOCKER_ARGS_FILE="+filepath.Join(dir, "docker-args"),
		"DOCKER_SUCCEED_ON="+succeedOn, "SLEEP_ARGS_FILE="+filepath.Join(dir, "sleep-args"))
	return freeSpec{dir: rootOf(t), env: env}.run("bash", append([]string{"ci/check_go.sh", "integration-prepull"}, args...)...)
}

func fileLines(dir, name string) []string {
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return nil
	}
	return strings.Split(strings.TrimRight(string(b), "\n"), "\n")
}

func TestIntegrationPrepull(t *testing.T) {
	t.Parallel()
	prefix := os.Getenv("TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX")
	t.Run("retries the exact source declared image", func(t *testing.T) {
		t.Parallel()
		dir := prepullStubs(t)
		postgres := mirroredImage(t, declaredImage(t, "PostgresImage"), prefix)
		r := runPrepull(t, dir, "3")
		if r.code != 0 {
			t.Fatalf("exit %d: %s%s", r.code, r.stdout, r.stderr)
		}
		want := []string{"pull " + postgres, "pull " + postgres, "pull " + postgres,
			"pull " + mirroredImage(t, pinnedClickHouseImage(t), prefix), "pull " + mirroredImage(t, declaredImage(t, "ValkeyImage"), prefix), "pull " + mirroredImage(t, declaredImage(t, "ReaperImage"), prefix)}
		if got := fileLines(dir, "docker-args"); strings.Join(got, "|") != strings.Join(want, "|") {
			t.Errorf("docker pulls %v, want %v", got, want)
		}
		if got := fileLines(dir, "sleep-args"); strings.Join(got, ",") != "5,10" {
			t.Errorf("sleeps %v, want [5 10]", got)
		}
		if !strings.Contains(r.stdout, "pre-pulled postgres test dependency image "+postgres+" on attempt 3/3") {
			t.Errorf("stdout lacks the attempt receipt: %s", r.stdout)
		}
		dir2 := prepullStubs(t)
		failed := runPrepull(t, dir2, "4")
		if failed.code != 1 {
			t.Fatalf("exhausting the budget exits %d, want 1", failed.code)
		}
		if got := fileLines(dir2, "docker-args"); strings.Join(got, "|") != strings.Join(want[:3], "|") {
			t.Errorf("after exhausting one image's budget docker pulls %v, want only the first image", got)
		}
		if got := fileLines(dir2, "sleep-args"); strings.Join(got, ",") != "5,10" {
			t.Errorf("sleeps %v, want [5 10]", got)
		}
		if !strings.Contains(failed.stderr, "failed to pre-pull postgres test dependency image "+postgres+" after 3 attempts") {
			t.Errorf("stderr lacks the failure receipt: %s", failed.stderr)
		}
	})
	t.Run("warms every declared image", func(t *testing.T) {
		t.Parallel()
		dir := prepullStubs(t)
		r := runPrepull(t, dir, "1")
		if r.code != 0 {
			t.Fatalf("exit %d: %s%s", r.code, r.stdout, r.stderr)
		}
		want := []string{"pull " + mirroredImage(t, declaredImage(t, "PostgresImage"), prefix), "pull " + mirroredImage(t, pinnedClickHouseImage(t), prefix),
			"pull " + mirroredImage(t, declaredImage(t, "ValkeyImage"), prefix), "pull " + mirroredImage(t, declaredImage(t, "ReaperImage"), prefix)}
		if got := fileLines(dir, "docker-args"); strings.Join(got, "|") != strings.Join(want, "|") {
			t.Errorf("docker pulls %v, want %v", got, want)
		}
	})
	t.Run("refuses a per job subset", func(t *testing.T) {
		t.Parallel()
		dir := prepullStubs(t)
		r := runPrepull(t, dir, "1", "postgres")
		if r.code != 2 || !strings.Contains(r.stderr, "integration-prepull accepts no arguments") {
			t.Fatalf("exit %d: %s", r.code, r.stderr)
		}
	})
}

func containsStr(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

func shardManifestFiles(t *testing.T, root string) []string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(shardManifestDir(root), "*.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)
	return names
}

// `check_go.sh` must redirect a test image through TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX exactly as testcontainers-go does:
// the pre-pull warms a ref and testcontainers resolves the image again when a test starts a container, so a disagreement of
// even one rule warms one image and pulls another. The explicit `docker.io/...` form is REFUSED, not matched.
func TestMirroredImageMatchesTestcontainersPrefixSemantics(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	src := readText(t, filepath.Join(root, "ci", "check_go.sh"))
	body := regexp.MustCompile(`(?ms)^mirrored_image\(\) \{.*?^\}`).FindString(src)
	if body == "" {
		t.Fatal("mirrored_image not found in check_go.sh")
	}
	run := func(image, prefix string) cmdResult {
		program := "die() { printf \"ERROR: %s\\n\" \"$*\" >&2; exit 2; }\n" + body + "\nmirrored_image \"" + image + "\"\n"
		return freeSpec{dir: root, env: inheritedEnv(nil, "TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX="+prefix)}.run("bash", "-c", program)
	}
	prefix := "ghcr.io/full-chaos"
	redirected := map[string]string{
		"postgres:18-alpine@sha256:abc":         prefix + "/postgres:18-alpine@sha256:abc",
		"clickhouse/clickhouse-server@sha256:d": prefix + "/clickhouse/clickhouse-server@sha256:d",
		"testcontainers/ryuk:0.14.0":            prefix + "/testcontainers/ryuk:0.14.0",
	}
	for image, want := range redirected {
		if got := run(image, prefix); got.code != 0 || strings.TrimSpace(got.stdout) != want {
			t.Errorf("%s with a prefix: exit %d, %q, want %q (%s)", image, got.code, got.stdout, want, got.stderr)
		}
		if got := run(image, ""); got.code != 0 || strings.TrimSpace(got.stdout) != image {
			t.Errorf("%s with no prefix must stay as is: exit %d, %q", image, got.code, got.stdout)
		}
	}
	for _, image := range []string{"ghcr.io/x/y:1", "registry.hub.docker.com/library/postgres:1"} {
		if got := run(image, prefix); got.code != 0 || strings.TrimSpace(got.stdout) != image {
			t.Errorf("an image naming a real registry is left alone: %s -> exit %d, %q", image, got.code, got.stdout)
		}
	}
	if got := run("docker.io/postgres:1", prefix); got.code == 0 || !strings.Contains(got.stderr, "names docker.io explicitly") {
		t.Errorf("the incoherent docker.io form must be refused: exit %d: %s%s", got.code, got.stdout, got.stderr)
	}
}

// dryRuns runs `integration-shard TARGET SHARD --dry-run` for every shard at once (the cap in freeSpec bounds the
// concurrency) and returns the results by shard.
func dryRuns(t *testing.T, target string, shards []int) map[int]cmdResult {
	t.Helper()
	results := make([]cmdResult, len(shards))
	var wg sync.WaitGroup
	for i, shard := range shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = runCheckGoSharding(t, shardEnv{}, "integration-shard", target, strconv.Itoa(shard), "--dry-run")
		}()
	}
	wg.Wait()
	out := map[int]cmdResult{}
	for i, shard := range shards {
		out[shard] = results[i]
	}
	return out
}

func TestEachIntegrationShardDryRunExecutesOnlyItsManifestAssignment(t *testing.T) {
	t.Parallel()
	stdout, gho := realPlan(t)
	facts := realPlanFacts(t)
	assignments, err := checkShardPlan(stdout, gho, facts)
	if err != nil {
		t.Fatal(err)
	}
	var shards, provider []int
	wants := map[int]map[string]bool{}
	for s, rows := range assignments {
		want := map[string]bool{}
		for p := range rows {
			if !splitPackages[p] {
				want[p] = true
			}
		}
		if len(want) > 0 {
			shards = append(shards, s)
			wants[s] = want
		}
	}
	sort.Ints(shards)
	for s := 1; s <= facts.providerShards; s++ {
		provider = append(provider, s)
	}
	var pkgResults, providerResults map[int]cmdResult
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); pkgResults = dryRuns(t, "packages", shards) }()
	go func() { defer wg.Done(); providerResults = dryRuns(t, "providersync", provider) }()
	wg.Wait()

	var selected []string
	for _, shard := range shards {
		r := pkgResults[shard]
		if r.code != 0 || !strings.Contains(r.stdout, fmt.Sprintf("integration package shard %d: DRY RUN", shard)) {
			t.Fatalf("shard %d: exit %d: %s%s", shard, r.code, r.stdout, r.stderr)
		}
		chosen := lineValues(r.stdout, "  SHARD-RUN ")
		got := map[string]bool{}
		for _, c := range chosen {
			got[c] = true
		}
		if !sameSet(got, wants[shard]) || len(chosen) != len(wants[shard]) {
			t.Errorf("shard %d would run %v, the plan assigned %v", shard, sortedKeys(got), sortedKeys(wants[shard]))
		}
		selected = append(selected, chosen...)
	}
	if len(shards) < 2 {
		t.Fatal("fewer than two packages shards ran: the loop measured nothing")
	}
	got := map[string]bool{}
	for _, s := range selected {
		got[s] = true
	}
	want := map[string]bool{}
	for p := range facts.expected {
		if !splitPackages[p] {
			want[p] = true
		}
	}
	if len(selected) != len(got) || !sameSet(got, want) {
		t.Errorf("the packages shards run %d packages (%d distinct), want %d", len(selected), len(got), len(want))
	}
	tf := integrationTaggedFacts(t)
	plantShard, plantKey := 0, ""
	for _, shard := range shards {
		if err := checkTaggedSelection(pkgResults[shard].stdout, wants[shard], tf); err != nil {
			t.Errorf("packages shard %d: %v", shard, err)
		}
		for _, k := range sortedKeys(wants[shard]) {
			if plantShard == 0 && tf.hook[k] == "" && len(tf.untagged[k]) > 0 && len(tf.tagged[k]) > 1 {
				plantShard, plantKey = shard, k
			}
		}
	}
	// Planted defects in one real dry run: each must be refused by the checker.
	if plantShard == 0 {
		t.Fatal("no packages shard holds a filtered package with untagged tests: the planted defects below measure nothing")
	}
	dry := pkgResults[plantShard].stdout
	var firstTest, firstKey, untaggedName, unfilteredLine, filteredKey string
	for _, l := range strings.Split(dry, "\n") {
		if v, ok := strings.CutPrefix(l, "  SHARD-TEST "); ok && firstTest == "" && strings.Fields(v)[0] == plantKey {
			firstTest, firstKey = l, strings.Fields(v)[0]
		}
		if strings.HasPrefix(l, "  SHARD-UNFILTERED ") && unfilteredLine == "" {
			unfilteredLine = l
		}
	}
	for k := range wants[plantShard] {
		if tf.hook[k] == "" && filteredKey == "" {
			filteredKey = k
		}
	}
	for n := range tf.untagged[firstKey] {
		if !tf.tagged[firstKey][n] && (untaggedName == "" || n < untaggedName) {
			untaggedName = n
		}
	}
	if firstTest == "" || untaggedName == "" || filteredKey == "" {
		t.Fatalf("shard %d dry run has no SHARD-TEST line or no untagged test to plant: %s", plantShard, dry)
	}
	dropLine := func(line string) string { return strings.Replace(dry, line+"\n", "", 1) }
	regexLine := ""
	for _, l := range strings.Split(dry, "\n") {
		if strings.HasPrefix(l, "  SHARD-REGEX ") {
			regexLine = l
			break
		}
	}
	firstName := strings.Fields(firstTest)[2]
	for name, bad := range map[string]string{
		"an integration-tagged test removed from the shard": dropLine(firstTest),
		"an untagged test added to the shard":               dry + "\n  SHARD-TEST " + firstKey + " " + untaggedName,
		"a tagged test selected twice":                      dry + "\n" + firstTest,
		"a filtered package marked unfiltered":              dry + "\n  SHARD-UNFILTERED " + filteredKey + " " + filteredKey + "/x_integration_test.go init",
		"the -run regex missing a selected name":            strings.Replace(dry, regexLine, strings.NewReplacer("("+firstName+")", "()", "("+firstName+"|", "(", "|"+firstName+"|", "|", "|"+firstName+")", ")").Replace(regexLine), 1),
	} {
		if err := checkTaggedSelection(bad, wants[plantShard], tf); err == nil {
			t.Errorf("planted defect not caught: %s", name)
		}
	}
	if unfilteredLine != "" {
		if err := checkTaggedSelection(dropLine(unfilteredLine), wants[plantShard], tf); err == nil {
			t.Error("planted defect not caught: an unfiltered package without its SHARD-UNFILTERED line")
		}
	}
	var tests []string
	for _, shard := range provider {
		r := providerResults[shard]
		if r.code != 0 || !strings.Contains(r.stdout, fmt.Sprintf("providersync test shard %d: DRY RUN", shard)) {
			t.Fatalf("providersync shard %d: exit %d: %s%s", shard, r.code, r.stdout, r.stderr)
		}
		tests = append(tests, lineValues(r.stdout, "  PROVIDER-TEST-RUN ")...)
	}
	_, integration := providerTests(t)
	gotT := map[string]bool{}
	for _, s := range tests {
		gotT[s] = true
	}
	if len(tests) != len(gotT) || !sameSet(gotT, integration) {
		t.Errorf("the providersync shards run %d tests (%d distinct), want the %d integration-tagged ones", len(tests), len(gotT), len(integration))
	}
}
