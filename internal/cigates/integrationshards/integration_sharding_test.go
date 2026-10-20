//go:build !race

package integrationshards_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
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
	return cmdSpec{dir: root, env: env}.run("bash", append([]string{"ci/check_go.sh"}, args...)...)
}

var (
	discoveredOnce sync.Once
	discovered     map[string]bool
)

func expectedPackages(t *testing.T) map[string]bool {
	t.Helper()
	discoveredOnce.Do(func() {
		res := cmdSpec{dir: rootOf(t), env: inheritedEnv(nil)}.run("bash", "ci/check_go.sh", "integration-coverage")
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
		res := cmdSpec{dir: root, env: inheritedEnv(nil, "GOTOOLCHAIN=go1.27.2", "GOWORK=off")}.run("go", "list", "-mod=readonly", "-tags=integration",
			"-f={{range .TestGoFiles}}{{println .}}{{end}}{{range .XTestGoFiles}}{{println .}}{{end}}", "./"+providerPackage)
		if res.code != 0 {
			return
		}
		providerTopLevel = map[string]bool{}
		fn := regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]+)\s*\(`)
		for _, f := range strings.Split(res.stdout, "\n") {
			if f == "" {
				continue
			}
			for _, m := range fn.FindAllStringSubmatch(readText(t, filepath.Join(root, providerPackage, f)), -1) {
				providerTopLevel[m[1]] = true
			}
		}
		providerIntegrationT = map[string]bool{}
		files, _ := filepath.Glob(filepath.Join(root, providerPackage, "*_test.go"))
		tagged := regexp.MustCompile(`(?m)^//go:build.*\bintegration\b`)
		fn2 := regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]+)`)
		for _, f := range files {
			src := readText(t, f)
			if !tagged.MatchString(src) {
				continue
			}
			for _, m := range fn2.FindAllStringSubmatch(src, -1) {
				providerIntegrationT[m[1]] = true
			}
		}
	})
	if len(providerTopLevel) == 0 || len(providerIntegrationT) == 0 {
		t.Fatal("providersync test discovery found nothing")
	}
	return providerTopLevel, providerIntegrationT
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

func TestIntegrationShardScriptShape(t *testing.T) {
	src := readText(t, filepath.Join(rootOf(t), "ci", "check_go.sh"))
	t.Run("provider shard discovery does not compile the test binary", func(t *testing.T) {
		m := regexp.MustCompile(`(?ms)^discover_providersync_tests\(\) \{\n(.*?)^\}`).FindStringSubmatch(src)
		if m == nil {
			t.Fatal("discover_providersync_tests moved")
		}
		body := m[1]
		if !strings.Contains(body, "go list -mod=readonly -tags=integration") || strings.Contains(body, "go test -mod=readonly") ||
			strings.Contains(body, `done <<< "${files_output}"`) || !strings.Contains(body, `done < <(printf '%s\n' "${files_output}")`) {
			t.Errorf("provider test discovery is not list-based and here-string free:\n%s", body)
		}
	})
	t.Run("package discovery does not spawn grep for every tracked test", func(t *testing.T) {
		m := regexp.MustCompile(`(?ms)^discover_integration_packages\(\) \{\n(.*?)^\}`).FindStringSubmatch(src)
		if m == nil {
			t.Fatal("discover_integration_packages moved")
		}
		body := m[1]
		if !strings.Contains(body, `git -C "${ROOT}/${module_dir}" grep -l -E`) || strings.Contains(body, "ls-files --cached --others") || !strings.Contains(body, "ls-files --others --exclude-standard") {
			t.Errorf("package discovery spawns per-file greps:\n%s", body)
		}
	})
	t.Run("the shard arity guard uses an explicit conditional", func(t *testing.T) {
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
	if !sameSet(seen, all) {
		t.Fatalf("the provider shards do not hold exactly the top-level tests: %d vs %d", len(seen), len(all))
	}
	gotInt, kinds := map[string]bool{}, map[string]bool{}
	for n, c := range class {
		kinds[c] = true
		if c == "integration" {
			gotInt[n] = true
		}
	}
	if !sameSet(gotInt, integration) || len(kinds) != 2 || !kinds["integration"] || !kinds["ordinary"] {
		t.Fatalf("provider test classification is wrong: %v", kinds)
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
	if len(weights) != facts.providerShards || spread(weights) > 1 || spread(ints) > 1 {
		t.Fatalf("provider shard weights %v / integration counts %v are not balanced to within one", weights, ints)
	}
}

func TestIntegrationShardPlanCheckerFailsEachPlantedDefect(t *testing.T) {
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

func TestEachIntegrationShardDryRunExecutesOnlyItsManifestAssignment(t *testing.T) {
	stdout, gho := realPlan(t)
	facts := realPlanFacts(t)
	assignments, err := checkShardPlan(stdout, gho, facts)
	if err != nil {
		t.Fatal(err)
	}
	var selected []string
	ran := 0
	shards := make([]int, 0, len(assignments))
	for s := range assignments {
		shards = append(shards, s)
	}
	sort.Ints(shards)
	for _, shard := range shards {
		want := map[string]bool{}
		for p := range assignments[shard] {
			if !splitPackages[p] {
				want[p] = true
			}
		}
		if len(want) == 0 {
			continue
		}
		r := runCheckGoSharding(t, shardEnv{}, "integration-shard", "packages", strconv.Itoa(shard), "--dry-run")
		if r.code != 0 || !strings.Contains(r.stdout, fmt.Sprintf("integration package shard %d: DRY RUN", shard)) {
			t.Fatalf("shard %d: exit %d: %s%s", shard, r.code, r.stdout, r.stderr)
		}
		chosen := lineValues(r.stdout, "  SHARD-RUN ")
		got := map[string]bool{}
		for _, c := range chosen {
			got[c] = true
		}
		if !sameSet(got, want) || len(chosen) != len(want) {
			t.Errorf("shard %d would run %v, the plan assigned %v", shard, sortedKeys(got), sortedKeys(want))
		}
		selected = append(selected, chosen...)
		ran++
	}
	if ran < 2 {
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
	var tests []string
	for shard := 1; shard <= facts.providerShards; shard++ {
		r := runCheckGoSharding(t, shardEnv{}, "integration-shard", "providersync", strconv.Itoa(shard), "--dry-run")
		if r.code != 0 || !strings.Contains(r.stdout, fmt.Sprintf("providersync test shard %d: DRY RUN", shard)) {
			t.Fatalf("providersync shard %d: exit %d: %s%s", shard, r.code, r.stdout, r.stderr)
		}
		tests = append(tests, lineValues(r.stdout, "  PROVIDER-TEST-RUN ")...)
	}
	all, _ := providerTests(t)
	gotT := map[string]bool{}
	for _, s := range tests {
		gotT[s] = true
	}
	if len(tests) != len(gotT) || !sameSet(gotT, all) {
		t.Errorf("the providersync shards run %d tests (%d distinct), want %d", len(tests), len(gotT), len(all))
	}
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
	m := regexp.MustCompile(`(?m)^\s*` + constant + `\s*=\s*"([^"]+)"`).FindStringSubmatch(readText(t, filepath.Join(rootOf(t), "internal", "testsupport", "containers", "harness.go")))
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
	return cmdSpec{dir: rootOf(t), env: env}.run("bash", append([]string{"ci/check_go.sh", "integration-prepull"}, args...)...)
}

func fileLines(dir, name string) []string {
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return nil
	}
	return strings.Split(strings.TrimRight(string(b), "\n"), "\n")
}

func TestIntegrationPrepull(t *testing.T) {
	prefix := os.Getenv("TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX")
	t.Run("retries the exact source declared image", func(t *testing.T) {
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
