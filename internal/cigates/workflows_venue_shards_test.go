package cigates_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-10177 (B2): Go port of the file-read tests of tests/tooling/test_venue_oracle_shards.py (CHAOS-6574, 6891,
// 6926) and of the YAML half of test_venue_oracle_triggers.py's changed-job matrix test (D6039a): the legs of both
// venue matrices are exactly 1..N and N is what the verb gets, a leg is bounded and does not fail fast, the receipt
// artifact is per shard, the leg budget plus setup and drift fits the job cap, and every registry `run` row has a
// weight while every weight names a `run` row. The tests that EXECUTE ci/check_go.sh (slice arithmetic, the plan verb,
// the weights generator, the planned-heaviest-leg budget) stay in Python until the venue-script port (CHAOS-10178).

const (
	venueChangedWorkflow = ".github/workflows/venue-oracles-changed.yml"
	venueRegistryDir     = "ci/venue_oracle_registry.d"
	venueWeightsDir      = "ci/venue_oracle_weights.d"
	venueSetupSeconds    = 200
	venueDriftSeconds    = 200
)

var matrixShardExpr = regexp.MustCompile(`\$\{\{\s*matrix\.shard\s*\}\}`)

var venueVerbCall = regexp.MustCompile(`^bash ci/check_go\.sh venue-oracles\s+"\$\{\{\s*matrix\.shard\s*\}\}"\s+(\d+)\b`)

func matrixShards(job yamlMap) ([]int, bool) {
	var out []int
	list := asList(asMap(asMap(job["strategy"])["matrix"])["shard"])
	for _, v := range list {
		n, ok := v.(int)
		if !ok {
			return nil, false
		}
		out = append(out, n)
	}
	return out, len(out) > 0
}

func isOneToN(shards []int) bool {
	for i, s := range shards {
		if s != i+1 {
			return false
		}
	}
	return true
}

func venueShardsViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	job := asMap(jobsOf(loadWf(t, root, "venue-oracles.yml"))[venueJobID])
	shards, ok := matrixShards(job)
	if !ok {
		return []string{fmt.Sprintf("venue-oracles matrix shard list is %v: want a non-empty list of integers", asMap(asMap(job["strategy"])["matrix"])["shard"])}
	}
	if !isOneToN(shards) {
		out = append(out, fmt.Sprintf("matrix shard list %v is not 1..N: a gap or duplicate leaves part of the registry unrun or run twice", shards))
	}
	if keys := sortedKeys(asMap(asMap(job["strategy"])["matrix"])); len(keys) != 1 || keys[0] != "shard" {
		out = append(out, fmt.Sprintf("the venue-oracles matrix declares %v, want only shard: an include or exclude can drop or add a leg while the list still reads 1..N", keys))
	}
	for _, key := range []string{"if", "continue-on-error"} {
		if v, has := job[key]; has {
			out = append(out, fmt.Sprintf("the venue-oracles job declares %s: %v: a skipped or failure-swallowing leg reads as a pass", key, v))
		}
	}
	var verbLines []string
	var verbStep yamlMap
	var verbCode []shLine
	var verbAt int
	for _, st := range stepsOf(job) {
		code := shellCode(asString(st["run"]))
		for i, l := range code {
			if strings.Contains(l.text, "check_go.sh venue-oracles") {
				verbLines = append(verbLines, l.text)
				verbStep, verbCode, verbAt = st, code, i
			}
		}
	}
	if len(verbLines) != 1 {
		out = append(out, fmt.Sprintf("want exactly one check_go.sh venue-oracles command in the job's code (comments do not count), found %v", verbLines))
	} else {
		call := verbCode[verbAt]
		if m := venueVerbCall.FindStringSubmatch(call.text); call.indent != 0 || m == nil {
			out = append(out, fmt.Sprintf("the verb is not a top-level statement `bash ci/check_go.sh venue-oracles \"${{ matrix.shard }}\" <count>` (an echo, a comment or a branch is not the call): %q", call.text))
		} else if n, _ := strconv.Atoi(m[1]); n != len(shards) {
			out = append(out, fmt.Sprintf("the workflow passes COUNT=%s but the matrix has %d legs", m[1], len(shards)))
		}
		pipefail := false
		for _, l := range verbCode[:verbAt] {
			pipefail = pipefail || (l.indent == 0 && l.text == "set -euo pipefail")
		}
		if !pipefail {
			out = append(out, "the verb step has no top-level `set -euo pipefail` before the call: a failing verb piped into tee would read as success")
		}
		if got := asString(verbStep["if"]); got != "steps.relevance.outputs.relevant == 'true'" {
			out = append(out, fmt.Sprintf("the verb step's if is %q, want the relevance gate steps.relevance.outputs.relevant == 'true'", got))
		}
		if v, has := verbStep["continue-on-error"]; has {
			out = append(out, fmt.Sprintf("the verb step declares continue-on-error: %v: a red leg must fail the job", v))
		}
	}
	if !regexp.MustCompile(`\$\{\{\s*matrix\.shard\s*\}\}/` + strconv.Itoa(len(shards)) + `\)$`).MatchString(asString(job["name"])) {
		out = append(out, fmt.Sprintf("the leg name %q states the wrong count (want ...${{ matrix.shard }}/%d))", asString(job["name"]), len(shards)))
	}
	timeout, _ := job["timeout-minutes"].(int)
	if timeout == 0 || timeout > 40 {
		out = append(out, fmt.Sprintf("a venue leg must stay well under the 75 min that cancelled the sequential job (timeout-minutes=%v)", job["timeout-minutes"]))
	}
	if ff, ok := asMap(job["strategy"])["fail-fast"].(bool); !ok || ff {
		out = append(out, "strategy.fail-fast must be false: one red shard must not cancel the others, the point is a signal per slice")
	}
	var uploads []yamlMap
	for _, s := range stepsOf(job) {
		if strings.HasPrefix(asString(s["uses"]), "actions/upload-artifact@") {
			uploads = append(uploads, s)
		}
	}
	if len(uploads) != 1 {
		out = append(out, fmt.Sprintf("want exactly one upload-artifact step, found %d", len(uploads)))
	} else if !matrixShardExpr.MatchString(asString(asMap(uploads[0]["with"])["name"])) {
		out = append(out, "the receipt artifact name has no matrix.shard: legs uploading one artifact name collide")
	}
	budget, err := strconv.Atoi(fmt.Sprint(asMap(job["env"])["VENUE_LEG_BUDGET_SECONDS"]))
	if err != nil {
		out = append(out, fmt.Sprintf("env VENUE_LEG_BUDGET_SECONDS is %v: want an integer", asMap(job["env"])["VENUE_LEG_BUDGET_SECONDS"]))
	} else if cap := timeout * 60; budget+venueSetupSeconds+venueDriftSeconds > cap {
		out = append(out, fmt.Sprintf("the budget %ds plus %ds setup and %ds drift does not fit the %ds job cap", budget, venueSetupSeconds, venueDriftSeconds, cap))
	}

	changed := asMap(jobsOf(loadWf(t, root, "venue-oracles-changed.yml"))["venue-oracles-changed"])
	legs, ok := matrixShards(changed)
	switch {
	case !ok:
		out = append(out, "venue-oracles-changed matrix shard list is empty or not integers")
	default:
		if !isOneToN(legs) {
			out = append(out, fmt.Sprintf("venue-oracles-changed matrix legs %v are not 1..N", legs))
		}
		if keys := sortedKeys(asMap(asMap(changed["strategy"])["matrix"])); len(keys) != 1 || keys[0] != "shard" {
			out = append(out, fmt.Sprintf("the venue-oracles-changed matrix declares %v, want only shard: an include or exclude can drop or add a leg", keys))
		}
		env := asMap(changed["env"])
		if fmt.Sprint(env["VENUE_LEGS"]) != strconv.Itoa(len(legs)) {
			out = append(out, fmt.Sprintf("venue-oracles-changed env VENUE_LEGS is %v but the matrix has %d legs", env["VENUE_LEGS"], len(legs)))
		}
		if asString(env["VENUE_LEG"]) != "${{ matrix.shard }}" {
			out = append(out, fmt.Sprintf("venue-oracles-changed env VENUE_LEG is %q, want ${{ matrix.shard }}", asString(env["VENUE_LEG"])))
		}
	}
	return append(out, venueWeightsViolations(t, root)...)
}

type venueRow struct{ pkg, test string }

func venueRegistryRunRows(t *testing.T, root string) (rows []venueRow, files map[string]bool, problems []string) {
	t.Helper()
	files = map[string]bool{}
	paths, err := filepath.Glob(filepath.Join(root, venueRegistryDir, "*.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		files[filepath.Base(p)] = true
		for _, line := range strings.Split(readFile(t, p), "\n") {
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			f := strings.Split(line, "\t")
			if len(f) != 3 {
				problems = append(problems, fmt.Sprintf("%s: registry row %q is not <package><TAB><test><TAB><mode>", filepath.Base(p), line))
				continue
			}
			if f[2] == "run" {
				rows = append(rows, venueRow{f[0], f[1]})
			}
		}
	}
	return rows, files, problems
}

func venueWeightsViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	registry, registryFiles, problems := venueRegistryRunRows(t, root)
	out = append(out, problems...)
	if len(registry) == 0 {
		out = append(out, venueRegistryDir+" holds no run row: a vacuous registry would pass every weight check")
	}
	paths, err := filepath.Glob(filepath.Join(root, venueWeightsDir, "*.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		return append(out, venueWeightsDir+"/ holds no *.tsv file")
	}
	sort.Strings(paths)
	have := map[venueRow]int{}
	for _, p := range paths {
		name := filepath.Base(p)
		var rows []venueRow
		pkgs := map[string]bool{}
		for _, line := range strings.Split(readFile(t, p), "\n") {
			if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimLeft(line, " \t"), "#") {
				continue
			}
			f := strings.Split(line, "\t")
			if len(f) < 3 || len(f) > 4 || (len(f) == 4 && f[3] != "unmeasured") {
				out = append(out, fmt.Sprintf("%s: weights row %q is not <package><TAB><test><TAB><seconds>[<TAB>unmeasured]", name, line))
				continue
			}
			seconds, err := strconv.Atoi(f[2])
			if err != nil || seconds < 1 {
				out = append(out, fmt.Sprintf("%s: weight of %s is %q, want an integer of at least 1 second", name, f[1], f[2]))
			}
			row := venueRow{f[0], f[1]}
			rows = append(rows, row)
			pkgs[f[0]] = true
			have[row]++
		}
		if len(rows) == 0 {
			out = append(out, fmt.Sprintf("%s holds no weight row", name))
			continue
		}
		if len(pkgs) != 1 {
			out = append(out, fmt.Sprintf("%s mixes packages %v", name, sortedSet(pkgs)))
			continue
		}
		pkg := rows[0].pkg
		if want := strings.ReplaceAll(pkg, "/", "__") + ".tsv"; name != want {
			out = append(out, fmt.Sprintf("%s holds the rows of %s; it must be named %s", name, pkg, want))
		}
		if !registryFiles[name] {
			out = append(out, fmt.Sprintf("%s has no registry file of the same name: the package has no venue run row left", name))
		}
		if !sort.SliceIsSorted(rows, func(i, j int) bool {
			if rows[i].pkg != rows[j].pkg {
				return rows[i].pkg < rows[j].pkg
			}
			return rows[i].test < rows[j].test
		}) {
			out = append(out, fmt.Sprintf("%s is not sorted by test", name))
		}
	}
	for row, n := range have {
		if n > 1 {
			out = append(out, fmt.Sprintf("a weight row is duplicated: %s %s", row.pkg, row.test))
		}
	}
	registrySet := map[venueRow]bool{}
	for _, r := range registry {
		registrySet[r] = true
	}
	var missing, stale []string
	for r := range registrySet {
		if have[r] == 0 {
			missing = append(missing, r.pkg+" "+r.test)
		}
	}
	for r := range have {
		if !registrySet[r] {
			stale = append(stale, r.pkg+" "+r.test)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	if len(missing) > 0 {
		out = append(out, fmt.Sprintf("registry run rows with no weight: %v: regenerate %s/ (python3 ci/venue_oracle_weights.py <saved run log>)", missing, venueWeightsDir))
	}
	if len(stale) > 0 {
		out = append(out, fmt.Sprintf("weights for tests that are not registry run rows: %v", stale))
	}
	return out
}

func sortedSet(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func venueShardFiles(t *testing.T, root string) []string {
	t.Helper()
	files := []string{venueWorkflow, venueChangedWorkflow}
	for _, dir := range []string{venueRegistryDir, venueWeightsDir} {
		paths, err := filepath.Glob(filepath.Join(root, dir, "*.tsv"))
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range paths {
			files = append(files, dir+"/"+filepath.Base(p))
		}
	}
	return files
}

func TestVenueShardsAndWeights(t *testing.T) {
	expectNoViolations(t, venueShardsViolations(t, repoRoot(t)))
}

// writeIn rewrites one file of a planted tree.
func writeIn(t *testing.T, tree, rel string, edit func(string) string) {
	t.Helper()
	p := filepath.Join(tree, rel)
	text := readFile(t, p)
	next := edit(text)
	if next == text {
		t.Fatalf("planted defect did not land in %s", rel)
	}
	if err := os.WriteFile(p, []byte(next), 0o644); err != nil {
		t.Fatal(err)
	}
}

func firstWeightsFile(t *testing.T, root string, minRows int) string {
	t.Helper()
	paths, _ := filepath.Glob(filepath.Join(root, venueWeightsDir, "*.tsv"))
	sort.Strings(paths)
	for _, p := range paths {
		if strings.Count(readFile(t, p), "\n") >= minRows {
			return venueWeightsDir + "/" + filepath.Base(p)
		}
	}
	t.Fatalf("no weights file with %d rows", minRows)
	return ""
}

func TestVenueShardsPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const f, c = venueWorkflow, venueChangedWorkflow
	files := venueShardFiles(t, root)
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"matrix excludes a leg", []mutation{{f, "shard: [1, 2, 3, 4, 5, 6, 7, 8]", "shard: [1, 2, 3, 4, 5, 6, 7, 8]\n        exclude:\n          - shard: 8"}}, []string{"declares [exclude shard]"}},
		{"matrix adds a leg by include", []mutation{{f, "shard: [1, 2, 3, 4, 5, 6, 7, 8]", "shard: [1, 2, 3, 4, 5, 6, 7, 8]\n        include:\n          - shard: 9"}}, []string{"declares [include shard]"}},
		{"the verb call is only a comment", []mutation{{f, `bash ci/check_go.sh venue-oracles "${{ matrix.shard }}" 8 2>&1`, `# bash ci/check_go.sh venue-oracles "${{ matrix.shard }}" 8 2>&1`}}, []string{"found []"}},
		{"the verb call is only echoed", []mutation{{f, `bash ci/check_go.sh venue-oracles "${{ matrix.shard }}" 8 2>&1`, `echo bash ci/check_go.sh venue-oracles "${{ matrix.shard }}" 8 2>&1`}}, []string{"not a top-level statement"}},
		{"the verb call sits in a dead branch", []mutation{{f, `bash ci/check_go.sh venue-oracles "${{ matrix.shard }}" 8 2>&1 | tee "${RUNNER_TEMP}/venue-oracle-receipt/receipt.log"`, "if false; then\n            bash ci/check_go.sh venue-oracles \"${{ matrix.shard }}\" 8 2>&1 | tee \"${RUNNER_TEMP}/venue-oracle-receipt/receipt.log\"\n          fi"}}, []string{"not a top-level statement"}},
		{"the verb step loses pipefail", []mutation{{f, "          set -euo pipefail\n          mkdir -p \"${RUNNER_TEMP}/venue-oracle-receipt\"\n          bash ci/check_go.sh", "          mkdir -p \"${RUNNER_TEMP}/venue-oracle-receipt\"\n          bash ci/check_go.sh"}}, []string{"no top-level `set -euo pipefail`"}},
		{"the verb step is gated by something else", []mutation{{f, "relevant == 'true'\n        run: |\n          set -euo pipefail\n          mkdir -p \"${RUNNER_TEMP}/venue-oracle-receipt\"", "relevant == 'false'\n        run: |\n          set -euo pipefail\n          mkdir -p \"${RUNNER_TEMP}/venue-oracle-receipt\""}}, []string{"the verb step's if is"}},
		{"the verb step may fail silently", []mutation{{f, "      - name: Run this shard's venue differential oracles against the live Python api\n", "      - name: Run this shard's venue differential oracles against the live Python api\n        continue-on-error: true\n"}}, []string{"the verb step declares continue-on-error"}},
		{"the leg job may fail silently", []mutation{{f, "    timeout-minutes: 40\n", "    timeout-minutes: 40\n    continue-on-error: true\n"}}, []string{"the venue-oracles job declares continue-on-error"}},
		{"the artifact action is another action", []mutation{{f, "actions/upload-artifact@", "evil/upload-artifact-fake@"}}, []string{"exactly one upload-artifact step"}},
		{"the artifact name only mentions matrix.shardless", []mutation{{f, "-shard-${{ matrix.shard }}", "-shard-${{ matrix.shardless }}"}}, []string{"collide"}},
		{"changed-job matrix excludes a leg", []mutation{{c, "shard: [1, 2, 3, 4]", "shard: [1, 2, 3, 4]\n        exclude:\n          - shard: 4"}}, []string{"venue-oracles-changed matrix declares [exclude shard]"}},
		{"matrix has a gap", []mutation{{f, "shard: [1, 2, 3, 4, 5, 6, 7, 8]", "shard: [1, 2, 3, 4, 5, 6, 7, 9]"}}, []string{"not 1..N"}},
		{"matrix repeats a leg", []mutation{{f, "shard: [1, 2, 3, 4, 5, 6, 7, 8]", "shard: [1, 2, 3, 4, 5, 6, 7, 7]"}}, []string{"not 1..N"}},
		{"verb count disagrees with the matrix", []mutation{{f, `venue-oracles "${{ matrix.shard }}" 8 2>&1`, `venue-oracles "${{ matrix.shard }}" 7 2>&1`}}, []string{"COUNT=7", "8 legs"}},
		{"verb is not given the shard", []mutation{{f, `venue-oracles "${{ matrix.shard }}" 8 2>&1`, `venue-oracles 1 8 2>&1`}}, []string{"not a top-level statement"}},
		{"a second verb call", []mutation{{f, "          mkdir -p \"${RUNNER_TEMP}/venue-oracle-receipt\"\n          bash ci/check_go.sh venue-oracles", "          bash ci/check_go.sh venue-oracles 1 1\n          mkdir -p \"${RUNNER_TEMP}/venue-oracle-receipt\"\n          bash ci/check_go.sh venue-oracles"}}, []string{"exactly one check_go.sh venue-oracles command"}},
		{"leg name states the wrong count", []mutation{{f, "(shard ${{ matrix.shard }}/8)", "(shard ${{ matrix.shard }}/7)"}}, []string{"wrong count"}},
		{"leg timeout is the sequential job's", []mutation{{f, "    timeout-minutes: 40\n", "    timeout-minutes: 75\n"}}, []string{"75 min"}},
		{"leg has no timeout", []mutation{{f, "    timeout-minutes: 40\n", ""}}, []string{"timeout-minutes"}},
		{"fail-fast comes back", []mutation{{f, "fail-fast: false", "fail-fast: true"}}, []string{"fail-fast must be false"}},
		{"fail-fast removed (GitHub default is true)", []mutation{{f, "      fail-fast: false\n", ""}}, []string{"fail-fast must be false"}},
		{"receipt artifact is shared", []mutation{{f, "-shard-${{ matrix.shard }}", ""}}, []string{"collide"}},
		{"budget outgrows the cap", []mutation{{f, "VENUE_LEG_BUDGET_SECONDS: '1800'", "VENUE_LEG_BUDGET_SECONDS: '2300'"}}, []string{"does not fit the 2400s job cap"}},
		{"changed-job matrix has a gap", []mutation{{c, "shard: [1, 2, 3, 4]", "shard: [1, 2, 3, 5]"}}, []string{"venue-oracles-changed matrix legs", "not 1..N"}},
		{"changed-job VENUE_LEGS disagrees", []mutation{{c, "      VENUE_LEGS: 4\n", "      VENUE_LEGS: 3\n"}}, []string{"VENUE_LEGS is 3", "4 legs"}},
		{"changed-job VENUE_LEG is not the matrix shard", []mutation{{c, "      VENUE_LEG: ${{ matrix.shard }}\n", "      VENUE_LEG: 1\n"}}, []string{"VENUE_LEG is"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tree := plantTree(t, root, files, tc.muts...)
			expectViolation(t, venueShardsViolations(t, tree), tc.want...)
		})
	}
}

func TestVenueWeightsPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	files := venueShardFiles(t, root)
	multi := firstWeightsFile(t, root, 2)
	single := firstWeightsFile(t, root, 1)
	cases := []struct {
		name  string
		plant func(t *testing.T, tree string)
		want  []string
	}{
		{"a registry row has no weight", func(t *testing.T, tree string) {
			if err := os.Remove(filepath.Join(tree, single)); err != nil {
				t.Fatal(err)
			}
		}, []string{"registry run rows with no weight"}},
		{"a weight names a test that is not a run row", func(t *testing.T, tree string) {
			writeIn(t, tree, single, func(s string) string { return s + strings.Split(s, "\t")[0] + "\tTestZzzGhost\t5\n" })
		}, []string{"weights for tests that are not registry run rows", "TestZzzGhost"}},
		{"a weight row is duplicated", func(t *testing.T, tree string) {
			writeIn(t, tree, single, func(s string) string { return s + strings.SplitAfter(s, "\n")[0] })
		}, []string{"a weight row is duplicated"}},
		{"a weight below one second", func(t *testing.T, tree string) {
			writeIn(t, tree, single, func(s string) string {
				f := strings.SplitN(s, "\t", 3)
				return f[0] + "\t" + f[1] + "\t0" + strings.TrimLeft(f[2], "0123456789")
			})
		}, []string{"at least 1 second"}},
		{"an unknown fourth column", func(t *testing.T, tree string) {
			writeIn(t, tree, single, func(s string) string { return strings.Replace(s, "\n", "\tmeasured\n", 1) })
		}, []string{"is not <package><TAB><test><TAB><seconds>"}},
		{"a row with two columns", func(t *testing.T, tree string) {
			writeIn(t, tree, single, func(s string) string { return s + "internal/x\tTestX\n" })
		}, []string{"is not <package><TAB><test><TAB><seconds>"}},
		{"a file mixes packages", func(t *testing.T, tree string) {
			writeIn(t, tree, single, func(s string) string { return s + "internal/other\tTestOther\t5\n" })
		}, []string{"mixes packages"}},
		{"a file is named for another package", func(t *testing.T, tree string) {
			if err := os.Rename(filepath.Join(tree, single), filepath.Join(tree, venueWeightsDir, "wrong__name.tsv")); err != nil {
				t.Fatal(err)
			}
		}, []string{"it must be named", "no registry file of the same name"}},
		{"a file has no row left", func(t *testing.T, tree string) {
			writeIn(t, tree, single, func(s string) string { return "# nothing\n" })
		}, []string{"holds no weight row"}},
		{"a weights file whose package has no registry file", func(t *testing.T, tree string) {
			if err := os.WriteFile(filepath.Join(tree, venueWeightsDir, "internal__zzz.tsv"), []byte("internal/zzz\tTestZzz\t5\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}, []string{"internal__zzz.tsv has no registry file of the same name"}},
		{"rows are not sorted by test", func(t *testing.T, tree string) {
			writeIn(t, tree, multi, func(s string) string {
				lines := strings.Split(strings.TrimSuffix(s, "\n"), "\n")
				lines[0], lines[len(lines)-1] = lines[len(lines)-1], lines[0]
				return strings.Join(lines, "\n") + "\n"
			})
		}, []string{"is not sorted by test"}},
		{"the weights directory is empty", func(t *testing.T, tree string) {
			paths, _ := filepath.Glob(filepath.Join(tree, venueWeightsDir, "*.tsv"))
			for _, p := range paths {
				if err := os.Remove(p); err != nil {
					t.Fatal(err)
				}
			}
		}, []string{"holds no *.tsv file"}},
		{"the registry is empty", func(t *testing.T, tree string) {
			paths, _ := filepath.Glob(filepath.Join(tree, venueRegistryDir, "*.tsv"))
			for _, p := range paths {
				if err := os.Remove(p); err != nil {
					t.Fatal(err)
				}
			}
		}, []string{"holds no run row"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tree := plantTree(t, root, files)
			tc.plant(t, tree)
			expectViolation(t, venueWeightsViolations(t, tree), tc.want...)
		})
	}
}
