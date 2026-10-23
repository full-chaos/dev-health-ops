package checkgo_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// CHAOS-10174 PR C: Go port of the five tests/tooling/test_check_go_*.py files that execute or read ci/check_go.sh:
//   test_check_go_gocache_precedence.py (CHAOS-5224, 5268)      -> TestCheckGoGocachePrecedence
//   test_check_go_public_verbs.py (CHAOS-3948)                   -> TestCheckGoStaticContract, TestCheckGoHelpDocumentsEveryPublicVerb
//   test_check_go_integration_ryuk_env.py (CHAOS-6634)           -> TestCheckGoStaticContract, TestCheckGoRyukEnv
//   test_check_go_integration_vet_positive_control.py (4959)     -> TestIntegrationVetCatchesABrokenTaggedFile
//   test_check_go_integration_coverage.py (3092 .. 6926)         -> TestCheckGoIntegrationCoverage

var publicVerbs = []string{
	"fmt", "vet", "test", "race", "build", "contract", "multi-replica-workers", "integration-vet", "integration-coverage",
	"integration-shard-plan", "integration-images", "integration-prepull", "integration-shard", "integration", "fast", "ci", "all",
}

// The composite verbs' exact step sequence. `ci` keeps go.yml's go-quality coverage byte for byte (fast plus the race
// detector); `all` is the honest full local signal (ci with its two plan-only integration calls replaced by one real
// check_integration call). Anything else is a coverage regression in one verb or the other.
var (
	fastSteps = []string{"check_format", "check_vet", "check_test", "check_build", "check_contract", "check_ratchets",
		"check_race_weights_rows", "check_integration_vet", "plan_integration_shards", "plan_providersync_test_shards", "check_plan_reads", "check_multi_replica_workers"}
	ciSteps = []string{"check_format", "check_vet", "check_test", "check_race", "check_build", "check_contract", "check_ratchets",
		"check_race_weights_rows", "check_integration_vet", "plan_integration_shards", "plan_providersync_test_shards", "check_plan_reads", "check_multi_replica_workers"}
	allSteps = []string{"check_format", "check_vet", "check_test", "check_race", "check_build", "check_contract", "check_ratchets",
		"check_race_weights_rows", "check_integration_vet", "check_integration", "check_plan_reads", "check_multi_replica_workers"}
)

var verbSteps = map[string][]string{"fast": fastSteps, "ci": ciSteps, "all": allSteps}

func stringSet(xs []string) map[string]bool {
	m := map[string]bool{}
	for _, x := range xs {
		m[x] = true
	}
	return m
}

func setDiff(a, b []string) []string {
	bs := stringSet(b)
	var out []string
	for _, x := range a {
		if !bs[x] {
			out = append(out, x)
		}
	}
	sort.Strings(out)
	return out
}

// verbTableDefects pins the relationships between the three tables, not just each verb's own list, so the invariant
// cannot erode one verb at a time.
func verbTableDefects(fast, ci, all []string) []string {
	var d []string
	if got := setDiff(ci, fast); strings.Join(got, ",") != "check_race" {
		d = append(d, fmt.Sprintf("ci minus fast is %v, want only check_race", got))
	}
	if got := setDiff(fast, ci); len(got) != 0 {
		d = append(d, fmt.Sprintf("fast minus ci is %v, want nothing", got))
	}
	if got := setDiff(ci, all); strings.Join(got, ",") != "plan_integration_shards,plan_providersync_test_shards" {
		d = append(d, fmt.Sprintf("ci minus all is %v, want the two plan-only integration calls", got))
	}
	if got := setDiff(all, ci); strings.Join(got, ",") != "check_integration" {
		d = append(d, fmt.Sprintf("all minus ci is %v, want only check_integration", got))
	}
	return d
}

func caseBlockBody(source, verb string) (string, error) {
	parts := strings.SplitN(source, `case "${1:-all}" in`, 2)
	if len(parts) != 2 {
		return "", fmt.Errorf(`no case "${1:-all}" dispatcher found`)
	}
	m := regexp.MustCompile(`(?s)\n  ` + regexp.QuoteMeta(verb) + `\)\n(.*?)\n    ;;`).FindStringSubmatch(parts[1])
	if m == nil {
		return "", fmt.Errorf("no `%s)` case block found in the dispatcher", verb)
	}
	return m[1], nil
}

var bareCall = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

func bareCallLines(block string) []string {
	var out []string
	for _, line := range strings.Split(block, "\n") {
		if strings.HasPrefix(line, "    ") && bareCall.MatchString(strings.TrimSpace(line)) {
			out = append(out, strings.TrimSpace(line))
		}
	}
	return out
}

// checkGoStaticDefects reads ci/check_go.sh text and returns every violated static contract.
func checkGoStaticDefects(src string) []string {
	var d []string
	for _, verb := range []string{"fast", "ci", "all"} {
		block, err := caseBlockBody(src, verb)
		if err != nil {
			d = append(d, err.Error())
			continue
		}
		if got := bareCallLines(block); strings.Join(got, ",") != strings.Join(verbSteps[verb], ",") {
			d = append(d, fmt.Sprintf("`%s` case block runs %v, want %v", verb, got, verbSteps[verb]))
		}
	}
	// the multi-replica gate is cold-measured and required
	fn := ""
	if parts := strings.SplitN(src, "check_multi_replica_workers() {", 2); len(parts) == 2 {
		fn = strings.SplitN(parts[1], "\n}", 2)[0]
	}
	for _, need := range []string{"-tags=integration -count=1", "DEV_HEALTH_MULTI_REPLICA_PROOF", "measured zero jobs"} {
		if !strings.Contains(fn, need) {
			d = append(d, fmt.Sprintf("check_multi_replica_workers lacks %q", need))
		}
	}
	if n := strings.Count(src, "    check_multi_replica_workers\n"); n != 4 {
		d = append(d, fmt.Sprintf("check_multi_replica_workers has %d bare call sites, want 4 (one public verb plus fast, ci, all)", n))
	}
	// every multi-package `go test -tags=integration` line carries INTEGRATION_TEST_ENV (one Ryuk session per invocation)
	siteRe := regexp.MustCompile(`\bgo test\b.*-tags=integration`)
	sites := 0
	for _, line := range strings.Split(src, "\n") {
		if siteRe.MatchString(line) && strings.Contains(line, `"${run_pkgs[@]}"`) {
			sites++
			if !strings.Contains(line, "INTEGRATION_TEST_ENV") {
				d = append(d, "a multi-package integration `go test` without INTEGRATION_TEST_ENV shares one Ryuk session across packages: "+strings.TrimSpace(line))
			}
		}
	}
	if sites < 2 {
		d = append(d, fmt.Sprintf("found %d multi-package integration go test call sites, want the shard and integration sites", sites))
	}
	if !strings.Contains(src, `[ "${CI:-}" = "true" ]`) || !strings.Contains(src, "TESTCONTAINERS_RYUK_DISABLED=true") {
		d = append(d, "the Ryuk env block is not gated on CI=true")
	}
	return d
}

func TestCheckGoStaticContract(t *testing.T) {
	t.Parallel()
	if d := verbTableDefects(fastSteps, ciSteps, allSteps); len(d) > 0 {
		t.Fatalf("the parity tables disagree with themselves: %v", d)
	}
	script := scriptPath(t, "ci/check_go.sh")
	src := readText(t, script)
	if d := checkGoStaticDefects(src); len(d) > 0 {
		t.Fatalf("ci/check_go.sh violates its static contract:\n  %s", strings.Join(d, "\n  "))
	}
	t.Run("plant/parity table edited", func(t *testing.T) {
		d := verbTableDefects(fastSteps, append(append([]string{}, ciSteps...), "check_extra"), allSteps)
		if len(d) == 0 {
			t.Fatal("a ci verb with an extra step passed the table relationships")
		}
		d = verbTableDefects(fastSteps, ciSteps, append(append([]string{}, allSteps...), "plan_integration_shards"))
		if len(d) == 0 {
			t.Fatal("an `all` verb that kept a plan-only call passed the table relationships")
		}
		d = verbTableDefects(fastSteps, fastSteps, allSteps)
		if len(d) == 0 {
			t.Fatal("a `ci` verb without the race detector passed the table relationships")
		}
	})
	plants := []struct{ name, old, repl, want string }{
		{"ci drops the race detector", "    check_test\n    check_race\n    check_build\n", "    check_test\n    check_build\n", "`ci` case block runs"},
		{"multi-replica gate loses the cold flag", "-tags=integration -count=1 -timeout=5m", "-tags=integration -timeout=5m", "check_multi_replica_workers lacks"},
		{"multi-replica proof not required", "measured zero jobs", "measured nothing", "check_multi_replica_workers lacks"},
		{"a multi-package call site without the env", `${INTEGRATION_TEST_ENV[@]+"${INTEGRATION_TEST_ENV[@]}"} go test -mod=readonly -trimpath -tags=integration -count=1 -timeout=30m "${run_pkgs[@]}"`, `go test -mod=readonly -trimpath -tags=integration -count=1 -timeout=30m "${run_pkgs[@]}"`, "without INTEGRATION_TEST_ENV"},
	}
	for _, pl := range plants {
		pl := pl
		t.Run("plant/"+pl.name, func(t *testing.T) {
			t.Parallel()
			if strings.Count(src, pl.old) < 1 {
				t.Fatalf("anchor %q is absent from ci/check_go.sh: the plant measured nothing", pl.old)
			}
			// replace the first occurrence only: enough for every plant, and keeps the other sites intact
			planted := strings.Replace(src, pl.old, pl.repl, 1)
			joined := strings.Join(checkGoStaticDefects(planted), "\n")
			if !strings.Contains(joined, pl.want) {
				t.Fatalf("planted defect %q not caught: defects %q, want one mentioning %q", pl.name, joined, pl.want)
			}
		})
	}
}

// ---- GOCACHE precedence --------------------------------------------------------------------------------------

func gocacheSnippet(script string) (string, error) {
	raw, err := os.ReadFile(script)
	if err != nil {
		return "", err
	}
	lines := strings.Split(string(raw), "\n")
	start, end := -1, -1
	for i, l := range lines {
		if start < 0 && strings.HasPrefix(l, "DEV_HEALTH_GO_CACHE=") {
			start = i
		}
		if strings.TrimSpace(l) == `export GOCACHE="${DEV_HEALTH_GO_CACHE}"` {
			end = i
			break
		}
	}
	if start < 0 || end < 0 || start >= end {
		return "", fmt.Errorf("%s: GOCACHE precedence markers not found in order (start %d end %d): the script's shape changed, re-derive the markers", script, start, end)
	}
	snippet := strings.Join(lines[start:end+1], "\n")
	if !strings.Contains(snippet, "${DEV_HEALTH_GO_CACHE:-${GOCACHE:-}}") {
		return "", fmt.Errorf("ci/check_go.sh's DEV_HEALTH_GO_CACHE precedence no longer falls back through an inherited GOCACHE before the tmp default (CHAOS-5224 regressed). Extracted snippet:\n%s", snippet)
	}
	return snippet, nil
}

// fakeGoBin returns a PATH whose `go env -changed GOCACHE` prints the configured value in Go's real form
// (GOCACHE='<path>'), or nothing when empty.
func fakeGoBin(work, configured string) (string, error) {
	bin := filepath.Join(work, "fakebin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		return "", err
	}
	body := "#!/usr/bin/env bash\n[ \"$*\" = \"env -changed GOCACHE\" ] || exit 0\n" +
		"[ -n \"" + configured + "\" ] && echo \"GOCACHE='" + configured + "'\"\nexit 0\n"
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(body), 0o755); err != nil {
		return "", err
	}
	return bin + ":" + withShims("/usr/bin:/bin"), nil
}

func runGocacheSnippet(script string, env []string) (value, stderr string, err error) {
	snippet, err := gocacheSnippet(script)
	if err != nil {
		return "", "", err
	}
	res := cmdSpec{env: env}.run("bash", "-c", snippet+"\nprintf '%s' \"$GOCACHE\"\n")
	if res.code != 0 {
		return "", res.stderr, fmt.Errorf("snippet exited %d: stdout=%q stderr=%q", res.code, res.stdout, res.stderr)
	}
	return res.stdout, res.stderr, nil
}

func isDir(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.IsDir()
}

func gocacheScenarios() []scenario {
	expect := func(env func(work string) ([]string, error), want func(work string) string, dirCreated bool, stderrWant string) func(string, string) error {
		return func(script, work string) error {
			e, err := env(work)
			if err != nil {
				return err
			}
			got, stderr, err := runGocacheSnippet(script, e)
			if err != nil {
				return err
			}
			if w := want(work); got != w {
				return fmt.Errorf("GOCACHE is %q, want %q", got, w)
			}
			if dirCreated && !isDir(got) {
				return fmt.Errorf("want %q to have been mkdir -p'd", got)
			}
			switch stderrWant {
			case "":
				if stderr != "" {
					return fmt.Errorf("no fallback warning expected, got stderr %q", stderr)
				}
			case "loud":
				if !strings.Contains(stderr, "no Go build cache dir is configured") || !strings.Contains(stderr, got) || strings.Contains(stderr, "WARNING") {
					return fmt.Errorf("the tmp fallback must be loud on stderr, got %q", stderr)
				}
			}
			return nil
		}
	}
	pathEnv := func(configured string, extra ...func(w string) string) func(string) ([]string, error) {
		return func(w string) ([]string, error) {
			path, err := fakeGoBin(w, configured)
			if err != nil {
				return nil, err
			}
			env := []string{"PATH=" + path}
			for _, x := range extra {
				env = append(env, x(w))
			}
			return env, nil
		}
	}
	return []scenario{
		{"snippet_matches_the_expected_precedence_shape", func(script, work string) error {
			_, err := gocacheSnippet(script)
			return err
		}},
		{"all_empty_falls_back_to_tmpdir_default_and_says_so", expect(
			pathEnv("", func(w string) string { return "TMPDIR=" + w }),
			func(w string) string { return filepath.Join(w, "dev-health-go-build-cache") }, true, "loud")},
		{"go_reports_only_the_default_takes_tmp_fallback", expect(
			pathEnv("", func(w string) string { return "TMPDIR=" + w }),
			func(w string) string { return filepath.Join(w, "dev-health-go-build-cache") }, true, "loud")},
		{"configured_go_env_path_wins_over_tmp_fallback_silently", func(script, work string) error {
			configured := filepath.Join(work, "from-go-env")
			path, err := fakeGoBin(work, configured)
			if err != nil {
				return err
			}
			got, stderr, err := runGocacheSnippet(script, []string{"PATH=" + path, "TMPDIR=" + filepath.Join(work, "unused")})
			if err != nil {
				return err
			}
			if got != configured || !isDir(got) || stderr != "" {
				return fmt.Errorf("GOCACHE %q (dir %v) stderr %q, want the go env path %q, created, silent", got, isDir(got), stderr, configured)
			}
			return nil
		}},
		{"a_go_env_value_with_a_space_is_parsed_not_evaled", func(script, work string) error {
			configured := filepath.Join(work, "with space")
			path, err := fakeGoBin(work, configured)
			if err != nil {
				return err
			}
			got, _, err := runGocacheSnippet(script, []string{"PATH=" + path, "TMPDIR=" + filepath.Join(work, "u")})
			if err != nil {
				return err
			}
			if got != configured {
				return fmt.Errorf("GOCACHE %q, want %q", got, configured)
			}
			return nil
		}},
		{"inherited_gocache_wins_over_go_env", func(script, work string) error {
			inherited := filepath.Join(work, "caller-chosen-cache")
			path, err := fakeGoBin(work, filepath.Join(work, "from-go-env"))
			if err != nil {
				return err
			}
			got, _, err := runGocacheSnippet(script, []string{"PATH=" + path, "TMPDIR=" + filepath.Join(work, "unused"), "GOCACHE=" + inherited})
			if err != nil {
				return err
			}
			if got != inherited {
				return fmt.Errorf("GOCACHE %q, want the inherited %q", got, inherited)
			}
			return nil
		}},
		{"explicit_dev_health_go_cache_wins_over_go_env", func(script, work string) error {
			chosen := filepath.Join(work, "explicitly-chosen-cache")
			path, err := fakeGoBin(work, filepath.Join(work, "from-go-env"))
			if err != nil {
				return err
			}
			got, _, err := runGocacheSnippet(script, []string{"PATH=" + path, "TMPDIR=" + filepath.Join(work, "unused"), "DEV_HEALTH_GO_CACHE=" + chosen})
			if err != nil {
				return err
			}
			if got != chosen {
				return fmt.Errorf("GOCACHE %q, want the explicit %q", got, chosen)
			}
			return nil
		}},
		{"inherited_gocache_is_respected_not_overwritten", func(script, work string) error {
			inherited := filepath.Join(work, "caller-chosen-cache")
			got, _, err := runGocacheSnippet(script, []string{"PATH=" + withShims("/usr/bin:/bin"), "TMPDIR=" + filepath.Join(work, "unused-tmpdir"), "GOCACHE=" + inherited})
			if err != nil {
				return err
			}
			if got != inherited || !isDir(got) {
				return fmt.Errorf("an inherited GOCACHE must be respected and created: got %q (dir %v), want %q", got, isDir(got), inherited)
			}
			return nil
		}},
		{"explicit_dev_health_go_cache_wins_over_inherited_gocache", func(script, work string) error {
			chosen, inherited := filepath.Join(work, "explicitly-chosen-cache"), filepath.Join(work, "caller-chosen-cache")
			got, _, err := runGocacheSnippet(script, []string{"PATH=" + withShims("/usr/bin:/bin"), "TMPDIR=" + filepath.Join(work, "unused-tmpdir"), "GOCACHE=" + inherited, "DEV_HEALTH_GO_CACHE=" + chosen})
			if err != nil {
				return err
			}
			if got != chosen || !isDir(got) {
				return fmt.Errorf("an explicit DEV_HEALTH_GO_CACHE must win over an inherited GOCACHE: got %q (dir %v), want %q", got, isDir(got), chosen)
			}
			return nil
		}},
		{"explicit_dev_health_go_cache_wins_with_no_gocache_set", func(script, work string) error {
			chosen := filepath.Join(work, "explicitly-chosen-cache")
			got, _, err := runGocacheSnippet(script, []string{"PATH=" + withShims("/usr/bin:/bin"), "TMPDIR=" + filepath.Join(work, "unused-tmpdir"), "DEV_HEALTH_GO_CACHE=" + chosen})
			if err != nil {
				return err
			}
			if got != chosen || !isDir(got) {
				return fmt.Errorf("got %q (dir %v), want the explicit %q", got, isDir(got), chosen)
			}
			return nil
		}},
	}
}

func TestCheckGoGocachePrecedence(t *testing.T) {
	script := scriptPath(t, "ci/check_go.sh")
	scenarios := gocacheScenarios()
	// "go_reports_only_the_default_takes_tmp_fallback" is a hosted runner: go prints nothing for -changed, the tmp
	// fallback is taken, as before.
	assertAllPass(t, script, scenarios)
	assertPlantsCaught(t, script, scenarios, []plant{
		p("inherited GOCACHE ignored", "${DEV_HEALTH_GO_CACHE:-${GOCACHE:-}}", "${DEV_HEALTH_GO_CACHE:-}",
			"snippet_matches_the_expected_precedence_shape", "CHAOS-5224 regressed"),
		p("inherited GOCACHE outranks an explicit DEV_HEALTH_GO_CACHE", "${DEV_HEALTH_GO_CACHE:-${GOCACHE:-}}", "${GOCACHE:-${DEV_HEALTH_GO_CACHE:-}} # ${DEV_HEALTH_GO_CACHE:-${GOCACHE:-}}",
			"explicit_dev_health_go_cache_wins_over_inherited_gocache", "must win over an inherited"),
		p("configured go env value not consulted", `go_changed_line="$(GOTOOLCHAIN=local go env -changed GOCACHE 2>/dev/null || true)"`, `go_changed_line=""`,
			"configured_go_env_path_wins_over_tmp_fallback_silently", "want the go env path"),
		p("value with a quote or space mis-parsed", `go_changed_value="${go_changed_line#GOCACHE=\'}"`, `go_changed_value="${go_changed_line#GOCACHE=}"`,
			"a_go_env_value_with_a_space_is_parsed_not_evaled", "want"),
		p("cache directory not created", `mkdir -p "${DEV_HEALTH_GO_CACHE}"`, `true`,
			"inherited_gocache_is_respected_not_overwritten", "must be respected and created"),
		p("tmp fallback silent", `echo "check_go.sh: no Go build cache dir is configured`, `: "check_go.sh: no Go build cache dir is configured`,
			"all_empty_falls_back_to_tmpdir_default_and_says_so", "must be loud"),
	})
}

// ---- --help ---------------------------------------------------------------------------------------------------

func helpScenarios() []scenario {
	return []scenario{
		{"help_completes_and_documents_every_public_verb", func(script, work string) error {
			res := cmdSpec{dir: filepath.Dir(filepath.Dir(script)), env: inheritedEnv([]string{"CI"})}.run("bash", script, "--help")
			if res.code != 0 {
				return fmt.Errorf("--help exited %d: %s%s", res.code, res.stdout, res.stderr)
			}
			// A newer Go patch release may be fetched on a cold cache ("go: downloading go1.x ..."), and check_go.sh prints one
			// informational line when no cache dir is configured: environment noise. Anything else on stderr fails.
			for _, line := range strings.Split(res.stderr, "\n") {
				if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "go: downloading ") || strings.HasPrefix(line, "check_go.sh: no Go build cache dir is configured") {
					continue
				}
				return fmt.Errorf("unexpected stderr line %q", line)
			}
			documented := map[string]bool{}
			for _, line := range strings.Split(res.stdout, "\n") {
				if strings.HasPrefix(line, "  ") && strings.TrimSpace(line) != "" {
					documented[strings.Fields(line)[0]] = true
				}
			}
			var missing []string
			for _, v := range publicVerbs {
				if !documented[v] {
					missing = append(missing, v)
				}
			}
			if len(missing) > 0 {
				return fmt.Errorf("--help does not document %v", missing)
			}
			return nil
		}},
	}
}

func TestCheckGoHelpDocumentsEveryPublicVerb(t *testing.T) {
	t.Parallel()
	script := scriptPath(t, "ci/check_go.sh")
	assertAllPass(t, script, helpScenarios())
}

// ---- Ryuk env (executed against check_go.sh with a stand-in `go test`) --------------------------------------

func recordedGoTest(work, script string, env []string, verb ...string) ([]string, error) {
	lines, res, err := recordedGoTestRaw(work, script, env, verb...)
	if err != nil {
		return nil, err
	}
	if res.code != 0 {
		return nil, fmt.Errorf("check_go.sh %v exited %d: %s", verb, res.code, tail(res.stderr+res.stdout, 2000))
	}
	return lines, nil
}

// recordedGoTestRaw runs check_go.sh with a stand-in `go` that records every `go test` call (env value + arguments),
// delegates every other subcommand to the real go, and, when FAKE_PROOF is set, writes it to the multi-replica proof file.
func recordedGoTestRaw(work, script string, env []string, verb ...string) ([]string, cmdResult, error) {
	realGo, err := exec.LookPath("go")
	if err != nil {
		return nil, cmdResult{}, fmt.Errorf("go is required: ci/check_go.sh refuses to run without it")
	}
	bin := filepath.Join(work, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		return nil, cmdResult{}, err
	}
	fake := "#!/usr/bin/env bash\n# Records `go test` calls (env value + package args); delegates everything else.\n" +
		"if [ \"$1\" != \"test\" ]; then exec \"" + realGo + "\" \"$@\"; fi\n" +
		"printf '%s|%s\\n' \"${TESTCONTAINERS_RYUK_DISABLED-<unset>}\" \"$*\" >> \"${FAKE_GO_LOG}\"\n" +
		"if [ -n \"${FAKE_PROOF-}\" ] && [ -n \"${DEV_HEALTH_MULTI_REPLICA_PROOF-}\" ]; then printf '%s' \"${FAKE_PROOF}\" > \"${DEV_HEALTH_MULTI_REPLICA_PROOF}\"; fi\nexit 0\n"
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(fake), 0o755); err != nil {
		return nil, cmdResult{}, err
	}
	logPath := filepath.Join(work, "go_test.log")
	if err := os.WriteFile(logPath, nil, 0o644); err != nil {
		return nil, cmdResult{}, err
	}
	full := inheritedEnv([]string{"CI", "TESTCONTAINERS_RYUK_DISABLED", "PATH", "FAKE_GO_LOG", "FAKE_PROOF", "TMPDIR"},
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "FAKE_GO_LOG="+logPath, "TMPDIR="+work)
	full = append(full, env...)
	res := cmdSpec{dir: filepath.Dir(filepath.Dir(script)), env: full}.run("bash", append([]string{script}, verb...)...)
	raw, err := os.ReadFile(logPath)
	if err != nil {
		return nil, res, err
	}
	var lines []string
	for _, l := range strings.Split(string(raw), "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	return lines, res, nil
}

func tail(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

var (
	multiShardOnce sync.Once
	multiShardNum  string
	multiShardErr  error
)

// multiPackageShard reads the real plan for a `packages` shard that runs several packages in one `go test` (the Ryuk
// case). Shard numbers move whenever a package is isolated or split, so it is never hard-coded. The plan is read once
// per process from the real repository: a planted copy of the script changes how the shard is RUN, not which one it is.
func multiPackageShard(script string) (string, error) {
	multiShardOnce.Do(func() {
		real := filepath.Join(repoRootNoT(), "ci", "check_go.sh")
		res := cmdSpec{dir: filepath.Dir(filepath.Dir(real)), env: inheritedEnv(nil)}.run("bash", real, "integration-shard-plan")
		if res.code != 0 {
			multiShardErr = fmt.Errorf("integration-shard-plan exited %d: %s", res.code, tail(res.stderr+res.stdout, 2000))
			return
		}
		re := regexp.MustCompile(`(?m)^integration shard (\d+): estimated \d+s, (\d+) package\(s\)$`)
		for _, m := range re.FindAllStringSubmatch(res.stdout, -1) {
			if n, _ := strconv.Atoi(m[2]); n >= 2 {
				multiShardNum = m[1]
				return
			}
		}
		multiShardErr = fmt.Errorf("the plan has no integration shard with more than one package")
	})
	return multiShardNum, multiShardErr
}

func ryukValues(lines []string) map[string]bool {
	out := map[string]bool{}
	for _, l := range lines {
		out[strings.SplitN(l, "|", 2)[0]] = true
	}
	return out
}

func ryukScenarios() []scenario {
	shard := func(script, work string, env ...string) ([]string, error) {
		n, err := multiPackageShard(script)
		if err != nil {
			return nil, err
		}
		return recordedGoTest(work, script, env, "integration-shard", "packages", n)
	}
	only := func(lines []string, want string) error {
		if len(lines) == 0 {
			return fmt.Errorf("the stand-in go recorded no `go test` call: a measurement that did not happen")
		}
		v := ryukValues(lines)
		if len(v) != 1 || !v[want] {
			return fmt.Errorf("TESTCONTAINERS_RYUK_DISABLED values %v, want only %q (calls: %v)", v, want, lines)
		}
		return nil
	}
	return []scenario{
		{"shard_run_disables_ryuk_in_ci", func(script, work string) error {
			lines, err := shard(script, work, "CI=true")
			if err != nil {
				return err
			}
			if err := only(lines, "true"); err != nil {
				return err
			}
			for _, l := range lines {
				if !strings.Contains(l, "-tags=integration") {
					return fmt.Errorf("a recorded go test lacks -tags=integration: %s", l)
				}
			}
			return nil
		}},
		{"shard_run_keeps_ryuk_outside_ci", func(script, work string) error {
			lines, err := shard(script, work)
			if err != nil {
				return err
			}
			return only(lines, "<unset>")
		}},
		{"a_caller_value_wins", func(script, work string) error {
			lines, err := shard(script, work, "CI=true", "TESTCONTAINERS_RYUK_DISABLED=false")
			if err != nil {
				return err
			}
			return only(lines, "false")
		}},
		{"the_integration_verb_disables_ryuk_in_ci", func(script, work string) error {
			lines, err := recordedGoTest(work, script, []string{"CI=true"}, "integration")
			if err != nil {
				return err
			}
			return only(lines, "true")
		}},
	}
}

func TestCheckGoRyukEnv(t *testing.T) {
	t.Parallel()
	script := scriptPath(t, "ci/check_go.sh")
	scenarios := ryukScenarios()
	assertAllPass(t, script, scenarios)
	assertPlantsCaught(t, script, scenarios, []plant{
		pc("CI run leaves the reaper on", "INTEGRATION_TEST_ENV=(TESTCONTAINERS_RYUK_DISABLED=true)", "INTEGRATION_TEST_ENV=()",
			"shard_run_disables_ryuk_in_ci", "want only"),
		pc("reaper disabled outside CI", `if [ "${CI:-}" = "true" ] && [ -z "${TESTCONTAINERS_RYUK_DISABLED+x}" ]; then`, `if [ -z "${TESTCONTAINERS_RYUK_DISABLED+x}" ]; then`,
			"shard_run_keeps_ryuk_outside_ci", "want only"),
		pc("a caller value overridden", `[ -z "${TESTCONTAINERS_RYUK_DISABLED+x}" ]`, `true`,
			"a_caller_value_wins", "want only"),
	})
}

// ---- multi-replica worker gate (executed) ------------------------------------------------------------------------

func multiReplicaScenarios() []scenario {
	run := func(script, work string, env ...string) ([]string, cmdResult, error) {
		return recordedGoTestRaw(work, script, env, "multi-replica-workers")
	}
	return []scenario{
		{"multi_replica_gate_runs_the_required_test", func(script, work string) error {
			lines, res, err := run(script, work, "FAKE_PROOF=3")
			if err != nil {
				return err
			}
			if res.code != 0 || !strings.Contains(res.stdout, "measured 3 terminal jobs") {
				return fmt.Errorf("exit %d stdout %q stderr %q, want exit 0 and 'measured 3 terminal jobs'", res.code, res.stdout, tail(res.stderr, 500))
			}
			if len(lines) != 1 {
				return fmt.Errorf("the gate made %d go test calls %v, want exactly 1", len(lines), lines)
			}
			for _, need := range []string{"-tags=integration", "-count=1", "-run ^TestExplicitQueueMultiReplicaClaimDrainRestart$", "./internal/workerservice"} {
				if !strings.Contains(lines[0], need) {
					return fmt.Errorf("the go test call %q lacks %q", lines[0], need)
				}
			}
			return nil
		}},
		{"multi_replica_gate_fails_on_zero_measured_jobs", func(script, work string) error {
			_, res, err := run(script, work, "FAKE_PROOF=0")
			if err != nil {
				return err
			}
			if res.code == 0 || !strings.Contains(res.stderr, "measured zero jobs") {
				return fmt.Errorf("exit %d stderr %q, want a failure that says 'measured zero jobs'", res.code, tail(res.stderr, 500))
			}
			return nil
		}},
		{"multi_replica_gate_fails_without_a_proof_file", func(script, work string) error {
			_, res, err := run(script, work)
			if err != nil {
				return err
			}
			if res.code == 0 || !strings.Contains(res.stderr, "no measured-job proof") {
				return fmt.Errorf("exit %d stderr %q, want a failure that says 'no measured-job proof'", res.code, tail(res.stderr, 500))
			}
			return nil
		}},
	}
}

// ---- integration vet positive control --------------------------------------------------------------------------

const brokenFixture = `//go:build integration

package positivecontrolscratch

// Deliberately invalid: a string literal cannot satisfy an int return. This file exists only for the duration of the test.
func Broken() int {
	return "not an int"
}
`

func TestIntegrationVetCatchesABrokenTaggedFile(t *testing.T) {
	t.Parallel()
	// The scratch package lives in its own module under t.TempDir: a directory inside the repository tree is walked by
	// other packages' source scanners (internal/httpguard failed on it in the same race leg) and by `./...` runs.
	scratch := t.TempDir()
	if err := os.WriteFile(filepath.Join(scratch, "go.mod"), []byte("module positivecontrolscratch\n\ngo 1.27\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scratch, "ok.go"), []byte("package positivecontrolscratch\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	fixture := filepath.Join(scratch, "broken_fixture.go")
	vet := func() cmdResult {
		return cmdSpec{dir: scratch, env: inheritedEnv([]string{"GOFLAGS", "GOWORK"}, "GOFLAGS=-mod=readonly", "GOWORK=off")}.run("go", "vet", "-tags=integration", "./...")
	}
	if err := os.WriteFile(fixture, []byte(brokenFixture), 0o644); err != nil {
		t.Fatal(err)
	}
	broken := vet()
	if broken.code == 0 {
		t.Fatalf("go vet -tags=integration exited 0 on a deliberately broken integration-tagged file: the tag is not reaching the compiler, "+
			"so a real compile error under //go:build integration would go uncaught by check_integration_vet\nstdout: %s\nstderr: %s", broken.stdout, broken.stderr)
	}
	if !strings.Contains(broken.stderr, "broken_fixture.go") || !strings.Contains(broken.stderr, "not an int") {
		t.Fatalf("go vet failed, but not on the injected error: confirm the failure is the planted one\nstdout: %s\nstderr: %s", broken.stdout, broken.stderr)
	}
	untagged := cmdSpec{dir: scratch, env: inheritedEnv([]string{"GOFLAGS", "GOWORK"}, "GOFLAGS=-mod=readonly", "GOWORK=off")}.run("go", "vet", "./...")
	if untagged.code != 0 {
		t.Fatalf("control: go vet WITHOUT the tag must not see the integration-tagged file, got exit %d\nstderr: %s", untagged.code, untagged.stderr)
	}
	fixed := strings.Replace(brokenFixture, `return "not an int"`, "return 0", 1)
	if err := os.WriteFile(fixture, []byte(fixed), 0o644); err != nil {
		t.Fatal(err)
	}
	if again := vet(); again.code != 0 {
		t.Fatalf("go vet -tags=integration still failed after the fixture's only error was fixed: this test's setup is broken, not the gate\nstdout: %s\nstderr: %s", again.stdout, again.stderr)
	}
}

// ---- integration-package inventory -------------------------------------------------------------------------------

var (
	coverageSummary = regexp.MustCompile(`(\d+) package\(s\) discovered, 0 denylisted, (\d+) will run`)
	// membership is named, not just counted: a bare count let CHAOS-4643's literal drift 31 -> 32 -> 33 unnoticed
	coverageMustRun = strings.Fields(`
internal/providersync
internal/jobs/investment
internal/goapiproof
internal/jobs/dimensionfold
internal/platform/config
internal/queryapi/analytics
internal/queryapi/featureflags
internal/goapicli/routing
internal/queryapi/explain
internal/api/audit
internal/apiservice/admin
internal/auth/ratelimitvalkey
`)
)

func manifestPackages(t *testing.T, dir string) []string {
	var pkgs []string
	for _, row := range dataRows(t, dir) {
		if row[0] != "shards" {
			pkgs = append(pkgs, row[0])
		}
	}
	return pkgs
}

// integrationCoverageDefects judges the discovery verb's stdout against the shard manifest's package rows. The count
// is derived, never pinned: adding an integration package edits ONE list (ci/go_integration_shards.d/).
func integrationCoverageDefects(stdout string, manifest []string) []string {
	var d []string
	if !strings.Contains(stdout, "  RUN  internal/providersync") {
		d = append(d, "the inventory does not run internal/providersync")
	}
	m := coverageSummary.FindStringSubmatch(stdout)
	if m == nil {
		return append(d, "no `N package(s) discovered, 0 denylisted, M will run` summary line: "+stdout)
	}
	discovered, _ := strconv.Atoi(m[1])
	willRun, _ := strconv.Atoi(m[2])
	if discovered != willRun || discovered != len(manifest) {
		d = append(d, fmt.Sprintf("discovered=%d will_run=%d manifest_rows=%d", discovered, willRun, len(manifest)))
	}
	for _, pkg := range coverageMustRun {
		if !strings.Contains(stdout, "  RUN  "+pkg+"\n") {
			d = append(d, "the inventory does not run "+pkg)
		}
	}
	if strings.Contains(stdout, "  SKIP internal/queryapi/analytics: ") {
		d = append(d, "internal/queryapi/analytics is skipped")
	}
	return d
}

func TestCheckGoIntegrationCoverage(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	// A large inventory must not deadlock before the first package is printed: a here-string whose producer fills a pipe
	// smaller than the inventory blocks forever. Running the real discovery verb exercises the real inventory size.
	res := cmdSpec{dir: root, env: inheritedEnv(nil)}.run("bash", filepath.Join(root, "ci", "check_go.sh"), "integration-coverage")
	if res.code != 0 {
		t.Fatalf("integration-coverage exited %d: %s%s", res.code, res.stdout, res.stderr)
	}
	manifest := manifestPackages(t, filepath.Join(root, "ci", "go_integration_shards.d"))
	if len(manifest) < 100 {
		t.Fatalf("manifest has %d package rows: the check measured nothing", len(manifest))
	}
	if d := integrationCoverageDefects(res.stdout, manifest); len(d) > 0 {
		t.Fatalf("integration inventory defects:\n  %s", strings.Join(d, "\n  "))
	}
	plants := []struct {
		name string
		edit func(stdout string, manifest []string) (string, []string)
		want string
	}{
		{"a package dropped from the inventory", func(o string, m []string) (string, []string) {
			return strings.Replace(o, "  RUN  internal/api/audit\n", "", 1), m
		}, "does not run internal/api/audit"},
		{"a manifest row the inventory does not run", func(o string, m []string) (string, []string) {
			return o, append(append([]string{}, m...), "internal/zzz/new")
		}, "manifest_rows="},
		{"a package skipped", func(o string, m []string) (string, []string) {
			return strings.Replace(o, "  RUN  internal/queryapi/analytics\n", "  SKIP internal/queryapi/analytics: planted\n", 1), m
		}, "internal/queryapi/analytics"},
		{"a denylisted package", func(o string, m []string) (string, []string) {
			return coverageSummary.ReplaceAllString(o, "$1 package(s) discovered, 1 denylisted, $2 will run"), m
		}, "no `N package(s) discovered"},
	}
	for _, pl := range plants {
		pl := pl
		t.Run("plant/"+pl.name, func(t *testing.T) {
			out, man := pl.edit(res.stdout, manifest)
			if out == res.stdout && len(man) == len(manifest) {
				t.Fatal("the plant changed nothing")
			}
			if joined := strings.Join(integrationCoverageDefects(out, man), "\n"); !strings.Contains(joined, pl.want) {
				t.Fatalf("planted defect %q not caught: defects %q, want one mentioning %q", pl.name, joined, pl.want)
			}
		})
	}
}

// ---- test-name discovery (executed against a scratch copy of the tracked tree) ---------------------------------

const sentinelTestName = "TestZzSentinelDiscoveryRow"

const sentinelTestSource = "//go:build integration\n\npackage providersync\n\nimport \"testing\"\n\nfunc " + sentinelTestName + "(t *testing.T) {}\n"

// withDiscoveryTree runs fn on ONE scratch copy of the tracked tree (built once per process, removed by TestMain with the
// shim directory) with the script under test at ci/check_go.sh and one integration test file named fileName in
// internal/providersync, so the shard planner meets a name only the scenario knows. The scenarios take turns; each one
// removes its file and restores the script before the next.
func withDiscoveryTree(script, fileName string, fn func(tree string) error) error {
	sharedTreeMu.Lock()
	defer sharedTreeMu.Unlock()
	tree, err := ensureSharedTree()
	if err != nil {
		return err
	}
	target := filepath.Join(tree, "ci", "check_go.sh")
	original, err := os.ReadFile(target)
	if err != nil {
		return err
	}
	planted, err := os.ReadFile(script)
	if err != nil {
		return err
	}
	if err := os.WriteFile(target, planted, 0o755); err != nil {
		return err
	}
	defer func() {
		if err := os.WriteFile(target, original, 0o755); err != nil {
			panic(err)
		}
	}()
	file := filepath.Join(tree, "internal", "providersync", fileName)
	if err := os.WriteFile(file, []byte(sentinelTestSource), 0o644); err != nil {
		return err
	}
	defer os.Remove(file)
	return fn(tree)
}

func discoveryScenarios() []scenario {
	return []scenario{
		{"a_new_test_name_is_planned_exactly_once", func(script, work string) error {
			return withDiscoveryTree(script, "zzsentinel_discovery_test.go", func(tree string) error {
				res := cmdSpec{dir: tree, env: inheritedEnv(nil)}.run("bash", filepath.Join(tree, "ci", "check_go.sh"), "integration-shard-plan")
				if res.code != 0 {
					return fmt.Errorf("integration-shard-plan exited %d: %s", res.code, tail(res.stderr+res.stdout, 1500))
				}
				rows := 0
				for _, l := range strings.Split(res.stdout, "\n") {
					if strings.HasPrefix(strings.TrimSpace(l), "PROVIDER-SHARD ") && strings.Contains(l, " "+sentinelTestName+" ") {
						rows++
					}
				}
				if rows != 1 {
					return fmt.Errorf("the planted test %s has %d PROVIDER-SHARD rows in the plan, want exactly 1", sentinelTestName, rows)
				}
				return nil
			})
		}},
		{"a_control_byte_in_a_test_file_name_is_refused", func(script, work string) error {
			return withDiscoveryTree(script, "zzsentinel\x1f_scan_test.go", func(tree string) error {
				res := cmdSpec{dir: tree, env: inheritedEnv(nil)}.run("bash", filepath.Join(tree, "ci", "check_go.sh"), "integration-shard-plan")
				if res.code != 2 || !strings.Contains(res.stderr, "zzsentinel") || !strings.Contains(res.stderr, "control byte") {
					return fmt.Errorf("exit %d stderr %q, want exit 2 naming the zzsentinel file and 'control byte'", res.code, tail(res.stderr, 500))
				}
				return nil
			})
		}},
	}
}

func TestCheckGoDiscoversEveryTestName(t *testing.T) {
	t.Parallel()
	script := scriptPath(t, "ci/check_go.sh")
	scenarios := discoveryScenarios()
	assertAllPass(t, script, scenarios)
	assertPlantsCaught(t, script, scenarios, []plant{
		p("the scan drops one test name", `printf "T\037%s\037%s\n", FILENAME, substr(s, 1, RLENGTH)`, `if (s !~ /^TestZz/) printf "T\037%s\037%s\n", FILENAME, substr(s, 1, RLENGTH)`,
			"a_new_test_name_is_planned_exactly_once", "has 0 PROVIDER-SHARD rows"),
		p("a control byte passes the name check", `*[[:cntrl:]]*) die "go list returned unsafe ${label} test filename with a control byte`, `*[[:cntrl:]][[:cntrl:]][[:cntrl:]]*) die "go list returned unsafe ${label} test filename with a control byte`,
			"a_control_byte_in_a_test_file_name_is_refused", "want exit 2"),
	})
}
