package cigates_test

import (
	"fmt"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-10177: Go port of the YAML/text-shape tests of tests/tooling/test_go_quality_legs.py (CHAOS-6690, 8135):
// go-quality runs as parallel legs whose union is exactly `check_go.sh ci` (race in its sharded form), the matrix
// carries exactly the legs the script knows, the fan-in keeps the required context `go-quality` and judges the legs
// `unconditional`, and the static-only steps run only on the static leg. The tests that EXECUTE check_go.sh against a
// stand-in go, the weights table versus `go list`, the race-excluded test census and the -trimpath derivation are
// MOVED-TO-C and stay in Python until PR-C replaces them.

// the command must start a line of a step's run: an echo of the same words runs nothing
var legRunRe = regexp.MustCompile(`(?m)^\s*bash ci/check_go\.sh ci-leg \$\{\{ matrix\.args \}\}\s*$`)

var (
	ciCaseStep  = regexp.MustCompile(`(?m)^    ((?:check|plan)_\w+)$`)
	ciLegStage  = regexp.MustCompile(`ci_leg_stage (?:"[^"]*"|\S+) (\w+)`)
	staticSteps = []string{
		"Prove the migration matrix contract",
		"Check the gqlgen output",
		"Install pinned shellcheck",
		"Check the shellcheck pin",
		"Validate River compatibility harness",
		"Prepare base job contracts",
		"Check the migration matrix has not gone stale",
	}
)

// caseBlock mirrors the Python helper: from the first start to the first end after it.
func caseBlock(script, start, end string) (string, bool) {
	a := strings.Index(script, start)
	if a < 0 {
		return "", false
	}
	b := strings.Index(script[a:], end)
	if b < 0 {
		return "", false
	}
	return script[a : a+b], true
}

func qualityLegViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	script := shellCode(readFile(t, filepath.Join(root, "ci/check_go.sh")))

	ciBlock, ok1 := caseBlock(script, "\n  ci)\n", "\n  all)\n")
	legBlock, ok2 := caseBlock(script, "check_ci_leg() {", "\n}\n")
	if !ok1 || !ok2 {
		add("could not read the `ci` case or check_ci_leg of ci/check_go.sh")
	} else {
		var ciSteps, legSteps []string
		for _, m := range ciCaseStep.FindAllStringSubmatch(ciBlock, -1) {
			ciSteps = append(ciSteps, m[1])
		}
		for _, m := range ciLegStage.FindAllStringSubmatch(legBlock, -1) {
			s := m[1]
			if s == "check_race_shard" {
				s = "check_race"
			}
			legSteps = append(legSteps, s)
		}
		if len(ciSteps) == 0 {
			add("could not read the stages of the `ci` case")
		}
		sort.Strings(ciSteps)
		sorted := append([]string{}, legSteps...)
		sort.Strings(sorted)
		if !reflect.DeepEqual(sorted, ciSteps) {
			add("the legs' stages %v differ from `check_go.sh ci` %v", sorted, ciSteps)
		}
		seen := map[string]bool{}
		for _, s := range legSteps {
			if seen[s] {
				add("stage %s runs in two legs", s)
			}
			seen[s] = true
		}
	}

	doc := loadWf(t, root, "go-quality.yml")
	jobs := jobsOf(doc)
	leg := asMap(jobs["go-quality-leg"])
	var args, names []string
	for _, e := range asList(asMap(asMap(leg["strategy"])["matrix"])["include"]) {
		args = append(args, asString(asMap(e)["args"]))
		names = append(names, asString(asMap(e)["name"]))
	}
	uniq := map[string]bool{}
	for _, n := range names {
		uniq[n] = true
	}
	if len(uniq) != len(names) {
		add("duplicate leg names in the go-quality matrix: %v", names)
	}
	var nonRace, races []string
	for _, a := range args {
		if strings.HasPrefix(a, "race ") {
			races = append(races, a)
		} else {
			nonRace = append(nonRace, a)
		}
	}
	sort.Strings(nonRace)
	sort.Strings(races)
	if !reflect.DeepEqual(nonRace, []string{"static", "test"}) {
		add("the non-race legs must be exactly static and test, got %v", nonRace)
	}
	want := make([]string, len(races))
	for k := range races {
		want[k] = fmt.Sprintf("race %d %d", k+1, len(races))
	}
	sort.Strings(want)
	if len(races) < 1 || !reflect.DeepEqual(races, want) {
		add("the race entries %v are not 1..N of N: a gap or a stale COUNT leaves packages unrun or run twice", races)
	}
	if b, ok := asMap(leg["strategy"])["fail-fast"].(bool); !ok || b {
		add("go-quality-leg strategy.fail-fast must be false")
	}
	if _, has := leg["if"]; has {
		add("a job-level `if` on the go-quality legs would let GitHub report them skipped; relevance is decided by a STEP inside each leg")
	}
	ran := false
	for _, s := range stepsOf(leg) {
		if legRunRe.MatchString(str(s["run"])) {
			ran = true
		}
	}
	if !ran {
		add("no go-quality-leg step runs `ci-leg ${{ matrix.args }}`")
	}
	const poolTimeout = "${{ (matrix.pool && vars.CI_RACE_RUNNER != '' && github.event_name == 'push' && github.ref == 'refs/heads/main' && github.repository == 'full-chaos/dev-health-ops') && 45 || 30 }}"
	if tm, ok := leg["timeout-minutes"].(int); !(ok && tm <= 30) && leg["timeout-minutes"] != poolTimeout {
		add("go-quality-leg timeout-minutes must be <= 30 (or the pool expression), got %v", leg["timeout-minutes"])
	}

	fan := asMap(jobs["go-quality"])
	if str(fan["name"]) != "go-quality" {
		add("the fan-in's required context name changed: %q", str(fan["name"]))
	}
	if strings.ReplaceAll(str(fan["if"]), " ", "") != "always()" {
		add("the fan-in must be always(): under any condition that lets it skip, a cancelled run leaves the REQUIRED check skipped (got %q)", str(fan["if"]))
	}
	if !reflect.DeepEqual(needsList(fan), []string{"go-quality-leg"}) {
		add("the fan-in must need exactly go-quality-leg, got %v", needsList(fan))
	}
	var env yamlMap
	for _, s := range stepsOf(fan) {
		if strings.Contains(str(s["run"]), "aggregate_gate_results") {
			env = asMap(s["env"])
			break
		}
	}
	if env == nil {
		add("the fan-in has no aggregate_gate_results step with an env block")
	} else {
		if str(env["GATE_NAME"]) != "go-quality" {
			add("fan-in GATE_NAME must be go-quality, got %q", str(env["GATE_NAME"]))
		}
		if str(env["GATE_HAS_SELECTOR"]) != "false" {
			add("fan-in GATE_HAS_SELECTOR must be false, got %q", str(env["GATE_HAS_SELECTOR"]))
		}
		if got := str(env["GATED_JOB_1"]); got != "go-quality-leg|unconditional|${{ needs.go-quality-leg.result }}" {
			add("the fan-in must judge the legs `unconditional` so a skipped, cancelled or missing leg fails the gate; got %q", got)
		}
		if _, has := env["GATED_JOB_2"]; has {
			add("the fan-in must not carry a GATED_JOB_2")
		}
	}

	for _, s := range stepsOf(leg) {
		name := str(s["name"])
		for _, prefix := range staticSteps {
			if strings.HasPrefix(name, prefix) && !strings.Contains(str(s["if"]), "matrix.name == 'static'") {
				add("static-only step %q must run only on the static leg (matrix.name == 'static'), if: %q", name, str(s["if"]))
			}
		}
	}

	if !strings.Contains(func() string { b, _ := caseBlock(script, "check_test() {", "\n}\n"); return b }(), "check_race_excluded_ran") {
		add("check_test no longer runs check_race_excluded_ran: tests compiled out of the race leg would run nowhere")
	}
	var goEnvOff string
	for _, line := range strings.Split(script, "\n") {
		if strings.HasPrefix(line, "GO_ENV_OFF=(") {
			goEnvOff = line
			break
		}
	}
	if !strings.Contains(goEnvOff, "-u GOFLAGS") {
		add("the GOFLAGS scrub (-u GOFLAGS in GO_ENV_OFF) must stay: -trimpath is an explicit flag, not an inherited one")
	}
	return out
}

func TestGoQualityLegs(t *testing.T) {
	expectNoViolations(t, qualityLegViolations(t, repoRoot(t)))
}

func TestGoQualityLegsPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const cg, wf = "ci/check_go.sh", ".github/workflows/go-quality.yml"
	files := []string{cg, wf}
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"a leg drops the ratchets stage", []mutation{{cg, "      ci_leg_stage ratchets check_ratchets\n", ""}}, []string{"differ from `check_go.sh ci`", "check_ratchets"}},
		{"a stage runs in two legs", []mutation{{cg, "      ci_leg_stage test check_test\n", "      ci_leg_stage test check_test\n      ci_leg_stage vet2 check_vet\n"}}, []string{"runs in two legs", "check_vet"}},
		{"a race entry goes missing from the matrix", []mutation{{wf, "args: race 2 3", "args: race 4 3"}}, []string{"not 1..N of N"}},
		{"fail-fast comes back on", []mutation{{wf, "fail-fast: false", "fail-fast: true"}}, []string{"fail-fast must be false"}},
		{"the fan-in can skip", []mutation{{wf, "\n    if: always()\n", "\n    if: success()\n"}}, []string{"fan-in must be always()"}},
		{"the pool timeout grows past 45", []mutation{{wf, "&& 45 || 30 }}", "&& 90 || 30 }}"}}, []string{"timeout-minutes must be <= 30"}},
		{"the fan-in stops judging the legs unconditional", []mutation{{wf, "go-quality-leg|unconditional|", "go-quality-leg|path-filtered|"}}, []string{"unconditional"}},
		{"the leg step echoes the command instead of running it", []mutation{{wf, "run: bash ci/check_go.sh ci-leg ${{ matrix.args }}", `run: echo "ci-leg ${{ matrix.args }}"`}}, []string{"no go-quality-leg step runs"}},
		{"a leg stage survives only in a comment", []mutation{{cg, "      ci_leg_stage ratchets check_ratchets\n", "      # ci_leg_stage ratchets check_ratchets\n"}}, []string{"differ from `check_go.sh ci`", "check_ratchets"}},
		{"check_test keeps the race-excluded proof only in a comment", []mutation{{cg, "\n  check_race_excluded_ran\n", "\n  # check_race_excluded_ran\n"}}, []string{"no longer runs check_race_excluded_ran"}},
		{"the GOFLAGS scrub is dropped", []mutation{{cg, "-u GOFLAGS ", ""}}, []string{"GOFLAGS scrub"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.muts...)
			expectViolation(t, qualityLegViolations(t, tree), c.want...)
		})
	}
}
