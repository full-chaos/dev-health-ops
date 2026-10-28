package workflows_test

import (
	"fmt"
	"strings"
	"testing"
)

// CHAOS-11684: the test and race legs run go test -json into a runner directory (GO_TEST_LEG_JSON_DIR); a step reduces it to
// package/test/action/elapsed and the leg uploads that, on merge_group and main runs only. The raw events carry test output and
// never leave the runner.

func goTestTimingsViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	steps := stepsOf(jobsOf(loadWf(t, root, "go-quality.yml"))["go-quality-leg"])
	legAt, reduceAt, uploadAt := -1, -1, -1
	var leg, reduce, upload yamlMap
	for i, s := range steps {
		switch {
		case strings.Contains(asString(s["run"]), "bash ci/check_go.sh ci-leg ${{ matrix.args }}"):
			legAt, leg = i, s
		case strings.Contains(asString(s["run"]), "go-test-leg-json"):
			reduceAt, reduce = i, s
		case strings.HasPrefix(asString(s["uses"]), "actions/upload-artifact@") && strings.Contains(asString(asMap(s["with"])["path"]), "go-test-timings"):
			uploadAt, upload = i, s
		}
	}
	if leg == nil || reduce == nil || upload == nil {
		return append(out, "go-quality.yml:go-quality-leg: the leg, the timings reduction and the timings upload steps must all exist")
	}
	if !(legAt < reduceAt && reduceAt < uploadAt) {
		add("go-quality.yml: the order must be leg, reduction, upload (got %d, %d, %d)", legAt, reduceAt, uploadAt)
	}
	if got := asString(asMap(leg["env"])["GO_TEST_LEG_JSON_DIR"]); got != "${{ runner.temp }}/go-test-leg-json" {
		add("go-quality.yml: the leg's GO_TEST_LEG_JSON_DIR is %q, want ${{ runner.temp }}/go-test-leg-json (a runner directory outside any uploaded path)", got)
	}
	for name, step := range map[string]yamlMap{"reduction": reduce, "upload": upload} {
		cond := asString(step["if"])
		for _, need := range []string{"always()", "steps.relevance.outputs.relevant == 'true'", "github.event_name != 'pull_request'", "startsWith(matrix.name, 'test-')", "startsWith(matrix.name, 'race-')"} {
			if !strings.Contains(cond, need) {
				add("go-quality.yml: the timings %s condition %q lacks %s", name, cond, need)
			}
		}
		if step["continue-on-error"] != true {
			add("go-quality.yml: the timings %s must be continue-on-error: it never decides the leg", name)
		}
	}
	script := asString(reduce["run"])
	if !strings.Contains(script, "[.Package, .Test, .Action, (.Elapsed // 0)] | @tsv") || strings.Contains(script, ".Output") {
		add("go-quality.yml: the reduction must emit package, test, action and elapsed only, never .Output")
	}
	if !pinnedUpload.MatchString(asString(upload["uses"])) {
		add("go-quality.yml: the timings upload is not pinned to a commit SHA: %q", asString(upload["uses"]))
	}
	with := asMap(upload["with"])
	if p := strings.TrimSuffix(strings.TrimSpace(asString(with["path"])), "/"); p != "go-test-timings" {
		add("go-quality.yml: the timings upload path %q is not go-test-timings/", asString(with["path"]))
	}
	if !strings.Contains(asString(with["name"]), "${{ matrix.name }}") {
		add("go-quality.yml: the timings artifact name %q does not name the leg", asString(with["name"]))
	}
	if r, ok := with["retention-days"].(int); !ok || r != 14 {
		add("go-quality.yml: the timings retention is %v, want 14 days", with["retention-days"])
	}
	return out
}

func TestLegsUploadOnlyReducedTimings(t *testing.T) {
	expectNoViolations(t, goTestTimingsViolations(t, rootOf(t)))
}

func TestGoTestTimingsGuardPlantedDefectsAreCaught(t *testing.T) {
	root := rootOf(t)
	const gq = ".github/workflows/go-quality.yml"
	for _, c := range []struct {
		name string
		m    mutation
		want string
	}{
		{"the raw events under the uploaded dir", mutation{gq, "GO_TEST_LEG_JSON_DIR: ${{ runner.temp }}/go-test-leg-json", "GO_TEST_LEG_JSON_DIR: go-test-json"}, "outside any uploaded path"},
		{"pull_request uploads too", mutation{gq, " && github.event_name != 'pull_request' && (startsWith(matrix.name, 'test-') || startsWith(matrix.name, 'race-'))\n        continue-on-error: true\n        # CHAOS-11684: a merge_group", " && (startsWith(matrix.name, 'test-') || startsWith(matrix.name, 'race-'))\n        continue-on-error: true\n        # CHAOS-11684: a merge_group"}, "lacks github.event_name != 'pull_request'"},
		{"the reduction keeps the output", mutation{gq, "(.Elapsed // 0)] | @tsv'", "(.Elapsed // 0), .Output] | @tsv'"}, "never .Output"},
		{"another retention", mutation{gq, "          path: go-test-timings/\n          retention-days: 14", "          path: go-test-timings/\n          retention-days: 90"}, "want 14 days"},
		{"one name for every leg", mutation{gq, "name: go-test-timings-${{ matrix.name }}", "name: go-test-timings"}, "does not name the leg"},
		{"the action by tag", mutation{gq, "uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7\n        with:\n          name: go-test-timings-", "uses: actions/upload-artifact@v4\n        with:\n          name: go-test-timings-"}, "not pinned"},
	} {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, goTestTimingsViolations(t, plantTree(t, root, []string{gq}, c.m)), c.want)
		})
	}
}
