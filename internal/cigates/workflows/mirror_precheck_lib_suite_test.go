package workflows_test

import (
	"fmt"
	"strings"
	"testing"
)

// CHAOS-11422: ci/mirror_precheck_lib_test.sh had no executing caller, and a change to the library or the suite did not
// even start the workflow whose precheck job sources the library.

const mirrorWf = ".github/workflows/mirror-test-images.yml"

var mirrorSuitePaths = []string{
	"ci/mirror_precheck_lib.sh",
	"ci/mirror_precheck_lib_test.sh",
	"ci/run_shell_tests.sh",
	"ci/suites.tsv",
}

// The publisher job and the push filter at the base commit 977adb6f95: the suite wiring must not change either.
const baseMirrorIf = "(github.event.pull_request.head.repo.full_name == github.repository || github.event_name != 'pull_request') && (github.event_name != 'pull_request' || needs.precheck.outputs.missing == 'true')"

var basePushPaths = []string{
	"internal/testsupport/containers/harness.go",
	"ci/check_go.sh",
	"tests/compatibility/river/compose.compatibility.yml",
	"ci/builder_images.tsv",
	"ci/builder_images.sh",
	".github/workflows/test.yml",
	".github/workflows/live-e2e.yml",
	".github/workflows/mirror-test-images.yml",
}

func mirrorSuiteDefects(doc yamlMap) []string {
	var out []string
	var pushList []string
	for _, p := range asList(asMap(asMap(doc["on"])["push"])["paths"]) {
		pushList = append(pushList, asString(p))
	}
	if strings.Join(pushList, "\n") != strings.Join(basePushPaths, "\n") {
		out = append(out, "push paths differ from the base list: "+strings.Join(pushList, ", "))
	}
	if got := strings.Join(strings.Fields(asString(asMap(jobsOf(doc)["mirror"])["if"])), " "); got != baseMirrorIf {
		out = append(out, "the mirror publisher if differs from the base: "+got)
	}
	on := asMap(doc["on"])
	for _, event := range []string{"push", "pull_request"} {
		paths := map[string]bool{}
		for _, p := range asList(asMap(on[event])["paths"]) {
			paths[asString(p)] = true
		}
		for _, want := range mirrorSuitePaths {
			if event == "pull_request" && !paths[want] {
				out = append(out, event+" paths lack "+want)
			}
			// A push filter is workflow-wide: a suite-only push to main would start the publisher job.
			if event == "push" && paths[want] {
				out = append(out, "push paths must not list "+want+": the mirror publisher would run on a suite-only push")
			}
		}
	}
	job := asMap(jobsOf(doc)["precheck-lib-test"])
	if job == nil {
		return append(out, "job precheck-lib-test is missing")
	}
	if job["needs"] != nil {
		out = append(out, "job precheck-lib-test must not wait on another job")
	}
	if asString(job["runs-on"]) != "ubuntu-latest" {
		out = append(out, "job precheck-lib-test must run on ubuntu-latest")
	}
	if n, _ := job["timeout-minutes"].(int); n == 0 || n > 10 {
		out = append(out, "job precheck-lib-test needs a timeout-minutes of at most 10")
	}
	if perms := asMap(job["permissions"]); len(perms) != 1 || asString(perms["contents"]) != "read" {
		out = append(out, "job precheck-lib-test permissions must be exactly contents: read")
	}
	for _, s := range stepsOf(job) {
		if strings.Contains(fmt.Sprint(s), "secrets.") {
			out = append(out, "job precheck-lib-test must use no secret")
		}
	}
	ran := false
	for _, s := range stepsOf(job) {
		if strings.Contains(asString(s["run"]), "bash ci/run_shell_tests.sh mirror-precheck-lib") {
			ran = true
		}
	}
	if !ran {
		out = append(out, "job precheck-lib-test does not run `bash ci/run_shell_tests.sh mirror-precheck-lib`")
	}
	return out
}

func TestMirrorPrecheckLibSuiteRunsOnItsTrigger(t *testing.T) {
	root := rootOf(t)
	if d := mirrorSuiteDefects(loadWf(t, root, "mirror-test-images.yml")); len(d) > 0 {
		t.Fatalf("%s", strings.Join(d, "\n"))
	}
	for _, m := range []mutation{
		{mirrorWf, "      - ci/mirror_precheck_lib.sh\n", allMarker},
		{mirrorWf, "      - ci/mirror_precheck_lib_test.sh\n", allMarker},
		{mirrorWf, "bash ci/run_shell_tests.sh mirror-precheck-lib", "true"},
		{mirrorWf, "  precheck-lib-test:\n", "  precheck-lib-test:\n    needs: precheck\n"},
		{mirrorWf, "      (github.event_name != 'pull_request' || needs.precheck.outputs.missing == 'true')", "      (github.event_name != 'pull_request' || needs.precheck.outputs.missing != 'true')"},
		{mirrorWf, "  push:\n    branches: [main]\n    paths:\n", "  push:\n    branches: [main]\n    paths:\n      - ci/suites.tsv\n"},
		{mirrorWf, "    timeout-minutes: 5\n    permissions:\n      contents: read\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n      - name: Run the precheck library suite", "    timeout-minutes: 5\n    permissions:\n      contents: read\n      packages: write\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n      - name: Run the precheck library suite"},
	} {
		tmp := plantTree(t, root, []string{mirrorWf}, m)
		if d := mirrorSuiteDefects(loadWf(t, tmp, "mirror-test-images.yml")); len(d) == 0 {
			t.Errorf("planted defect was not caught: %q -> %q", m.old, m.new)
		}
	}
}
