package workflows_test

import (
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

func mirrorSuiteDefects(doc yamlMap) []string {
	var out []string
	on := asMap(doc["on"])
	for _, event := range []string{"push", "pull_request"} {
		paths := map[string]bool{}
		for _, p := range asList(asMap(on[event])["paths"]) {
			paths[asString(p)] = true
		}
		for _, want := range mirrorSuitePaths {
			if !paths[want] {
				out = append(out, event+" paths lack "+want)
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
	} {
		tmp := plantTree(t, root, []string{mirrorWf}, m)
		if d := mirrorSuiteDefects(loadWf(t, tmp, "mirror-test-images.yml")); len(d) == 0 {
			t.Errorf("planted defect was not caught: %q -> %q", m.old, m.new)
		}
	}
}
