package workflows_test

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-11422: ci/mirror_precheck_lib_test.sh had no executing caller, and a change to the library or the suite did not
// even start the workflow whose precheck job sources the library.

const mirrorWf = ".github/workflows/mirror-test-images.yml"

var mirrorSuitePaths = []string{
	"ci/mirror_precheck_lib.sh",
	"ci/mirror_precheck_lib_test.sh",
	"ci/verify_base_images_test.sh",
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
	"ci/base_images.tsv",
	"ci/verify_base_images.sh",
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
	if n, _ := job["timeout-minutes"].(int); n != 5 {
		out = append(out, "job precheck-lib-test needs a timeout-minutes of exactly 5")
	}
	if perms := asMap(job["permissions"]); len(perms) != 1 || asString(perms["contents"]) != "read" {
		out = append(out, "job precheck-lib-test permissions must be exactly contents: read")
	}
	dockerWord := regexp.MustCompile(`(^|[\s;&|(/'"\\])(docker|podman)(['"\s]|$)`)
	var jobKeys []string
	for k := range job {
		jobKeys = append(jobKeys, k)
	}
	sort.Strings(jobKeys)
	if strings.Join(jobKeys, ",") != "permissions,runs-on,steps,timeout-minutes" {
		out = append(out, "job precheck-lib-test job keys must be exactly permissions, runs-on, steps, timeout-minutes (no services, container or env): "+strings.Join(jobKeys, ", "))
	}
	for _, s := range stepsOf(job) {
		for k := range s {
			if k != "uses" && k != "name" && k != "run" {
				out = append(out, "job precheck-lib-test step keys must be uses, name or run only, got "+k)
			}
		}
		if dockerWord.MatchString(asString(s["run"])) {
			out = append(out, "job precheck-lib-test must run no docker command")
		}
		if u := strings.ToLower(asString(s["uses"])); strings.HasPrefix(u, "docker/") || strings.Contains(u, "login") {
			out = append(out, "job precheck-lib-test must use no docker or registry-login action")
		}
	}
	for _, s := range stepsOf(job) {
		if t := fmt.Sprint(s); strings.Contains(t, "secrets.") || strings.Contains(t, "secrets[") {
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
	const (
		pushLine = "  push:\n    branches: [main]\n    paths:\n"
		perms    = "    timeout-minutes: 5\n    permissions:\n      contents: read\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n      - name: Run the precheck library suite\n        run: bash ci/run_shell_tests.sh mirror-precheck-lib\n"
	)
	for _, c := range []struct {
		m    mutation
		want string
	}{
		{mutation{mirrorWf, "      - ci/mirror_precheck_lib.sh\n", allMarker}, "pull_request paths lack ci/mirror_precheck_lib.sh"},
		{mutation{mirrorWf, "      - ci/mirror_precheck_lib_test.sh\n", allMarker}, "pull_request paths lack ci/mirror_precheck_lib_test.sh"},
		{mutation{mirrorWf, "bash ci/run_shell_tests.sh mirror-precheck-lib", "true"}, "does not run"},
		{mutation{mirrorWf, "  precheck-lib-test:\n", "  precheck-lib-test:\n    needs: precheck\n"}, "must not wait on another job"},
		{mutation{mirrorWf, "(github.event_name != 'pull_request' || needs.precheck.outputs.missing == 'true')", "(github.event_name != 'pull_request' || needs.precheck.outputs.missing != 'true')"}, "publisher if differs from the base"},
		{mutation{mirrorWf, pushLine, pushLine + "      - ci/suites.tsv\n"}, "push paths must not list ci/suites.tsv"},
		{mutation{mirrorWf, perms, strings.Replace(perms, "contents: read\n", "contents: read\n      packages: write\n", 1)}, "permissions must be exactly contents: read"},
		{mutation{mirrorWf, perms, strings.Replace(perms, "timeout-minutes: 5", "timeout-minutes: 6", 1)}, "timeout-minutes of exactly 5"},
		{mutation{mirrorWf, perms, strings.Replace(perms, "timeout-minutes: 5", "timeout-minutes: 10", 1)}, "timeout-minutes of exactly 5"},
		{mutation{mirrorWf, perms, perms + "      - name: Pull\n        run: docker pull alpine\n"}, "must run no docker command"},
		{mutation{mirrorWf, perms, perms + "      - name: Compose\n        run: docker compose up -d\n"}, "must run no docker command"},
		{mutation{mirrorWf, perms, perms + "      - uses: docker/login-action@v3\n"}, "must use no docker or registry-login action"},
		{mutation{mirrorWf, perms, perms + "      - uses: some-org/registry-login@v1\n"}, "must use no docker or registry-login action"},
		{mutation{mirrorWf, perms, perms + "      - name: Quoted\n        run: |\n          \"docker\" pull alpine\n"}, "must run no docker command"},
		{mutation{mirrorWf, perms, perms + "      - name: Quoted single\n        run: |\n          'podman' pull alpine\n"}, "must run no docker command"},
		{mutation{mirrorWf, perms, perms + "      - name: Index secret\n        run: echo ${{ secrets['GHCR_TOKEN'] }}\n"}, "must use no secret"},
		{mutation{mirrorWf, perms, perms + "      - name: Pod\n        run: podman run alpine true\n"}, "must run no docker command"},
		{mutation{mirrorWf, perms, "    services:\n      pg:\n        image: postgres:18\n" + perms}, "job keys must be exactly"},
		{mutation{mirrorWf, perms, "    container: alpine\n" + perms}, "job keys must be exactly"},
		{mutation{mirrorWf, perms, "    env:\n      X: y\n" + perms}, "job keys must be exactly"},
		{mutation{mirrorWf, perms, perms + "      - name: Env step\n        env:\n          X: y\n        run: true\n"}, "step keys must be"},
		{mutation{mirrorWf, perms, perms + "      - env:\n          TOKEN: ${{ secrets.GHCR_TOKEN }}\n        run: true\n"}, "must use no secret"},
	} {
		tmp := plantTree(t, root, []string{mirrorWf}, c.m)
		d := strings.Join(mirrorSuiteDefects(loadWf(t, tmp, "mirror-test-images.yml")), "\n")
		if !strings.Contains(d, c.want) {
			t.Errorf("planted defect was not caught with %q: got %q", c.want, d)
		}
	}
}
