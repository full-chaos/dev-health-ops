package workflows_test

import (
	"fmt"
	"strings"
	"testing"
)

// The packages shards of go-storage-integration-shard run go test -json into a runner directory (GO_INTEGRATION_LEG_JSON_DIR);
// a step reduces it to package/test/action/elapsed and the job uploads that on every event. The raw events carry test output
// and never leave the runner; the other targets keep plain go test.

func integrationLegTimingsViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	steps := stepsOf(jobsOf(loadWf(t, root, "go.yml"))["go-storage-integration-shard"])
	runAt, reduceAt, uploadAt := -1, -1, -1
	var run, reduce, upload yamlMap
	for i, s := range steps {
		switch {
		case strings.Contains(asString(s["run"]), "bash ci/check_go.sh integration-shard "):
			runAt, run = i, s
		case strings.Contains(asString(s["run"]), "go-integration-leg-json"):
			reduceAt, reduce = i, s
		case strings.HasPrefix(asString(s["uses"]), "actions/upload-artifact@") && strings.Contains(asString(asMap(s["with"])["path"]), "go-test-timings"):
			uploadAt, upload = i, s
		}
	}
	if run == nil || reduce == nil || upload == nil {
		return append(out, "go.yml:go-storage-integration-shard: the shard run, the timings reduction and the timings upload steps must all exist")
	}
	if !(runAt < reduceAt && reduceAt < uploadAt) {
		add("go.yml: the order must be shard run, reduction, upload (got %d, %d, %d)", runAt, reduceAt, uploadAt)
	}
	if got := asString(asMap(run["env"])["GO_INTEGRATION_LEG_JSON_DIR"]); got != "${{ matrix.target == 'packages' && format('{0}/go-integration-leg-json', runner.temp) || '' }}" {
		add("go.yml: the shard run's GO_INTEGRATION_LEG_JSON_DIR is %q, want the packages target only, under runner.temp (outside any uploaded path)", got)
	}
	if _, ok := asMap(run["env"])["GO_TEST_LEG_JSON_DIR"]; ok {
		add("go.yml: the shard run sets GO_TEST_LEG_JSON_DIR (the test legs' variable); the shards use GO_INTEGRATION_LEG_JSON_DIR")
	}
	for name, step := range map[string]yamlMap{"reduction": reduce, "upload": upload} {
		if cond := asString(step["if"]); cond != "always() && matrix.target == 'packages'" {
			add("go.yml: the timings %s condition is %q, want always() && matrix.target == 'packages'", name, cond)
		}
		if step["continue-on-error"] != true {
			add("go.yml: the timings %s must be continue-on-error: it never decides the shard", name)
		}
	}
	script := asString(reduce["run"])
	if !strings.Contains(script, "[.Package, (.Test // \"\"), .Action, (.Elapsed // 0)] | @tsv") || strings.Contains(script, ".Output") {
		add("go.yml: the reduction must emit package, test, action and elapsed only, never .Output")
	}
	if !strings.Contains(script, "go-test-timings/integration-packages-${{ matrix.shard }}.tsv.gz") {
		add("go.yml: the reduction does not write go-test-timings/integration-packages-<shard>.tsv.gz")
	}
	if !pinnedUpload.MatchString(asString(upload["uses"])) {
		add("go.yml: the timings upload is not pinned to a commit SHA: %q", asString(upload["uses"]))
	}
	with := asMap(upload["with"])
	if p := strings.TrimSuffix(strings.TrimSpace(asString(with["path"])), "/"); p != "go-test-timings" {
		add("go.yml: the timings upload path %q is not go-test-timings/", asString(with["path"]))
	}
	if got := asString(with["name"]); got != "go-test-timings-integration-packages-${{ matrix.shard }}" {
		add("go.yml: the timings artifact name %q does not name the packages shard", got)
	}
	if r, ok := with["retention-days"].(int); !ok || r != 14 {
		add("go.yml: the timings retention is %v, want 14 days", with["retention-days"])
	}
	return out
}

func TestIntegrationPackagesShardsUploadOnlyReducedTimings(t *testing.T) {
	expectNoViolations(t, integrationLegTimingsViolations(t, rootOf(t)))
}

func TestIntegrationLegTimingsGuardPlantedDefectsAreCaught(t *testing.T) {
	root := rootOf(t)
	const gy = ".github/workflows/go.yml"
	for _, c := range []struct {
		name string
		m    mutation
		want string
	}{
		{"the raw events under the uploaded dir", mutation{gy, "format('{0}/go-integration-leg-json', runner.temp)", "'go-test-timings'"}, "outside any uploaded path"},
		{"every target keeps -json", mutation{gy, "GO_INTEGRATION_LEG_JSON_DIR: ${{ matrix.target == 'packages' && format('{0}/go-integration-leg-json', runner.temp) || '' }}", "GO_INTEGRATION_LEG_JSON_DIR: ${{ format('{0}/go-integration-leg-json', runner.temp) }}"}, "packages target only"},
		{"the reduction keeps the output", mutation{gy, "(.Elapsed // 0)] | @tsv' \"${f}\"\n          done | gzip -9 > \"go-test-timings/integration-packages-", "(.Elapsed // 0), .Output] | @tsv' \"${f}\"\n          done | gzip -9 > \"go-test-timings/integration-packages-"}, "never .Output"},
		{"the reduction runs only on success", mutation{gy, "      - name: Reduce the packages shard go test -json to per-test timings\n        if: always() && matrix.target == 'packages'", "      - name: Reduce the packages shard go test -json to per-test timings\n        if: matrix.target == 'packages'"}, "timings reduction condition"},
		{"the upload decides the shard", mutation{gy, "      - name: Upload the packages shard per-test timings\n        if: always() && matrix.target == 'packages'\n        continue-on-error: true\n", "      - name: Upload the packages shard per-test timings\n        if: always() && matrix.target == 'packages'\n"}, "never decides the shard"},
		{"another retention", mutation{gy, "          path: go-test-timings/\n          retention-days: 14", "          path: go-test-timings/\n          retention-days: 90"}, "want 14 days"},
		{"one name for every shard", mutation{gy, "name: go-test-timings-integration-packages-${{ matrix.shard }}", "name: go-test-timings-integration-packages"}, "does not name the packages shard"},
		{"the action by tag", mutation{gy, "uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7\n        with:\n          name: go-test-timings-integration-packages-", "uses: actions/upload-artifact@v4\n        with:\n          name: go-test-timings-integration-packages-"}, "not pinned"},
	} {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, integrationLegTimingsViolations(t, plantTree(t, root, []string{gy}, c.m)), c.want)
		})
	}
}
