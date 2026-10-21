package workflows_test

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// CHAOS-10498: the race legs' providersync slice is written as go test -json (check_providersync_race_shard, CHAOS-10481); each
// race leg uploads it as an artifact, after a failed leg too, so a timeout or a slow test can be read after the run.

var (
	pinnedUpload    = regexp.MustCompile(`^actions/upload-artifact@[0-9a-f]{40}( |$)`)
	raceJSONDirLine = regexp.MustCompile(`(?m)^\s*local json_dir="\$\{GO_TEST_JSON_DIR:-\$\{ROOT\}/([^"}]+)\}"`)
)

func raceJSONArtifactViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }

	script := readFile(t, filepath.Join(root, "ci/check_go.sh"))
	start := strings.Index(script, "check_providersync_race_shard() {")
	if start < 0 {
		return append(out, "ci/check_go.sh: no check_providersync_race_shard")
	}
	end := strings.Index(script[start:], "\n}\n")
	m := raceJSONDirLine.FindStringSubmatch(script[start : start+end])
	if m == nil {
		return append(out, "ci/check_go.sh: check_providersync_race_shard writes its go test -json to no ${GO_TEST_JSON_DIR:-${ROOT}/<dir>}")
	}
	dir := m[1]

	steps := stepsOf(jobsOf(loadWf(t, root, "go-quality.yml"))["go-quality-leg"])
	legAt, uploadAt := -1, -1
	var upload yamlMap
	for i, s := range steps {
		if strings.Contains(asString(s["run"]), "bash ci/check_go.sh ci-leg ${{ matrix.args }}") {
			legAt = i
		}
		if strings.HasPrefix(asString(s["uses"]), "actions/upload-artifact@") && strings.Contains(asString(asMap(s["with"])["path"]), dir) {
			uploadAt, upload = i, s
		}
	}
	if upload == nil {
		return append(out, fmt.Sprintf("go-quality.yml:go-quality-leg: no actions/upload-artifact step uploads %s/ (the race legs' go test -json)", dir))
	}
	if legAt < 0 || uploadAt < legAt {
		add("go-quality.yml: the go test -json upload must run after the leg")
	}
	if !pinnedUpload.MatchString(asString(upload["uses"])) {
		add("go-quality.yml: the go test -json upload is not pinned to a commit SHA: %q", asString(upload["uses"]))
	}
	cond := asString(upload["if"])
	for _, part := range []string{"always()", "steps.relevance.outputs.relevant == 'true'", "startsWith(matrix.name, 'race-')"} {
		if !strings.Contains(cond, part) {
			add("go-quality.yml: the go test -json upload condition %q does not hold %s", cond, part)
		}
	}
	with := asMap(upload["with"])
	if p := strings.TrimSuffix(strings.TrimSpace(asString(with["path"])), "/"); p != dir {
		add("go-quality.yml: the go test -json upload path %q is not %s/", asString(with["path"]), dir)
	}
	if !strings.Contains(asString(with["name"]), "${{ matrix.name }}") {
		add("go-quality.yml: the go test -json artifact name %q does not name the leg (three race legs would collide)", asString(with["name"]))
	}
	if r, ok := with["retention-days"].(int); !ok || r != 7 {
		add("go-quality.yml: the go test -json artifact retention is %v, want 7 days", with["retention-days"])
	}
	return out
}

func TestRaceLegsUploadTheirGoTestJSON(t *testing.T) {
	expectNoViolations(t, raceJSONArtifactViolations(t, rootOf(t)))
}

func TestRaceJSONArtifactGuardPlantedDefectsAreCaught(t *testing.T) {
	root := rootOf(t)
	const gq = ".github/workflows/go-quality.yml"
	files := []string{gq, "ci/check_go.sh"}
	for _, c := range []struct {
		name string
		m    mutation
		want string
	}{
		{"no upload step", mutation{gq, "          path: go-test-json/\n          retention-days: 7", "          path: other-dir/\n          retention-days: 7"}, "no actions/upload-artifact step uploads go-test-json/"},
		{"a failed leg does not upload", mutation{gq, "if: always() && steps.relevance.outputs.relevant == 'true' && startsWith(matrix.name, 'race-')", "if: steps.relevance.outputs.relevant == 'true' && startsWith(matrix.name, 'race-')"}, "always()"},
		{"every leg uploads", mutation{gq, " && startsWith(matrix.name, 'race-')\n        uses: actions/upload-artifact", "\n        uses: actions/upload-artifact"}, "startsWith(matrix.name, 'race-')"},
		{"the action by tag", mutation{gq, "uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7\n        with:\n          name: go-test-json-", "uses: actions/upload-artifact@v7\n        with:\n          name: go-test-json-"}, "not pinned to a commit SHA"},
		{"one name for three legs", mutation{gq, "name: go-test-json-${{ matrix.name }}", "name: go-test-json"}, "does not name the leg"},
		{"another retention", mutation{gq, "          path: go-test-json/\n          retention-days: 7", "          path: go-test-json/\n          retention-days: 90"}, "want 7 days"},
		{"check_go.sh writes elsewhere", mutation{"ci/check_go.sh", `local json_dir="${GO_TEST_JSON_DIR:-${ROOT}/go-test-json}" json run_rc=0`, `local json_dir="${GO_TEST_JSON_DIR:-${ROOT}/race-json}" json run_rc=0`}, "no actions/upload-artifact step uploads race-json/"},
	} {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, raceJSONArtifactViolations(t, plantTree(t, root, files, c.m)), c.want)
		})
	}
}
