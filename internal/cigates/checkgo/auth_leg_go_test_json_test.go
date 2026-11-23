//go:build !race

package checkgo_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// CHAOS-14494: the auth legs run go test -json (when GO_TEST_LEG_JSON_DIR is set) so the workflow can turn it into a JUnit part,
// and rebuild the plain log from the events so the race-integration marker check reads the same lines as before.

func bashFunction(t *testing.T, name string) string {
	t.Helper()
	body, err := os.ReadFile(scriptPath(t, "ci/check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?ms)^` + regexp.QuoteMeta(name) + `\(\) \{\n.*?^\}\n`).Find(body)
	if m == nil {
		t.Fatalf("%s is not defined in ci/check_go.sh", name)
	}
	return string(m)
}

func legJQ(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile(scriptPath(t, "ci/check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?ms)^GO_TEST_LEG_JQ='\n.*?\n    \.out\)'\n`).Find(body)
	if m == nil {
		t.Fatal("GO_TEST_LEG_JQ is not defined in ci/check_go.sh")
	}
	return string(m)
}

const authLegFakeGo = `#!/usr/bin/env bash
if [ "$2" = "-json" ]; then
  printf '{"Action":"output","Package":"p","Test":"TestRaceX","Output":"    x_test.go:1: AUTH_RACE_EXECUTED TestRaceX\\n"}\n'
  printf '{"Action":"pass","Package":"p","Test":"TestRaceX","Elapsed":0.1}\n'
  printf '{"Action":"output","Package":"p","Output":"ok  \\tp\\t0.1s\\n"}\n'
  printf '{"Action":"pass","Package":"p","Elapsed":0.1}\n'
else
  echo "PLAIN-RUN $*"
fi
exit "${FAKE_RC:-0}"
`

func runAuthLegGoTest(t *testing.T, jsonFile string, rc string) (string, string, error) {
	t.Helper()
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is not installed")
	}
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(authLegFakeGo), 0o755); err != nil {
		t.Fatal(err)
	}
	script := "set -euo pipefail\nROOT=" + t.TempDir() + "\nGO_ENV_OFF=(env)\ndie() { echo \"$*\" >&2; exit 1; }\n" +
		legJQ(t) + bashFunction(t, "auth_go") + bashFunction(t, "auth_leg_go_test") +
		"auth_leg_go_test \"$1\" -mod=readonly -trimpath -count=1 ./p\n"
	cmd := exec.Command("bash", "-c", script, "x", jsonFile)
	cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "FAKE_RC="+rc)
	out, err := cmd.Output()
	return string(out), jsonFile, err
}

func TestAuthLegGoTestWritesJSONAndRebuildsThePlainLog(t *testing.T) {
	t.Parallel()
	jsonFile := filepath.Join(t.TempDir(), "auth-race-integration-1-of-4.json")
	out, _, err := runAuthLegGoTest(t, jsonFile, "0")
	if err != nil {
		t.Fatalf("a green call failed: %v", err)
	}
	if !strings.Contains(out, "AUTH_RACE_EXECUTED TestRaceX") || !strings.Contains(out, "ok  \tp\t0.1s") {
		t.Errorf("the plain log lost the marker or the ok line:\n%s", out)
	}
	raw, rerr := os.ReadFile(jsonFile)
	if rerr != nil || strings.Count(string(raw), `"Action"`) != 4 {
		t.Errorf("the leg's json file lacks the 4 events: %v\n%s", rerr, raw)
	}
}

func TestAuthLegGoTestKeepsGoTestsExitCode(t *testing.T) {
	t.Parallel()
	jsonFile := filepath.Join(t.TempDir(), "leg.json")
	if _, _, err := runAuthLegGoTest(t, jsonFile, "7"); err == nil {
		t.Fatal("a red go test call reported success: the tee or the converter hid its exit code")
	}
}

func TestAuthLegGoTestWithoutAJSONFileIsPlainGoTest(t *testing.T) {
	t.Parallel()
	out, _, err := runAuthLegGoTest(t, "/dev/null", "0")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "PLAIN-RUN test -mod=readonly -trimpath -count=1 ./p") || strings.Contains(out, "-json") {
		t.Errorf("without GO_TEST_LEG_JSON_DIR the call must be the old plain go test, got:\n%s", out)
	}
}

func TestEveryAuthLegGoTestCallGoesThroughTheJSONSeam(t *testing.T) {
	t.Parallel()
	leg := bashFunction(t, "check_auth_leg")
	if strings.Contains(leg, "auth_go test") {
		t.Errorf("check_auth_leg calls auth_go test directly: that call writes no go test -json")
	}
	if n := strings.Count(leg, `auth_leg_go_test "${json}"`); n != 2 {
		t.Errorf("want 2 go test call sites through auth_leg_go_test (the whole-package call and the race-integration call), got %d", n)
	}
	if !strings.Contains(leg, `leg_json_file "auth-${kind}" "${leg}" "${count}"`) {
		t.Errorf("check_auth_leg does not take its json file from leg_json_file")
	}
}
