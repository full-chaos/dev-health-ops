package checkgo_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestIntegrationGoTestRunsThroughTheSessionReapHelper(t *testing.T) {
	text, err := os.ReadFile(scriptPath(t, "ci/check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(text)
	if got := strings.Count(s, `run_integration_go_test "${module_dir}" "${run_pkgs[@]}"`); got != 2 {
		t.Errorf("integration go test call sites through run_integration_go_test = %d; want 2", got)
	}
	start := strings.Index(s, "run_integration_go_test() {")
	if start < 0 {
		t.Fatal("run_integration_go_test is absent")
	}
	body := s[start:]
	body = body[:strings.Index(body, "\n}\n")]
	for _, want := range []string{
		`TESTCONTAINERS_SESSION_ID="${sid}"`,
		`INT TERM`,
		`reap-testcontainers-session.sh`,
		`trap - INT TERM`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("run_integration_go_test lacks %q", want)
		}
	}
}

func TestReapTestcontainersSessionRemovesOnlyThatSessionByID(t *testing.T) {
	script := scriptPath(t, "ci/reap-testcontainers-session.sh")
	if st, err := os.Stat(script); err != nil || st.Mode()&0o111 == 0 {
		t.Fatalf("%s must exist and be executable: %v", script, err)
	}
	if text, _ := os.ReadFile(script); strings.Contains(string(text), "docker system prune") || strings.Contains(string(text), "container prune") {
		t.Error("the reap script must never prune")
	}
	dir := t.TempDir()
	log := filepath.Join(dir, "docker.log")
	fake := "#!/bin/sh\necho \"$*\" >> " + log + "\n" +
		"if [ \"$1\" = ps ]; then printf 'aaa111\\nbbb222\\n'; fi\n"
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(fake), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", script, "sess-12345678")
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("reap failed: %v\n%s", err, out)
	}
	raw, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	want := "ps -aq --filter label=org.testcontainers.sessionId=sess-12345678\nrm -f aaa111\nrm -f bbb222\n"
	if string(raw) != want {
		t.Errorf("docker calls:\n%s\nwant:\n%s", raw, want)
	}

	bad := exec.Command("bash", script, "x;rm -rf")
	bad.Env = cmd.Env
	if err := bad.Run(); err == nil {
		t.Error("an unsafe session id must be refused")
	}
}

// helperHarness extracts tc_trap and run_integration_go_test from ci/check_go.sh and runs them against a stand-in
// `go` that records its start and a stand-in reaper that records the session id it is given.
func helperHarness(t *testing.T, sid string, signal bool) (exit int, marker, goStarted bool, reaped string) {
	t.Helper()
	text, err := os.ReadFile(scriptPath(t, "ci/check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(text)
	end := strings.Index(s, "run_integration_go_test() {")
	start := strings.Index(s, "tc_trap() {")
	if start < 0 {
		start = end
	}
	end += strings.Index(s[end:], "\n}\n") + 3
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	started := filepath.Join(dir, "go-started")
	reapLog := filepath.Join(dir, "reaped")
	markerPath := filepath.Join(dir, "marker")
	fakeGo := "#!/bin/sh\ntouch " + started + "\nexec sleep 30\n"
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(fakeGo), 0o755); err != nil {
		t.Fatal(err)
	}
	fakeReap := "#!/bin/sh\necho \"$1\" >> " + reapLog + "\n"
	if err := os.WriteFile(filepath.Join(dir, "reap-testcontainers-session.sh"), []byte(fakeReap), 0o755); err != nil {
		t.Fatal(err)
	}
	script := "set -uo pipefail\n" +
		"SCRIPT_DIR=" + dir + "\nROOT=" + dir + "\nGO_ENV_OFF=(env)\nINTEGRATION_TEST_ENV=()\n" +
		"die() { echo \"$*\" >&2; exit 2; }\n" + s[start:end] +
		"run_integration_go_test . ./x &\nHP=$!\n" +
		"if [ \"" + map[bool]string{true: "1", false: "0"}[signal] + "\" = 1 ]; then sleep 1; kill -TERM $HP; fi\n" +
		"wait $HP\n"
	// The helper must run in the foreground shell to own its trap: call it directly when no signal is sent.
	if !signal {
		script = "set -uo pipefail\nSCRIPT_DIR=" + dir + "\nROOT=" + dir + "\nGO_ENV_OFF=(env)\nINTEGRATION_TEST_ENV=()\n" +
			"die() { echo \"$*\" >&2; exit 2; }\n" + s[start:end] + "run_integration_go_test . ./x\n"
	}
	cmd := exec.Command("bash", "-c", script)
	cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"),
		"TESTCONTAINERS_SESSION_ID="+sid, "PROOF_MARKER="+markerPath)
	err = cmd.Run()
	exit = 0
	if ee, ok := err.(*exec.ExitError); ok {
		exit = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	_, e1 := os.Stat(markerPath)
	_, e2 := os.Stat(started)
	raw, _ := os.ReadFile(reapLog)
	return exit, e1 == nil, e2 == nil, strings.TrimSpace(string(raw))
}

func TestIntegrationHelperRefusesAnUnsafeSessionIDBeforeGoTestStarts(t *testing.T) {
	for _, sid := range []string{
		`$(touch "$PROOF_MARKER")`,
		"has space1234",
		"line\nbreak12345",
		"short",
		strings.Repeat("a", 65),
	} {
		exit, marker, goStarted, _ := helperHarness(t, sid, true)
		if exit == 0 || marker || goStarted {
			t.Errorf("sid %q: exit=%d marker=%v goStarted=%v; want a refusal before go test, no marker", sid, exit, marker, goStarted)
		}
	}
}

func TestIntegrationHelperTrapReapsTheSessionOnTERM(t *testing.T) {
	exit, marker, goStarted, reaped := helperHarness(t, "sess-abcdef12", true)
	if exit != 143 || marker || !goStarted || reaped != "sess-abcdef12" {
		t.Errorf("exit=%d marker=%v goStarted=%v reaped=%q; want 143, no marker, go started, reaped sess-abcdef12", exit, marker, goStarted, reaped)
	}
}
