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
