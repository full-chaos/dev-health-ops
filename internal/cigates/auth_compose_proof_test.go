package cigates_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func authProofRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

// The proof's compose files are read from ci/check_auth_compose.sh itself, so a file dropped from
// its compose() wrapper turns this test red.
func authProofComposeFiles(t *testing.T, root string) []string {
	t.Helper()
	script, err := os.ReadFile(filepath.Join(root, "ci", "check_auth_compose.sh"))
	if err != nil {
		t.Fatal(err)
	}
	line := regexp.MustCompile(`docker compose -p "\$\{PROJECT\}"([^\n]*) "\$@"`).FindSubmatch(script)
	if line == nil {
		t.Fatal("the proof's compose() wrapper moved")
	}
	var files []string
	for _, m := range regexp.MustCompile(`-f "\$\{(ROOT|SCRIPT_DIR)\}/([^"]+)"`).FindAllStringSubmatch(string(line[1]), -1) {
		base := root
		if m[1] == "SCRIPT_DIR" {
			base = filepath.Join(root, "ci")
		}
		files = append(files, filepath.Join(base, m[2]))
	}
	return files
}

// A throwaway project name does not isolate a fixed container_name or a published host port, so
// the proof must not depend on either (CHAOS-10182).
func TestAuthComposeProofPostgresHasNoFixedNameOrPublishedPort(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker compose is not installed")
	}
	root := authProofRoot(t)
	args := []string{"compose", "-p", "authproofx"}
	for _, f := range authProofComposeFiles(t, root) {
		args = append(args, "-f", f)
	}
	args = append(args, "--profile", "auth", "config", "--format", "json")
	cmd := exec.Command("docker", args...)
	cmd.Env = append(os.Environ(), "BUGSINK_SECRET_KEY=x", "POSTGRES_PASSWORD=x", "AUTH_RUNTIME_DATABASE_PASSWORD=x")
	out, err := cmd.Output()
	if err != nil {
		t.Skipf("docker compose config unavailable: %v", err)
	}
	var cfg struct {
		Services map[string]struct {
			ContainerName string            `json:"container_name"`
			Ports         []json.RawMessage `json:"ports"`
		} `json:"services"`
	}
	if err := json.Unmarshal(out, &cfg); err != nil {
		t.Fatal(err)
	}
	pg, ok := cfg.Services["postgres"]
	if !ok {
		t.Fatal("the proof's compose config has no postgres service")
	}
	if pg.ContainerName != "" {
		t.Errorf("postgres keeps the fixed container_name %q, which is host-global", pg.ContainerName)
	}
	if len(pg.Ports) != 0 {
		t.Errorf("postgres publishes %d host port(s), which collide with other stacks", len(pg.Ports))
	}
}

func TestAuthComposeProofJobRunsBesideADecoyPostgres(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(authProofRoot(t), ".github", "workflows", "go.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	start := strings.Index(text, "\n  go-auth-compose-proof:")
	end := strings.Index(text, "\n  graphql-wire-parity:")
	if start < 0 || end < start {
		t.Fatal("go-auth-compose-proof job not found in go.yml")
	}
	job := text[start:end]
	decoy := strings.Index(job, "--name postgres")
	proof := strings.Index(job, "bash ci/check_auth_compose.sh")
	if decoy < 0 || proof < 0 || decoy > proof {
		t.Fatal("the proof job must start a decoy container named postgres before the proof runs")
	}
	if !strings.Contains(job[proof:], "docker inspect") {
		t.Error("the decoy is not checked after the proof")
	}
}
