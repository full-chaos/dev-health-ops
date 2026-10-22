package webproxyproof_test

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

// CHAOS-10230: the Web -> go-api -> auth-service proof starts go-api beside auth-service in its
// own compose project. go-api carries a fixed network subnet, host ports and dependencies with
// fixed container names; none may reach the host's namespace.
func TestAuthWebProxyProofStackHasNoHostGlobalName(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}
	if err := exec.Command("docker", "compose", "version").Run(); err != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("docker is installed but its compose plugin is not usable in CI: %v", err)
		}
		t.Skip("the docker compose plugin is not installed")
	}
	root := authProofRoot(t)
	script, err := os.ReadFile(filepath.Join(root, "ci", "check_auth_web_proxy_compose.sh"))
	if err != nil {
		t.Fatal(err)
	}
	line := regexp.MustCompile(`docker compose -p "\$\{PROJECT\}"([^\n]*) "\$@"`).FindSubmatch(script)
	if line == nil {
		t.Fatal("the proof's compose() wrapper moved")
	}
	args := []string{"compose", "-p", "authwebproofx"}
	for _, m := range regexp.MustCompile(`-f "\$\{(ROOT|SCRIPT_DIR)\}/([^"]+)"`).FindAllStringSubmatch(string(line[1]), -1) {
		base := root
		if m[1] == "SCRIPT_DIR" {
			base = filepath.Join(root, "ci")
		}
		args = append(args, "-f", filepath.Join(base, m[2]))
	}
	args = append(args, "--profile", "auth", "config", "--format", "json")
	cmd := exec.Command("docker", args...)
	cmd.Env = append(os.Environ(), "BUGSINK_SECRET_KEY=x", "POSTGRES_PASSWORD=x", "AUTH_RUNTIME_DATABASE_PASSWORD=x", "AUTH_SESSION_SUCCESSOR_KEY=x", "AUTH_PROOF_SMTP_PORT=2525")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("docker compose config failed: %v", err)
	}
	var cfg struct {
		Services map[string]struct {
			ContainerName string `json:"container_name"`
			Ports         []struct {
				Published string `json:"published"`
			} `json:"ports"`
			Networks    map[string]json.RawMessage `json:"networks"`
			DependsOn   map[string]json.RawMessage `json:"depends_on"`
			Environment map[string]*string         `json:"environment"`
		} `json:"services"`
	}
	if err := json.Unmarshal(out, &cfg); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	var walk func(string)
	walk = func(name string) {
		if seen[name] {
			return
		}
		seen[name] = true
		svc, ok := cfg.Services[name]
		if !ok {
			t.Fatalf("the proof's compose config has no %s service", name)
		}
		if svc.ContainerName != "" {
			t.Errorf("%s keeps the fixed container_name %q, which is host-global", name, svc.ContainerName)
		}
		for _, p := range svc.Ports {
			if p.Published != "" {
				t.Errorf("%s publishes the fixed host port %s", name, p.Published)
			}
		}
		if _, ok := svc.Networks["planes"]; ok {
			t.Errorf("%s joins the planes network, whose subnet is fixed and host-global", name)
		}
		for dep := range svc.DependsOn {
			walk(dep)
		}
	}
	walk("go-api")
	walk("auth-service")
	if cfg.Services["auth-service"].Environment["AUTH_SESSION_SUCCESSOR_KEY"] == nil {
		t.Error("auth-service has no AUTH_SESSION_SUCCESSOR_KEY, so its session routes are not mounted")
	}
	if _, ok := cfg.Services["go-api"].Networks["auth_plane"]; !ok {
		t.Error("go-api is not on auth_plane, so the proxy cannot reach auth-service")
	}
}

// The proxy flags are ON only inside the proof's own override; the default stack and the chart
// keep them OFF (chris: login does not switch to the control plane until social login works).
func TestAuthProxyFlagsAreOffOutsideTheProofOverrides(t *testing.T) {
	root := authProofRoot(t)
	on := regexp.MustCompile(`(?im)^\s*AUTH_PROXY_(SESSIONS|LOCAL_LOGIN|CREDENTIALS|SOCIAL|OIDC|SAML)\s*[:=]\s*["']?(true|1|on)\b`)
	for _, rel := range []string{"compose.yml", "ci/auth_compose_proof.override.yml", "deploy/helm/dev-health/values.yaml"} {
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		if m := on.Find(raw); m != nil {
			t.Errorf("%s turns an auth proxy flag on: %q", rel, strings.TrimSpace(string(m)))
		}
	}
	over, err := os.ReadFile(filepath.Join(root, "ci", "auth_webproof_compose.override.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if !on.Match(over) {
		t.Error("the proof override does not turn the proxy flags on, so the proof would not exercise the proxy")
	}
}

func TestAuthWebProxyProofJobIsWired(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(authProofRoot(t), ".github", "workflows", "go.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	start := strings.Index(text, "\n  go-auth-web-proxy-proof:")
	if start < 0 {
		t.Fatal("go-auth-web-proxy-proof job not found in go.yml")
	}
	job := text[start+1:]
	if end := regexp.MustCompile(`\n  [a-z0-9-]+:\n`).FindStringIndex(job[2:]); end != nil {
		job = job[:end[0]+2]
	}
	for _, want := range []string{"--name postgres", "bash ci/check_auth_web_proxy_compose.sh", "full-chaos/dev-health-web", "AUTH_PROOF_WEB_DIR"} {
		if !strings.Contains(job, want) {
			t.Errorf("go-auth-web-proxy-proof does not contain %q", want)
		}
	}
}
