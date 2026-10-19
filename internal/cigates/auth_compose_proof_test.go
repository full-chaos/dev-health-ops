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
		t.Skip("docker is not installed")
	}
	if err := exec.Command("docker", "compose", "version").Run(); err != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("docker is installed but its compose plugin is not usable in CI: %v", err)
		}
		t.Skip("the docker compose plugin is not installed")
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
		t.Fatalf("docker compose config failed: %v", err)
	}
	var cfg struct {
		Services map[string]struct {
			ContainerName string `json:"container_name"`
			Ports         []struct {
				Published string `json:"published"`
			} `json:"ports"`
		} `json:"services"`
	}
	if err := json.Unmarshal(out, &cfg); err != nil {
		t.Fatal(err)
	}
	if svc, ok := cfg.Services["auth-service"]; !ok {
		t.Fatal("the proof's compose config has no auth-service")
	} else {
		for _, p := range svc.Ports {
			if p.Published != "" {
				t.Errorf("auth-service publishes the fixed host port %s, which collides with any other listener", p.Published)
			}
		}
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
	after := job[proof:]
	if !strings.Contains(after, "docker inspect") || !strings.Contains(after, "StartedAt") {
		t.Error("the decoy is checked for running only, not for being the same untouched container")
	}
}

// A reused project name makes compose recreate or remove the earlier project's containers,
// volumes and networks, and cleanup removes volumes and networks too. The proof must refuse
// before touching anything when any of them exists, whether found by label or by name.
func TestAuthComposeProofRefusesAProjectThatAlreadyHasResources(t *testing.T) {
	root := authProofRoot(t)
	cases := []struct{ hit, match string }{
		{"label-container", "ps*label=com.docker.compose.project=*"},
		{"name-container", "ps*name=*"},
		{"label-volume", "volume?ls*label=com.docker.compose.project=*"},
		{"name-volume", "volume?ls*name=*"},
		{"label-network", "network?ls*label=com.docker.compose.project=*"},
		{"name-network", "network?ls*name=*"},
		{"none", ""},
	}
	for _, c := range cases {
		t.Run(c.hit, func(t *testing.T) {
			bin := t.TempDir()
			log := filepath.Join(bin, "docker.log")
			stub := "#!/bin/sh\necho \"$@\" >>\"" + log + "\"\n"
			if c.match != "" {
				stub += "case \"$*\" in " + c.match + ") echo deadbeef;; esac\n"
			}
			stub += "exit 0\n"
			if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(stub), 0o755); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("bash", filepath.Join(root, "ci", "check_auth_compose.sh"))
			cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "AUTH_COMPOSE_PROOF_PROJECT=authproofcollision")
			out, err := cmd.CombinedOutput()
			calls, _ := os.ReadFile(log)
			reached := strings.Contains(string(calls), " up ")
			if c.hit == "none" {
				if !reached {
					t.Fatalf("with nothing present the proof never reached compose up (the stub proves nothing):\n%s", out)
				}
				return
			}
			if err == nil {
				t.Fatalf("the proof ran although a project %s exists:\n%s", c.hit, out)
			}
			for _, line := range strings.Split(string(calls), "\n") {
				if strings.Contains(line, " up ") || strings.Contains(line, " down ") || strings.Contains(line, " rm ") {
					t.Errorf("the proof touched the project before refusing: %q", line)
				}
			}
		})
	}
}

// Every container the proof creates must be project-named and checked: an unnamed helper from
// `docker run --rm` is created and destroyed between the two snapshots and is never seen.
func TestAuthComposeProofNamesAndChecksEveryContainerItCreates(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(authProofRoot(t), "ci", "check_auth_compose.sh"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		if strings.Contains(line, "docker run") && !strings.Contains(line, "--name \"${PROJECT}") {
			t.Errorf("a docker run without a project-prefixed --name: %q", line)
		}
	}
	end := strings.Index(text, "AUTH COMPOSE PROOF PASSED")
	if end < 0 || !strings.Contains(text[:end], "assert_project_prefixed") || strings.Count(text[:end], "assert_project_prefixed") < 3 {
		t.Error("the prefix check is not run again at the end of the proof, after every helper container")
	}
}
