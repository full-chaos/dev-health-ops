package cigates_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The external auth corpus job (CHAOS-10160): TestAuthFlowVenueOracle driven through the base-URL runner against
// go-api (and, on leg acp, behind auth-service) brought up by a private compose project.

const corpusLegAcpReason = "leg acp: auth-service C1 PR2 (10228) and the C2-C5 route mounts are not on main yet; remove this line when they land"

func readRepoFile(t *testing.T, rel ...string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(append([]string{authProofRoot(t)}, rel...)...))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestAuthCorpusExternalJobIsInTheVenueWorkflow(t *testing.T) {
	text := readRepoFile(t, ".github", "workflows", "venue-oracles.yml")
	var workflow struct {
		Jobs map[string]struct {
			ContinueOnError any `yaml:"continue-on-error"`
			TimeoutMinutes  int `yaml:"timeout-minutes"`
			Strategy        struct {
				FailFast any `yaml:"fail-fast"`
				Matrix   struct {
					Leg []string `yaml:"leg"`
				} `yaml:"matrix"`
			} `yaml:"strategy"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(text), &workflow); err != nil {
		t.Fatal(err)
	}
	job, ok := workflow.Jobs["auth-corpus-external"]
	if !ok {
		t.Fatal("venue-oracles.yml has no auth-corpus-external job")
	}
	if got := strings.Join(job.Strategy.Matrix.Leg, ","); got != "go-api,acp" {
		t.Errorf("matrix legs = %q, want go-api,acp", got)
	}
	if failFast, ok := job.Strategy.FailFast.(bool); !ok || failFast {
		t.Error("strategy.fail-fast must be false: leg acp failing must not cancel leg go-api")
	}
	if job.TimeoutMinutes <= 0 || job.TimeoutMinutes > 60 {
		t.Errorf("timeout-minutes = %d, want 1..60", job.TimeoutMinutes)
	}
	if got, _ := job.ContinueOnError.(string); got != "${{ matrix.leg == 'acp' }}" {
		t.Errorf("continue-on-error = %v, want only leg acp to be allowed to fail", job.ContinueOnError)
	}
	start := strings.Index(text, "\n  auth-corpus-external:")
	if start < 0 {
		t.Fatal("job text not found")
	}
	body := text[start+1:]
	if !strings.Contains(body, corpusLegAcpReason) {
		t.Errorf("the job does not carry the named reason for leg acp: %q", corpusLegAcpReason)
	}
	if !strings.Contains(body, `bash ci/auth_corpus_external.sh "${{ matrix.leg }}"`) {
		t.Error("the job does not run ci/auth_corpus_external.sh with its leg")
	}
}

func runScript(t *testing.T, script string, env []string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command("bash", append([]string{filepath.Join(authProofRoot(t), "ci", script)}, args...)...)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err == nil {
		return string(out), 0
	}
	if exit, ok := err.(*exec.ExitError); ok {
		return string(out), exit.ExitCode()
	}
	t.Fatal(err)
	return "", -1
}

func TestAuthCorpusSetEnvScriptIsBoundedAndComposeOnly(t *testing.T) {
	path := filepath.Join(authProofRoot(t), "ci", "auth_corpus_set_env.sh")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Error("ci/auth_corpus_set_env.sh is not executable: the harness execs it by path")
	}
	text := readRepoFile(t, "ci", "auth_corpus_set_env.sh")
	for _, want := range []string{"set -euo pipefail", "docker compose -p", "--no-deps", "--force-recreate", "AUTH_PROXY_"} {
		if !strings.Contains(text, want) {
			t.Errorf("the script lacks %q", want)
		}
	}
	// Built from parts so this file does not itself spell the banned verbs.
	killers := regexp.MustCompile(`(?m)(^|[^A-Za-z_])(` + "pk" + `ill|` + "kill" + `all|` + "ki" + `ll)\b`)
	if killers.MatchString(text) {
		t.Error("the script stops a process: it may recreate go-api only through compose")
	}
	if !regexp.MustCompile(`(?m)deadline|timeout [0-9]+|seq 1 [0-9]+`).MatchString(text) {
		t.Error("the readiness wait is not bounded")
	}
	env := []string{"PATH=" + os.Getenv("PATH")}
	if out, code := runScript(t, "auth_corpus_set_env.sh", env, "X", "Y"); code == 0 || !strings.Contains(out, "AUTH_CORPUS_PROJECT") {
		t.Errorf("without AUTH_CORPUS_PROJECT: exit %d, output %q; want a refusal naming AUTH_CORPUS_PROJECT", code, out)
	}
	if out, code := runScript(t, "auth_corpus_set_env.sh", append(env, "AUTH_CORPUS_PROJECT=../x"), "X", "Y"); code == 0 || !strings.Contains(out, "AUTH_CORPUS_PROJECT") {
		t.Errorf("with a project name that is not authcorpus<hex>: exit %d, output %q; want a refusal", code, out)
	}
	if _, code := runScript(t, "auth_corpus_set_env.sh", append(env, "AUTH_CORPUS_PROJECT=authcorpus1a2b3c"), "ONLYNAME"); code != 2 {
		t.Errorf("one argument: exit %d, want 2 (usage)", code)
	}
}

// The override file is the only place proof settings live (compose.yml is not changed). Every service the
// go-api leg starts that compose.yml gives a host-global container_name or a fixed host port must be released
// from it, or a second stack on the runner collides.
func TestAuthCorpusOverrideReleasesHostGlobalNamesAndPorts(t *testing.T) {
	var compose struct {
		Services map[string]struct {
			ContainerName string `yaml:"container_name"`
			Ports         []any  `yaml:"ports"`
			DependsOn     any    `yaml:"depends_on"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(readRepoFile(t, "compose.yml")), &compose); err != nil {
		t.Fatal(err)
	}
	needed := map[string]bool{}
	var walk func(name string)
	walk = func(name string) {
		if needed[name] {
			return
		}
		needed[name] = true
		switch deps := compose.Services[name].DependsOn.(type) {
		case map[string]any:
			for dep := range deps {
				walk(dep)
			}
		case []any:
			for _, dep := range deps {
				walk(dep.(string))
			}
		}
	}
	walk("go-api")
	walk("auth-service")
	var override yaml.Node
	if err := yaml.Unmarshal([]byte(readRepoFile(t, "ci", "auth_corpus_external.override.yml")), &override); err != nil {
		t.Fatal(err)
	}
	services := map[string]*yaml.Node{}
	if len(override.Content) > 0 {
		root := override.Content[0]
		for i := 0; i+1 < len(root.Content); i += 2 {
			if root.Content[i].Value != "services" {
				continue
			}
			sv := root.Content[i+1]
			for j := 0; j+1 < len(sv.Content); j += 2 {
				services[sv.Content[j].Value] = sv.Content[j+1]
			}
		}
	}
	member := func(svc *yaml.Node, key string) *yaml.Node {
		if svc == nil {
			return nil
		}
		for i := 0; i+1 < len(svc.Content); i += 2 {
			if svc.Content[i].Value == key {
				return svc.Content[i+1]
			}
		}
		return nil
	}
	for name := range needed {
		spec := compose.Services[name]
		svc := services[name]
		if spec.ContainerName != "" {
			if node := member(svc, "container_name"); node == nil || node.Tag != "!reset" {
				t.Errorf("service %s keeps the host-global container_name %q: reset it in the override", name, spec.ContainerName)
			}
		}
		if len(spec.Ports) > 0 {
			node := member(svc, "ports")
			if node == nil {
				t.Errorf("service %s keeps its fixed host ports: reset or override them", name)
				continue
			}
			if node.Tag == "!reset" {
				continue
			}
			for _, entry := range node.Content {
				if !strings.HasPrefix(entry.Value, "127.0.0.1::") {
					t.Errorf("service %s publishes %q: only loopback ephemeral ports (127.0.0.1::PORT) are allowed", name, entry.Value)
				}
			}
		}
	}
}
