package workflows_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The Go workers compose proof (ci/check_go_workers_compose.sh, .github/workflows/go-workers-compose-proof.yml)
// starts every Go worker service of compose.yml in a private project. No docker runs here: these tests read the
// script, the override, the workflow and compose.yml and require that the proof still names every worker service,
// drops every fixed container name and host port of the services it starts, publishes each worker's operator port,
// and that the env-name programs of the script (run with a fake docker) read names, not values, and count an empty
// value as absent.

const goWorkersProofScript = "ci/check_go_workers_compose.sh"

var goWorkersProofDataServices = []string{"postgres", "pgbouncer", "pgbouncer-river-queue", "pgbouncer-river-coordinator", "clickhouse", "valkey", "migrate"}

type goWorkersProofInputs struct{ script, override, compose, workflow string }

func readGoWorkersProofInputs(t *testing.T) goWorkersProofInputs {
	t.Helper()
	root := repoRoot(t)
	read := func(rel string) string {
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	return goWorkersProofInputs{
		script:   read(goWorkersProofScript),
		override: read("ci/go_workers_compose_proof.override.yml"),
		compose:  read("compose.yml"),
		workflow: read(".github/workflows/go-workers-compose-proof.yml"),
	}
}

func goWorkersProofViolations(t *testing.T, in goWorkersProofInputs) []string {
	t.Helper()
	var doc struct {
		Services map[string]struct {
			ContainerName string    `yaml:"container_name"`
			Labels        yaml.Node `yaml:"labels"`
			Ports         yaml.Node `yaml:"ports"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(in.compose), &doc); err != nil {
		t.Fatalf("parse compose.yml: %v", err)
	}
	var out []string
	// 1. the script names exactly the worker services of compose.yml.
	want := map[string]bool{}
	for name, service := range doc.Services {
		var labels map[string]string
		if service.Labels.Kind == yaml.MappingNode {
			_ = service.Labels.Decode(&labels)
		}
		group := labels["dev-health.io/worker-group"]
		if group == "reconciler" || group == "scheduler" || group == "" {
			continue
		}
		want[name] = true
	}
	got := map[string]bool{}
	for _, array := range regexp.MustCompile(`(?m)^(?:RIVER|STREAM)_WORKERS=\(([^)]*)\)`).FindAllStringSubmatch(in.script, -1) {
		for _, name := range strings.Fields(array[1]) {
			got[name] = true
		}
	}
	for name := range want {
		if !got[name] {
			out = append(out, "the proof script does not start worker service "+name)
		}
	}
	for name := range got {
		if !want[name] {
			out = append(out, "the proof script starts "+name+", which is not a worker service of compose.yml")
		}
	}
	// 2. every fixed name and host port of a service the proof starts is dropped by the override.
	for _, name := range goWorkersProofDataServices {
		service, ok := doc.Services[name]
		if !ok {
			out = append(out, "compose.yml has no service "+name+", which the proof starts")
			continue
		}
		if !strings.Contains(in.script, name) {
			out = append(out, "the proof script does not start "+name)
		}
		block := regexp.MustCompile(`(?m)^  ` + regexp.QuoteMeta(name) + `:\n((?:    .*\n)+)`).FindStringSubmatch(in.override + "\n")
		body := ""
		if block != nil {
			body = block[1]
		}
		if service.ContainerName != "" && !strings.Contains(body, "container_name: !reset null") {
			out = append(out, name+" keeps its fixed container_name "+service.ContainerName+": the override must reset it")
		}
		if service.Ports.Kind != 0 && !strings.Contains(body, "ports: !reset []") {
			out = append(out, name+" keeps its host ports: the override must reset them")
		}
	}
	// 3. every worker publishes its operator port on an ephemeral loopback port.
	for name := range want {
		block := regexp.MustCompile(`(?m)^  ` + regexp.QuoteMeta(name) + `:\n((?:    .*\n)+)`).FindStringSubmatch(in.override + "\n")
		if block == nil || !strings.Contains(block[1], `ports: !override ["127.0.0.1::8080"]`) {
			out = append(out, name+" does not publish its operator port 8080 on an ephemeral loopback port in the override")
		}
	}
	if !strings.Contains(in.compose, `http-addr: &flag-http-addr "--http-addr=:8080"`) {
		out = append(out, "the workers no longer listen on :8080: the proof reads that port")
	}
	// 4. the proof's compose() wrapper uses the override; the workflow runs the script and always tears down.
	if !regexp.MustCompile(`docker compose -p "\$\{PROJECT\}" -f "\$\{ROOT\}/compose\.yml" -f "\$\{SCRIPT_DIR\}/go_workers_compose_proof\.override\.yml" "\$@"`).MatchString(in.script) {
		out = append(out, "the proof's compose() wrapper does not use compose.yml with the override")
	}
	var workflow struct {
		Jobs map[string]struct {
			TimeoutMinutes int `yaml:"timeout-minutes"`
			Steps          []struct {
				If  string `yaml:"if"`
				Run string `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(in.workflow), &workflow); err != nil {
		t.Fatalf("parse the workflow: %v", err)
	}
	job, ok := workflow.Jobs["go-workers-compose-proof"]
	if !ok {
		return append(out, "the workflow has no job go-workers-compose-proof")
	}
	if job.TimeoutMinutes < 1 || job.TimeoutMinutes > 40 {
		out = append(out, "the job timeout must be 1..40 minutes")
	}
	var runs, tears bool
	for _, step := range job.Steps {
		if strings.TrimSpace(step.Run) == "bash "+goWorkersProofScript {
			runs = true
		}
		if strings.Contains(step.If, "always()") && strings.Contains(step.Run, "docker rm -f") {
			tears = true
		}
	}
	if !runs {
		out = append(out, "no step runs bash "+goWorkersProofScript)
	}
	if !tears {
		out = append(out, "no always() step removes what the proof leaves behind")
	}
	sort.Strings(out)
	return out
}

func TestGoWorkersComposeProofIsWiredToComposeYml(t *testing.T) {
	for _, violation := range goWorkersProofViolations(t, readGoWorkersProofInputs(t)) {
		t.Error(violation)
	}
}

func TestGoWorkersComposeProofPlantedDefects(t *testing.T) {
	base := readGoWorkersProofInputs(t)
	cases := []struct {
		name   string
		mutate func(in *goWorkersProofInputs)
		want   string
	}{
		{"a worker service the script does not start", func(in *goWorkersProofInputs) {
			in.script = strings.Replace(in.script, "go-stream-pagerduty)", ")", 1)
		}, "does not start worker service go-stream-pagerduty"},
		{"a fixed container name kept", func(in *goWorkersProofInputs) {
			in.override = strings.Replace(in.override, "  valkey:\n    container_name: !reset null\n", "  valkey:\n", 1)
		}, "valkey keeps its fixed container_name"},
		{"a host port kept", func(in *goWorkersProofInputs) {
			in.override = strings.Replace(in.override, "  clickhouse:\n    container_name: !reset null\n    ports: !reset []\n", "  clickhouse:\n    container_name: !reset null\n", 1)
		}, "clickhouse keeps its host ports"},
		{"a worker port not published", func(in *goWorkersProofInputs) {
			in.override = strings.Replace(in.override, "  go-stream-ingest:\n    ports: !override [\"127.0.0.1::8080\"]\n", "", 1)
		}, "go-stream-ingest does not publish"},
		{"no always() teardown", func(in *goWorkersProofInputs) {
			in.workflow = strings.Replace(in.workflow, "if: always() && ", "if: ", 1)
		}, "no always() step"},
		{"the override dropped from the wrapper", func(in *goWorkersProofInputs) {
			in.script = strings.Replace(in.script, ` -f "${SCRIPT_DIR}/go_workers_compose_proof.override.yml"`, "", 1)
		}, "wrapper does not use compose.yml with the override"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			planted := base
			tc.mutate(&planted)
			if planted == base {
				t.Fatal("the plant changed nothing")
			}
			for _, violation := range goWorkersProofViolations(t, planted) {
				if strings.Contains(violation, tc.want) {
					return
				}
			}
			t.Fatalf("no violation containing %q", tc.want)
		})
	}
}

// bashFunction returns the text of one top-level function of the script.
func bashFunction(t *testing.T, script, name string) string {
	t.Helper()
	match := regexp.MustCompile(`(?ms)^` + regexp.QuoteMeta(name) + `\(\) \{\n.*?^\}\n`).FindString(script)
	if match == "" {
		t.Fatalf("function %s not found in the script", name)
	}
	return match
}

// runScriptFunction runs one function of the script with a fake docker that answers `docker inspect` with a
// fixed document, and returns its stdout.
func runScriptFunction(t *testing.T, in goWorkersProofInputs, function, inspectJSON string, args ...string) string {
	t.Helper()
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is not installed")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "inspect.json"), []byte(inspectJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte("#!/usr/bin/env bash\ncat \"$(dirname \"$0\")/inspect.json\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	program := bashFunction(t, in.script, function) + "\n" + function + ` "$@"` + "\n"
	cmd := exec.Command("bash", append([]string{"-c", program, "fn"}, args...)...)
	cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"), "MANIFEST="+filepath.Join(repoRoot(t), "deploy", "go-workers", "deployment.json"))
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("%s: %v", function, err)
	}
	return string(out)
}

func TestGoWorkersComposeProofReadsDSNNamesNotValues(t *testing.T) {
	in := readGoWorkersProofInputs(t)
	const inspect = `[{"Config":{"Env":[
	  "PATH=/usr/bin",
	  "DEV_HEALTH_PG_DOMAIN_HOST=pgbouncer",
	  "DEV_HEALTH_PG_QUEUE_HOST=",
	  "DEV_HEALTH_PG_QUEUE_PORT=",
	  "DEV_HEALTH_PG_COORDINATOR_HOST=pgbouncer-river-coordinator",
	  "WORKER_DATABASE_URI_FILE=/run/secrets/queue",
	  "REPORTS_DATABASE_URI=",
	  "VALKEY_URI=redis://valkey:6379/1",
	  "POSTGRES_URI=secret-value-never-printed"
	]}}]`
	got := runScriptFunction(t, in, "carried_dsn_names", inspect, "container")
	want := "COORDINATOR_DATABASE_URI\nPOSTGRES_URI\nWORKER_DATABASE_URI\n"
	if got != want {
		t.Fatalf("carried_dsn_names = %q, want %q", got, want)
	}
	if strings.Contains(got, "secret-value") {
		t.Fatal("a value was printed")
	}
}

func TestGoWorkersComposeProofAllowedNamesComeFromTheManifest(t *testing.T) {
	in := readGoWorkersProofInputs(t)
	for class, want := range map[string]string{
		"heavy":           "POSTGRES_URI\nWORKER_DATABASE_URI\n",
		"sync-provider":   "POSTGRES_URI\nWORKER_DATABASE_URI\n",
		"stream-ingest":   "POSTGRES_URI\n",
		"stream-external": "POSTGRES_URI\n",
	} {
		if got := runScriptFunction(t, in, "allowed_dsn_names", "[]", class); got != want {
			t.Errorf("allowed_dsn_names %s = %q, want %q", class, got, want)
		}
	}
}
