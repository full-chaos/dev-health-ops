package bigboycompose_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/dsnclass"
)

// A Go worker service of the bigboy compose file carries only the Postgres DSN names of its own process class.
// The class comes from the service's command: `worker --worker-group=<g>` (process <g> of
// deploy/go-workers/deployment.json), `reconciler`, `scheduler`, `stream-runner --profile=<p>` (process
// stream-<p>), and the one-shots named go-*-route-activate (the operator CLI). The allowed set per class is that
// manifest's secret_env filtered by the DSN name pattern (internal/testsupport/dsnclass). An empty value counts
// as absent. The file has no component-form Postgres variables, so only the `*_URI` names are measured.

type bigboyClassedService struct {
	name, class string
	env         map[string]string
}

func envOfBigboy(t *testing.T, name string, node yaml.Node) map[string]string {
	t.Helper()
	env := map[string]string{}
	switch node.Kind {
	case 0:
	case yaml.MappingNode:
		var raw map[string]*string
		if err := node.Decode(&raw); err != nil {
			t.Fatalf("%s: decode environment map: %v", name, err)
		}
		for key, value := range raw {
			if value != nil {
				env[key] = *value
			}
		}
	case yaml.SequenceNode:
		var list []string
		if err := node.Decode(&list); err != nil {
			t.Fatalf("%s: decode environment list: %v", name, err)
		}
		for _, entry := range list {
			if key, value, has := strings.Cut(entry, "="); has {
				env[key] = value
			}
		}
	default:
		t.Fatalf("%s: environment is neither a map nor a list", name)
	}
	return env
}

func hostEnvExample(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(opsRoot(t), ".env.example"))
	if err != nil {
		t.Fatalf("read .env.example: %v", err)
	}
	return raw
}

func bigboyClassedServices(t *testing.T, text []byte) []bigboyClassedService {
	t.Helper()
	var doc struct {
		Services map[string]struct {
			Command     yaml.Node `yaml:"command"`
			Environment yaml.Node `yaml:"environment"`
			EnvFile     yaml.Node `yaml:"env_file"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(text, &doc); err != nil {
		t.Fatalf("parse the bigboy compose file: %v", err)
	}
	var found []bigboyClassedService
	for name, service := range doc.Services {
		class := ""
		var command []string
		if service.Command.Kind == yaml.SequenceNode {
			if err := service.Command.Decode(&command); err != nil {
				t.Fatalf("%s: decode command: %v", name, err)
			}
		}
		switch {
		case len(command) > 0 && command[0] == "worker":
			for _, argument := range command {
				if group, ok := strings.CutPrefix(argument, "--worker-group="); ok {
					class = group
				}
			}
		case len(command) > 0 && (command[0] == "reconciler" || command[0] == "scheduler"):
			class = command[0]
		case len(command) > 0 && command[0] == "stream-runner":
			for _, argument := range command {
				if profile, ok := strings.CutPrefix(argument, "--profile="); ok {
					class = "stream-" + profile
				}
			}
		case strings.HasPrefix(name, "go-") && strings.HasSuffix(name, "-route-activate"):
			class = "operator"
		}
		if class == "" {
			continue
		}
		env := envOfBigboy(t, name, service.Environment)
		loads, err := dsnclass.LoadsHostEnvFile(service.EnvFile)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if loads {
			env = dsnclass.WithEnvFile(dsnclass.HostEnvFile(hostEnvExample(t)), env)
		}
		found = append(found, bigboyClassedService{name: name, class: class, env: env})
	}
	sort.Slice(found, func(a, b int) bool { return found[a].name < found[b].name })
	return found
}

func bigboyDSNFindings(t *testing.T, text []byte) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(opsRoot(t), "deploy", "go-workers", "deployment.json"))
	if err != nil {
		t.Fatalf("read the deployment manifest: %v", err)
	}
	classes, err := dsnclass.Classes(raw)
	if err != nil {
		t.Fatal(err)
	}
	covered := map[string]bool{}
	var findings []string
	for _, service := range bigboyClassedServices(t, text) {
		allowed, known := classes[service.class]
		if !known {
			findings = append(findings, fmt.Sprintf("%s: class %q is not a process of the deployment manifest: classify it", service.name, service.class))
			continue
		}
		covered[service.class] = true
		for _, name := range dsnclass.Foreign(dsnclass.Carried(service.env), allowed) {
			findings = append(findings, fmt.Sprintf("%s (class %s) carries %s, which is not a DSN of its class", service.name, service.class, name))
		}
	}
	// bigboy runs the sync-provider queue inside the sync group: no service of that class exists here.
	for _, class := range []string{"heavy", "ops", "sync", "reconciler", "scheduler", "stream-external", "stream-ingest", "stream-pagerduty", "operator"} {
		if !covered[class] {
			findings = append(findings, fmt.Sprintf("no bigboy service of class %s: the check measured nothing for it", class))
		}
	}
	return findings
}

func TestBigboyGoWorkersCarryOnlyTheirClassDSN(t *testing.T) {
	text, err := os.ReadFile(filepath.Join(toolsDir(t), composeName))
	if err != nil {
		t.Fatalf("read %s: %v", composeName, err)
	}
	for _, finding := range bigboyDSNFindings(t, text) {
		t.Error(finding)
	}
}

// plantBigboyEnv adds one environment entry to a service, in the form the service already uses.
func plantBigboyEnv(t *testing.T, text, service, name, value string) []byte {
	t.Helper()
	rows := strings.Split(text, "\n")
	inService := false
	for index, row := range rows {
		switch {
		case row == "  "+service+":":
			inService = true
		case inService && strings.HasPrefix(row, "    environment:"):
			entry := "      " + name + ": " + value
			if len(rows) > index+1 && strings.HasPrefix(rows[index+1], "    - ") {
				entry = "    - " + name + "=" + value
			}
			rows = append(rows[:index+1], append([]string{entry}, rows[index+1:]...)...)
			return []byte(strings.Join(rows, "\n"))
		case inService && len(row) > 2 && row[:2] == "  " && row[2] != ' ' && row[2] != '#':
			inService = false
		}
	}
	t.Fatalf("no environment found for service %s", service)
	return nil
}

func TestBigboyDSNClassSeesPlantedForeignNames(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(toolsDir(t), composeName))
	if err != nil {
		t.Fatalf("read %s: %v", composeName, err)
	}
	text := string(raw)
	cases := []struct{ name, service, env, value, want string }{
		{"river map form with a coordinator URI file", "go-worker-ops", "COORDINATOR_DATABASE_URI_FILE", "/run/secrets/c", "go-worker-ops (class ops) carries COORDINATOR_DATABASE_URI"},
		{"river list form with a coordinator URI file", "go-worker-heavy", "COORDINATOR_DATABASE_URI_FILE", "/run/secrets/c", "go-worker-heavy (class heavy) carries COORDINATOR_DATABASE_URI"},
		{"stream with a queue URI file", "go-stream-ingest", "WORKER_DATABASE_URI_FILE", "/run/secrets/q", "go-stream-ingest (class stream-ingest) carries WORKER_DATABASE_URI"},
		{"stream with a coordinator URI file", "go-stream-external", "COORDINATOR_DATABASE_URI_FILE", "/run/secrets/c", "go-stream-external (class stream-external) carries COORDINATOR_DATABASE_URI"},
		{"a new DSN-like name fails closed", "go-scheduler", "REPORTS_POSTGRES_URI", "postgresql://x@h/db", "go-scheduler (class scheduler) carries REPORTS_POSTGRES_URI"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var hit bool
			for _, finding := range bigboyDSNFindings(t, plantBigboyEnv(t, text, tc.service, tc.env, tc.value)) {
				if strings.HasPrefix(finding, tc.want) {
					hit = true
				}
			}
			if !hit {
				t.Fatalf("no finding %q for the planted entry", tc.want)
			}
		})
	}
	t.Run("the host env file reaches a worker with no blank", func(t *testing.T) {
		rows := strings.Split(text, "\n")
		inOps, removed := false, false
		var kept []string
		for _, row := range rows {
			switch {
			case row == "  go-worker-ops:":
				inOps = true
			case inOps && len(row) > 2 && row[:2] == "  " && row[2] != ' ':
				inOps = false
			}
			if inOps && row == "      DATABASE_URI: ''" {
				removed = true
				continue
			}
			kept = append(kept, row)
		}
		if !removed {
			t.Fatal("go-worker-ops carries no blank DATABASE_URI to remove")
		}
		for _, finding := range bigboyDSNFindings(t, []byte(strings.Join(kept, "\n"))) {
			if strings.HasPrefix(finding, "go-worker-ops (class ops) carries DATABASE_URI") {
				return
			}
		}
		t.Fatal("a worker that loads ./ops/.env with no blank DATABASE_URI was not refused")
	})
	before := map[string]bool{}
	for _, finding := range bigboyDSNFindings(t, []byte(text)) {
		before[finding] = true
	}
	for _, finding := range bigboyDSNFindings(t, plantBigboyEnv(t, text, "go-stream-pagerduty", "WORKER_DATABASE_URI_FILE", "''")) {
		if !before[finding] {
			t.Errorf("an empty value was refused: %s", finding)
		}
	}
}
