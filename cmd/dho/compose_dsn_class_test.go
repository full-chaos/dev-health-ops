package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/dsnclass"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// A Go worker service of compose.yml carries only the Postgres DSN names of its own process class: the full
// `*_URI` names and the component groups (DEV_HEALTH_PG_{DOMAIN,QUEUE,COORDINATOR}_*). The class of a service is
// its dev-health.io/worker-group label (the process name of deploy/go-workers/deployment.json) or, for the
// route-activate one-shots, the `workers` verb (the operator CLI). The allowed set per class is that manifest's
// secret_env filtered by the DSN name pattern (internal/testsupport/dsnclass). A merge anchor is resolved by the
// YAML decoder; an empty value counts as absent.

const workerGroupLabel = "dev-health.io/worker-group"

type classedService struct {
	name  string
	class string
	env   map[string]string
}

type classedComposeFile struct {
	Services map[string]struct {
		Image       string    `yaml:"image"`
		Labels      yaml.Node `yaml:"labels"`
		Command     yaml.Node `yaml:"command"`
		Environment yaml.Node `yaml:"environment"`
		EnvFile     yaml.Node `yaml:"env_file"`
	} `yaml:"services"`
}

func labelsOf(t *testing.T, node yaml.Node) map[string]string {
	t.Helper()
	labels := map[string]string{}
	switch node.Kind {
	case 0:
	case yaml.MappingNode:
		if err := node.Decode(&labels); err != nil {
			t.Fatalf("decode labels map: %v", err)
		}
	case yaml.SequenceNode:
		for _, entry := range nodeStrings(t, node) {
			key, value, _ := strings.Cut(entry, "=")
			labels[key] = value
		}
	default:
		t.Fatalf("labels is neither a map nor a list")
	}
	return labels
}

func classedServicesOf(t *testing.T, text []byte) []classedService {
	t.Helper()
	var file classedComposeFile
	if err := yaml.Unmarshal(text, &file); err != nil {
		t.Fatalf("parse compose file: %v", err)
	}
	var found []classedService
	for name, service := range file.Services {
		if !goImage.MatchString(service.Image) {
			continue
		}
		class := labelsOf(t, service.Labels)[workerGroupLabel]
		if command := nodeStrings(t, service.Command); class == "" && len(command) > 0 && command[0] == "workers" {
			class = "operator"
		}
		if class == "" {
			continue
		}
		env := environmentOf(t, service.Environment)
		loads, err := dsnclass.LoadsHostEnvFile(service.EnvFile)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if loads {
			example, err := os.ReadFile(filepath.Join(moduleRootOrFatal(t), ".env.example"))
			if err != nil {
				t.Fatalf("read .env.example: %v", err)
			}
			env = dsnclass.WithEnvFile(dsnclass.HostEnvFile(example), env)
		}
		found = append(found, classedService{name: name, class: class, env: env})
	}
	sort.Slice(found, func(a, b int) bool { return found[a].name < found[b].name })
	return found
}

func moduleRootOrFatal(t *testing.T) string {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatalf("find the module root: %v", err)
	}
	return root
}

func readDeploymentManifest(t *testing.T) []byte {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatalf("find the module root: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "deploy", "go-workers", "deployment.json"))
	if err != nil {
		t.Fatalf("read the deployment manifest: %v", err)
	}
	return raw
}

func dsnClassFindings(t *testing.T, text []byte) []string {
	t.Helper()
	classes, err := dsnclass.Classes(readDeploymentManifest(t))
	if err != nil {
		t.Fatal(err)
	}
	services := classedServicesOf(t, text)
	covered := map[string]bool{}
	var findings []string
	for _, service := range services {
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
	for class := range classes {
		if class != "migration" && !covered[class] {
			findings = append(findings, fmt.Sprintf("no compose service of class %s: the check measured nothing for it", class))
		}
	}
	return findings
}

func TestComposeGoWorkersCarryOnlyTheirClassDSN(t *testing.T) {
	for _, finding := range dsnClassFindings(t, readCompose(t, "compose.yml")) {
		t.Error(finding)
	}
}

// plantEnv adds literal environment lines to one service of the checked-in file: after its `environment:` key, or
// as a new `environment:` key when the service inherits it from a template.
func plantEnv(t *testing.T, text, service string, lines ...string) []byte {
	t.Helper()
	rows := strings.Split(text, "\n")
	header := -1
	for index, row := range rows {
		if row == "  "+service+":" || strings.HasPrefix(row, "  "+service+": ") {
			header = index
			break
		}
	}
	if header < 0 {
		t.Fatalf("service %s not found", service)
	}
	planted := make([]string, 0, len(lines)+1)
	at := header + 1
	for index := header + 1; index < len(rows); index++ {
		row := rows[index]
		if len(row) > 2 && row[0] == ' ' && row[1] == ' ' && row[2] != ' ' && row[2] != '#' {
			break
		}
		if strings.HasPrefix(row, "    environment:") {
			at = index + 1
			goto insert
		}
	}
	planted = append(planted, "    environment:")
insert:
	for _, line := range lines {
		planted = append(planted, "      "+line)
	}
	rows = append(rows[:at], append(planted, rows[at:]...)...)
	return []byte(strings.Join(rows, "\n"))
}

func TestComposeDSNClassSeesPlantedForeignNames(t *testing.T) {
	text := string(readCompose(t, "compose.yml"))
	cases := []struct {
		name, service, want string
		lines               []string
	}{
		{"river with the coordinator group", "go-worker-ops", "go-worker-ops (class ops) carries COORDINATOR_DATABASE_URI", []string{`DEV_HEALTH_PG_COORDINATOR_HOST: pgbouncer-river-coordinator`}},
		{"river with a coordinator URI", "go-worker-sync", "go-worker-sync (class sync) carries COORDINATOR_DATABASE_URI", []string{`COORDINATOR_DATABASE_URI: postgresql://x@h/db`}},
		{"stream with the queue group", "go-stream-ingest", "go-stream-ingest (class stream-ingest) carries WORKER_DATABASE_URI", []string{`DEV_HEALTH_PG_QUEUE_HOST: pgbouncer-river-queue`}},
		{"stream with a queue URI file", "go-stream-pagerduty", "go-stream-pagerduty (class stream-pagerduty) carries WORKER_DATABASE_URI", []string{`WORKER_DATABASE_URI_FILE: /run/secrets/q`}},
		{"stream with the coordinator group", "go-stream-external", "go-stream-external (class stream-external) carries COORDINATOR_DATABASE_URI", []string{`DEV_HEALTH_PG_COORDINATOR_HOST: pgbouncer-river-coordinator`}},
		{"a new DSN-like name fails closed", "go-worker-heavy", "go-worker-heavy (class heavy) carries REPORTS_DATABASE_URI", []string{`REPORTS_DATABASE_URI: postgresql://x@h/db`}},
		{"operator with the api group", "go-sync-post-route-activate", "go-sync-post-route-activate (class operator) carries API_DATABASE_URI", []string{`DEV_HEALTH_PG_API_HOST: postgres`}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var hit bool
			for _, finding := range dsnClassFindings(t, plantEnv(t, text, tc.service, tc.lines...)) {
				if strings.HasPrefix(finding, tc.want) {
					hit = true
				}
			}
			if !hit {
				t.Fatalf("no finding %q for the planted line", tc.want)
			}
		})
	}
}

func TestComposeDSNClassAcceptsEmptyAndOwnClassNames(t *testing.T) {
	text := string(readCompose(t, "compose.yml"))
	before := map[string]bool{}
	for _, finding := range dsnClassFindings(t, []byte(text)) {
		before[finding] = true
	}
	for service, lines := range map[string][]string{
		"go-stream-ingest": {`WORKER_DATABASE_URI: ""`, `COORDINATOR_DATABASE_URI: ''`},
		"go-worker-ops":    {`WORKER_DATABASE_URI: postgresql://x@h/db`, `POSTGRES_URI_FILE: /run/secrets/d`},
	} {
		for _, finding := range dsnClassFindings(t, plantEnv(t, text, service, lines...)) {
			if !before[finding] {
				t.Errorf("an empty or own-class name was refused: %s", finding)
			}
		}
	}
}
