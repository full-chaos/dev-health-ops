package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// CHAOS-8599. The compose stack starts the Go services with the environment
// and the command that compose.yml gives them. A part-set variable group (the
// coordinator PORT, USER and PASSWORD without HOST) made go-stream-external,
// go-stream-ingest and go-stream-pagerduty exit with configuration_error on
// every start since the stack became Go-only (PR #3747); no test read the
// environment of those services, only a real start showed it.
//
// This guard reads compose.yml (no docker, no `docker compose config`), builds
// the environment compose gives each service that runs a binary of this
// repository, and runs the production start path on it:
//
//   - A service started by `dho <service> ...` (api, query-api, worker,
//     reconciler, scheduler, stream-runner) goes through cli.Execute with the
//     real command tree, so the flag parser, the profile check and config.Load
//     are the ones the container runs. The context is already cancelled, so no
//     dependency is opened; the test fails when the process reports a
//     configuration_error or an argument error.
//   - A one-shot verb (migrate, workers, mint, contracts) has its own loader
//     per verb. For these the guard runs only the all-or-none component DSN
//     groups (config.ResolveDSN on every group config.Load checks). The rest
//     of what a verb validates is NOT covered; see verbServices.

// verbServices names every compose service that is a one-shot verb and why it
// is not run through its verb. A name here that is not a compose service
// fails TestComposeEnvVerbListNamesRealServices.
var verbServices = map[string]string{
	"migrate":                          "`dho migrate upgrade`: the verb opens the database at once (no dependency-free start path); only the DSN groups are checked",
	"go-river-provision":               "`dho migrate roles`: opens the database at once; only the DSN groups are checked",
	"go-river-migrate":                 "`dho migrate river`: opens the database at once; only the DSN groups are checked",
	"go-contractcheck":                 "`dho contracts validate`: reads contract files, no DSN; only the DSN groups are checked",
	"envelope-keys-init":               "`dho mint envelope-keys`: writes key files; only the DSN groups are checked",
	"go-sync-dispatch-route-activate":  "`dho workers routes apply`: opens the database at once; only the DSN groups are checked",
	"go-sync-finalize-route-activate":  "`dho workers routes apply`: opens the database at once; only the DSN groups are checked",
	"go-sync-post-route-activate":      "`dho workers routes apply`: opens the database at once; only the DSN groups are checked",
	"go-sync-reference-route-activate": "`dho workers routes apply`: opens the database at once; only the DSN groups are checked",
}

// dsnGroups are the component DSN groups config.Load checks all-or-none, with
// the raw URI key each one shadows (internal/platform/config dsnBindings and
// MigrationDatabaseSpec).
var dsnGroups = []struct {
	rawKey string
	spec   config.ComponentSpec
}{
	{"POSTGRES_URI", config.DomainDatabaseSpec},
	{"WORKER_DATABASE_URI", config.QueueDatabaseSpec},
	{"COORDINATOR_DATABASE_URI", config.CoordinatorDatabaseSpec},
	{"API_DATABASE_URI", config.APIDatabaseSpec},
	{"CLICKHOUSE_URI", config.ClickHouseSpec},
	{"API_CLICKHOUSE_URI", config.APIClickHouseSpec},
	{"MIGRATION_DATABASE_URI", config.MigrationDatabaseSpec},
}

var shellServices = map[string]bool{
	"api": true, "query-api": true, "worker": true,
	"reconciler": true, "scheduler": true, "stream-runner": true,
}

// goImage matches the images of this repository's Go binaries.
var goImage = regexp.MustCompile(`dev-health-go-[a-z]+`)

type composeEnvFile struct {
	Services map[string]struct {
		Image       string    `yaml:"image"`
		Command     yaml.Node `yaml:"command"`
		Environment yaml.Node `yaml:"environment"`
	} `yaml:"services"`
}

type composeGoService struct {
	name    string
	command []string
	env     map[string]string
}

var interpolation = regexp.MustCompile(`\$\$|\$\{([A-Za-z_][A-Za-z0-9_]*)(?:(:?[-?])([^}]*))?\}`)

// interpolate resolves ${VAR:-default} and ${VAR-default} to the default and
// ${VAR}, ${VAR:?msg} to a neutral non-empty placeholder: the host environment
// is never read.
func interpolate(text string) string {
	return interpolation.ReplaceAllStringFunc(text, func(match string) string {
		if match == "$$" {
			return "$"
		}
		parts := interpolation.FindStringSubmatch(match)
		if parts[2] == ":-" || parts[2] == "-" {
			return parts[3]
		}
		return "placeholder"
	})
}

func nodeStrings(t *testing.T, node yaml.Node) []string {
	t.Helper()
	if node.Kind == 0 {
		return nil
	}
	var list []string
	if err := node.Decode(&list); err != nil {
		t.Fatalf("a compose list is not a list of strings: %v", err)
	}
	return list
}

// environmentOf reads `environment:` in the map form and the list form.
func environmentOf(t *testing.T, node yaml.Node) map[string]string {
	t.Helper()
	env := map[string]string{}
	switch node.Kind {
	case 0:
	case yaml.MappingNode:
		var raw map[string]*string
		if err := node.Decode(&raw); err != nil {
			t.Fatalf("decode environment map: %v", err)
		}
		for key, value := range raw {
			if value == nil {
				continue // `KEY:` with no value is passed through from the host: unset here
			}
			env[key] = interpolate(*value)
		}
	case yaml.SequenceNode:
		for _, entry := range nodeStrings(t, node) {
			key, value, has := strings.Cut(entry, "=")
			if !has {
				continue // pass-through from the host: unset here
			}
			env[key] = interpolate(value)
		}
	default:
		t.Fatalf("environment is neither a map nor a list")
	}
	return env
}

func goServicesOf(t *testing.T, text []byte) []composeGoService {
	t.Helper()
	var file composeEnvFile
	if err := yaml.Unmarshal(text, &file); err != nil {
		t.Fatalf("parse compose file: %v", err)
	}
	var found []composeGoService
	for name, service := range file.Services {
		if !goImage.MatchString(service.Image) {
			continue
		}
		command := nodeStrings(t, service.Command)
		for index := range command {
			command[index] = interpolate(command[index])
		}
		found = append(found, composeGoService{
			name: name, command: command, env: environmentOf(t, service.Environment),
		})
	}
	sort.Slice(found, func(a, b int) bool { return found[a].name < found[b].name })
	return found
}

func readCompose(t *testing.T, relative string) []byte {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatalf("find the module root: %v", err)
	}
	text, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil {
		t.Fatalf("read %s: %v", relative, err)
	}
	return text
}

func lookupOf(env map[string]string) secrets.LookupEnv {
	return func(name string) (string, bool) {
		value, ok := env[name]
		return value, ok
	}
}

// syncBuffer is a buffer a background goroutine of the service under test may
// still write to (the dependency stage starts probes) after Execute returned.
type syncBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *syncBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Len()
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

// startFindings returns what the production start path refuses in the
// environment of one compose service; empty means it accepts it.
func startFindings(service composeGoService) []string {
	var findings []string
	for _, group := range dsnGroups {
		if _, _, err := config.ResolveDSN(lookupOf(service.env), group.rawKey, group.spec); err != nil {
			findings = append(findings, fmt.Sprintf("%s: DSN group %s: %v", service.name, group.rawKey, err))
		}
	}
	if len(service.command) == 0 || !shellServices[service.command[0]] {
		return findings
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // no dependency is opened: only the configuration is read
	var stdout, stderr syncBuffer
	done := make(chan int, 1)
	go func() {
		done <- cli.Execute(ctx, "dho", commands(), cli.Env{
			Args:   service.command,
			Lookup: lookupOf(service.env),
			Stdout: &stdout,
			Stderr: &stderr,
		})
	}()
	select {
	case code := <-done:
		out := stderr.String()
		if strings.Contains(out, "configuration_error") || strings.Contains(out, "argument error") || code == 2 {
			findings = append(findings, fmt.Sprintf("%s: `dho %s` exit %d: %s", service.name, service.command[0], code, strings.TrimSpace(out)))
		} else if stdout.Len() == 0 {
			// Rule: a measurement that did not happen must fail. The process
			// is expected to pass the configuration and stop at the
			// cancelled dependency stage. The JSON logger that writes to
			// stdout is built only after config.Load succeeded (a Load
			// error writes to stderr alone), so an empty stdout means the
			// start path did not get past the configuration.
			findings = append(findings, fmt.Sprintf("%s: `dho %s` exit %d without getting past the configuration: the guard measured nothing; stderr=%.300s", service.name, service.command[0], code, out))
		}
	case <-time.After(60 * time.Second):
		findings = append(findings, service.name+": the start path did not return in 60s; the guard measured nothing")
	}
	return findings
}

func guardFindings(t *testing.T, text []byte) []string {
	t.Helper()
	services := goServicesOf(t, text)
	if len(services) == 0 {
		t.Fatal("no Go service found in the compose file: the guard measured nothing")
	}
	var findings []string
	for _, service := range services {
		if len(service.command) == 0 {
			findings = append(findings, service.name+": a Go image with no command: classify it")
			continue
		}
		if _, verb := verbServices[service.name]; !verb && !shellServices[service.command[0]] {
			findings = append(findings, fmt.Sprintf("%s: command %q is neither a shell service nor in verbServices: classify it", service.name, service.command[0]))
			continue
		}
		findings = append(findings, startFindings(service)...)
	}
	return findings
}

func TestComposeGoServicesAcceptTheirEnvironment(t *testing.T) {
	for _, relative := range []string{"compose.yml"} {
		t.Run(relative, func(t *testing.T) {
			for _, finding := range guardFindings(t, readCompose(t, relative)) {
				t.Error(finding)
			}
		})
	}
}

func TestComposeEnvVerbListNamesRealServices(t *testing.T) {
	present := map[string]bool{}
	for _, service := range goServicesOf(t, readCompose(t, "compose.yml")) {
		present[service.name] = true
	}
	for name := range verbServices {
		if !present[name] {
			t.Errorf("verbServices names %q, which is not a Go service of compose.yml", name)
		}
	}
}

// TestComposeEnvGuardSeesPartSetCoordinatorGroup plants the CHAOS-8599 defect
// in the text of the checked-in file: go-stream-ingest gets HOST blanked while
// PORT, USER and PASSWORD stay (the shared base's values). The guard must name
// that service.
func TestComposeEnvGuardSeesPartSetCoordinatorGroup(t *testing.T) {
	text := string(readCompose(t, "compose.yml"))
	start := strings.Index(text, "  go-stream-ingest:")
	if start < 0 {
		t.Fatal("go-stream-ingest not found")
	}
	const merge = "<<: [*go-worker-no-coordinator, *go-worker-env-base]"
	at := strings.Index(text[start:], merge)
	if at < 0 {
		t.Fatal("the plant needs go-stream-ingest to blank its coordinator group through the merge: it has changed")
	}
	at += start
	planted := text[:at] + "<<: *go-worker-env-base\n      DEV_HEALTH_PG_COORDINATOR_HOST: \"\"" + text[at+len(merge):]
	var hit bool
	for _, finding := range guardFindings(t, []byte(planted)) {
		if strings.HasPrefix(finding, "go-stream-ingest:") {
			hit = true
		}
	}
	if !hit {
		t.Fatal("the guard did not see a part-set coordinator group on go-stream-ingest")
	}
}
