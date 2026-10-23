// Package dsnclass is the test-side rule that a Go worker process carries only the Postgres DSN names of its own
// class: the full `*_URI`/`*_URI_FILE` names and the component groups (DEV_HEALTH_PG_*_HOST) a compose service gives
// it. The expected set per class is read from deploy/go-workers/deployment.json secret_env, filtered by the same
// name pattern; callers hand in the file's bytes.
package dsnclass

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// IsDSN reports whether an environment variable name is a full DSN name: a `_URI` or `_URI_FILE` suffix and DATABASE or
// POSTGRES in the name. A new name that matches is refused until a class lists it.
func IsDSN(name string) bool {
	base := Base(name)
	if !strings.HasSuffix(base, "_URI") {
		return false
	}
	return strings.Contains(base, "DATABASE") || strings.Contains(base, "POSTGRES")
}

// Base maps NAME_FILE to NAME.
func Base(name string) string {
	return strings.TrimSuffix(name, "_FILE")
}

var componentGroups = []struct {
	dsn  string
	spec config.ComponentSpec
}{
	{"POSTGRES_URI", config.DomainDatabaseSpec},
	{"WORKER_DATABASE_URI", config.QueueDatabaseSpec},
	{"COORDINATOR_DATABASE_URI", config.CoordinatorDatabaseSpec},
	{"API_DATABASE_URI", config.APIDatabaseSpec},
	{"MIGRATION_DATABASE_URI", config.MigrationDatabaseSpec},
}

// Carried returns the sorted DSN names an environment hands to a process. An empty value counts as absent. A
// component group counts as its DSN name when its HOST value is non-empty.
func Carried(env map[string]string) []string {
	seen := map[string]bool{}
	for name, value := range env {
		if IsDSN(name) && strings.TrimSpace(value) != "" {
			seen[Base(name)] = true
		}
	}
	for _, group := range componentGroups {
		if strings.TrimSpace(env[group.spec.HostKey]) != "" {
			seen[group.dsn] = true
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Classes reads the manifest and returns, per process name, the DSN names its class may carry. The key "operator"
// is the operator CLI and "migration" the migration job.
func Classes(manifest []byte) (map[string]map[string]bool, error) {
	var doc struct {
		Processes []struct {
			Name      string   `json:"name"`
			SecretEnv []string `json:"secret_env"`
		} `json:"processes"`
		OperatorCLI struct {
			SecretEnv []string `json:"secret_env"`
		} `json:"operator_cli"`
		MigrationJob struct {
			SecretEnv []string `json:"secret_env"`
		} `json:"migration_job"`
	}
	if err := json.Unmarshal(manifest, &doc); err != nil {
		return nil, fmt.Errorf("parse deployment manifest: %w", err)
	}
	classes := map[string]map[string]bool{}
	add := func(key string, names []string) {
		set := map[string]bool{}
		for _, name := range names {
			if IsDSN(name) {
				set[Base(name)] = true
			}
		}
		classes[key] = set
	}
	for _, process := range doc.Processes {
		add(process.Name, process.SecretEnv)
	}
	add("operator", doc.OperatorCLI.SecretEnv)
	add("migration", doc.MigrationJob.SecretEnv)
	if len(doc.Processes) == 0 {
		return nil, fmt.Errorf("deployment manifest names no process: the expected sets would be empty")
	}
	return classes, nil
}

// HostEnvFileNames are DSN names the host's own env file (./ops/.env, a file the repository does not hold) may
// define; a service that loads it must set each of them in its environment, which wins over the file, unless its
// class allows the name.
var HostEnvFileNames = []string{"DATABASE_URI"}

// HostEnvFile returns what a service that loads the host env file can receive from it: the active assignments of the
// repository's .env.example plus a non-empty value for every name in HostEnvFileNames.
func HostEnvFile(example []byte) map[string]string {
	env := map[string]string{}
	for _, line := range strings.Split(string(example), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if key, value, ok := strings.Cut(line, "="); ok {
			env[strings.TrimSpace(key)] = strings.Trim(strings.TrimSpace(value), `"'`)
		}
	}
	for _, name := range HostEnvFileNames {
		env[name] = "defined-by-the-host-env-file"
	}
	return env
}

// WithEnvFile applies compose's precedence: the environment of a service wins over its env file, an empty value
// included.
func WithEnvFile(file, environment map[string]string) map[string]string {
	merged := map[string]string{}
	for name, value := range file {
		merged[name] = value
	}
	for name, value := range environment {
		merged[name] = value
	}
	return merged
}

// LoadsHostEnvFile reports whether a compose `env_file:` value (a list of paths or of {path, required} maps) names
// the host file ./ops/.env.
func LoadsHostEnvFile(node yaml.Node) (bool, error) {
	if node.Kind == 0 {
		return false, nil
	}
	var entries []yaml.Node
	if err := node.Decode(&entries); err != nil {
		return false, fmt.Errorf("decode env_file: %w", err)
	}
	for _, entry := range entries {
		path := entry.Value
		if entry.Kind == yaml.MappingNode {
			var item struct {
				Path string `yaml:"path"`
			}
			if err := entry.Decode(&item); err != nil {
				return false, fmt.Errorf("decode env_file entry: %w", err)
			}
			path = item.Path
		}
		if path == "./ops/.env" {
			return true, nil
		}
	}
	return false, nil
}

// Foreign returns the carried names that are not in the allowed set.
func Foreign(carried []string, allowed map[string]bool) []string {
	var out []string
	for _, name := range carried {
		if !allowed[name] {
			out = append(out, name)
		}
	}
	return out
}
