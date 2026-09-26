package rivermigrate

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func envLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) { value, ok := values[key]; return value, ok }
}

// CHAOS-6901: the roles verb reads the variables the provisioning script took,
// resolves passwords through the shared secret rules (env or _FILE), provisions an
// optional role only when its role is named, and never puts a password in output.
func TestRolesOptionsFromEnvironment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	file := filepath.Join(dir, "queue-pw")
	if err := os.WriteFile(file, []byte("from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	options, ok := rolesOptionsFromEnvironment(envLookup(map[string]string{
		"RIVER_DOMAIN_DATABASE_ROLE":          " devhealth_domain ",
		"RIVER_DOMAIN_DATABASE_PASSWORD":      "pw-domain",
		"RIVER_QUEUE_DATABASE_ROLE":           "devhealth_queue",
		"RIVER_QUEUE_DATABASE_PASSWORD_FILE":  file,
		"RIVER_COORDINATOR_DATABASE_PASSWORD": "pw-coordinator",
	}), &stderr)
	if !ok {
		t.Fatalf("unexpected refusal: %s", stderr.String())
	}
	if options.Domain.Name != "devhealth_domain" || options.Queue.Password != "from-file" {
		t.Fatalf("names are trimmed and _FILE passwords resolved: %+v", options)
	}
	if options.Coordinator.Name != defaultCoordinatorRole {
		t.Fatalf("the coordinator role defaults to %q like `migrate river`, got %q", defaultCoordinatorRole, options.Coordinator.Name)
	}
	if options.API.Name != "" || options.QueryAPI.Name != "" || options.Keda.Name != "" {
		t.Fatalf("an optional role is provisioned only when named: %+v", options)
	}
	if got := roleLabels(options); strings.Join(got, ",") != "domain,queue,coordinator" {
		t.Fatalf("labels = %v", got)
	}

	// Naming an optional role switches its block on and requires its password.
	stderr.Reset()
	if _, ok := rolesOptionsFromEnvironment(envLookup(map[string]string{
		"RIVER_DOMAIN_DATABASE_ROLE": "d", "RIVER_DOMAIN_DATABASE_PASSWORD": "x",
		"RIVER_QUEUE_DATABASE_ROLE": "q", "RIVER_QUEUE_DATABASE_PASSWORD": "x",
		"RIVER_COORDINATOR_DATABASE_PASSWORD": "x",
		"QUERY_API_DATABASE_ROLE":             "qa",
	}), &stderr); ok || !strings.Contains(stderr.String(), "QUERY_API_DATABASE_PASSWORD") {
		t.Fatalf("a named optional role without its password must be refused naming the variable: ok=%v %s", ok, stderr.String())
	}
	stderr.Reset()
	if _, ok := rolesOptionsFromEnvironment(envLookup(map[string]string{}), &stderr); ok ||
		!strings.Contains(stderr.String(), "RIVER_DOMAIN_DATABASE_ROLE is required") {
		t.Fatalf("a missing mandatory role must be refused: ok=%v %s", ok, stderr.String())
	}
}

func TestRolesVerbIsRegisteredUnderMigrate(t *testing.T) {
	t.Parallel()
	found := false
	for _, child := range Command().Children {
		if child.Name == "roles" {
			found = child.Run != nil
		}
	}
	if !found {
		t.Fatal("`dho migrate roles` is not registered")
	}
}

func TestRolesHelpNamesEveryVariableAndExitsZero(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if code := ExecuteRoles(nil, "dho", []string{"-h"}, envLookup(nil), &stdout, &stderr); code != 0 { //nolint:staticcheck // help needs no context
		t.Fatalf("help exit %d", code)
	}
	for _, want := range []string{"RIVER_DOMAIN_DATABASE_PASSWORD", "QUERY_API_DATABASE_ROLE", "RIVER_KEDA_READONLY_PASSWORD", "RIVER_DATABASE_SCHEMA", "_FILE", "-check"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("help lacks %s:\n%s", want, stderr.String())
		}
	}
	if code := ExecuteRoles(nil, "dho", []string{"positional"}, envLookup(nil), &stdout, &stderr); code != 2 { //nolint:staticcheck
		t.Fatalf("a positional argument must be an argument error, got %d", code)
	}
}
