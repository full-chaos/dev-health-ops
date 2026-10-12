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
	if options.Domain.Name != " devhealth_domain " || options.Queue.Password != "from-file" {
		t.Fatalf("names are used EXACTLY as configured (never trimmed) and _FILE passwords resolved: %+v", options)
	}
	if options.Coordinator.Name != defaultCoordinatorRole {
		t.Fatalf("the coordinator role defaults to %q like `migrate river`, got %q", defaultCoordinatorRole, options.Coordinator.Name)
	}
	// A blank optional role name means "not set", not a role named "   ".
	blank, ok := rolesOptionsFromEnvironment(envLookup(map[string]string{
		"RIVER_DOMAIN_DATABASE_ROLE": "d", "RIVER_DOMAIN_DATABASE_PASSWORD": "x",
		"RIVER_QUEUE_DATABASE_ROLE": "q", "RIVER_QUEUE_DATABASE_PASSWORD": "x",
		"RIVER_COORDINATOR_DATABASE_PASSWORD": "x",
		"RIVER_KEDA_READONLY_DATABASE_ROLE":   "   ",
	}), &stderr)
	if !ok || blank.Keda.Name != "" {
		t.Fatalf("a blank optional role is unset: ok=%v %+v", ok, blank.Keda)
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
	for _, want := range []string{"RIVER_DOMAIN_DATABASE_PASSWORD", "QUERY_API_DATABASE_ROLE", "RIVER_KEDA_READONLY_PASSWORD", "RIVER_DATABASE_SCHEMA", "OTEL_PG_MONITOR_DATABASE_ROLE", "OTEL_PG_MONITOR_DATABASE_PASSWORD", "_FILE", "-check"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("help lacks %s:\n%s", want, stderr.String())
		}
	}
	if code := ExecuteRoles(nil, "dho", []string{"positional"}, envLookup(nil), &stdout, &stderr); code != 2 { //nolint:staticcheck
		t.Fatalf("a positional argument must be an argument error, got %d", code)
	}
}

// CHAOS-9369: the optional monitor login is read from OTEL_PG_MONITOR_DATABASE_ROLE /
// _PASSWORD, only when the role is named, and its password is required then.
func TestRolesOptionsFromEnvironmentMonitorRole(t *testing.T) {
	t.Parallel()
	base := map[string]string{
		"RIVER_DOMAIN_DATABASE_ROLE": "d", "RIVER_DOMAIN_DATABASE_PASSWORD": "x",
		"RIVER_QUEUE_DATABASE_ROLE": "q", "RIVER_QUEUE_DATABASE_PASSWORD": "x",
		"RIVER_COORDINATOR_DATABASE_PASSWORD": "x",
	}
	with := func(extra map[string]string) map[string]string {
		out := map[string]string{}
		for k, v := range base {
			out[k] = v
		}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}
	var stderr bytes.Buffer
	off, ok := rolesOptionsFromEnvironment(envLookup(base), &stderr)
	if !ok || off.Monitor.Name != "" || strings.Contains(strings.Join(roleLabels(off), ","), "monitor") {
		t.Fatalf("unset = off: ok=%v %+v", ok, off.Monitor)
	}
	blank, ok := rolesOptionsFromEnvironment(envLookup(with(map[string]string{"OTEL_PG_MONITOR_DATABASE_ROLE": "  "})), &stderr)
	if !ok || blank.Monitor.Name != "" {
		t.Fatalf("a blank monitor role is unset: ok=%v %+v", ok, blank.Monitor)
	}
	on, ok := rolesOptionsFromEnvironment(envLookup(with(map[string]string{
		"OTEL_PG_MONITOR_DATABASE_ROLE": "otel_monitor", "OTEL_PG_MONITOR_DATABASE_PASSWORD": "pw-mon"})), &stderr)
	if !ok || on.Monitor.Name != "otel_monitor" || on.Monitor.Password != "pw-mon" {
		t.Fatalf("monitor not read: ok=%v %+v", ok, on.Monitor)
	}
	if got := roleLabels(on); got[len(got)-1] != "monitor" {
		t.Fatalf("labels = %v", got)
	}
	if roleNames(on)["monitor"] != "otel_monitor" {
		t.Fatalf("roleNames must carry the monitor role (the migration-login distinctness check): %v", roleNames(on))
	}
	stderr.Reset()
	if _, ok := rolesOptionsFromEnvironment(envLookup(with(map[string]string{"OTEL_PG_MONITOR_DATABASE_ROLE": "otel_monitor"})), &stderr); ok ||
		!strings.Contains(stderr.String(), "OTEL_PG_MONITOR_DATABASE_PASSWORD") {
		t.Fatalf("a named monitor role without its password must be refused naming the variable: ok=%v %s", ok, stderr.String())
	}
}
