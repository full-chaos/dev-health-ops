//go:build integration

package devhealth_test

import (
	"bytes"
	"context"
	"os/exec"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/rivermigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// CHAOS-6951: the provision-roles hook Job, RENDERED by helm, is executed. Its args
// and the environment its pod would see (plain values, secretKeyRef values and the
// envFrom Secret, all read from the same render) are handed to the real
// `dho migrate roles`, on a database with the migrated application schema and a
// River schema (the chart's order: Alembic, then this hook). What is asserted is the
// state the hook exists to reach -- every configured login exists, authenticates
// with the password the chart's Secret carries, and the KEDA login reads river_job
// -- not that a manifest contains a string.
func TestRenderedProvisionRolesHookProvisionsEveryLoginItConfigures(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Fatal("helm is not installed: this test executes the RENDERED hook, so a missing helm must fail, not skip")
	}
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	for _, statement := range []string{"CREATE SCHEMA river", "CREATE TABLE river.river_job (id bigint)"} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	jobs, secrets, refusal := renderJobs(t,
		"migrations.hook.provisionRoles.enabled=true", "migrations.hook.riverMigrate.enabled=true",
		"migrations.hook.routeActivate.image="+pinnedOperatorImage,
		"migrations.hook.secretData.MIGRATION_DATABASE_URI="+instance.URI)
	if refusal != "" {
		t.Fatalf("render refused: %s", refusal)
	}
	job := jobs["t-dev-health-provision-roles"]
	if job == nil {
		t.Fatal("the hook did not render")
	}
	c := container(t, job, "provision-roles")
	args := stringsOf(c["args"])
	if len(args) < 2 || args[0] != "migrate" || args[1] != "roles" {
		t.Fatalf("args = %v, want [migrate roles]", args)
	}

	// The pod's environment, resolved from the render alone.
	environment := podEnvironment(t, c, secrets)
	if environment["MIGRATION_DATABASE_URI"] != instance.URI {
		t.Fatal("the elevated DSN did not reach the hook's environment")
	}
	// The chart's default values autoscale go-sync, so the KEDA login is part of the
	// hook; without it this test would not cover the KEDA leg it exists for.
	if environment["RIVER_KEDA_READONLY_DATABASE_ROLE"] == "" || environment["RIVER_KEDA_READONLY_PASSWORD"] == "" {
		t.Fatalf("the rendered hook does not provision the KEDA login: %v", environment)
	}
	lookup := func(key string) (string, bool) { value, ok := environment[key]; return value, ok }

	var stdout, stderr bytes.Buffer
	if code := rivermigrate.ExecuteRoles(ctx, "dho", args[2:], lookup, &stdout, &stderr); code != 0 {
		t.Fatalf("the rendered hook failed: exit %d\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	if code := rivermigrate.ExecuteRoles(ctx, "dho", append([]string{"--check"}, args[2:]...), lookup, &stdout, &stderr); code != 0 {
		t.Fatalf("--check after the hook: exit %d\n%s", code, stderr.String())
	}

	for _, entry := range []struct{ roleKey, passwordKey string }{
		{"RIVER_DOMAIN_DATABASE_ROLE", "RIVER_DOMAIN_DATABASE_PASSWORD"},
		{"RIVER_QUEUE_DATABASE_ROLE", "RIVER_QUEUE_DATABASE_PASSWORD"},
		{"RIVER_COORDINATOR_DATABASE_ROLE", "RIVER_COORDINATOR_DATABASE_PASSWORD"},
		{"RIVER_KEDA_READONLY_DATABASE_ROLE", "RIVER_KEDA_READONLY_PASSWORD"},
	} {
		config, err := pgx.ParseConfig(instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		config.User, config.Password = environment[entry.roleKey], environment[entry.passwordKey]
		connection, err := pgx.ConnectConfig(ctx, config)
		if err != nil {
			t.Errorf("the %s login does not authenticate with the chart's Secret password: %v", strings.TrimSuffix(entry.roleKey, "_DATABASE_ROLE"), err)
			continue
		}
		if entry.roleKey == "RIVER_KEDA_READONLY_DATABASE_ROLE" {
			var reads int
			if err := connection.QueryRow(ctx, `SELECT count(*) FROM river.river_job`).Scan(&reads); err != nil {
				t.Errorf("the KEDA login cannot read river_job: %v", err)
			}
		}
		_ = connection.Close(ctx)
	}
}

// podEnvironment is the environment a container would see: the envFrom Secrets'
// keys, then each env entry (plain value, or a secretKeyRef read from the rendered
// Secrets). A reference to a Secret or key the chart does not render fails.
func podEnvironment(t *testing.T, c map[string]any, secrets map[string]map[string]any) map[string]string {
	t.Helper()
	environment := map[string]string{}
	for _, item := range func() []any { list, _ := c["envFrom"].([]any); return list }() {
		name := item.(map[string]any)["secretRef"].(map[string]any)["name"].(string)
		secret := secrets[name]
		if secret == nil {
			t.Fatalf("envFrom names Secret %s, which the chart does not render", name)
		}
		data, _ := secret["stringData"].(map[string]any)
		for key, value := range data {
			environment[key] = value.(string)
		}
	}
	for name, raw := range envOf(c) {
		entry := raw.(map[string]any)
		if value, ok := entry["value"].(string); ok {
			environment[name] = value
			continue
		}
		ref := entry["valueFrom"].(map[string]any)["secretKeyRef"].(map[string]any)
		secret := secrets[ref["name"].(string)]
		if secret == nil {
			t.Fatalf("%s reads Secret %v, which the chart does not render", name, ref["name"])
		}
		data, _ := secret["stringData"].(map[string]any)
		value, ok := data[ref["key"].(string)].(string)
		if !ok {
			t.Fatalf("%s reads key %v of Secret %v, which it does not carry", name, ref["key"], ref["name"])
		}
		environment[name] = value
	}
	return environment
}
