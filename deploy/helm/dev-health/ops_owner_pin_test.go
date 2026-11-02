package devhealth_test

import (
	"strings"
	"testing"
)

// CHAOS-12614: migrations.hook.ownerRole pins the owner of the database and the ops schemas for the four ops migrator
// Jobs (MIGRATION_OWNER_ROLE); provisionRoles.migratorRole provisions that login; provisionRoles.externalSecretName
// keeps provision-roles on the superuser DSN. Empty values render nothing.

var opsPinJobs = map[string]string{
	"a-dev-health-migrate":          "migrate",
	"a-dev-health-open-units-index": "open-units-index",
	"a-dev-health-streak-index":     "streak-index",
	"a-dev-health-river-migrate":    "river-migrate",
}

func opsPinArgs(extra ...string) []string {
	return queryAPIRoleArgs(riverHookSets, []string{
		"--set", "migrations.hook.openUnitsIndex.enabled=true",
		"--set", "migrations.hook.streakIndex.enabled=true",
	}, extra)
}

func opsPinContainers(t *testing.T, args ...string) map[string]map[string]any {
	t.Helper()
	docs, out, err := renderAuth(t, args...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	containers := map[string]map[string]any{}
	for job, name := range opsPinJobs {
		doc := authDoc(docs, "Job", job)
		if doc == nil {
			t.Fatalf("no %s Job", job)
		}
		containers[job] = container(t, doc, name)
	}
	return containers
}

func TestOpsMigratorJobsRenderTheOwnerPin(t *testing.T) {
	for job, c := range opsPinContainers(t, opsPinArgs("--set", "migrations.hook.ownerRole=devhealth_migrator")...) {
		got, ok := envValue(c, "MIGRATION_OWNER_ROLE")
		if !ok || got != "devhealth_migrator" {
			t.Errorf("%s: MIGRATION_OWNER_ROLE = %q (present %v), want devhealth_migrator", job, got, ok)
		}
	}
}

func TestOpsMigratorJobsRenderNoPinByDefault(t *testing.T) {
	for job, c := range opsPinContainers(t, opsPinArgs()...) {
		if _, ok := envValue(c, "MIGRATION_OWNER_ROLE"); ok {
			t.Errorf("%s: MIGRATION_OWNER_ROLE is rendered while migrations.hook.ownerRole is empty", job)
		}
	}
	docs, out, err := renderAuth(t, opsPinArgs()...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.300s", err, out)
	}
	for _, doc := range docs {
		if doc["kind"] != "Job" {
			continue
		}
		for _, c := range func() []any { l, _ := at(doc, "spec", "template", "spec", "containers").([]any); return l }() {
			if _, ok := envValue(c, "MIGRATION_OWNER_ROLE"); ok {
				t.Errorf("Job %v renders MIGRATION_OWNER_ROLE with the pin off", at(doc, "metadata", "name"))
			}
		}
	}
}

func TestOpsOwnerPinKeepsExtraEnvOnTheIndexJobs(t *testing.T) {
	args := opsPinArgs("--set", "migrations.hook.ownerRole=devhealth_migrator",
		"--set-json", `migrations.hook.extraEnv=[{"name":"EXTRA_ONE","value":"1"}]`)
	containers := opsPinContainers(t, args...)
	for _, job := range []string{"a-dev-health-open-units-index", "a-dev-health-streak-index"} {
		if got, ok := envValue(containers[job], "EXTRA_ONE"); !ok || got != "1" {
			t.Errorf("%s: extraEnv EXTRA_ONE = %q (present %v)", job, got, ok)
		}
		if got, _ := envValue(containers[job], "MIGRATION_OWNER_ROLE"); got != "devhealth_migrator" {
			t.Errorf("%s: MIGRATION_OWNER_ROLE = %q", job, got)
		}
	}
}

func TestOpsOwnerRoleRefusals(t *testing.T) {
	cases := map[string][]string{
		"not a plain name":    {"--set", "migrations.hook.ownerRole=Bad-Name"},
		"the domain login":    {"--set", "migrations.hook.ownerRole=devhealth_domain"},
		"the queue login":     {"--set", "migrations.hook.ownerRole=devhealth_queue"},
		"the query-api login": append([]string{"--set", "migrations.hook.ownerRole=devhealth_query_api"}, roleEnv("devhealth_query_api")...),
		"the api login": {"--set", "migrations.hook.ownerRole=devhealth_api",
			"--set", "migrations.hook.provisionRoles.apiRole.name=devhealth_api",
			"--set", "migrations.hook.provisionRoles.apiRole.secretName=api-login"},
		"sessions owned, another auth-grants reader": append(opsSessionsOn("--set", "queryApi.enabled=false", "--set", "authService.migrate.sessionReaders.goApi=devhealth_api",
			"--set", "authService.migrate.sessionReaders.queryApi=someone_else"), "--set", "migrations.hook.ownerRole=devhealth_migrator"),
	}
	for name, extra := range cases {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, opsPinArgs(extra...)...)
			if err == nil {
				t.Fatal("the render succeeded; it must refuse")
			}
			if !strings.Contains(out, "migrations.hook.ownerRole") {
				t.Fatalf("the refusal does not name migrations.hook.ownerRole: %.400s", out)
			}
		})
	}
}

// opsSessionsOn is a phase 2 release (auth-service owns sessions) with the query-api login named.
func opsSessionsOn(extra ...string) []string {
	out := append(append(append([]string{}, sessionsOn...), sessionSuccessor...), roleEnv("devhealth_query_api")...)
	return append(out, extra...)
}

func TestOpsOwnerRoleWithSessionsOwnedNeedsTheAuthGrantsReader(t *testing.T) {
	args := opsPinArgs(append(opsSessionsOn("--set", "authService.migrate.sessionReaders.goApi=devhealth_api",
		"--set", "authService.migrate.sessionReaders.queryApi=devhealth_query_api"), "--set", "migrations.hook.ownerRole=devhealth_migrator")...)
	for job, c := range opsPinContainers(t, args...) {
		if got, _ := envValue(c, "MIGRATION_OWNER_ROLE"); got != "devhealth_migrator" {
			t.Errorf("%s: MIGRATION_OWNER_ROLE = %q", job, got)
		}
	}
	// The same release without the pin renders without the reader: the guard binds only with the pin.
	if _, out, err := renderAuth(t, opsPinArgs(opsSessionsOn("--set", "queryApi.enabled=false", "--set", "authService.migrate.sessionReaders.goApi=devhealth_api",
		"--set", "authService.migrate.sessionReaders.queryApi=someone_else")...)...); err != nil {
		t.Fatalf("render without the pin refused: %.400s", out)
	}
}

func TestProvisionRolesJobPassesTheMigratorLoginWhenNamed(t *testing.T) {
	docs, out, err := renderAuth(t, provisionRolesArgs(
		"--set", "migrations.hook.provisionRoles.migratorRole.name=devhealth_migrator",
		"--set", "migrations.hook.provisionRoles.migratorRole.secretName=dev-health-ops-migrator")...)
	if err != nil {
		t.Fatalf("render: %v\n%.500s", err, out)
	}
	job := authDoc(docs, "Job", provisionRolesJob)
	if job == nil {
		t.Fatal("provision-roles Job did not render")
	}
	role := jobEnvEntry(t, job, "OPS_MIGRATOR_DATABASE_ROLE")
	if role == nil || role["value"] != "devhealth_migrator" {
		t.Fatalf("OPS_MIGRATOR_DATABASE_ROLE = %v, want value devhealth_migrator", role)
	}
	password := jobEnvEntry(t, job, "OPS_MIGRATOR_DATABASE_PASSWORD")
	if password == nil {
		t.Fatal("OPS_MIGRATOR_DATABASE_PASSWORD is not set on the Job")
	}
	if _, literal := password["value"]; literal {
		t.Errorf("OPS_MIGRATOR_DATABASE_PASSWORD must be a secretKeyRef, not a literal: %v", password)
	}
	if at(password, "valueFrom", "secretKeyRef", "name") != "dev-health-ops-migrator" || at(password, "valueFrom", "secretKeyRef", "key") != "OPS_MIGRATOR_DATABASE_PASSWORD" {
		t.Errorf("OPS_MIGRATOR_DATABASE_PASSWORD secretKeyRef = %v", at(password, "valueFrom", "secretKeyRef"))
	}
}

func TestProvisionRolesJobHasNoMigratorLoginByDefault(t *testing.T) {
	docs, out, err := renderAuth(t, provisionRolesArgs()...)
	if err != nil {
		t.Fatalf("render: %v\n%.500s", err, out)
	}
	job := authDoc(docs, "Job", provisionRolesJob)
	for _, name := range []string{"OPS_MIGRATOR_DATABASE_ROLE", "OPS_MIGRATOR_DATABASE_PASSWORD"} {
		if jobEnvEntry(t, job, name) != nil {
			t.Errorf("migratorRole.name is empty, the Job still sets %s", name)
		}
	}
}

func TestProvisionRolesMigratorLoginNeedsItsSecret(t *testing.T) {
	for name, sets := range map[string][]string{
		"no secretName": {"--set", "migrations.hook.provisionRoles.migratorRole.name=devhealth_migrator"},
		"no secretKey": {"--set", "migrations.hook.provisionRoles.migratorRole.name=devhealth_migrator",
			"--set", "migrations.hook.provisionRoles.migratorRole.secretName=dev-health-ops-migrator",
			"--set", "migrations.hook.provisionRoles.migratorRole.secretKey="},
	} {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, provisionRolesArgs(sets...)...)
			if err == nil || !strings.Contains(out, "migrations.hook.provisionRoles.migratorRole") {
				t.Fatalf("expected a render refusal naming migratorRole, got err=%v\n%.500s", err, out)
			}
		})
	}
}

// With secrets.create=false the provision-roles Job envFroms provisionRoles.externalSecretName, else the hook Secret;
// the other ops Jobs keep the hook Secret.
func TestProvisionRolesExternalSecretName(t *testing.T) {
	external := []string{"--set", "secrets.create=false", "--set", "secrets.externalSecretName=dev-health-app", "--set", "hs256Secret.externalSecretName=hs256",
		"--set", "migrations.hook.externalSecretName=dev-health-ops-migrate",
		"--set", "goWorkers.pgbouncer.secret.create=false", "--set", "goWorkers.pgbouncer.secret.externalSecretName=pooler"}
	envFromSecret := func(job map[string]any) string {
		list, _ := at(job, "spec", "template", "spec", "containers", 0, "envFrom").([]any)
		for _, item := range list {
			if name, ok := at(item, "secretRef", "name").(string); ok {
				return name
			}
		}
		return ""
	}
	for name, c := range map[string]struct {
		extra                []string
		provision, otherJobs string
	}{
		"default":  {nil, "dev-health-ops-migrate", "dev-health-ops-migrate"},
		"own name": {[]string{"--set", "migrations.hook.provisionRoles.externalSecretName=dev-health-ops-migrate-owner"}, "dev-health-ops-migrate-owner", "dev-health-ops-migrate"},
	} {
		t.Run(name, func(t *testing.T) {
			docs, out, err := renderAuth(t, provisionRolesArgs(append(append([]string{}, external...), c.extra...)...)...)
			if err != nil {
				t.Fatalf("render: %v\n%.500s", err, out)
			}
			if got := envFromSecret(authDoc(docs, "Job", provisionRolesJob)); got != c.provision {
				t.Errorf("provision-roles envFrom Secret = %q, want %q", got, c.provision)
			}
			for _, job := range []string{"a-dev-health-migrate", "a-dev-health-river-migrate"} {
				if got := envFromSecret(authDoc(docs, "Job", job)); got != c.otherJobs {
					t.Errorf("%s envFrom Secret = %q, want %q", job, got, c.otherJobs)
				}
			}
		})
	}
}
