package deploymentcontract

import (
	"slices"
	"strings"
	"testing"
)

const (
	processForeignDSN = "process carries a database DSN secret outside its class"
	migrationForeign  = "migration job carries a database DSN secret outside its class"
)

func TestEveryProcessClassRefusesDSNSecretsOfAnotherClass(t *testing.T) {
	for _, row := range []struct {
		name    string
		runtime string
		secret  string
	}{
		{"river with the coordinator DSN", "river", "COORDINATOR_DATABASE_URI"},
		{"river with the coordinator DSN file", "river", "COORDINATOR_DATABASE_URI_FILE"},
		{"river with the API DSN", "river", "API_DATABASE_URI"},
		{"river with the query API DSN", "river", "GO_API_REGISTRY_POSTGRES_URI"},
		{"river with an unlisted database DSN", "river", "ANALYTICS_DATABASE_URI"},
		{"control with the API DSN", "control", "API_DATABASE_URI"},
		{"control with an unlisted database DSN", "control", "ANALYTICS_DATABASE_URI"},
		{"stream with the queue-control DSN", "stream", "WORKER_DATABASE_URI"},
		{"stream with the queue-control DSN file", "stream", "WORKER_DATABASE_URI_FILE"},
		{"stream with the coordinator DSN", "stream", "COORDINATOR_DATABASE_URI"},
		{"stream with an unlisted database DSN", "stream", "ANALYTICS_DATABASE_URI"},
	} {
		t.Run(row.name, func(t *testing.T) {
			_, registry := loadFixture(t)
			mutated := 0
			for index := range len(mustLoad(t).Processes) {
				manifest := mustLoad(t)
				process := &manifest.Processes[index]
				if process.Runtime != row.runtime {
					continue
				}
				process.SecretEnv = withValue(process.SecretEnv, row.secret)
				_, err := manifest.Validate(registry)
				if err == nil || !strings.Contains(err.Error(), "deployment process "+process.Name+": "+processForeignDSN) {
					t.Errorf("process %s: Validate = %v, want %q", process.Name, err, processForeignDSN)
				}
				mutated++
			}
			if mutated == 0 {
				t.Fatalf("no process of runtime %q was mutated", row.runtime)
			}
		})
	}
}

func TestMigrationJobRefusesDSNSecretsOfAnotherClass(t *testing.T) {
	for _, secret := range []string{"COORDINATOR_DATABASE_URI", "API_DATABASE_URI", "ANALYTICS_DATABASE_URI"} {
		t.Run(secret, func(t *testing.T) {
			manifest, registry := loadFixture(t)
			manifest.MigrationJob.SecretEnv = withValue(manifest.MigrationJob.SecretEnv, secret)
			if _, err := manifest.Validate(registry); err == nil || !strings.Contains(err.Error(), migrationForeign) {
				t.Fatalf("Validate = %v, want %q", err, migrationForeign)
			}
		})
	}
}

func TestCheckedInManifestCarriesOnlyItsClassDSNSecrets(t *testing.T) {
	manifest, registry := loadFixture(t)
	if _, err := manifest.Validate(registry); err != nil {
		t.Fatalf("the checked-in manifest is refused: %v", err)
	}
	for _, process := range manifest.Processes {
		if foreign := ForeignDSNSecrets(process.Runtime, process.SecretEnv); len(foreign) > 0 {
			t.Errorf("process %s carries %v outside class %s", process.Name, foreign, process.Runtime)
		}
		if len(AllowedDSNSecrets(process.Runtime)) == 0 {
			t.Errorf("no allowed DSN set for class %s", process.Runtime)
		}
	}
	for class, secrets := range map[string][]string{
		"operator":  manifest.OperatorCLI.SecretEnv,
		"migration": manifest.MigrationJob.SecretEnv,
	} {
		if foreign := ForeignDSNSecrets(class, secrets); len(foreign) > 0 {
			t.Errorf("class %s carries %v outside its set", class, foreign)
		}
	}
}

func TestDatabaseDSNNamePattern(t *testing.T) {
	for name, want := range map[string]bool{
		"POSTGRES_URI":                 true,
		"WORKER_DATABASE_URI":          true,
		"COORDINATOR_DATABASE_URI":     true,
		"MIGRATION_DATABASE_URI_FILE":  true,
		"GO_API_REGISTRY_POSTGRES_URI": true,
		"AUTH_DATABASE_URI":            true,
		"CLICKHOUSE_URI":               false,
		"VALKEY_URI":                   false,
		"SETTINGS_ENCRYPTION_KEY":      false,
		"RIVER_DATABASE_SCHEMA":        false,
	} {
		if got := databaseDSNName(name); got != want {
			t.Errorf("DatabaseDSNSecret(%q) = %v, want %v", name, got, want)
		}
	}
	if allowed := AllowedDSNSecrets("stream"); !slices.Equal(allowed, []string{"POSTGRES_URI"}) {
		t.Errorf("stream allowed set = %v", allowed)
	}
	if AllowedDSNSecrets("nonexistent") != nil {
		t.Error("an unknown class must have no allowed DSN secrets")
	}
}

func databaseDSNName(name string) bool {
	_, ok := DatabaseDSNSecret(name)
	return ok
}

func TestComponentFormVariablesBelongToTheirDSNSecret(t *testing.T) {
	for name, want := range map[string]string{
		"DEV_HEALTH_PG_QUEUE_HOST":           "WORKER_DATABASE_URI",
		"DEV_HEALTH_PG_COORDINATOR_PASSWORD": "COORDINATOR_DATABASE_URI",
		"DEV_HEALTH_PG_DOMAIN_USER":          "POSTGRES_URI",
		"WORKER_DATABASE_URI_FILE":           "WORKER_DATABASE_URI",
	} {
		if got, ok := DatabaseDSNSecret(name); !ok || got != want {
			t.Errorf("DatabaseDSNSecret(%q) = %q, %v, want %q", name, got, ok, want)
		}
	}
	if _, ok := DatabaseDSNSecret("DEV_HEALTH_PG_DB"); ok {
		t.Error("the shared database name belongs to no DSN secret")
	}
	if foreign := ForeignDSNSecrets("stream", []string{"DEV_HEALTH_PG_QUEUE_HOST", "DEV_HEALTH_PG_DOMAIN_HOST"}); !slices.Equal(foreign, []string{"DEV_HEALTH_PG_QUEUE_HOST"}) {
		t.Errorf("stream foreign component variables = %v", foreign)
	}
}
