package deploymentcontract

import (
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/dsnclass"
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
		if foreign := dsnclass.Foreign(process.Runtime, process.SecretEnv); len(foreign) > 0 {
			t.Errorf("process %s carries %v outside class %s", process.Name, foreign, process.Runtime)
		}
		if len(dsnclass.Allowed(process.Runtime)) == 0 {
			t.Errorf("no allowed DSN set for class %s", process.Runtime)
		}
	}
	for class, secrets := range map[string][]string{
		"operator":  manifest.OperatorCLI.SecretEnv,
		"migration": manifest.MigrationJob.SecretEnv,
	} {
		if foreign := dsnclass.Foreign(class, secrets); len(foreign) > 0 {
			t.Errorf("class %s carries %v outside its set", class, foreign)
		}
	}
}

// The closed table and the checked-in manifest say the same thing: for every
// class, the DSN secrets its processes carry in deploy/go-workers/deployment.json
// are exactly the table's allowed set, in both directions. A name added to one and
// not the other fails here.
func TestAllowedDSNSecretsEqualTheManifestPerClass(t *testing.T) {
	manifest := mustLoad(t)
	carried := map[string]map[string]bool{}
	add := func(class string, secretEnv []string) {
		if carried[class] == nil {
			carried[class] = map[string]bool{}
		}
		for _, name := range secretEnv {
			if secret, ok := dsnclass.Secret(name); ok {
				carried[class][secret] = true
			}
		}
	}
	for _, process := range manifest.Processes {
		add(process.Runtime, process.SecretEnv)
	}
	add(dsnclass.Operator, manifest.OperatorCLI.SecretEnv)
	add(dsnclass.Migration, manifest.MigrationJob.SecretEnv)
	for _, class := range []string{dsnclass.River, dsnclass.Control, dsnclass.Stream, dsnclass.Operator, dsnclass.Migration} {
		var got []string
		for name := range carried[class] {
			got = append(got, name)
		}
		sort.Strings(got)
		if want := dsnclass.Allowed(class); !slices.Equal(got, want) {
			t.Errorf("class %s: the manifest carries %v, the allowed set is %v", class, got, want)
		}
	}
}
