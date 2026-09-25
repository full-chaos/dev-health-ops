package rivermigrate

import (
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// repoFile reads a file relative to the repository root.
func repoFile(t *testing.T, relative string) []byte {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test")
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(filename), "..", "..", relative))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// composeMigrate is the `migrate` service of a Compose or Swarm file.
type composeMigrate struct {
	Image       string            `yaml:"image"`
	Build       map[string]any    `yaml:"build"`
	Entrypoint  any               `yaml:"entrypoint"`
	Command     []string          `yaml:"command"`
	Restart     string            `yaml:"restart"`
	User        string            `yaml:"user"`
	ReadOnly    bool              `yaml:"read_only"`
	Environment map[string]string `yaml:"environment"`
	Deploy      struct {
		RestartPolicy struct {
			Condition string `yaml:"condition"`
		} `yaml:"restart_policy"`
	} `yaml:"deploy"`
}

// Every Compose and Swarm migrate service runs `dho migrate upgrade --river`
// from a dho image, one-shot, non-root, with no shell, and with production's
// settings as its defaults: the River cutover, operational ordering contract
// 2, and ClickHouse over its native protocol.
func TestComposeMigrateServicesRunDhoMigrateUpgrade(t *testing.T) {
	for _, file := range []string{"compose.yml", "deploy/docker-compose/compose.production.yml", "deploy/docker-swarm/stack.yml"} {
		t.Run(file, func(t *testing.T) {
			var doc struct {
				Services map[string]yaml.Node `yaml:"services"`
			}
			if err := yaml.Unmarshal(repoFile(t, file), &doc); err != nil {
				t.Fatal(err)
			}
			node, ok := doc.Services["migrate"]
			if !ok {
				t.Fatal("no migrate service")
			}
			var migrate composeMigrate
			if err := node.Decode(&migrate); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(migrate.Image, "dev-health-go-dho") && !strings.Contains(migrate.Image, "dev-health-go-operator") {
				t.Fatalf("image = %s, want a dho image", migrate.Image)
			}
			if migrate.Entrypoint != nil {
				t.Fatalf("entrypoint = %v, want none: the dho image's entrypoint runs", migrate.Entrypoint)
			}
			if !reflect.DeepEqual(migrate.Command, []string{"migrate", "upgrade", "--river"}) {
				t.Fatalf("command = %v, want [migrate upgrade --river]", migrate.Command)
			}
			if migrate.Restart != "no" && migrate.Deploy.RestartPolicy.Condition != "none" {
				t.Fatal("migrate is not one-shot")
			}
			if migrate.User != "65532:65532" || !migrate.ReadOnly {
				t.Fatalf("user %q read_only %v, want the distroless non-root user on a read-only root", migrate.User, migrate.ReadOnly)
			}
			for name, want := range map[string]string{
				"DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER": "${DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER:-1}",
				"OPERATIONAL_ORDERING_CONTRACT":         "${OPERATIONAL_ORDERING_CONTRACT:-2}",
			} {
				if got := migrate.Environment[name]; got != want {
					t.Fatalf("%s = %q, want %q", name, got, want)
				}
			}
			if uri := migrate.Environment["CLICKHOUSE_URI"]; !strings.Contains(uri, ":9000/") {
				t.Fatalf("CLICKHOUSE_URI = %q, want the native port 9000", uri)
			}
			if uri := migrate.Environment["POSTGRES_URI"]; strings.Contains(uri, "+") {
				t.Fatalf("POSTGRES_URI = %q, want a plain postgresql:// DSN", uri)
			}
		})
	}
}

// The raw Kubernetes migrate Job runs the same verb from the dho image, and
// its Secret gives it DSNs dho reads.
func TestKubernetesMigrateJobRunsDhoMigrateUpgrade(t *testing.T) {
	var job struct {
		Spec struct {
			Template struct {
				Spec struct {
					RestartPolicy string `yaml:"restartPolicy"`
					Containers    []struct {
						Image   string   `yaml:"image"`
						Command []string `yaml:"command"`
						Args    []string `yaml:"args"`
						Env     []struct {
							Name  string `yaml:"name"`
							Value string `yaml:"value"`
						} `yaml:"env"`
					} `yaml:"containers"`
				} `yaml:"spec"`
			} `yaml:"template"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(repoFile(t, "deploy/kubernetes/migrate-job.yaml"), &job); err != nil {
		t.Fatal(err)
	}
	spec := job.Spec.Template.Spec
	if spec.RestartPolicy != "Never" || len(spec.Containers) != 1 {
		t.Fatalf("restartPolicy %q with %d container(s)", spec.RestartPolicy, len(spec.Containers))
	}
	c := spec.Containers[0]
	if !strings.Contains(c.Image, "dev-health-go-dho") || c.Command != nil || !reflect.DeepEqual(c.Args, []string{"migrate", "upgrade", "--river"}) {
		t.Fatalf("image %s command %v args %v, want the dho image running [migrate upgrade --river]", c.Image, c.Command, c.Args)
	}
	env := map[string]string{}
	for _, entry := range c.Env {
		env[entry.Name] = entry.Value
	}
	if env["DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER"] != "1" || env["OPERATIONAL_ORDERING_CONTRACT"] != "2" {
		t.Fatalf("env = %v, want the cutover 1 and contract 2", env)
	}

	decoder := yaml.NewDecoder(strings.NewReader(string(repoFile(t, "deploy/kubernetes/secrets.yaml"))))
	for {
		var secret struct {
			Metadata   struct{ Name string } `yaml:"metadata"`
			StringData map[string]string     `yaml:"stringData"`
		}
		if err := decoder.Decode(&secret); err != nil {
			t.Fatal("no dev-health-migration-secrets Secret")
		}
		if secret.Metadata.Name != "dev-health-migration-secrets" {
			continue
		}
		if _, ok := secret.StringData["MIGRATION_DATABASE_URI"]; ok {
			t.Fatal("the checked-in migration Secret carries MIGRATION_DATABASE_URI; it is the operator's to add")
		}
		if uri := secret.StringData["POSTGRES_URI"]; !strings.HasPrefix(uri, "postgresql://") {
			t.Fatalf("POSTGRES_URI = %q, want a plain postgresql:// DSN", uri)
		}
		if uri := secret.StringData["CLICKHOUSE_URI"]; !strings.Contains(uri, ":9000/") {
			t.Fatalf("CLICKHOUSE_URI = %q, want the native port 9000", uri)
		}
		return
	}
}

// The api Deployment's wait-for-migrations initContainer (the safety net of
// a naive `kubectl apply -k`) runs the read-only Go probe from the dho image:
// exactly `migrate clickhouse status --check`, in exec form with no shell,
// never the upgrade, against the ClickHouse the migrate Job migrates (the
// migration Secret's native-protocol URI), under contract 2.
func TestKubernetesApiWaitsForMigrationsWithDho(t *testing.T) {
	decoder := yaml.NewDecoder(strings.NewReader(string(repoFile(t, "deploy/kubernetes/api.yaml"))))
	for {
		var doc struct {
			Kind string `yaml:"kind"`
			Spec struct {
				Template struct {
					Spec struct {
						InitContainers []struct {
							Name    string   `yaml:"name"`
							Image   string   `yaml:"image"`
							Command []string `yaml:"command"`
							Args    []string `yaml:"args"`
							EnvFrom []any    `yaml:"envFrom"`
							Env     []struct {
								Name      string `yaml:"name"`
								Value     string `yaml:"value"`
								ValueFrom struct {
									SecretKeyRef struct {
										Name string `yaml:"name"`
										Key  string `yaml:"key"`
									} `yaml:"secretKeyRef"`
								} `yaml:"valueFrom"`
							} `yaml:"env"`
						} `yaml:"initContainers"`
					} `yaml:"spec"`
				} `yaml:"template"`
			} `yaml:"spec"`
		}
		if err := decoder.Decode(&doc); err != nil {
			t.Fatal("api.yaml has no Deployment with a wait-for-migrations initContainer")
		}
		if doc.Kind != "Deployment" {
			continue
		}
		for _, init := range doc.Spec.Template.Spec.InitContainers {
			if init.Name != "wait-for-migrations" {
				continue
			}
			if !strings.Contains(init.Image, "dev-health-go-dho") || init.Command != nil ||
				!reflect.DeepEqual(init.Args, []string{"migrate", "clickhouse", "status", "--check"}) {
				t.Fatalf("image %s command %v args %v, want the dho image running exactly [migrate clickhouse status --check]", init.Image, init.Command, init.Args)
			}
			if len(init.EnvFrom) != 0 {
				t.Fatalf("envFrom = %v: the probe needs one key, not a whole Secret", init.EnvFrom)
			}
			env := map[string]string{}
			var uriSecret, uriKey string
			for _, entry := range init.Env {
				env[entry.Name] = entry.Value
				if entry.Name == "CLICKHOUSE_URI" {
					uriSecret, uriKey = entry.ValueFrom.SecretKeyRef.Name, entry.ValueFrom.SecretKeyRef.Key
				}
			}
			if env["OPERATIONAL_ORDERING_CONTRACT"] != "2" {
				t.Fatalf("OPERATIONAL_ORDERING_CONTRACT = %q, want 2", env["OPERATIONAL_ORDERING_CONTRACT"])
			}
			if uriSecret != "dev-health-migration-secrets" || uriKey != "CLICKHOUSE_URI" {
				t.Fatalf("CLICKHOUSE_URI from secret %q key %q, want the migrate Job's dev-health-migration-secrets", uriSecret, uriKey)
			}
			return
		}
	}
}

// Every Go worker's default contract is production's: 2.
func TestGoWorkersDefaultToOrderingContract2(t *testing.T) {
	for _, testCase := range []struct{ file, want string }{
		{"deploy/kubernetes/go-workers.yaml", `OPERATIONAL_ORDERING_CONTRACT: "2"`},
		{"deploy/helm/dev-health/values.yaml", `operationalOrderingContract: "2"`},
		{"deploy/docker-compose/compose.go-workers.yml", "${OPERATIONAL_ORDERING_CONTRACT:-2}"},
		{"deploy/docker-swarm/stack.go-workers.yml", "${OPERATIONAL_ORDERING_CONTRACT:-2}"},
	} {
		text := string(repoFile(t, testCase.file))
		if !strings.Contains(text, testCase.want) || strings.Contains(text, "OPERATIONAL_ORDERING_CONTRACT:-1}") {
			t.Fatalf("%s does not default the contract to 2 (%s)", testCase.file, testCase.want)
		}
	}
}
