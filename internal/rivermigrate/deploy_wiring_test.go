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

// The image repositories a migrate path may run. The check is on the exact
// repository, not on a substring of the name: an image called
// dev-health-go-dho-not-real is not the dho image.
const (
	dhoRepository      = "ghcr.io/full-chaos/dev-health-go-dho"
	operatorRepository = "ghcr.io/full-chaos/dev-health-go-operator"
)

// imageRepository is the repository of an image reference: a Compose
// `${VAR:-default}` is read as its default, and the digest and the tag are
// dropped.
func imageRepository(reference string) string {
	if strings.HasPrefix(reference, "${") && strings.HasSuffix(reference, "}") {
		if _, fallback, found := strings.Cut(reference[2:len(reference)-1], ":-"); found {
			reference = fallback
		}
	}
	reference, _, _ = strings.Cut(reference, "@")
	if slash := strings.LastIndex(reference, "/"); strings.Contains(reference[slash+1:], ":") {
		reference = reference[:slash+1+strings.Index(reference[slash+1:], ":")]
	}
	return reference
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

// The Compose migrate service runs `dho migrate upgrade --river`
// from a dho image, one-shot, non-root, with no shell, and with production's
// settings as its defaults: the River cutover, operational ordering contract
// 2, and ClickHouse over its native protocol.
func TestComposeMigrateServicesRunDhoMigrateUpgrade(t *testing.T) {
	for _, file := range []string{"compose.yml"} {
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
			if repository := imageRepository(migrate.Image); repository != dhoRepository && repository != operatorRepository {
				t.Fatalf("image = %s, want the dho or operator image repository", migrate.Image)
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

// Every Go worker's default contract is production's: 2.
func TestGoWorkersDefaultToOrderingContract2(t *testing.T) {
	for _, testCase := range []struct{ file, want string }{
		{"deploy/helm/dev-health/values.yaml", `operationalOrderingContract: "2"`},
		{"compose.yml", "${OPERATIONAL_ORDERING_CONTRACT:-2}"},
	} {
		text := string(repoFile(t, testCase.file))
		if !strings.Contains(text, testCase.want) || strings.Contains(text, "OPERATIONAL_ORDERING_CONTRACT:-1}") {
			t.Fatalf("%s does not default the contract to 2 (%s)", testCase.file, testCase.want)
		}
	}
}

func TestImageRepositoryIsTheExactRepository(t *testing.T) {
	for reference, want := range map[string]string{
		"ghcr.io/full-chaos/dev-health-go-dho:latest":                                      dhoRepository,
		"ghcr.io/full-chaos/dev-health-go-dho":                                             dhoRepository,
		"ghcr.io/full-chaos/dev-health-go-dho@sha256:abc":                                  dhoRepository,
		"ghcr.io/full-chaos/dev-health-go-dho:v1@sha256:abc":                               dhoRepository,
		"${DEV_HEALTH_GO_DHO_IMAGE:-ghcr.io/full-chaos/dev-health-go-dho:latest}":          dhoRepository,
		"${DEV_HEALTH_GO_OPERATOR_IMAGE:-ghcr.io/full-chaos/dev-health-go-operator:local}": operatorRepository,
		"ghcr.io/full-chaos/dev-health-go-dho-not-real:latest":                             "ghcr.io/full-chaos/dev-health-go-dho-not-real",
		"registry:5000/dev-health-go-dho:1":                                                "registry:5000/dev-health-go-dho",
	} {
		if got := imageRepository(reference); got != want {
			t.Errorf("imageRepository(%q) = %q, want %q", reference, got, want)
		}
	}
	if imageRepository("ghcr.io/full-chaos/dev-health-go-dho-not-real:latest") == dhoRepository {
		t.Fatal("a look-alike image name reads as the dho repository")
	}
}
