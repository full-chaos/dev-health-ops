package devhealth_test

import (
	"bytes"
	"io"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// pinnedOperatorImage is a digest-pinned operator image, the form production
// values give migrations.hook.routeActivate.image.
var pinnedOperatorImage = "ghcr.io/full-chaos/dev-health-go-operator@sha256:" + strings.Repeat("a", 64)

// renderJobs renders the chart with sets and returns its Jobs and Secrets by
// name, or helm's error output when the render is refused.
func renderJobs(t *testing.T, sets ...string) (jobs, secrets map[string]map[string]any, refusal string) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	args := []string{"template", "t", "."}
	for _, set := range sets {
		args = append(args, "--set", set)
	}
	out, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		return nil, nil, string(out)
	}
	jobs, secrets = map[string]map[string]any{}, map[string]map[string]any{}
	decoder := yaml.NewDecoder(bytes.NewReader(out))
	for {
		var doc map[string]any
		if err := decoder.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("decode the render: %v", err)
		}
		if doc == nil {
			continue
		}
		name, _ := doc["metadata"].(map[string]any)["name"].(string)
		switch doc["kind"] {
		case "Job":
			jobs[name] = doc
		case "Secret":
			secrets[name] = doc
		}
	}
	return jobs, secrets, ""
}

func podSpec(t *testing.T, job map[string]any) map[string]any {
	t.Helper()
	return job["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
}

func container(t *testing.T, job map[string]any, name string) map[string]any {
	t.Helper()
	spec := podSpec(t, job)
	for _, key := range []string{"initContainers", "containers"} {
		list, _ := spec[key].([]any)
		for _, item := range list {
			if c := item.(map[string]any); c["name"] == name {
				return c
			}
		}
	}
	t.Fatalf("no container %s", name)
	return nil
}

func stringsOf(value any) []string {
	var out []string
	list, _ := value.([]any)
	for _, item := range list {
		out = append(out, item.(string))
	}
	return out
}

func envOf(c map[string]any) map[string]any {
	out := map[string]any{}
	list, _ := c["env"].([]any)
	for _, item := range list {
		entry := item.(map[string]any)
		out[entry["name"].(string)] = entry
	}
	return out
}

// The migrate Job runs `dho migrate upgrade` from the dho image in exec form,
// with production's settings; --river exactly when no River hook owns the
// step.
func TestMigrateJobRunsDhoMigrateUpgrade(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		sets      []string
		wantImage string
		wantArgs  []string
	}{
		{"chart defaults", nil, "ghcr.io/full-chaos/dev-health-go-dho:", []string{"migrate", "upgrade", "--river"}},
		{"the operator image, no River hook", []string{"migrations.hook.routeActivate.image=" + pinnedOperatorImage},
			pinnedOperatorImage, []string{"migrate", "upgrade", "--river"}},
		{"the River hook owns River", []string{"migrations.hook.routeActivate.image=" + pinnedOperatorImage, "migrations.hook.riverMigrate.enabled=true"},
			pinnedOperatorImage, []string{"migrate", "upgrade"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			jobs, _, refusal := renderJobs(t, testCase.sets...)
			if refusal != "" {
				t.Fatalf("render refused: %s", refusal)
			}
			c := container(t, jobs["t-dev-health-migrate"], "migrate")
			if image := c["image"].(string); !strings.HasPrefix(image, testCase.wantImage) {
				t.Fatalf("image = %s, want %s…", image, testCase.wantImage)
			}
			if args := stringsOf(c["args"]); !reflect.DeepEqual(args, testCase.wantArgs) {
				t.Fatalf("args = %v, want %v", args, testCase.wantArgs)
			}
			if _, hasCommand := c["command"]; hasCommand {
				t.Fatalf("the Job sets command %v; the dho image's entrypoint must run", c["command"])
			}
			env := envOf(c)
			for name, want := range map[string]string{"DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER": "1", "OPERATIONAL_ORDERING_CONTRACT": "2"} {
				entry, ok := env[name].(map[string]any)
				if !ok || entry["value"] != want {
					t.Fatalf("env %s = %v, want the literal %q", name, env[name], want)
				}
			}
		})
	}
}

// The Job refuses the Python image by name and any image an operator sets
// without a pin; a commit-tagged image must match the api image's commit.
func TestMigrateJobRefusesImagesThatCannotRunTheVerb(t *testing.T) {
	for name, testCase := range map[string]struct {
		sets []string
		want string
	}{
		"the Python image":           {[]string{"migrations.hook.image=ghcr.io/full-chaos/dev-hops-api@sha256:" + strings.Repeat("b", 64)}, "is the Python image"},
		"an unpinned operator image": {[]string{"migrations.hook.routeActivate.image=ghcr.io/full-chaos/dev-health-go-operator:latest"}, "is not a pinned dho image"},
		"a different commit":         {[]string{"migrations.hook.image=ghcr.io/full-chaos/dev-health-go-operator:sha-aaaaaaaaaaaa", "image.tag=sha-bbbbbbbbbbbb"}, "pinned to different commits"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, refusal := renderJobs(t, testCase.sets...)
			if !strings.Contains(refusal, testCase.want) {
				t.Fatalf("render = %q, want a refusal containing %q", refusal, testCase.want)
			}
		})
	}
}

// dho speaks ClickHouse's native protocol, so the migrate Secret carries a
// native (9000) URI: an explicit hook value wins, then goWorkers.clickhouseURI,
// then the bundled ClickHouse addressed natively.
func TestMigrateSecretCarriesANativeClickHouseURI(t *testing.T) {
	for name, testCase := range map[string]struct {
		sets []string
		want string
	}{
		"the bundled ClickHouse":  {nil, "@t-dev-health-clickhouse:9000/"},
		"goWorkers.clickhouseURI": {[]string{"goWorkers.clickhouseURI=clickhouse://u:p@ch.example:9000/db"}, "clickhouse://u:p@ch.example:9000/db"},
		"an explicit hook value":  {[]string{"goWorkers.clickhouseURI=clickhouse://u:p@ch.example:9000/db", "migrations.hook.secretData.CLICKHOUSE_URI=clickhouse://m:p@mig.example:9000/db"}, "clickhouse://m:p@mig.example:9000/db"},
	} {
		t.Run(name, func(t *testing.T) {
			_, secrets, refusal := renderJobs(t, testCase.sets...)
			if refusal != "" {
				t.Fatalf("render refused: %s", refusal)
			}
			data := secrets["t-dev-health-migrate-secrets"]["stringData"].(map[string]any)
			if got, _ := data["CLICKHOUSE_URI"].(string); !strings.Contains(got, testCase.want) {
				t.Fatalf("migrate Secret CLICKHOUSE_URI host part = %q, want it to contain %q", got[strings.LastIndex(got, "@")+1:], testCase.want)
			}
		})
	}
}

// Role provisioning and the route-activate DSN init container need a shell
// and the SQL/Python the ops runtime image carries; they keep that image
// (image.repository/image.tag), not the distroless dho image the migrate Job
// now runs.
func TestShellHooksKeepTheOpsRuntimeImage(t *testing.T) {
	jobs, _, refusal := renderJobs(t,
		"migrations.hook.provisionRoles.enabled=true", "migrations.hook.riverMigrate.enabled=true",
		"migrations.hook.routeActivate.enabled=true", "migrations.hook.routeActivate.image="+pinnedOperatorImage)
	if refusal != "" {
		t.Fatalf("render refused: %s", refusal)
	}
	for _, testCase := range []struct{ job, container string }{
		{"t-dev-health-provision-roles", "provision-roles"},
		{"t-dev-health-route-activate", "route-dsn"},
	} {
		image := container(t, jobs[testCase.job], testCase.container)["image"].(string)
		if !strings.HasPrefix(image, "ghcr.io/full-chaos/dev-hops-api:") {
			t.Fatalf("%s/%s image = %s, want the ops runtime image", testCase.job, testCase.container, image)
		}
	}
	if image := container(t, jobs["t-dev-health-migrate"], "migrate")["image"]; image != pinnedOperatorImage {
		t.Fatalf("migrate image = %v, want the operator image the River hook runs", image)
	}
}
