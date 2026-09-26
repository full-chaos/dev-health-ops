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
			pinnedOperatorImage, []string{"migrate", "upgrade", "--river=false"}},
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

// The provision-roles hook Job runs `dho migrate roles` on the pinned operator
// image, exec form, args only (CHAOS-6951): no shell, no psql, no Python image, no
// baked SQL, no password or database name in argv. The route-activate hook
// already runs only that image (CHAOS-6902): see TestRouteActivateRunsOnlyTheOperatorImage.
func TestProvisionRolesRunsDhoMigrateRolesOnTheOperatorImage(t *testing.T) {
	const dsn = "postgresql://migrator:pw@postgres:5432/devhealth"
	jobs, secrets, refusal := renderJobs(t,
		"migrations.hook.provisionRoles.enabled=true", "migrations.hook.riverMigrate.enabled=true",
		"migrations.hook.routeActivate.enabled=true", "migrations.hook.routeActivate.image="+pinnedOperatorImage,
		"migrations.hook.secretData.MIGRATION_DATABASE_URI="+dsn)
	if refusal != "" {
		t.Fatalf("render refused: %s", refusal)
	}
	job := jobs["t-dev-health-provision-roles"]
	pod := podSpec(t, job)
	if list, _ := pod["containers"].([]any); len(list) != 1 {
		t.Fatalf("%d containers, want exactly one", len(list))
	}
	if _, has := pod["initContainers"]; has {
		t.Fatalf("the pod declares init containers: %v", pod["initContainers"])
	}
	c := container(t, job, "provision-roles")
	if c["image"] != pinnedOperatorImage {
		t.Fatalf("image = %v, want the pinned operator image", c["image"])
	}
	if _, has := c["command"]; has {
		t.Fatalf("a command override (a shell) the distroless image cannot run: %v", c["command"])
	}
	if args := stringsOf(c["args"]); !reflect.DeepEqual(args, []string{"migrate", "roles"}) {
		t.Fatalf("args = %v, want [migrate roles]", args)
	}
	rendered, err := yaml.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	for _, retired := range []string{"psql", "provision_river_roles", "PGPASSWORD", "APP_DATABASE", "dev-hops-api", "/bin/sh"} {
		if strings.Contains(string(rendered), retired) {
			t.Errorf("the hook still carries %q", retired)
		}
	}
	// The elevated DSN arrives under the key the verb prefers, from a hook Secret
	// created before the Job.
	conn := secrets["t-dev-health-provision-roles-conn"]
	if conn == nil {
		t.Fatalf("no hook-scoped DSN Secret; have %v", secrets)
	}
	data, _ := conn["stringData"].(map[string]any)
	if got, _ := data["MIGRATION_DATABASE_URI"].(string); got != dsn {
		t.Fatalf("the DSN Secret carries MIGRATION_DATABASE_URI = %q, want the configured DSN: %v", got, data)
	}
}

// The route-activate hook Job runs no Python and no shell (CHAOS-6902): every
// container, init and main, is the operator image, none has a command override,
// none mounts a volume, and each `routes apply` step is given the component form
// of the three River DSNs (host, port, user, database as plain values, the
// password by secretKeyRef) for `dho workers` to assemble.
func TestRouteActivateRunsOnlyTheOperatorImage(t *testing.T) {
	jobs, _, refusal := renderJobs(t,
		"migrations.hook.provisionRoles.enabled=true", "migrations.hook.riverMigrate.enabled=true",
		"migrations.hook.routeActivate.enabled=true", "migrations.hook.routeActivate.image="+pinnedOperatorImage)
	if refusal != "" {
		t.Fatalf("render refused: %s", refusal)
	}
	pod := jobs["t-dev-health-route-activate"]["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	if _, has := pod["volumes"]; has {
		t.Fatalf("the pod declares volumes: %v", pod["volumes"])
	}
	steps := append(append([]any{}, pod["initContainers"].([]any)...), pod["containers"].([]any)...)
	if len(steps) != 5 {
		t.Fatalf("%d containers, want the four routes and the no-op main container", len(steps))
	}
	for _, raw := range steps {
		step := raw.(map[string]any)
		name := step["name"].(string)
		if step["image"] != pinnedOperatorImage {
			t.Errorf("%s image = %v, want the operator image", name, step["image"])
		}
		if _, has := step["command"]; has {
			t.Errorf("%s has a command override (a shell) the distroless image cannot run: %v", name, step["command"])
		}
		if _, has := step["volumeMounts"]; has {
			t.Errorf("%s mounts a volume: %v", name, step["volumeMounts"])
		}
	}
	for _, raw := range pod["initContainers"].([]any) {
		step := raw.(map[string]any)
		name := step["name"].(string)
		env := map[string]map[string]any{}
		for _, item := range step["env"].([]any) {
			entry := item.(map[string]any)
			env[entry["name"].(string)] = entry
		}
		if env["DEV_HEALTH_PG_DB"]["value"] != "devhealth" {
			t.Errorf("%s DEV_HEALTH_PG_DB = %v", name, env["DEV_HEALTH_PG_DB"])
		}
		for _, role := range []string{"DOMAIN", "QUEUE", "COORDINATOR"} {
			for _, part := range []string{"HOST", "PORT", "USER"} {
				if _, ok := env["DEV_HEALTH_PG_"+role+"_"+part]["value"]; !ok {
					t.Errorf("%s lacks DEV_HEALTH_PG_%s_%s as a plain value", name, role, part)
				}
			}
			password := env["DEV_HEALTH_PG_"+role+"_PASSWORD"]
			if _, inlined := password["value"]; inlined || password["valueFrom"] == nil {
				t.Errorf("%s DEV_HEALTH_PG_%s_PASSWORD must come from a secretKeyRef only: %v", name, role, password)
			}
		}
		for _, forbidden := range []string{"POSTGRES_URI", "WORKER_DATABASE_URI", "COORDINATOR_DATABASE_URI", "POSTGRES_URI_FILE", "WORKER_DATABASE_URI_FILE", "COORDINATOR_DATABASE_URI_FILE"} {
			if _, has := env[forbidden]; has {
				t.Errorf("%s sets %s: the component form and the URI form are mutually exclusive", name, forbidden)
			}
		}
	}
}
