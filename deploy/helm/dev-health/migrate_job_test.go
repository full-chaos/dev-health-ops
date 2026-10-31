package devhealth_test

import (
	"bytes"
	"io"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	platformconfig "github.com/full-chaos/dev-health-ops/internal/platform/config"
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
const lockstepHook = "migrations.hook.image=ghcr.io/full-chaos/dev-health-go-operator:sha-aaaaaaaaaaaa"

func TestMigrateJobRefusesImagesThatCannotRunTheVerb(t *testing.T) {
	for name, testCase := range map[string]struct {
		sets []string
		want string
	}{
		"the Python image":           {[]string{"migrations.hook.image=ghcr.io/full-chaos/dev-hops-api@sha256:" + strings.Repeat("b", 64)}, "is the Python image"},
		"an unpinned operator image": {[]string{"migrations.hook.routeActivate.image=ghcr.io/full-chaos/dev-health-go-operator:latest"}, "is not a pinned dho image"},
		// CHAOS-8309: the lockstep targets are the Go serving images, each enabled one on its own clause;
		// with both enabled the hook tag must equal BOTH (the last two cases flip one clause at a time).
		// CHAOS-7520: the Python image clause is gone with .Values.image (TestMigrateJobIgnoresAStalePythonImageTag).
		"a different commit than queryApi.image":         {[]string{lockstepHook, "queryApi.enabled=true", "queryApi.image.tag=sha-bbbbbbbbbbbb"}, "and queryApi.image (ghcr.io/full-chaos/dev-health-go-dho:sha-bbbbbbbbbbbb) are pinned to different commits"},
		"a different commit than goApi.image":            {[]string{lockstepHook, "goApi.enabled=true", "goApi.image.tag=sha-bbbbbbbbbbbb"}, "and goApi.image (ghcr.io/full-chaos/dev-health-go-dho:sha-bbbbbbbbbbbb) are pinned to different commits"},
		"queryApi matches, goApi differs (both enabled)": {[]string{lockstepHook, "queryApi.enabled=true", "queryApi.image.tag=sha-aaaaaaaaaaaa", "goApi.enabled=true", "goApi.image.tag=sha-bbbbbbbbbbbb"}, "and goApi.image (ghcr.io/full-chaos/dev-health-go-dho:sha-bbbbbbbbbbbb) are pinned to different commits"},
		"goApi matches, queryApi differs (both enabled)": {[]string{lockstepHook, "queryApi.enabled=true", "queryApi.image.tag=sha-bbbbbbbbbbbb", "goApi.enabled=true", "goApi.image.tag=sha-aaaaaaaaaaaa"}, "and queryApi.image (ghcr.io/full-chaos/dev-health-go-dho:sha-bbbbbbbbbbbb) are pinned to different commits"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, refusal := renderJobs(t, testCase.sets...)
			if !strings.Contains(refusal, testCase.want) {
				t.Fatalf("render = %q, want a refusal containing %q", refusal, testCase.want)
			}
		})
	}
}

// CHAOS-7520: a values file written before the Python api image values were deleted still sets image.tag; the lockstep
// check no longer reads it, so a hook tag that differs from it renders.
func TestMigrateJobIgnoresAStalePythonImageTag(t *testing.T) {
	_, _, refusal := renderJobs(t, lockstepHook, "image.tag=sha-bbbbbbbbbbbb")
	if refusal != "" {
		t.Fatalf("a stale image.tag must not refuse the render: %s", refusal)
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

// CHAOS-8309: each row states one semantic no refusal case can see fail: a digest or a disabled component is not
// compared; a serving image without a sha tag is not compared. (CHAOS-7520: a Python image tag is no
// longer read; see TestMigrateJobIgnoresAStalePythonImageTag.)
func TestMigrateJobLockstepAcceptsMatchingGoImagesAndSkipsWhatItCannotCompare(t *testing.T) {
	const digest = "ghcr.io/full-chaos/dev-health-go-dho@sha256:" + "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	for name, sets := range map[string][]string{
		"queryApi by digest (never compared), hook tag set":                     {lockstepHook, "queryApi.enabled=true", "queryApi.image.repository=" + digest},
		"hook by digest, queryApi has a sha tag (never compared)":               {"migrations.hook.image=" + digest, "queryApi.enabled=true", "queryApi.image.tag=sha-bbbbbbbbbbbb"},
		"a disabled component with another tag is not a target":                 {lockstepHook, "goApi.enabled=true", "goApi.image.tag=sha-aaaaaaaaaaaa", "queryApi.image.tag=sha-bbbbbbbbbbbb"},
		"queryApi matches, a disabled goApi has another tag":                    {lockstepHook, "queryApi.enabled=true", "queryApi.image.tag=sha-aaaaaaaaaaaa", "goApi.image.tag=sha-bbbbbbbbbbbb"},
		"a goApi image without a sha tag (chart default tag) is not compared":   {lockstepHook, "goApi.enabled=true"},
		"a serving image without a sha tag (chart default tag) is not compared": {lockstepHook, "queryApi.enabled=true"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, refusal := renderJobs(t, sets...); refusal != "" {
				t.Fatalf("render refused: %q", refusal)
			}
		})
	}
}

// CHAOS-8744: migrations.hook.classDecisionLiveSchemaDigest reaches the
// migrate Job as DHO_CLASS_DECISION_LIVE_SCHEMA_DIGEST only when it is set;
// a value that is not sha256:<64 hex> is refused.
func TestMigrateJobClassDecisionLiveSchemaDigest(t *testing.T) {
	digest := "sha256:" + strings.Repeat("b", 64)
	for _, testCase := range []struct {
		name    string
		sets    []string
		want    string
		present bool
	}{
		{"unset: no env", nil, "", false},
		{"set: the env carries the value", []string{"migrations.hook.classDecisionLiveSchemaDigest=" + digest}, digest, true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			jobs, _, refusal := renderJobs(t, testCase.sets...)
			if refusal != "" {
				t.Fatalf("render refused: %s", refusal)
			}
			entry, ok := envOf(container(t, jobs["t-dev-health-migrate"], "migrate"))["DHO_CLASS_DECISION_LIVE_SCHEMA_DIGEST"].(map[string]any)
			if ok != testCase.present {
				t.Fatalf("env DHO_CLASS_DECISION_LIVE_SCHEMA_DIGEST present = %v, want %v", ok, testCase.present)
			}
			if ok && entry["value"] != testCase.want {
				t.Fatalf("env DHO_CLASS_DECISION_LIVE_SCHEMA_DIGEST = %v, want %q", entry["value"], testCase.want)
			}
		})
	}
	for _, bad := range []string{"sha256:abc", "fdff794c3fa3", "sha256:" + strings.Repeat("B", 64)} {
		t.Run("refused "+bad, func(t *testing.T) {
			_, _, refusal := renderJobs(t, "migrations.hook.classDecisionLiveSchemaDigest="+bad)
			if !strings.Contains(refusal, "classDecisionLiveSchemaDigest") {
				t.Fatalf("render of %q was not refused by name: %q", bad, refusal)
			}
		})
	}
}

// CHAOS-10754 / CHAOS-11995: migrations.hook.clickhouseCloud gives the
// migrate Job the Cloud login in dho's component form: host, user and password
// from three secretKeyRefs, port, database and TLS as values, and a blank
// CLICKHOUSE_URI over the envFrom key of the same name. Off, the render
// carries none of it.
func TestMigrateJobClickHouseCloudDSN(t *testing.T) {
	const cloud = "migrations.hook.clickhouseCloud."
	ref := func(t *testing.T, env map[string]any, name string) (string, string) {
		t.Helper()
		entry, ok := env[name].(map[string]any)
		if !ok {
			t.Fatalf("env %s missing", name)
		}
		if _, literal := entry["value"]; literal {
			t.Fatalf("env %s carries a literal value, want a secretKeyRef", name)
		}
		secret := entry["valueFrom"].(map[string]any)["secretKeyRef"].(map[string]any)
		return secret["name"].(string), secret["key"].(string)
	}
	literal := func(t *testing.T, env map[string]any, name string) string {
		t.Helper()
		entry, ok := env[name].(map[string]any)
		if !ok {
			t.Fatalf("env %s missing", name)
		}
		if _, from := entry["valueFrom"]; from {
			t.Fatalf("env %s is a reference, want a literal value", name)
		}
		value, ok := entry["value"].(string)
		if !ok {
			t.Fatalf("env %s has no string value: %v", name, entry["value"])
		}
		return value
	}
	envNames := []string{"DEV_HEALTH_CH_HOST", "DEV_HEALTH_CH_PORT", "DEV_HEALTH_CH_USER", "DEV_HEALTH_CH_PASSWORD", "DEV_HEALTH_CH_DB", "DEV_HEALTH_CH_SECURE", "CLICKHOUSE_URI"}

	t.Run("off by default: none of it renders", func(t *testing.T) {
		for _, sets := range [][]string{nil, {cloud + "enabled=false", cloud + "secretName=ch-cloud-default"}} {
			jobs, _, refusal := renderJobs(t, sets...)
			if refusal != "" {
				t.Fatalf("render refused: %s", refusal)
			}
			env := envOf(container(t, jobs["t-dev-health-migrate"], "migrate"))
			for _, name := range envNames {
				if _, present := env[name]; present {
					t.Fatalf("sets %v: env %s present with the block off", sets, name)
				}
			}
		}
	})

	t.Run("off: an explicit extraEnv CLICKHOUSE_URI is the only DSN source and still renders", func(t *testing.T) {
		jobs, _, refusal := renderJobs(t, "migrations.hook.extraEnv[0].name=CLICKHOUSE_URI", "migrations.hook.extraEnv[0].value=clickhouse://u:p@h:9000/db")
		if refusal != "" {
			t.Fatalf("render refused: %s", refusal)
		}
		entry, _ := envOf(container(t, jobs["t-dev-health-migrate"], "migrate"))["CLICKHOUSE_URI"].(map[string]any)
		if entry["value"] != "clickhouse://u:p@h:9000/db" {
			t.Fatalf("CLICKHOUSE_URI = %v", entry["value"])
		}
	})

	t.Run("on: three secretKeyRefs, port, database, TLS and a blank CLICKHOUSE_URI", func(t *testing.T) {
		jobs, secrets, refusal := renderJobs(t, cloud+"enabled=true", cloud+"secretName=ch-cloud-default")
		if refusal != "" {
			t.Fatalf("render refused: %s", refusal)
		}
		job := jobs["t-dev-health-migrate"]
		env := envOf(container(t, job, "migrate"))
		for name, wantKey := range map[string]string{"DEV_HEALTH_CH_HOST": "CH_CLOUD_HOST", "DEV_HEALTH_CH_USER": "CH_CLOUD_USER", "DEV_HEALTH_CH_PASSWORD": "CH_CLOUD_PASSWORD"} {
			if secret, key := ref(t, env, name); secret != "ch-cloud-default" || key != wantKey {
				t.Fatalf("env %s = secretKeyRef %s/%s, want ch-cloud-default/%s", name, secret, key, wantKey)
			}
		}
		for name, want := range map[string]string{"DEV_HEALTH_CH_PORT": "9440", "DEV_HEALTH_CH_DB": "default", "DEV_HEALTH_CH_SECURE": "true", "CLICKHOUSE_URI": ""} {
			if got := literal(t, env, name); got != want {
				t.Fatalf("env %s = %q, want %q", name, got, want)
			}
		}
		for _, item := range container(t, job, "migrate")["env"].([]any) {
			if value, _ := item.(map[string]any)["value"].(string); strings.Contains(value, "$(") {
				t.Fatalf("env %v still expands a $(...) reference", item)
			}
		}
		if got, _ := secrets["t-dev-health-migrate-secrets"]["stringData"].(map[string]any)["CLICKHOUSE_URI"].(string); strings.Contains(got, "secure=true") {
			t.Fatalf("the chart-made migrate Secret must stay unchanged, got %q", got)
		}
	})

	t.Run("on: the rendered env resolves through dho to the exact Cloud login over TLS, any password", func(t *testing.T) {
		jobs, _, refusal := renderJobs(t, cloud+"enabled=true", cloud+"secretName=ch-cloud-default")
		if refusal != "" {
			t.Fatalf("render refused: %s", refusal)
		}
		env := envOf(container(t, jobs["t-dev-health-migrate"], "migrate"))
		for _, password := range []string{"pw#frag", "pw@at:colon/slash?q", "pw%zz&amp+plus=eq", "pw space'quote"} {
			secret := map[string]string{"CH_CLOUD_HOST": "abc123.eu-west-1.aws.clickhouse.cloud", "CH_CLOUD_USER": "default", "CH_CLOUD_PASSWORD": password}
			// The Pod environment: the envFrom runtime DSN first, then the env entries (an entry wins).
			pod := map[string]string{"CLICKHOUSE_URI": "clickhouse://dho_app:runtime@ch.internal:9000/default"}
			for name, raw := range env {
				entry := raw.(map[string]any)
				if value, ok := entry["value"].(string); ok {
					pod[name] = value
					continue
				}
				if from, ok := entry["valueFrom"].(map[string]any); ok {
					if ref, ok := from["secretKeyRef"].(map[string]any); ok && ref["name"] == "ch-cloud-default" {
						pod[name] = secret[ref["key"].(string)]
					}
				}
			}
			lookup := func(key string) (string, bool) { value, ok := pod[key]; return value, ok }
			dsn, configured, err := platformconfig.ResolveDSN(lookup, "CLICKHOUSE_URI", platformconfig.ClickHouseSpec)
			if err != nil || !configured {
				t.Fatalf("dho does not resolve the rendered env: configured=%v err=%v", configured, err)
			}
			options, err := clickhouse.ParseDSN(dsn.Reveal())
			if err != nil {
				t.Fatalf("the driver refused the DSN dho built: %v", err)
			}
			if options.Auth.Password != password || options.Auth.Username != "default" || options.Auth.Database != "default" {
				t.Fatalf("the driver reads a different login back (user %q db %q, password len %d want %d)", options.Auth.Username, options.Auth.Database, len(options.Auth.Password), len(password))
			}
			if len(options.Addr) != 1 || options.Addr[0] != "abc123.eu-west-1.aws.clickhouse.cloud:9440" || options.TLS == nil {
				t.Fatalf("addr %v tls %v, want the one Cloud endpoint on 9440 over TLS", options.Addr, options.TLS != nil)
			}
		}
	})

	t.Run("on: custom keys, port and database flow through", func(t *testing.T) {
		jobs, _, refusal := renderJobs(t, cloud+"enabled=true", cloud+"secretName=s", cloud+"hostKey=H", cloud+"userKey=U", cloud+"passwordKey=P", cloud+"port=8443", cloud+"database=analytics")
		if refusal != "" {
			t.Fatalf("render refused: %s", refusal)
		}
		env := envOf(container(t, jobs["t-dev-health-migrate"], "migrate"))
		for name, wantKey := range map[string]string{"DEV_HEALTH_CH_HOST": "H", "DEV_HEALTH_CH_USER": "U", "DEV_HEALTH_CH_PASSWORD": "P"} {
			if secret, key := ref(t, env, name); secret != "s" || key != wantKey {
				t.Fatalf("env %s = %s/%s, want s/%s", name, secret, key, wantKey)
			}
		}
		if got := literal(t, env, "DEV_HEALTH_CH_PORT"); got != "8443" {
			t.Fatalf("DEV_HEALTH_CH_PORT = %q, want 8443", got)
		}
		if got := literal(t, env, "DEV_HEALTH_CH_DB"); got != "analytics" {
			t.Fatalf("DEV_HEALTH_CH_DB = %q, want analytics", got)
		}
	})

	refused := map[string][]string{
		"empty secretName":            {cloud + "enabled=true"},
		"port zero":                   {cloud + "enabled=true", cloud + "secretName=s", cloud + "port=0"},
		"port too large":              {cloud + "enabled=true", cloud + "secretName=s", cloud + "port=65536"},
		"port not a number":           {cloud + "enabled=true", cloud + "secretName=s", cloud + "port=abc"},
		"database with a slash":       {cloud + "enabled=true", cloud + "secretName=s", cloud + "database=a/b"},
		"database with a query":       {cloud + "enabled=true", cloud + "secretName=s", cloud + "database=a?x=1"},
		"hostKey outside the key set": {cloud + "enabled=true", cloud + "secretName=s", cloud + "hostKey=H K"},
		"empty passwordKey":           {cloud + "enabled=true", cloud + "secretName=s", cloud + "passwordKey="},
	}
	for _, key := range []string{"CLICKHOUSE_URI", "CLICKHOUSE_URI_FILE", "DEV_HEALTH_CH_HOST", "DEV_HEALTH_CH_PORT", "DEV_HEALTH_CH_USER", "DEV_HEALTH_CH_USER_FILE", "DEV_HEALTH_CH_PASSWORD", "DEV_HEALTH_CH_PASSWORD_FILE", "DEV_HEALTH_CH_DB", "DEV_HEALTH_CH_SECURE"} {
		refused["extraEnv "+key] = []string{cloud + "enabled=true", cloud + "secretName=s", "migrations.hook.extraEnv[0].name=" + key, "migrations.hook.extraEnv[0].value=x"}
		refused["hook secretData "+key] = []string{cloud + "enabled=true", cloud + "secretName=s", "migrations.hook.secretData." + key + "=x"}
	}
	for name, sets := range refused {
		t.Run("refused: "+name, func(t *testing.T) {
			_, _, refusal := renderJobs(t, sets...)
			if !strings.Contains(refusal, "clickhouseCloud") {
				t.Fatalf("render was not refused by name: %q", refusal)
			}
		})
	}
}
