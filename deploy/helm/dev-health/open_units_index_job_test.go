package devhealth_test

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// renderedOpenUnitsIndexArgs is the args of the open-units-index hook container, rendered with the flag on.
func renderedOpenUnitsIndexArgs(t *testing.T, sets ...string) (args []string, env map[string]string) {
	t.Helper()
	jobs, secrets, refusal := renderJobs(t, append([]string{"web.enabled=false", "migrations.hook.openUnitsIndex.enabled=true"}, sets...)...)
	if refusal != "" {
		t.Fatalf("render refused: %s", refusal)
	}
	job := jobs["t-dev-health-open-units-index"]
	if job == nil {
		t.Fatal("the hook did not render")
	}
	c := container(t, job, "open-units-index")
	return stringsOf(c["args"]), podEnvironment(t, c, secrets)
}

// The open-units index Job is OFF by default and, ON, runs the build-index verb for open-units as a weight -2 pre-hook, so
// a failed build fails the release before the migrate Job runs revision 0160.
func TestOpenUnitsIndexJobIsOffByDefaultAndOrderedBeforeTheMigrateJob(t *testing.T) {
	base := []string{"web.enabled=false"}
	jobs, _, refusal := renderJobs(t, base...)
	if refusal != "" {
		t.Fatalf("render refused: %s", refusal)
	}
	if _, ok := jobs["t-dev-health-open-units-index"]; ok {
		t.Fatal("the open-units index Job renders with the flag off")
	}
	jobs, _, refusal = renderJobs(t, append(base, "migrations.hook.openUnitsIndex.enabled=true")...)
	if refusal != "" {
		t.Fatalf("render refused: %s", refusal)
	}
	job, ok := jobs["t-dev-health-open-units-index"]
	if !ok {
		t.Fatal("no open-units index Job with the flag on")
	}
	annotations := job["metadata"].(map[string]any)["annotations"].(map[string]any)
	if annotations["helm.sh/hook"] != "pre-install,pre-upgrade" || annotations["helm.sh/hook-weight"] != "-2" {
		t.Fatalf("hook annotations = %v, want pre-install,pre-upgrade at weight -2", annotations)
	}
	c := container(t, job, "open-units-index")
	want := []string{"migrate", "postgres", "build-index", "--lock-timeout=5s", "--deadline=240s", "--attempts=3", "--retry-delay=15s", "open-units"}
	if args := stringsOf(c["args"]); !reflect.DeepEqual(args, want) {
		t.Fatalf("args = %v, want %v", args, want)
	}
	if migrate := container(t, jobs["t-dev-health-migrate"], "migrate"); c["image"] != migrate["image"] {
		t.Fatalf("image = %v, the migrate Job runs %v", c["image"], migrate["image"])
	}
}

func TestOpenUnitsIndexJobRefusesBadBounds(t *testing.T) {
	for name, sets := range map[string][]string{
		"lock timeout over a minute": {"web.enabled=false", "migrations.hook.openUnitsIndex.enabled=true", "migrations.hook.openUnitsIndex.lockTimeoutSeconds=61"},
		"null enabled":               {"web.enabled=false", "migrations.hook.openUnitsIndex=null"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, refusal := renderJobs(t, sets...)
			if refusal == "" {
				t.Fatal("the render was not refused")
			}
			if !strings.Contains(refusal, "openUnitsIndex") {
				t.Fatalf("the refusal does not name openUnitsIndex: %.300s", refusal)
			}
		})
	}
}

// The rendered hook's arguments are accepted by the verb's own parser.
func TestRenderedOpenUnitsIndexHookArgumentsPassTheVerbParser(t *testing.T) {
	args, _ := renderedOpenUnitsIndexArgs(t)
	var stdout, stderr bytes.Buffer
	code := runDho(context.Background(), args, func(string) (string, bool) { return "", false }, &stdout, &stderr)
	if code == cli.ExitUsage || strings.Contains(stderr.String(), "argument error") {
		t.Fatalf("the rendered args %v were refused: exit %d: %s", args, code, stderr.String())
	}
}

func TestOpenUnitsIndexJobWithBundledPostgresIsPreUpgradeOnly(t *testing.T) {
	bundled := []string{"web.enabled=false", "migrations.hook.localBundledPostgres=true",
		"migrations.hook.events={post-install,pre-upgrade}", "postgresql.enabled=true"}
	jobs, _, refusal := renderJobs(t, bundled...)
	if refusal != "" {
		t.Fatalf("render refused: %s", refusal)
	}
	if _, ok := jobs["t-dev-health-open-units-index"]; ok {
		t.Fatal("the open-units index Job renders with the flag off")
	}
	jobs, _, refusal = renderJobs(t, append(bundled, "migrations.hook.openUnitsIndex.enabled=true")...)
	if refusal != "" {
		t.Fatalf("render refused: %s", refusal)
	}
	job, ok := jobs["t-dev-health-open-units-index"]
	if !ok {
		t.Fatal("no open-units index Job with the flag on")
	}
	annotations := job["metadata"].(map[string]any)["annotations"].(map[string]any)
	if annotations["helm.sh/hook"] != "pre-upgrade" || annotations["helm.sh/hook-weight"] != "-2" {
		t.Fatalf("hook annotations = %v, want pre-upgrade at weight -2", annotations)
	}
	wait := func(j map[string]any) map[string]any {
		inits := j["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["initContainers"].([]any)
		if len(inits) != 1 {
			t.Fatalf("initContainers = %d, want 1", len(inits))
		}
		return inits[0].(map[string]any)
	}
	got, want := wait(job), wait(jobs["t-dev-health-migrate"])
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("wait-for-postgresql differs from the migrate Job's:\n%v\n%v", got, want)
	}
	if args := stringsOf(container(t, job, "open-units-index")["args"]); args[len(args)-1] != "open-units" {
		t.Fatalf("args = %v", args)
	}
}

// The hook Job reads no Kubernetes API, so it mounts no service account token, as the migrate and streak Jobs do.
func TestOpenUnitsIndexJobDoesNotMountTheServiceAccountToken(t *testing.T) {
	jobs, _, refusal := renderJobs(t, "web.enabled=false", "migrations.hook.openUnitsIndex.enabled=true", "migrations.hook.streakIndex.enabled=true")
	if refusal != "" {
		t.Fatalf("render refused: %s", refusal)
	}
	for _, name := range []string{"t-dev-health-migrate", "t-dev-health-streak-index", "t-dev-health-open-units-index"} {
		job, ok := jobs[name]
		if !ok {
			t.Fatalf("no %s Job in the render", name)
		}
		pod := job["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
		if v, ok := pod["automountServiceAccountToken"].(bool); !ok || v {
			t.Errorf("%s does not set automountServiceAccountToken: false (got %v)", name, pod["automountServiceAccountToken"])
		}
	}
}
