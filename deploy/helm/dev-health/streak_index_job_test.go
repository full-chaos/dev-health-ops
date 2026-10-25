package devhealth_test

import (
	"reflect"
	"strings"
	"testing"
)

// The failure-streak index Job is OFF by default, so a prod render carries no such Job; ON it runs the
// build-index verb from the migrate image as a pre-hook that runs after the weight -5 ConfigMap and Secret it reads and
// before the weight-0 migrate Job, and a failed build fails the release before the migrate Job.
func TestStreakIndexJobIsOffByDefaultAndOrderedBeforeTheMigrateJob(t *testing.T) {
	base := []string{"web.enabled=false"}
	jobs, _, refusal := renderJobs(t, base...)
	if refusal != "" {
		t.Fatalf("render refused: %s", refusal)
	}
	if _, ok := jobs["t-dev-health-streak-index"]; ok {
		t.Fatal("the streak index Job renders with the flag off")
	}
	jobs, _, refusal = renderJobs(t, append(base, "migrations.hook.streakIndex.enabled=true")...)
	if refusal != "" {
		t.Fatalf("render refused: %s", refusal)
	}
	job, ok := jobs["t-dev-health-streak-index"]
	if !ok {
		t.Fatal("no streak index Job with the flag on")
	}
	annotations := job["metadata"].(map[string]any)["annotations"].(map[string]any)
	if annotations["helm.sh/hook"] != "pre-install,pre-upgrade" || annotations["helm.sh/hook-weight"] != "-2" {
		t.Fatalf("hook annotations = %v, want pre-install,pre-upgrade at weight -2", annotations)
	}
	spec := job["spec"].(map[string]any)
	if spec["backoffLimit"] != 0 || spec["activeDeadlineSeconds"] != 3600 {
		t.Fatalf("backoffLimit/activeDeadlineSeconds = %v/%v, want 0/3600", spec["backoffLimit"], spec["activeDeadlineSeconds"])
	}
	c := container(t, job, "streak-index")
	want := []string{"migrate", "postgres", "build-index", "--lock-timeout=5s", "--attempts=20", "--retry-delay=15s", "failure-streak"}
	if args := stringsOf(c["args"]); !reflect.DeepEqual(args, want) {
		t.Fatalf("args = %v, want %v", args, want)
	}
	if migrate := container(t, jobs["t-dev-health-migrate"], "migrate"); c["image"] != migrate["image"] {
		t.Fatalf("image = %v, the migrate Job runs %v", c["image"], migrate["image"])
	}
	if _, hasCommand := c["command"]; hasCommand {
		t.Fatal("the Job sets command; the dho image's entrypoint must run")
	}
}

func TestStreakIndexJobRefusesTheBundledPostgresAndBadBounds(t *testing.T) {
	for name, sets := range map[string][]string{
		"bundled postgres": {"web.enabled=false", "migrations.hook.streakIndex.enabled=true", "migrations.hook.localBundledPostgres=true",
			"migrations.hook.events={post-install,pre-upgrade}", "postgresql.enabled=true"},
		"lock timeout over a minute": {"web.enabled=false", "migrations.hook.streakIndex.enabled=true", "migrations.hook.streakIndex.lockTimeoutSeconds=61"},
		"null enabled":               {"web.enabled=false", "migrations.hook.streakIndex=null"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, refusal := renderJobs(t, sets...)
			if refusal == "" {
				t.Fatal("the render was not refused")
			}
			if !strings.Contains(refusal, "streakIndex") {
				t.Fatalf("the refusal does not name streakIndex: %.300s", refusal)
			}
		})
	}
}
