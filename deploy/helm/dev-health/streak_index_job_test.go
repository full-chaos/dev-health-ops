package devhealth_test

import (
	"reflect"
	"strconv"
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
	if spec["backoffLimit"] != 0 || spec["activeDeadlineSeconds"] != 2400 {
		t.Fatalf("backoffLimit/activeDeadlineSeconds = %v/%v, want 0/2400", spec["backoffLimit"], spec["activeDeadlineSeconds"])
	}
	c := container(t, job, "streak-index")
	want := []string{"migrate", "postgres", "build-index", "--lock-timeout=5s", "--deadline=1800s", "--attempts=3", "--retry-delay=15s", "failure-streak"}
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

func TestStreakIndexJobRefusesBadBounds(t *testing.T) {
	for name, sets := range map[string][]string{
		"lock timeout over a minute": {"web.enabled=false", "migrations.hook.streakIndex.enabled=true", "migrations.hook.streakIndex.lockTimeoutSeconds=61"},
		"null enabled":               {"web.enabled=false", "migrations.hook.streakIndex=null"},
		"deadline over the job":      {"web.enabled=false", "migrations.hook.streakIndex.enabled=true", "migrations.hook.streakIndex.deadlineSeconds=3300"},
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

// With the bundled PostgreSQL the Job runs on pre-upgrade only (a fresh install builds the index in revision 0159 on an
// empty table), waits for the database like the migrate Job, and the flag off renders no Job.
func TestStreakIndexJobWithBundledPostgresIsPreUpgradeOnly(t *testing.T) {
	bundled := []string{"web.enabled=false", "migrations.hook.localBundledPostgres=true",
		"migrations.hook.events={post-install,pre-upgrade}", "postgresql.enabled=true"}
	jobs, _, refusal := renderJobs(t, bundled...)
	if refusal != "" {
		t.Fatalf("render refused: %s", refusal)
	}
	if _, ok := jobs["t-dev-health-streak-index"]; ok {
		t.Fatal("the streak index Job renders with the flag off")
	}
	jobs, _, refusal = renderJobs(t, append(bundled, "migrations.hook.streakIndex.enabled=true")...)
	if refusal != "" {
		t.Fatalf("render refused: %s", refusal)
	}
	job, ok := jobs["t-dev-health-streak-index"]
	if !ok {
		t.Fatal("no streak index Job with the flag on")
	}
	annotations := job["metadata"].(map[string]any)["annotations"].(map[string]any)
	if annotations["helm.sh/hook"] != "pre-upgrade" || annotations["helm.sh/hook-weight"] != "-2" {
		t.Fatalf("hook annotations = %v, want pre-upgrade at weight -2", annotations)
	}
	if strings.Contains(annotations["helm.sh/hook"].(string), "post-install") {
		t.Fatal("the streak index Job carries post-install")
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
	if args := stringsOf(container(t, job, "streak-index")["args"]); args[len(args)-1] != "failure-streak" {
		t.Fatalf("args = %v", args)
	}
}

// The kit pins helm --timeout 90m (roll 24b PLAN-ROLL-24b.md, "./dev-health ... --timeout 90m"): the streak-index hook runs
// first, the migrate Job (weight 0) after it, and the rollout needs a margin. The chart defaults must fit all three.
func TestStreakIndexHookAndMigrateJobFitTheHelmTimeout(t *testing.T) {
	const helmTimeoutSeconds, rolloutMarginSeconds, startupAndCleanupSeconds = 5400, 600, 360
	jobs, _, refusal := renderJobs(t, "web.enabled=false", "migrations.hook.streakIndex.enabled=true")
	if refusal != "" {
		t.Fatalf("render refused: %s", refusal)
	}
	hook := jobs["t-dev-health-streak-index"]["spec"].(map[string]any)["activeDeadlineSeconds"].(int)
	migrate := jobs["t-dev-health-migrate"]["spec"].(map[string]any)["activeDeadlineSeconds"].(int)
	if hook+migrate+rolloutMarginSeconds > helmTimeoutSeconds {
		t.Fatalf("hook %d + migrate %d + rollout margin %d exceeds helm --timeout %d", hook, migrate, rolloutMarginSeconds, helmTimeoutSeconds)
	}
	args := stringsOf(container(t, jobs["t-dev-health-streak-index"], "streak-index")["args"])
	var window int
	for _, a := range args {
		if v, ok := strings.CutPrefix(a, "--deadline="); ok {
			n, err := strconv.Atoi(strings.TrimSuffix(v, "s"))
			if err != nil {
				t.Fatalf("bad --deadline %q", a)
			}
			window = n
		}
	}
	if window == 0 || window+startupAndCleanupSeconds > hook {
		t.Fatalf("build window %d + startup and cleanup %d exceeds the hook's activeDeadlineSeconds %d", window, startupAndCleanupSeconds, hook)
	}
}
