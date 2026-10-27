package devhealth_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

const barrierScript = "../../scripts/auth-users-owned-barrier.sh"

// fakeBarrierKubectl records argv and answers `get <resource> -l ...component=<c>` from $FAKE_DIR/<resource>.<c>
// (component "all" when the selector has none). FAKE_FAIL=<resource>.<c> makes that one call exit 1.
const fakeBarrierKubectl = `#!/usr/bin/env bash
ALL="$*"
printf '%s\n' "$ALL" >>"${FAKE_DIR}/argv"
res=""; sel=""
while [ $# -gt 0 ]; do
  case "$1" in
  get) res="$2"; shift ;;
  -l) sel="$2"; shift ;;
  esac
  shift
done
comp=all
case "$sel" in *component=*) comp="${sel##*component=}" ;; esac
key="${res}.${comp}"
if [ "${FAKE_HANG:-}" = "$key" ]; then
  # a hung API call: a real kubectl gives up after --request-timeout, and waits for ever without it
  case "$ALL" in *--request-timeout=*) sleep 1; exit 1 ;; esac
  sleep 30
fi
[ "${FAKE_FAIL:-}" = "$key" ] && exit 1
[ -f "${FAKE_DIR}/${key}" ] && cat "${FAKE_DIR}/${key}"
exit 0
`

func goodBarrierWorld() map[string]string {
	return map[string]string{
		"deploy.go-api":       "3 3 3 3 3 3\n",
		"rs.go-api":           "rs-old||4\nrs-new|3|5\n",
		"pods.go-api":         "p1||true|true\np2||true|true\np3||true|true\n",
		"pods.all":            "p1|go-api|[\"api\"];||\nm1|migrate|[\"migrate\"];||\n",
		"jobs.all":            "",
		"cronjobs.all":        "",
		"pods.auth-service":   "a1|\na2|\n",
		"deploy.auth-service": "2 2 2 2 2 2\n",
		"rs.auth-service":     "as-old||7\nas-new|2|8\n",
	}
}

type barrierRun struct {
	rc             int
	stdout, stderr string
	argv           string
	// deadline is true when the test deadline killed the run instead of the script ending on its own.
	deadline bool
}

func runBarrier(t *testing.T, world map[string]string, env []string, args ...string) barrierRun {
	t.Helper()
	return runBarrierWithin(t, 60*time.Second, world, env, args...)
}

func runBarrierWithin(t *testing.T, limit time.Duration, world map[string]string, env []string, args ...string) barrierRun {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	dir, bin := t.TempDir(), t.TempDir()
	for name, text := range world {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte(fakeBarrierKubectl), 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, "bash", append([]string{barrierScript}, args...)...)
	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "FAKE_DIR="+dir, "RELEASE=a")
	cmd.Env = append(cmd.Env, env...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	cmd.WaitDelay = 2 * time.Second
	err := cmd.Run()
	rc := 0
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			rc = exit.ExitCode()
		} else if ctx.Err() != nil {
			rc = -1
		} else {
			t.Fatalf("run: %v", err)
		}
	}
	argv, _ := os.ReadFile(filepath.Join(dir, "argv"))
	return barrierRun{rc, out.String(), errb.String(), string(argv), ctx.Err() != nil}
}

func TestBarrierPassesAndPrintsTheTime(t *testing.T) {
	for _, mode := range []string{"up-go-api", "up-owned", "down-auth"} {
		r := runBarrier(t, goodBarrierWorld(), nil, mode)
		if r.rc != 0 || !regexp.MustCompile(`(?m)^barrier_ok mode=`+mode+` time=\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$`).MatchString(r.stdout) {
			t.Errorf("%s: rc=%d stdout=%q stderr=%q", mode, r.rc, r.stdout, r.stderr)
		}
	}
}

func TestBarrierPlanRunsNothing(t *testing.T) {
	r := runBarrier(t, goodBarrierWorld(), nil, "up-go-api", "--plan")
	if r.rc != 0 || r.argv != "" || !strings.Contains(r.stdout, "plan:") {
		t.Errorf("--plan: rc=%d argv=%q stdout=%q", r.rc, r.argv, r.stdout)
	}
}

func TestBarrierRefusesBadUsage(t *testing.T) {
	if r := runBarrier(t, goodBarrierWorld(), nil); r.rc == 0 {
		t.Error("no mode passed")
	}
	if r := runBarrier(t, goodBarrierWorld(), nil, "sideways"); r.rc == 0 {
		t.Error("unknown mode passed")
	}
	if r := runBarrier(t, goodBarrierWorld(), []string{"RELEASE="}, "up-go-api"); r.rc == 0 {
		t.Error("empty RELEASE passed")
	}
}

func TestBarrierFailsOnEachBrokenState(t *testing.T) {
	cases := []struct {
		name   string
		mode   string
		mutate func(w map[string]string)
	}{
		{"old ReplicaSet still has a pod", "up-go-api", func(w map[string]string) { w["rs.go-api"] = "rs-old|1|4\nrs-new|3|5\n" }},
		{"old ReplicaSet newer revision number only", "up-go-api", func(w map[string]string) { w["rs.go-api"] = "rs-a|2|9\nrs-b|2|10\n" }},
		{"one pod lacks AUTH_USERS_OWNED", "up-go-api", func(w map[string]string) { w["pods.go-api"] = "p1||true|true\np2|||true\np3||true|true\n" }},
		{"one pod has AUTH_USERS_OWNED false", "up-go-api", func(w map[string]string) { w["pods.go-api"] = "p1||true|true\np2||true|true\np3||false|true\n" }},
		{"the last pod lacks AUTH_PROXY_CREDENTIALS", "up-go-api", func(w map[string]string) { w["pods.go-api"] = "p1||true|true\np2||true|true\np3||true|\n" }},
		{"a terminating pod", "up-go-api", func(w map[string]string) { w["pods.go-api"] += "p4|2026-10-26T15:00:00Z|true|true\n" }},
		{"no go-api pod", "up-go-api", func(w map[string]string) { w["pods.go-api"] = "" }},
		{"deployment mid-roll", "up-go-api", func(w map[string]string) { w["deploy.go-api"] = "3 3 3 2 2 3\n" }},
		{"observedGeneration behind", "up-go-api", func(w map[string]string) { w["deploy.go-api"] = "4 3 3 3 3 3\n" }},
		{"no Deployment", "up-go-api", func(w map[string]string) { w["deploy.go-api"] = "" }},
		{"an admin users Job", "up-go-api", func(w map[string]string) {
			w["pods.all"] += "j1|operator|[\"admin\",\"users\",\"create\"];||\n"
		}},
		{"an auth-service pod already owned", "up-owned", func(w map[string]string) { w["pods.auth-service"] = "a1|true\na2|\n" }},
		{"an owned auth-service pod on the way down", "down-auth", func(w map[string]string) { w["pods.auth-service"] = "a1|true\na2|\n" }},
		{"old auth-service ReplicaSet alive on the way down", "down-auth", func(w map[string]string) { w["rs.auth-service"] = "as-old|1|7\nas-new|2|8\n" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			world := goodBarrierWorld()
			c.mutate(world)
			r := runBarrier(t, world, nil, c.mode)
			if r.rc == 0 || strings.Contains(r.stdout, "barrier_ok") {
				t.Fatalf("passed: rc=%d stdout=%q stderr=%q", r.rc, r.stdout, r.stderr)
			}
		})
	}
}

// A kubectl error is never a pass, whichever call it hits.
func TestBarrierFailsClosedWhenKubectlFails(t *testing.T) {
	for _, key := range []string{"deploy.go-api", "rs.go-api", "pods.go-api", "pods.all", "jobs.all", "cronjobs.all"} {
		r := runBarrier(t, goodBarrierWorld(), []string{"FAKE_FAIL=" + key}, "up-go-api")
		if r.rc == 0 || strings.Contains(r.stdout, "barrier_ok") {
			t.Errorf("kubectl %s failing passed: rc=%d", key, r.rc)
		}
	}
	for _, key := range []string{"deploy.auth-service", "rs.auth-service", "pods.auth-service"} {
		r := runBarrier(t, goodBarrierWorld(), []string{"FAKE_FAIL=" + key}, "down-auth")
		if r.rc == 0 || strings.Contains(r.stdout, "barrier_ok") {
			t.Errorf("kubectl %s failing passed: rc=%d", key, r.rc)
		}
	}
}

// Read-only: every call is `get ... -o jsonpath=...`, never a Secret, never a write, exec or describe.
func TestBarrierCallsOnlyKubectlGetJSONPath(t *testing.T) {
	for _, mode := range []string{"up-go-api", "up-owned", "down-auth"} {
		r := runBarrier(t, goodBarrierWorld(), nil, mode)
		lines := strings.Split(strings.TrimSpace(r.argv), "\n")
		if len(lines) < 3 {
			t.Fatalf("%s: kubectl called %d times", mode, len(lines))
		}
		for _, line := range lines {
			if !regexp.MustCompile(`^-n dev-health --request-timeout=30s get (deploy|rs|pods|jobs|cronjobs) (-l app\.kubernetes\.io/instance=a\S* )?-o jsonpath=`).MatchString(line) {
				t.Errorf("%s: kubectl call outside the read-only shape: %s", mode, line)
			}
			if regexp.MustCompile(`\b(secret|secrets|exec|describe|apply|patch|replace|delete|logs|env)\b`).MatchString(strings.SplitN(line, "-o jsonpath=", 2)[0]) {
				t.Errorf("%s: forbidden word in %s", mode, line)
			}
		}
	}
}

// Every kubectl call carries --request-timeout=30s, so a hung API call ends the run instead of holding it.
func TestBarrierEveryKubectlCallHasARequestTimeout(t *testing.T) {
	for _, mode := range []string{"up-go-api", "up-owned", "down-auth"} {
		r := runBarrier(t, goodBarrierWorld(), nil, mode)
		for _, line := range strings.Split(strings.TrimSpace(r.argv), "\n") {
			if !strings.Contains(line, " --request-timeout=30s ") {
				t.Errorf("%s: kubectl call without --request-timeout=30s: %s", mode, line)
			}
		}
	}
}

// A hung kubectl call fails closed within the request timeout: non-zero exit, no barrier_ok, and not a deadline kill.
func TestBarrierFailsClosedWhenKubectlHangs(t *testing.T) {
	for _, key := range []string{"deploy.go-api", "rs.go-api", "pods.go-api", "pods.all"} {
		r := runBarrierWithin(t, 15*time.Second, goodBarrierWorld(), []string{"FAKE_HANG=" + key}, "up-go-api")
		if r.deadline {
			t.Errorf("kubectl %s hung: the test deadline killed the run (no --request-timeout)", key)
		}
		if r.rc == 0 || strings.Contains(r.stdout, "barrier_ok") {
			t.Errorf("kubectl %s hanging passed: rc=%d", key, r.rc)
		}
	}
}

// The admin users scan reads every container of every pod, Job and CronJob template in the namespace (no release label:
// an ad-hoc pod has none). One segment per container holds that container's command and args as kubectl prints them
// (JSON arrays, `;` after each container); fields: name|component|containers|initContainers|ephemeralContainers.
func TestBarrierRefusesAdminUsersInEveryContainerShape(t *testing.T) {
	rows := map[string]string{
		"command only":           "x1|operator|[\"dho\",\"admin\",\"users\",\"create\"];||\n",
		"args only":              "x1|operator|[\"admin\",\"users\",\"create\"];||\n",
		"command and args split": "x1|operator|[\"dho\"][\"admin\",\"users\",\"create\"];||\n",
		"sh -c one string":       "x1|operator|[\"sh\",\"-c\"][\"dho admin users create x\"];||\n",
		"any users subcommand":   "x1|operator|[\"dho\",\"admin\",\"users\",\"list\"];||\n",
		"init container":         "x1|operator||[\"dho\",\"admin\",\"users\",\"create\"];|\n",
		"ephemeral container":    "x1|operator|||[\"dho\",\"admin\",\"users\",\"create\"];\n",
		"second container":       "x1|operator|[\"sleep\"];[\"dho\",\"admin\",\"users\",\"create\"];||\n",
	}
	for name, row := range rows {
		t.Run(name, func(t *testing.T) {
			world := goodBarrierWorld()
			world["pods.all"] += row
			r := runBarrier(t, world, nil, "up-go-api")
			if r.rc == 0 || strings.Contains(r.stdout, "barrier_ok") {
				t.Fatalf("passed: rc=%d stdout=%q stderr=%q", r.rc, r.stdout, r.stderr)
			}
		})
	}
}

func TestBarrierAdminUsersInJobAndCronJobTemplates(t *testing.T) {
	for _, key := range []string{"jobs.all", "cronjobs.all"} {
		for _, row := range []string{
			"t1|-|[\"dho\",\"admin\",\"users\",\"create\"];||\n",
			"t1|-|[\"sh\",\"-c\"][\"dho admin users create\"];||\n",
			"t1|-||[\"dho\",\"admin\",\"users\",\"create\"];|\n",
		} {
			world := goodBarrierWorld()
			world[key] += row
			r := runBarrier(t, world, nil, "up-go-api")
			if r.rc == 0 || strings.Contains(r.stdout, "barrier_ok") {
				t.Errorf("%s %q passed: rc=%d stdout=%q", key, row, r.rc, r.stdout)
			}
		}
	}
}

// Controls: none of these is an admin users verb in one container of a pod that is not go-api or auth-service.
func TestBarrierAllowsWhatIsNotAnAdminUsersVerb(t *testing.T) {
	rows := map[string]string{
		"admin and users in different containers": "x1|operator|[\"admin\"];[\"users\"];||\n",
		"the verb in a go-api pod":                "x1|go-api|[\"dho\",\"admin\",\"users\",\"create\"];||\n",
		"the verb in an auth-service pod":         "x1|auth-service|[\"dho\",\"admin\",\"users\",\"create\"];||\n",
		"words that only contain the names":       "x1|operator|[\"administer\",\"users2\"];||\n",
		"a hyphenated users name":                 "x1|operator|[\"dho\",\"admin\",\"users-export\"];||\n",
		"an unrelated command":                    "x1|operator|[\"dho\",\"worker\"];||\n",
	}
	for name, row := range rows {
		t.Run(name, func(t *testing.T) {
			world := goodBarrierWorld()
			world["pods.all"] += row
			r := runBarrier(t, world, nil, "up-go-api")
			if r.rc != 0 || !strings.Contains(r.stdout, "barrier_ok") {
				t.Fatalf("refused: rc=%d stdout=%q stderr=%q", r.rc, r.stdout, r.stderr)
			}
		})
	}
}
