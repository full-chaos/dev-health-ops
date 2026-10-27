package scripts_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// A stub kubectl for the step-6 pod pick of ci/check_auth_k8s.sh, in the state just after the step-5 upgrade, pods
// listed by name as the API does: a terminating and a live Ready pod of the old ReplicaSet first, then of the current
// ReplicaSet a terminating Ready pod, a pod not yet Ready and one Ready pod (the only acceptable pick). The dbg ephemeral container starts only after
// its state was read DBG_POLLS times; before that its logs are "not available".
const podPickKubectl = `#!/usr/bin/env bash
set -euo pipefail
S="$STUB_DIR"
args="$*"
case "$args" in
  "get deploy ap-dev-health-auth-service -o jsonpath={.metadata.annotations.deployment\.kubernetes\.io/revision}")
    printf '2' ;;
  "get rs -l app.kubernetes.io/component=auth-service -o json")
    cat "$S/rs.json" ;;
  "get pod -l app.kubernetes.io/component=auth-service -o json")
    cat "$S/pods.json" ;;
  "get pod -l app.kubernetes.io/component=auth-service --field-selector=status.phase=Running -o jsonpath={.items[0].metadata.name}")
    jq -j '[.items[] | select(.status.phase == "Running")][0].metadata.name' "$S/pods.json" ;;
  debug\ *)
    printf '%s\n' "$2" >"$S/debugged" ;;
  "get pod "*" -o json")
    n=$(( $(cat "$S/polls" 2>/dev/null || echo 0) + 1 )); printf '%s' "$n" >"$S/polls"
    if [ "$n" -ge "$DBG_POLLS" ]; then state='{"terminated":{"exitCode":0}}'; else state='{"waiting":{"reason":"PodInitializing"}}'; fi
    printf '{"metadata":{"name":"%s"},"status":{"ephemeralContainerStatuses":[{"name":"dbg","state":%s}]}}\n' "$3" "$state" ;;
  "logs "*" -c dbg")
    n=$(cat "$S/polls" 2>/dev/null || echo 0)
    if [ "$n" -lt "$DBG_POLLS" ]; then
      printf 'Error from server (BadRequest): container "dbg" in pod "%s" is not available\n' "$2" >&2; exit 1
    fi
    printf '400 65532 65532 /proc/1/root/run/auth/signing/signing-key.pem\n' ;;
  *)
    printf 'stub kubectl: unexpected call: %s\n' "$args" >&2; exit 97 ;;
esac
`

const podPickRS = `{"items":[
 {"metadata":{"name":"ap-dev-health-auth-service-7d8f855979","labels":{"pod-template-hash":"7d8f855979"},
  "annotations":{"deployment.kubernetes.io/revision":"1"},
  "ownerReferences":[{"kind":"Deployment","name":"ap-dev-health-auth-service"}]}},
 {"metadata":{"name":"ap-dev-health-auth-service-8f489f88b","labels":{"pod-template-hash":"8f489f88b"},
  "annotations":{"deployment.kubernetes.io/revision":"2"},
  "ownerReferences":[{"kind":"Deployment","name":"ap-dev-health-auth-service"}]}}]}
`

const podPickPods = `{"items":[
 {"metadata":{"name":"ap-dev-health-auth-service-7d8f855979-hdgz7","labels":{"pod-template-hash":"7d8f855979"},
  "deletionTimestamp":"2026-10-27T19:42:00Z"},
  "status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}},
 {"metadata":{"name":"ap-dev-health-auth-service-7d8f855979-kq2mp","labels":{"pod-template-hash":"7d8f855979"}},
  "status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}},
 {"metadata":{"name":"ap-dev-health-auth-service-8f489f88b-aaaa1","labels":{"pod-template-hash":"8f489f88b"},
  "deletionTimestamp":"2026-10-27T19:42:01Z"},
  "status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}},
 {"metadata":{"name":"ap-dev-health-auth-service-8f489f88b-fx8sc","labels":{"pod-template-hash":"8f489f88b"}},
  "status":{"phase":"Running","conditions":[{"type":"Ready","status":"False"}]}},
 {"metadata":{"name":"ap-dev-health-auth-service-8f489f88b-gvtns","labels":{"pod-template-hash":"8f489f88b"}},
  "status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}]}
`

// podPickHarness stands in for the proof's own helpers; kubectl and sleep come from the stub directory on PATH.
const podPickHarness = `set -euo pipefail
RELEASE=ap
LOCAL_PG=img
die() { printf 'ERROR: %s\n' "$1" >&2; exit 1; }
K() { kubectl "$@"; }
`

// podPickSteps returns the proof's step-6 pod pick and debug-container read: any helper function they call, the POD
// assignment, and the lines from "K debug" through the key_stat read.
func podPickSteps(t *testing.T) (pick, debug string) {
	t.Helper()
	text := readText(t, filepath.Join(rootOf(t), "ci", "check_auth_k8s.sh"))
	helpers := strings.Join(regexp.MustCompile(`(?ms)^(current_auth_pod|await_debug_container)\(\) \{.*?^\}\n`).FindAllString(text, -1), "")
	podLine := regexp.MustCompile(`(?m)^POD="\$\(.*$`).FindString(text)
	debugBlock := regexp.MustCompile(`(?ms)^K debug "\$\{POD\}".*?^key_stat=[^\n]*\n`).FindString(text)
	if podLine == "" || debugBlock == "" {
		t.Fatal("the step-6 pod pick or debug read of ci/check_auth_k8s.sh moved")
	}
	return helpers + podLine + "\nprintf 'POD=%s\\n' \"${POD}\"\n", helpers + "POD=" + podPickNew + "\n" + debugBlock + "printf 'KEY=%s\\n' \"${key_stat}\"\n"
}

const (
	podPickOld = "ap-dev-health-auth-service-7d8f855979-hdgz7"
	podPickNew = "ap-dev-health-auth-service-8f489f88b-gvtns"
)

func runPodPick(t *testing.T, body string, dbgPolls string) cmdResult {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		filepath.Join(bin, "kubectl"): podPickKubectl,
		filepath.Join(bin, "sleep"):   "#!/usr/bin/env bash\nexit 0\n",
	} {
		if err := os.WriteFile(name, []byte(content), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, content := range map[string]string{"rs.json": podPickRS, "pods.json": podPickPods} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	script := filepath.Join(dir, "step6.sh")
	if err := os.WriteFile(script, []byte(podPickHarness+body), 0o644); err != nil {
		t.Fatal(err)
	}
	return cmdSpec{dir: dir, env: inheritedEnv([]string{"PATH"}, "PATH="+bin+":/usr/bin:/bin:/usr/local/bin", "STUB_DIR="+dir, "DBG_POLLS="+dbgPolls)}.run("bash", script)
}

// After the step-5 upgrade the step-6 pick takes a Ready pod of the Deployment's current ReplicaSet, never the
// terminating pod of the old one that the API lists first.
func TestTheAuthK8sProofPicksAReadyPodOfTheCurrentReplicaSet(t *testing.T) {
	pick, _ := podPickSteps(t)
	res := runPodPick(t, pick, "1")
	if res.code != 0 {
		t.Fatalf("pick exited %d: %s", res.code, res.stderr)
	}
	if !strings.Contains(res.stdout, "POD="+podPickNew+"\n") {
		t.Fatalf("picked %q, want %s (Ready, not terminating, current ReplicaSet); %s is the terminating old pod",
			strings.TrimSpace(res.stdout), podPickNew, podPickOld)
	}
}

// The debug container's output is read only once the container has run: a container that starts late is waited
// for, not read as "not available".
func TestTheAuthK8sProofWaitsForTheDebugContainer(t *testing.T) {
	_, debug := podPickSteps(t)
	res := runPodPick(t, debug, "4")
	if res.code != 0 {
		t.Fatalf("debug read exited %d: %s", res.code, res.stderr)
	}
	if !strings.Contains(res.stdout, "KEY=400 65532 65532 ") {
		t.Fatalf("key_stat = %q, want the staged key line", strings.TrimSpace(res.stdout))
	}
}

// A debug container that never starts fails the proof with a clear message, within the bound.
func TestTheAuthK8sProofFailsWhenTheDebugContainerNeverRuns(t *testing.T) {
	_, debug := podPickSteps(t)
	res := runPodPick(t, debug, "100000")
	if res.code == 0 {
		t.Fatalf("debug read passed with a container that never ran: %s", res.stdout)
	}
	if !strings.Contains(res.stderr, "ERROR: ") || !strings.Contains(res.stderr, "dbg") {
		t.Fatalf("no clear failure naming the dbg container: %s", res.stderr)
	}
}
