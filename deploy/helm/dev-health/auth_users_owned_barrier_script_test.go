package devhealth_test

import (
	"bytes"
	"context"
	"encoding/json"
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
TPL=""
for a in "$@"; do case "$a" in jsonpath=*) TPL="${a#jsonpath=}" ;; esac; done
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
# a fixture <key>.json (a kubectl List) is rendered FROM the jsonpath the script asked for, so a field the jsonpath does
# not name is not seen; any other file is canned text.
if [ -f "${FAKE_DIR}/${key}.json" ]; then
  python3 "${FAKE_BIN}/jsonpath.py" "${FAKE_DIR}/${key}.json" "$TPL" || exit 1
  exit 0
fi
[ -f "${FAKE_DIR}/${key}" ] && cat "${FAKE_DIR}/${key}"
exit 0
`

// fakeJSONPathPy renders the small jsonpath subset the barrier uses ({range}, {.a.b[*].c}, {"\n"}) the way kubectl does:
// arrays and objects print as compact JSON, a missing field prints nothing.
const fakeJSONPathPy = `#!/usr/bin/env python3
# A small kubectl jsonpath renderer for the barrier test fake: {range PATH}...{end}, {.a.b[*].c}, {"\n"} and literal text.
# Arrays and objects print as compact JSON, a missing field prints nothing, like kubectl.
import json, re, sys

def split_path(path):
    parts, cur, i = [], "", 0
    while i < len(path):
        c = path[i]
        if c == "\\" and i + 1 < len(path):
            cur += path[i + 1]; i += 2; continue
        if c in ".[":
            if cur: parts.append(cur)
            cur = ""
            if c == "[":
                j = path.index("]", i)
                parts.append(path[i:j + 1]); i = j + 1; continue
            i += 1; continue
        cur += c; i += 1
    if cur: parts.append(cur)
    return parts

def walk(nodes, parts):
    for p in parts:
        nxt = []
        for n in nodes:
            if p == "[*]":
                if isinstance(n, list): nxt.extend(n)
            elif isinstance(n, dict) and p in n:
                nxt.append(n[p])
        nodes = nxt
    return nodes

def fmt(v):
    if isinstance(v, (dict, list)): return json.dumps(v, separators=(",", ":"))
    if isinstance(v, bool): return "true" if v else "false"
    return str(v)

def tokens(tpl):
    out, i = [], 0
    while i < len(tpl):
        if tpl[i] == "{":
            j = tpl.index("}", i)
            out.append(("expr", tpl[i + 1:j])); i = j + 1
        else:
            j = tpl.find("{", i)
            j = len(tpl) if j < 0 else j
            out.append(("lit", tpl[i:j])); i = j
    return out

def render(toks, pos, ctx):
    res = ""
    while pos < len(toks):
        kind, val = toks[pos]
        if kind == "lit": res += val; pos += 1; continue
        if val == "end": return res, pos
        if val.startswith("range "):
            nodes = walk([ctx], split_path(val[6:].strip().lstrip(".")))
            end = pos + 1; depth = 1
            while depth:
                k, v = toks[end]
                if k == "expr" and v.startswith("range "): depth += 1
                if k == "expr" and v == "end": depth -= 1
                end += 1
            for n in nodes:
                r, _ = render(toks[pos + 1:end - 1] + [("expr", "end")], 0, n)
                res += r
            pos = end; continue
        if val.startswith('"'):
            res += json.loads(val.replace("\\n", "\\n")); pos += 1; continue
        nodes = walk([ctx], split_path(val.lstrip(".")))
        res += " ".join(fmt(n) for n in nodes); pos += 1
    return res, pos

if __name__ == "__main__":
    data = json.load(open(sys.argv[1]))
    toks = tokens(sys.argv[2])
    sys.stdout.write(render(toks, 0, data)[0])
`

func goodBarrierWorld() map[string]string {
	return map[string]string{
		"deploy.go-api": "3 3 3 3 3 3\n",
		"rs.go-api":     "rs-old||4\nrs-new|3|5\n",
		"pods.go-api":   "p1||true|true\np2||true|true\np3||true|true\n",
		"pods.all.json": podList(
			podItem("p1", "go-api", map[string]any{"containers": []any{map[string]any{"name": "api", "command": []any{"api"}}}}),
			podItem("m1", "migrate", map[string]any{"containers": []any{map[string]any{"name": "m", "command": []any{"migrate"}}}})),
		"jobs.all.json":       podList(),
		"cronjobs.all.json":   podList(),
		"pods.auth-service":   "a1|\na2|\n",
		"deploy.auth-service": "2 2 2 2 2 2\n",
		"rs.auth-service":     "as-old||7\nas-new|2|8\n",
	}
}

// podList, podItem, jobItem, cronJobItem and worldAdd build the kubectl List fixtures. A pod spec is a map with containers,
// initContainers and ephemeralContainers (each a list of {name, command, args} maps, fields present only when set).
func podList(items ...map[string]any) string {
	list := []any{}
	for _, item := range items {
		list = append(list, item)
	}
	raw, _ := json.Marshal(map[string]any{"items": list})
	return string(raw)
}

func podItem(name, component string, spec map[string]any) map[string]any {
	return map[string]any{
		"metadata": map[string]any{"name": name, "labels": map[string]any{"app.kubernetes.io/component": component}},
		"spec":     spec,
	}
}

func jobItem(name string, spec map[string]any) map[string]any {
	return map[string]any{"metadata": map[string]any{"name": name}, "spec": map[string]any{"template": map[string]any{"spec": spec}}}
}

func cronJobItem(name string, spec map[string]any) map[string]any {
	return map[string]any{"metadata": map[string]any{"name": name},
		"spec": map[string]any{"jobTemplate": map[string]any{"spec": map[string]any{"template": map[string]any{"spec": spec}}}}}
}

func worldAdd(t *testing.T, world map[string]string, key string, item map[string]any) {
	t.Helper()
	var list map[string]any
	if err := json.Unmarshal([]byte(world[key]), &list); err != nil {
		t.Fatal(err)
	}
	list["items"] = append(list["items"].([]any), item)
	raw, _ := json.Marshal(list)
	world[key] = string(raw)
}

func ctr(command, args []any) map[string]any {
	c := map[string]any{"name": "c"}
	if command != nil {
		c["command"] = command
	}
	if args != nil {
		c["args"] = args
	}
	return c
}

func strs(words ...string) []any {
	out := make([]any, len(words))
	for i, w := range words {
		out[i] = w
	}
	return out
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
	if err := os.WriteFile(filepath.Join(bin, "jsonpath.py"), []byte(fakeJSONPathPy), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, "bash", append([]string{barrierScript}, args...)...)
	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "FAKE_DIR="+dir, "FAKE_BIN="+bin, "RELEASE=a")
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
			worldAdd(t, w, "pods.all.json", podItem("j1", "operator", map[string]any{"containers": []any{ctr(nil, strs("admin", "users", "create"))}}))
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

// The admin users scan reads every container of every pod, Job and CronJob template in the namespace (no release label: an
// ad-hoc pod has none). The fake renders FROM the jsonpath the script sends, so a spec field the jsonpath does not name
// (command, args, initContainers, ephemeralContainers) is invisible to the script, as with kubectl.
func TestBarrierRefusesAdminUsersInEveryContainerShape(t *testing.T) {
	verb := strs("dho", "admin", "users", "create")
	specs := map[string]map[string]any{
		"command only":           {"containers": []any{ctr(verb, nil)}},
		"args only":              {"containers": []any{ctr(nil, strs("admin", "users", "create"))}},
		"command and args split": {"containers": []any{ctr(strs("dho"), strs("admin", "users", "create"))}},
		"sh -c in the args":      {"containers": []any{ctr(strs("sh", "-c"), strs("dho admin users create x"))}},
		"sh -c in the command":   {"containers": []any{ctr(strs("sh", "-c", "dho admin users create x"), nil)}},
		"any users subcommand":   {"containers": []any{ctr(strs("dho", "admin", "users", "list"), nil)}},
		"init container":         {"containers": []any{ctr(strs("sleep"), nil)}, "initContainers": []any{ctr(verb, nil)}},
		"init container args":    {"containers": []any{ctr(strs("sleep"), nil)}, "initContainers": []any{ctr(nil, strs("admin", "users", "create"))}},
		"ephemeral container":    {"containers": []any{ctr(strs("sleep"), nil)}, "ephemeralContainers": []any{ctr(verb, nil)}},
		"second container":       {"containers": []any{ctr(strs("sleep"), nil), ctr(verb, nil)}},
	}
	for name, spec := range specs {
		t.Run(name, func(t *testing.T) {
			world := goodBarrierWorld()
			worldAdd(t, world, "pods.all.json", podItem("x1", "operator", spec))
			r := runBarrier(t, world, nil, "up-go-api")
			if r.rc == 0 || strings.Contains(r.stdout, "barrier_ok") {
				t.Fatalf("passed: rc=%d stdout=%q stderr=%q", r.rc, r.stdout, r.stderr)
			}
		})
	}
}

func TestBarrierAdminUsersInJobAndCronJobTemplates(t *testing.T) {
	verb := strs("dho", "admin", "users", "create")
	specs := map[string]map[string]any{
		"command":        {"containers": []any{ctr(verb, nil)}},
		"sh -c":          {"containers": []any{ctr(strs("sh", "-c", "dho admin users create"), nil)}},
		"init container": {"containers": []any{ctr(strs("sleep"), nil)}, "initContainers": []any{ctr(verb, nil)}},
	}
	for name, spec := range specs {
		for _, kind := range []string{"jobs", "cronjobs"} {
			world := goodBarrierWorld()
			if kind == "jobs" {
				worldAdd(t, world, "jobs.all.json", jobItem("t1", spec))
			} else {
				worldAdd(t, world, "cronjobs.all.json", cronJobItem("t1", spec))
			}
			r := runBarrier(t, world, nil, "up-go-api")
			if r.rc == 0 || strings.Contains(r.stdout, "barrier_ok") {
				t.Errorf("%s %s passed: rc=%d stdout=%q", kind, name, r.rc, r.stdout)
			}
		}
	}
}

// Controls: none of these is an admin users verb in one container of a pod that is not go-api or auth-service.
func TestBarrierAllowsWhatIsNotAnAdminUsersVerb(t *testing.T) {
	verb := strs("dho", "admin", "users", "create")
	cases := map[string]map[string]any{
		"admin and users in different containers": {"containers": []any{ctr(strs("admin"), nil), ctr(strs("users"), nil)}},
		"words that only contain the names":       {"containers": []any{ctr(strs("administer", "users2"), nil)}},
		"a hyphenated users name":                 {"containers": []any{ctr(strs("dho", "admin", "users-export"), nil)}},
		"an unrelated command":                    {"containers": []any{ctr(strs("dho", "worker"), nil)}},
	}
	for name, spec := range cases {
		t.Run(name, func(t *testing.T) {
			world := goodBarrierWorld()
			worldAdd(t, world, "pods.all.json", podItem("x1", "operator", spec))
			r := runBarrier(t, world, nil, "up-go-api")
			if r.rc != 0 || !strings.Contains(r.stdout, "barrier_ok") {
				t.Fatalf("refused: rc=%d stdout=%q stderr=%q", r.rc, r.stdout, r.stderr)
			}
		})
	}
	for _, component := range []string{"go-api", "auth-service"} {
		world := goodBarrierWorld()
		worldAdd(t, world, "pods.all.json", podItem("x1", component, map[string]any{"containers": []any{ctr(verb, nil)}}))
		r := runBarrier(t, world, nil, "up-go-api")
		if r.rc != 0 || !strings.Contains(r.stdout, "barrier_ok") {
			t.Errorf("the verb in a %s pod was refused: rc=%d stdout=%q stderr=%q", component, r.rc, r.stdout, r.stderr)
		}
	}
}

// The fake answers canned text, so the jsonpath the script sends is pinned here: the admin users scan asks, for pods, Jobs
// and CronJobs, for command and args of containers, initContainers and ephemeralContainers (pods only for the last).
func TestBarrierAdminUsersScanAsksForEveryContainerField(t *testing.T) {
	r := runBarrier(t, goodBarrierWorld(), nil, "up-go-api")
	want := map[string][]string{
		"get pods":     {"{.command}{.args}", ".spec.containers[*]", ".spec.initContainers[*]", ".spec.ephemeralContainers[*]"},
		"get jobs":     {"{.command}{.args}", ".spec.template.spec.containers[*]", ".spec.template.spec.initContainers[*]"},
		"get cronjobs": {"{.command}{.args}", ".spec.jobTemplate.spec.template.spec.containers[*]", ".spec.jobTemplate.spec.template.spec.initContainers[*]"},
	}
	for verb, parts := range want {
		found := false
		for _, line := range strings.Split(r.argv, "\n") {
			if !strings.Contains(line, verb+" -o jsonpath=") {
				continue
			}
			found = true
			for _, part := range parts {
				if !strings.Contains(line, part) {
					t.Errorf("%s: the jsonpath lacks %q: %s", verb, part, line)
				}
			}
		}
		if !found {
			t.Errorf("no namespace-wide `%s -o jsonpath=` call (the scan must not depend on the release label)", verb)
		}
	}
}
