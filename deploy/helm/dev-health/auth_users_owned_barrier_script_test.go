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

// fakeBarrierKubectl records argv and answers `get <resource> -l ...component=<c>` from the fixture $FAKE_DIR/<resource>.<c>.json
// (component "all" when the selector has none), ALWAYS rendered from the jsonpath the script sent: a field the jsonpath
// does not name is not seen, and a misspelt path prints nothing. FAKE_FAIL=<resource>.<c> makes that call exit 1,
// FAKE_HANG=<resource>.<c> makes it hang unless --request-timeout is on the line.
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
if [ -f "${FAKE_DIR}/${key}.json" ]; then
  python3 "${FAKE_BIN}/jsonpath.py" "${FAKE_DIR}/${key}.json" "$TPL" || exit 1
fi
exit 0
`

// fakeJSONPathPy renders the jsonpath subset the barrier uses ({range}, {.a.b[*].c}, [?(@.name=="x")], {"\n"}) the way
// kubectl does: arrays and objects print as compact JSON, a missing field prints nothing.
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
            elif p.startswith("[?(@."):
                m = re.match(r'\[\?\(@\.([^=]+)=="([^"]*)"\)\]$', p)
                if m and isinstance(n, list):
                    nxt.extend(x for x in n if isinstance(x, dict) and x.get(m.group(1)) == m.group(2))
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

// The world of one barrier run: a coherent set of kubectl Lists. files() writes every fixture the script may ask for.
type bworld struct {
	goDeploy, authDeploy        map[string]any
	goRS, authRS                []any
	goPods, authPods, otherPods []any
	jobs, cronJobs              []any
	statefulSets                []any
}

const (
	goAPIImage   = "ghcr.io/example/dev-health-go-api"
	authImage    = "ghcr.io/example/dev-health-auth-service"
	stagingImage = "docker.io/library/busybox"
)

func bwServerGoAPI(env ...map[string]any) map[string]any {
	c := map[string]any{"name": "go-api", "image": goAPIImage + ":v1", "args": bwStrs("api", "--api-addr=:8000", "--http-addr=:8001")}
	if len(env) == 0 {
		env = []map[string]any{{"name": "AUTH_USERS_OWNED", "value": "true"}, {"name": "AUTH_PROXY_CREDENTIALS", "value": "true"}}
	}
	list := []any{}
	for _, e := range env {
		list = append(list, e)
	}
	c["env"] = list
	return c
}

func bwServerAuth(owned string) map[string]any {
	c := map[string]any{"name": "auth-service", "image": authImage + ":v1", "args": bwStrs("serve", "--api-addr=:8095")}
	if owned != "" {
		c["env"] = []any{map[string]any{"name": "AUTH_USERS_OWNED", "value": owned}}
	}
	return c
}

func bwAuthInit() map[string]any {
	return map[string]any{"name": "stage-signing-key", "image": stagingImage + ":1.36",
		"command": bwStrs("sh", "-ec", "install -m 0400 /in/signing-key.pem /out/signing-key.pem")}
}

func bwDeployItem(gen, obs, replicas, updated, available, total int) map[string]any {
	return map[string]any{
		"metadata": map[string]any{"generation": gen},
		"spec":     map[string]any{"replicas": replicas},
		"status":   map[string]any{"observedGeneration": obs, "updatedReplicas": updated, "availableReplicas": available, "replicas": total},
	}
}

// rsItem: replicas < 0 leaves status.replicas out, as the API does for a ReplicaSet at 0.
func bwRsItem(name string, replicas int, revision string, containers []any, initContainers []any) map[string]any {
	status := map[string]any{}
	if replicas >= 0 {
		status["replicas"] = replicas
	}
	spec := map[string]any{"containers": containers}
	if initContainers != nil {
		spec["initContainers"] = initContainers
	}
	return map[string]any{
		"metadata": map[string]any{"name": name, "annotations": map[string]any{"deployment.kubernetes.io/revision": revision}},
		"status":   status,
		"spec":     map[string]any{"template": map[string]any{"spec": spec}},
	}
}

func bwPodAt(name, component, deleting string, spec map[string]any) map[string]any {
	meta := map[string]any{"name": name, "labels": map[string]any{"app.kubernetes.io/component": component}}
	if deleting != "" {
		meta["deletionTimestamp"] = deleting
	}
	return map[string]any{"metadata": meta, "spec": spec}
}

func bwGoPod(name string) map[string]any {
	return bwPodAt(name, "go-api", "", map[string]any{"containers": []any{bwServerGoAPI()}})
}

func bwAuthPod(name, owned string) map[string]any {
	return bwPodAt(name, "auth-service", "", map[string]any{"initContainers": []any{bwAuthInit()}, "containers": []any{bwServerAuth(owned)}})
}

func bwNewWorld() *bworld {
	return &bworld{
		goDeploy:   bwDeployItem(3, 3, 3, 3, 3, 3),
		authDeploy: bwDeployItem(2, 2, 2, 2, 2, 2),
		goRS: []any{
			bwRsItem("rs-old", -1, "4", []any{map[string]any{"name": "go-api", "image": goAPIImage + ":v0", "args": bwStrs("api", "--api-addr=:8000", "--http-addr=:8001")}}, nil),
			bwRsItem("rs-new", 3, "5", []any{bwServerGoAPI()}, nil),
		},
		authRS: []any{
			bwRsItem("as-old", -1, "7", []any{bwServerAuth("")}, []any{bwAuthInit()}),
			bwRsItem("as-new", 2, "8", []any{bwServerAuth("")}, []any{bwAuthInit()}),
		},
		goPods:    []any{bwGoPod("p1"), bwGoPod("p2"), bwGoPod("p3")},
		authPods:  []any{bwAuthPod("a1", ""), bwAuthPod("a2", "")},
		otherPods: []any{bwPodAt("m1", "migrate", "", map[string]any{"containers": []any{map[string]any{"name": "m", "image": goAPIImage + ":v1", "args": bwStrs("migrate")}}})},
	}
}

func bwList(items []any) string {
	if items == nil {
		items = []any{}
	}
	raw, _ := json.Marshal(map[string]any{"items": items})
	return string(raw)
}

func (w *bworld) files() map[string]string {
	deploy := func(d map[string]any) string {
		if d == nil {
			return bwList(nil)
		}
		return bwList([]any{d})
	}
	all := append(append(append([]any{}, w.goPods...), w.authPods...), w.otherPods...)
	return map[string]string{
		"deploy.go-api.json":       deploy(w.goDeploy),
		"deploy.auth-service.json": deploy(w.authDeploy),
		"rs.go-api.json":           bwList(w.goRS),
		"rs.auth-service.json":     bwList(w.authRS),
		"pods.go-api.json":         bwList(w.goPods),
		"pods.auth-service.json":   bwList(w.authPods),
		"pods.all.json":            bwList(all),
		"jobs.all.json":            bwList(w.jobs),
		"cronjobs.all.json":        bwList(w.cronJobs),
		"statefulsets.all.json":    bwList(w.statefulSets),
	}
}

func bwGoodBarrierWorld() map[string]string { return bwNewWorld().files() }

func bwJobItem(name string, spec map[string]any) map[string]any {
	return map[string]any{"metadata": map[string]any{"name": name}, "spec": map[string]any{"template": map[string]any{"spec": spec}}}
}

func bwCronJobItem(name string, spec map[string]any) map[string]any {
	return map[string]any{"metadata": map[string]any{"name": name},
		"spec": map[string]any{"jobTemplate": map[string]any{"spec": map[string]any{"template": map[string]any{"spec": spec}}}}}
}

// ctr is an operator container: command and args only when set.
func bwCtr(command, args []any) map[string]any {
	c := map[string]any{"name": "c", "image": "registry.example/tool:1"}
	if command != nil {
		c["command"] = command
	}
	if args != nil {
		c["args"] = args
	}
	return c
}

func bwStrs(words ...string) []any {
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

func bwPassed(r barrierRun) bool { return r.rc == 0 && strings.Contains(r.stdout, "barrier_ok") }

func TestBarrierPassesAndPrintsTheTime(t *testing.T) {
	for _, mode := range []string{"up-go-api", "up-owned", "down-auth"} {
		r := runBarrier(t, bwGoodBarrierWorld(), nil, mode)
		if r.rc != 0 || !regexp.MustCompile(`(?m)^barrier_ok mode=`+mode+` time=\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$`).MatchString(r.stdout) {
			t.Errorf("%s: rc=%d stdout=%q stderr=%q", mode, r.rc, r.stdout, r.stderr)
		}
	}
}

func TestBarrierPlanRunsNothing(t *testing.T) {
	r := runBarrier(t, bwGoodBarrierWorld(), nil, "up-go-api", "--plan")
	if r.rc != 0 || r.argv != "" || !strings.Contains(r.stdout, "plan:") {
		t.Errorf("--plan: rc=%d argv=%q stdout=%q", r.rc, r.argv, r.stdout)
	}
}

func TestBarrierRefusesBadUsage(t *testing.T) {
	if r := runBarrier(t, bwGoodBarrierWorld(), nil); r.rc == 0 {
		t.Error("no mode passed")
	}
	if r := runBarrier(t, bwGoodBarrierWorld(), nil, "sideways"); r.rc == 0 {
		t.Error("unknown mode passed")
	}
	if r := runBarrier(t, bwGoodBarrierWorld(), []string{"RELEASE="}, "up-go-api"); r.rc == 0 {
		t.Error("empty RELEASE passed")
	}
}

func TestBarrierFailsOnEachBrokenState(t *testing.T) {
	cases := []struct {
		name   string
		mode   string
		mutate func(w *bworld)
	}{
		{"old ReplicaSet still has a pod", "up-go-api", func(w *bworld) { w.goRS[0] = bwRsItem("rs-old", 1, "4", []any{bwServerGoAPI()}, nil) }},
		{"old ReplicaSet newer revision number only", "up-go-api", func(w *bworld) {
			w.goRS = []any{bwRsItem("rs-a", 2, "9", []any{bwServerGoAPI()}, nil), bwRsItem("rs-b", 2, "10", []any{bwServerGoAPI()}, nil)}
		}},
		{"one pod lacks AUTH_USERS_OWNED", "up-go-api", func(w *bworld) {
			w.goPods[1] = bwPodAt("p2", "go-api", "", map[string]any{"containers": []any{bwServerGoAPI(map[string]any{"name": "AUTH_PROXY_CREDENTIALS", "value": "true"})}})
		}},
		{"one pod has AUTH_USERS_OWNED false", "up-go-api", func(w *bworld) {
			w.goPods[2] = bwPodAt("p3", "go-api", "", map[string]any{"containers": []any{bwServerGoAPI(map[string]any{"name": "AUTH_USERS_OWNED", "value": "false"}, map[string]any{"name": "AUTH_PROXY_CREDENTIALS", "value": "true"})}})
		}},
		{"the last pod lacks AUTH_PROXY_CREDENTIALS", "up-go-api", func(w *bworld) {
			w.goPods[2] = bwPodAt("p3", "go-api", "", map[string]any{"containers": []any{bwServerGoAPI(map[string]any{"name": "AUTH_USERS_OWNED", "value": "true"})}})
		}},
		{"a terminating pod", "up-go-api", func(w *bworld) {
			w.goPods = append(w.goPods, bwPodAt("p4", "go-api", "2026-10-26T15:00:00Z", map[string]any{"containers": []any{bwServerGoAPI()}}))
		}},
		{"no go-api pod", "up-go-api", func(w *bworld) { w.goPods = nil }},
		{"deployment mid-roll", "up-go-api", func(w *bworld) { w.goDeploy = bwDeployItem(3, 3, 3, 2, 2, 3) }},
		{"observedGeneration behind", "up-go-api", func(w *bworld) { w.goDeploy = bwDeployItem(4, 3, 3, 3, 3, 3) }},
		{"no Deployment", "up-go-api", func(w *bworld) { w.goDeploy = nil }},
		{"an auth-service pod already owned", "up-owned", func(w *bworld) { w.authPods[0] = bwAuthPod("a1", "true") }},
		{"an owned auth-service pod on the way down", "down-auth", func(w *bworld) { w.authPods[0] = bwAuthPod("a1", "true") }},
		{"old auth-service ReplicaSet alive on the way down", "down-auth", func(w *bworld) {
			w.authRS[0] = bwRsItem("as-old", 1, "7", []any{bwServerAuth("")}, []any{bwAuthInit()})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := bwNewWorld()
			c.mutate(w)
			r := runBarrier(t, w.files(), nil, c.mode)
			if bwPassed(r) || r.rc == 0 {
				t.Fatalf("passed: rc=%d stdout=%q stderr=%q", r.rc, r.stdout, r.stderr)
			}
		})
	}
}

// A kubectl error is never a pass, whichever call it hits.
func TestBarrierFailsClosedWhenKubectlFails(t *testing.T) {
	for _, key := range []string{"deploy.go-api", "rs.go-api", "pods.go-api", "rs.auth-service", "pods.all", "jobs.all", "cronjobs.all", "statefulsets.all"} {
		r := runBarrier(t, bwGoodBarrierWorld(), []string{"FAKE_FAIL=" + key}, "up-go-api")
		if r.rc == 0 || strings.Contains(r.stdout, "barrier_ok") {
			t.Errorf("kubectl %s failing passed: rc=%d", key, r.rc)
		}
	}
	for _, key := range []string{"deploy.auth-service", "rs.auth-service", "pods.auth-service", "pods.all", "jobs.all", "cronjobs.all"} {
		r := runBarrier(t, bwGoodBarrierWorld(), []string{"FAKE_FAIL=" + key}, "down-auth")
		if r.rc == 0 || strings.Contains(r.stdout, "barrier_ok") {
			t.Errorf("kubectl %s failing passed on down-auth: rc=%d", key, r.rc)
		}
	}
}

// Read-only: every call is `get ... -o jsonpath=...`, never a Secret, never a write, exec or describe.
func TestBarrierCallsOnlyKubectlGetJSONPath(t *testing.T) {
	for _, mode := range []string{"up-go-api", "up-owned", "down-auth"} {
		r := runBarrier(t, bwGoodBarrierWorld(), nil, mode)
		lines := strings.Split(strings.TrimSpace(r.argv), "\n")
		if len(lines) < 3 {
			t.Fatalf("%s: kubectl called %d times", mode, len(lines))
		}
		for _, line := range lines {
			if !regexp.MustCompile(`^-n dev-health --request-timeout=30s get (deploy|rs|pods|jobs|cronjobs|statefulsets) (-l app\.kubernetes\.io/instance=a\S* )?-o jsonpath=`).MatchString(line) {
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
		r := runBarrier(t, bwGoodBarrierWorld(), nil, mode)
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
		r := runBarrierWithin(t, 15*time.Second, bwGoodBarrierWorld(), []string{"FAKE_HANG=" + key}, "up-go-api")
		if r.deadline {
			t.Errorf("kubectl %s hung: the test deadline killed the run (no --request-timeout)", key)
		}
		if r.rc == 0 || strings.Contains(r.stdout, "barrier_ok") {
			t.Errorf("kubectl %s hanging passed: rc=%d", key, r.rc)
		}
	}
}

// Containers that must STOP the barrier outside the exactly matched server containers: the verb in every shape, and
// everything that can hide it (a shell -c, a command substitution, a variable, eval, any mention of dho).
func writerSpecs() map[string]map[string]any {
	verb := bwStrs("dho", "admin", "users", "create")
	return map[string]map[string]any{
		"command only":                {"containers": []any{bwCtr(verb, nil)}},
		"args only":                   {"containers": []any{bwCtr(nil, bwStrs("admin", "users", "create"))}},
		"command and args split":      {"containers": []any{bwCtr(bwStrs("dho"), bwStrs("admin", "users", "create"))}},
		"sh -c in the args":           {"containers": []any{bwCtr(bwStrs("sh", "-c"), bwStrs("dho admin users create x"))}},
		"sh -c in the command":        {"containers": []any{bwCtr(bwStrs("sh", "-c", "dho admin users create x"), nil)}},
		"any users subcommand":        {"containers": []any{bwCtr(bwStrs("dho", "admin", "users", "list"), nil)}},
		"init container":              {"containers": []any{bwCtr(bwStrs("sleep"), nil)}, "initContainers": []any{bwCtr(verb, nil)}},
		"init container args":         {"containers": []any{bwCtr(bwStrs("sleep"), nil)}, "initContainers": []any{bwCtr(nil, bwStrs("admin", "users", "create"))}},
		"ephemeral container":         {"containers": []any{bwCtr(bwStrs("sleep"), nil)}, "ephemeralContainers": []any{bwCtr(verb, nil)}},
		"second container":            {"containers": []any{bwCtr(bwStrs("sleep"), nil), bwCtr(verb, nil)}},
		"a shell expansion hides it":  {"containers": []any{bwCtr(bwStrs("sh", "-c"), bwStrs("dho admin u$(printf s)ers create"))}},
		"bash -c true":                {"containers": []any{bwCtr(bwStrs("bash", "-c", "true"), nil)}},
		"sh -ec":                      {"containers": []any{bwCtr(bwStrs("sh", "-ec", "echo hi"), nil)}},
		"a variable":                  {"containers": []any{bwCtr(bwStrs("echo", "${X}"), nil)}},
		"a backtick":                  {"containers": []any{bwCtr(bwStrs("echo", "`id`"), nil)}},
		"a command substitution":      {"containers": []any{bwCtr(bwStrs("echo", "$(id)"), nil)}},
		"eval":                        {"containers": []any{bwCtr(bwStrs("eval", "x"), nil)}},
		"any mention of dho":          {"containers": []any{bwCtr(bwStrs("dho", "version"), nil)}},
		"a path to dho":               {"containers": []any{bwCtr(bwStrs("/usr/local/bin/dho"), bwStrs("worker"))}},
		"a field separator in an arg": {"containers": []any{bwCtr(bwStrs("echo", "a|b"), nil)}},
	}
}

func TestBarrierStopsOnEveryWriterShapeInAPod(t *testing.T) {
	for name, spec := range writerSpecs() {
		t.Run(name, func(t *testing.T) {
			w := bwNewWorld()
			w.otherPods = append(w.otherPods, bwPodAt("x1", "operator", "", spec))
			r := runBarrier(t, w.files(), nil, "up-go-api")
			if bwPassed(r) || r.rc == 0 {
				t.Fatalf("passed: rc=%d stdout=%q stderr=%q", r.rc, r.stdout, r.stderr)
			}
			if !strings.Contains(r.stderr, "x1") {
				t.Errorf("the STOP does not name the object x1: %q", r.stderr)
			}
		})
	}
}

func TestBarrierStopsOnEveryWriterShapeInAJobAndACronJob(t *testing.T) {
	for name, spec := range writerSpecs() {
		if spec["ephemeralContainers"] != nil {
			continue
		}
		t.Run(name, func(t *testing.T) {
			for _, kind := range []string{"job", "cronjob"} {
				w := bwNewWorld()
				if kind == "job" {
					w.jobs = append(w.jobs, bwJobItem("t1", spec))
				} else {
					w.cronJobs = append(w.cronJobs, bwCronJobItem("t1", spec))
				}
				r := runBarrier(t, w.files(), nil, "up-go-api")
				if bwPassed(r) || r.rc == 0 {
					t.Errorf("%s passed: rc=%d stdout=%q stderr=%q", kind, r.rc, r.stdout, r.stderr)
				}
				if !strings.Contains(r.stderr, "t1") {
					t.Errorf("%s: the STOP does not name the object t1: %q", kind, r.stderr)
				}
			}
		})
	}
}

// down-auth runs the same namespace scan, for a pod, a Job and a CronJob, before the next release may roll.
func TestBarrierDownAuthStopsOnAWriter(t *testing.T) {
	spec := map[string]any{"containers": []any{bwCtr(bwStrs("dho", "admin", "users", "create"), nil)}}
	for _, kind := range []string{"pod", "job", "cronjob"} {
		w := bwNewWorld()
		switch kind {
		case "pod":
			w.otherPods = append(w.otherPods, bwPodAt("x1", "operator", "", spec))
		case "job":
			w.jobs = append(w.jobs, bwJobItem("x1", spec))
		default:
			w.cronJobs = append(w.cronJobs, bwCronJobItem("x1", spec))
		}
		r := runBarrier(t, w.files(), nil, "down-auth")
		if bwPassed(r) || r.rc == 0 || !strings.Contains(r.stderr, "x1") {
			t.Errorf("down-auth %s: rc=%d stdout=%q stderr=%q", kind, r.rc, r.stdout, r.stderr)
		}
	}
}

// A pod labelled go-api or auth-service is not trusted by its label: every container that is not exactly the chart's server
// container (name, image repository, command and args as the ReplicaSet template of that component says) is scanned.
func TestBarrierScansServerPodsExceptTheExactServerContainers(t *testing.T) {
	verb := bwStrs("dho", "admin", "users", "create")
	refused := map[string]*bworld{}
	mk := func(name string, mut func(w *bworld)) {
		w := bwNewWorld()
		mut(w)
		refused[name] = w
	}
	mk("a rogue container named go-api-like in a go-api labelled pod", func(w *bworld) {
		w.otherPods = append(w.otherPods, bwPodAt("x1", "go-api", "", map[string]any{"containers": []any{map[string]any{"name": "rogue", "image": "registry.example/tool:1", "command": verb}}}))
	})
	mk("a rogue pod labelled auth-service", func(w *bworld) {
		w.otherPods = append(w.otherPods, bwPodAt("x1", "auth-service", "", map[string]any{"containers": []any{map[string]any{"name": "rogue", "image": "registry.example/tool:1", "command": verb}}}))
	})
	mk("an extra container in a go-api pod", func(w *bworld) {
		w.otherPods = append(w.otherPods, bwPodAt("x1", "go-api", "", map[string]any{"containers": []any{bwServerGoAPI(), map[string]any{"name": "sidecar", "image": "registry.example/tool:1", "args": bwStrs("admin", "users", "create")}}}))
	})
	mk("the server container with changed args", func(w *bworld) {
		c := bwServerGoAPI()
		c["args"] = bwStrs("admin", "users", "create")
		w.otherPods = append(w.otherPods, bwPodAt("x1", "go-api", "", map[string]any{"containers": []any{c}}))
	})
	mk("the server container with a substitution added to its args", func(w *bworld) {
		c := bwServerGoAPI()
		c["args"] = bwStrs("api", "--x=$(id)")
		w.otherPods = append(w.otherPods, bwPodAt("x1", "go-api", "", map[string]any{"containers": []any{c}}))
	})
	mk("the server container from another image repository", func(w *bworld) {
		c := bwServerGoAPI()
		c["image"] = "registry.example/other:1"
		c["command"] = bwStrs("dho", "admin", "users", "create")
		w.otherPods = append(w.otherPods, bwPodAt("x1", "go-api", "", map[string]any{"containers": []any{c}}))
	})
	mk("the auth-service init container with a changed command", func(w *bworld) {
		bad := bwAuthInit()
		bad["command"] = bwStrs("sh", "-ec", "echo hi")
		w.otherPods = append(w.otherPods, bwPodAt("x1", "auth-service", "", map[string]any{"initContainers": []any{bad}, "containers": []any{bwServerAuth("")}}))
	})
	for name, w := range refused {
		t.Run(name, func(t *testing.T) {
			r := runBarrier(t, w.files(), nil, "up-go-api")
			if bwPassed(r) || r.rc == 0 || !strings.Contains(r.stderr, "x1") {
				t.Fatalf("rc=%d stdout=%q stderr=%q", r.rc, r.stdout, r.stderr)
			}
		})
	}
}

// Controls: the exact server containers of both components (the auth-service init container runs `sh -ec`) pass, and so do
// containers that hold none of the STOP words.
func TestBarrierPassesTheExactServerContainersAndPlainContainers(t *testing.T) {
	plain := map[string]map[string]any{
		"admin and users in different containers": {"containers": []any{bwCtr(bwStrs("admin"), nil), bwCtr(bwStrs("users"), nil)}},
		"words that only contain the names":       {"containers": []any{bwCtr(bwStrs("administer", "users2"), nil)}},
		"a hyphenated users name":                 {"containers": []any{bwCtr(bwStrs("admin", "users-export"), nil)}},
		"an unrelated command":                    {"containers": []any{bwCtr(bwStrs("worker"), nil)}},
		"a sleep":                                 {"containers": []any{bwCtr(bwStrs("sleep", "1"), nil)}},
	}
	for name, spec := range plain {
		t.Run(name, func(t *testing.T) {
			w := bwNewWorld()
			w.otherPods = append(w.otherPods, bwPodAt("x1", "operator", "", spec))
			r := runBarrier(t, w.files(), nil, "up-go-api")
			if !bwPassed(r) {
				t.Fatalf("refused: rc=%d stdout=%q stderr=%q", r.rc, r.stdout, r.stderr)
			}
		})
	}
	for _, mode := range []string{"up-go-api", "up-owned", "down-auth"} {
		if r := runBarrier(t, bwGoodBarrierWorld(), nil, mode); !bwPassed(r) {
			t.Errorf("%s: the good world was refused: rc=%d stderr=%q", mode, r.rc, r.stderr)
		}
	}
}

// The fake answers FROM the jsonpath, so the jsonpath the script sends is also pinned: the admin users scan asks, for pods,
// Jobs and CronJobs, for name, image, command and args of containers, initContainers and ephemeralContainers (pods only
// for the last), and the ReplicaSet templates of both server components.
func TestBarrierAdminUsersScanAsksForEveryContainerField(t *testing.T) {
	r := runBarrier(t, bwGoodBarrierWorld(), nil, "up-go-api")
	want := map[string][]string{
		"get pods":     {"{.name}", "{.image}", "{.command}{.args}", ".spec.containers[*]", ".spec.initContainers[*]", ".spec.ephemeralContainers[*]"},
		"get jobs":     {"{.image}", "{.command}{.args}", ".spec.template.spec.containers[*]", ".spec.template.spec.initContainers[*]"},
		"get cronjobs": {"{.image}", "{.command}{.args}", ".spec.jobTemplate.spec.template.spec.containers[*]", ".spec.jobTemplate.spec.template.spec.initContainers[*]"},
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
	for _, comp := range []string{"go-api", "auth-service"} {
		found := false
		for _, line := range strings.Split(r.argv, "\n") {
			if strings.Contains(line, "get rs -l app.kubernetes.io/instance=a,app.kubernetes.io/component="+comp+" ") && strings.Contains(line, ".spec.template.spec.containers[*]") && strings.Contains(line, "{.command}{.args}") {
				found = true
			}
		}
		if !found {
			t.Errorf("no ReplicaSet template read for %s (the exact-match set)", comp)
		}
	}
}

// The bundled ClickHouse StatefulSet (chart: templates/clickhouse.yaml:10-12, name <fullname>-clickhouse; its init container
// clickhouse-usersd-links runs `sh -c`, clickhouse.yaml:53-59) is the one long-lived release workload on the script's fixed
// list. Its pods are trusted only through kind + name + the ownerReference chain, never through a label.
const clickHouseSTS = "a-dev-health-clickhouse"

func bwClickHouseInit() map[string]any {
	return map[string]any{"name": "clickhouse-usersd-links", "image": "clickhouse/clickhouse-server:24.3",
		"command": bwStrs("sh", "-c", `for n in "$@"; do ln -sfn "../usersd-cm/$n" "/links/$n"; done`, "--", "users.xml")}
}

func bwClickHouseMain() map[string]any {
	return map[string]any{"name": "clickhouse", "image": "clickhouse/clickhouse-server:24.3"}
}

func bwStatefulSet(name string) map[string]any {
	return map[string]any{
		"metadata": map[string]any{"name": name},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"initContainers": []any{bwClickHouseInit()}, "containers": []any{bwClickHouseMain()}}}},
	}
}

func bwOwned(pod map[string]any, kind, name string) map[string]any {
	pod["metadata"].(map[string]any)["ownerReferences"] = []any{map[string]any{"kind": kind, "name": name}}
	return pod
}

func bwClickHousePod(name string, extra ...any) map[string]any {
	containers := append([]any{bwClickHouseMain()}, extra...)
	return bwOwned(bwPodAt(name, "clickhouse", "", map[string]any{"initContainers": []any{bwClickHouseInit()}, "containers": containers}), "StatefulSet", clickHouseSTS)
}

func clickHouseWorld() *bworld {
	w := bwNewWorld()
	w.statefulSets = []any{bwStatefulSet(clickHouseSTS)}
	w.otherPods = append(w.otherPods, bwClickHousePod(clickHouseSTS+"-0"))
	return w
}

// The named StatefulSet pod as rendered (sh -c in its init container) passes.
func TestBarrierPassesTheNamedClickHouseStatefulSetPod(t *testing.T) {
	for _, mode := range []string{"up-go-api", "up-owned", "down-auth"} {
		if r := runBarrier(t, clickHouseWorld().files(), nil, mode); !bwPassed(r) {
			t.Errorf("%s: the named ClickHouse pod was refused: rc=%d stderr=%q", mode, r.rc, r.stderr)
		}
	}
}

// Nothing else rides on the named workload: a Job, a Deployment, a StatefulSet of another name, a pod without the owner, a
// changed template, an extra container. The release label is never a reason to trust.
func TestBarrierStopsWhereTheNamedWorkloadIsImitated(t *testing.T) {
	chInit := []any{bwClickHouseInit()}
	chMain := []any{bwClickHouseMain()}
	rows := map[string]func(w *bworld){
		"a rogue Job with the release label and the ClickHouse command": func(w *bworld) {
			j := bwJobItem("x1", map[string]any{"initContainers": chInit, "containers": chMain})
			j["metadata"].(map[string]any)["labels"] = map[string]any{"app.kubernetes.io/instance": "a"}
			w.jobs = append(w.jobs, j)
		},
		"a rogue CronJob with the ClickHouse command": func(w *bworld) {
			w.cronJobs = append(w.cronJobs, bwCronJobItem("x1", map[string]any{"initContainers": chInit, "containers": chMain}))
		},
		"a rogue Deployment pod with the release label, a name not on the list": func(w *bworld) {
			pod := bwOwned(bwPodAt("x1", "clickhouse", "", map[string]any{"initContainers": chInit, "containers": chMain}), "ReplicaSet", "rogue-5d9f")
			w.otherPods = append(w.otherPods, pod)
		},
		"a StatefulSet pod of another name": func(w *bworld) {
			pod := bwOwned(bwPodAt("x1", "clickhouse", "", map[string]any{"initContainers": chInit, "containers": chMain}), "StatefulSet", "rogue-clickhouse")
			w.statefulSets = append(w.statefulSets, bwStatefulSet("rogue-clickhouse"))
			w.otherPods = append(w.otherPods, pod)
		},
		"the ClickHouse tuple on a pod without an owner": func(w *bworld) {
			w.otherPods = append(w.otherPods, bwPodAt("x1", "clickhouse", "", map[string]any{"initContainers": chInit, "containers": chMain}))
		},
		"the named StatefulSet pod with an extra container": func(w *bworld) {
			w.otherPods = append(w.otherPods, bwClickHousePod("x1", map[string]any{"name": "sidecar", "image": "registry.example/tool:1", "args": bwStrs("admin", "users", "create")}))
		},
		"the named StatefulSet pod with an extra shell container": func(w *bworld) {
			w.otherPods = append(w.otherPods, bwClickHousePod("x1", map[string]any{"name": "sidecar", "image": "registry.example/tool:1", "command": bwStrs("bash", "-c", "true")}))
		},
		"the named StatefulSet pod with a changed init command": func(w *bworld) {
			pod := bwClickHousePod("x1")
			pod["spec"].(map[string]any)["initContainers"] = []any{map[string]any{"name": "clickhouse-usersd-links", "image": "clickhouse/clickhouse-server:24.3",
				"command": bwStrs("sh", "-c", `echo "$(id)"`)}}
			w.otherPods = append(w.otherPods, pod)
		},
	}
	for name, mut := range rows {
		t.Run(name, func(t *testing.T) {
			w := clickHouseWorld()
			mut(w)
			r := runBarrier(t, w.files(), nil, "up-go-api")
			if bwPassed(r) || r.rc == 0 || !strings.Contains(r.stderr, "x1") {
				t.Fatalf("rc=%d stdout=%q stderr=%q", r.rc, r.stdout, r.stderr)
			}
		})
	}
}

// The scan asks for the owner chain and for the StatefulSets of the namespace (by kind and name, never by label).
func TestBarrierScanAsksForOwnersAndStatefulSets(t *testing.T) {
	r := runBarrier(t, clickHouseWorld().files(), nil, "up-go-api")
	foundOwner, foundSTS := false, false
	for _, line := range strings.Split(r.argv, "\n") {
		if strings.Contains(line, "get pods -o jsonpath=") && strings.Contains(line, ".metadata.ownerReferences[*]") {
			foundOwner = true
		}
		if strings.Contains(line, "get statefulsets -o jsonpath=") && strings.Contains(line, ".spec.template.spec.containers[*]") {
			foundSTS = true
		}
	}
	if !foundOwner {
		t.Error("the pod scan does not read the ownerReferences")
	}
	if !foundSTS {
		t.Error("no namespace-wide `get statefulsets -o jsonpath=` call with the template containers")
	}
}

func bwClickHouseWorldNamed(sts string) *bworld {
	w := bwNewWorld()
	w.statefulSets = []any{bwStatefulSet(sts)}
	pod := bwOwned(bwPodAt(sts+"-0", "clickhouse", "", map[string]any{"initContainers": []any{bwClickHouseInit()}, "containers": []any{bwClickHouseMain()}}), "StatefulSet", sts)
	w.otherPods = append(w.otherPods, pod)
	return w
}

// The StatefulSet the script trusts is named <fullname>-clickhouse with the chart's fullname rule (_helpers.tpl:11-22): the
// FULLNAME override, else RELEASE when it contains "dev-health", else RELEASE-dev-health. A wrong derivation must not trust the
// wrong StatefulSet (and trusting nothing shows as a STOP on the real one, tested by the pass rows).
func TestBarrierFullnameRuleNamesTheTrustedStatefulSet(t *testing.T) {
	cases := []struct {
		name string
		env  []string
		sts  string
		pass bool
	}{
		{"release without the chart name", []string{"RELEASE=a"}, "a-dev-health-clickhouse", true},
		{"release that contains the chart name", []string{"RELEASE=prod-dev-health"}, "prod-dev-health-clickhouse", true},
		{"the FULLNAME override", []string{"RELEASE=a", "FULLNAME=custom"}, "custom-clickhouse", true},
		{"release without the chart name, no fullname", []string{"RELEASE=a"}, "a-clickhouse", false},
		{"release that contains the chart name, doubled", []string{"RELEASE=prod-dev-health"}, "prod-dev-health-dev-health-clickhouse", false},
		{"the override replaces the default", []string{"RELEASE=a", "FULLNAME=custom"}, "a-dev-health-clickhouse", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := runBarrier(t, bwClickHouseWorldNamed(c.sts).files(), c.env, "up-go-api")
			if c.pass && !bwPassed(r) {
				t.Fatalf("refused: rc=%d stderr=%q", r.rc, r.stderr)
			}
			if !c.pass && (bwPassed(r) || r.rc == 0) {
				t.Fatalf("passed: rc=%d stdout=%q", r.rc, r.stdout)
			}
		})
	}
}

// A server container is trusted for a pod only. A Job or a CronJob that carries a go-api or auth-service server container
// (here the auth-service init container, which runs `sh -ec`) is scanned like any other and STOPs the barrier.
func TestBarrierNeverTrustsAJobOrCronJobWithAServerContainer(t *testing.T) {
	spec := map[string]any{"initContainers": []any{bwAuthInit()}, "containers": []any{bwServerAuth("")}}
	for _, kind := range []string{"job", "cronjob"} {
		t.Run(kind, func(t *testing.T) {
			w := bwNewWorld()
			if kind == "job" {
				w.jobs = append(w.jobs, bwJobItem("t1", spec))
			} else {
				w.cronJobs = append(w.cronJobs, bwCronJobItem("t1", spec))
			}
			r := runBarrier(t, w.files(), nil, "up-go-api")
			if bwPassed(r) || r.rc == 0 || !strings.Contains(r.stderr, "t1") {
				t.Fatalf("a %s with a server container was trusted: rc=%d stdout=%q stderr=%q", kind, r.rc, r.stdout, r.stderr)
			}
		})
	}
}
