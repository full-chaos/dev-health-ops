package bigboy_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	cutCompose  = "ci/bigboy/compose.bigboy.yml"
	cutRequired = "ci/bigboy/check-web-env-required.sh"
	cutHook     = "ci/bigboy/bigboy-hook-check.sh"
	cutRiver    = "ci/bigboy/bigboy-river-apply.sh"
	cutPGGrants = "ci/bigboy/pg-grants-check.sh"
	cutProve    = "ci/bigboy/bigboy-graphql-prove.sh"
)

func cutCheckOneComposeFile(g *guard) {
	r := cutRunCut(g, cutOpts{env: append([]string{"COMPOSE_FILE=compose.yml:compose/x.yml", "COMPOSE_PROFILES=x"}, cutDeployEnv...)})
	want := r.tools + "/compose.bigboy.yml"
	n := 0
	for _, c := range r.calls {
		g.expect(!c.composeFile, "COMPOSE_FILE reached `%s`: the file chain must come from -f only", c.line())
		if !c.compose() {
			continue
		}
		n++
		if fs := c.flagValues("-f"); len(fs) != 1 || fs[0] != want {
			g.failf("the cut must hand compose exactly one -f %s, got %v in `%s`", want, fs, c.line())
		}
	}
	g.expect(n > 0, "the cut ran no docker compose: %s", r.dump())
	// A reference to a retired chain file is not observable by running the cut, so the file text is scanned.
	text := g.read(cutScript)
	for _, old := range []string{"stack.sh", "stack.files", "compose.bigboy.router.yml"} {
		g.expect(!strings.Contains(text, old), "bigboy-cut.sh still names the retired %s", old)
	}
}

func cutEnvMap(v any) map[string]string {
	out := map[string]string{}
	switch e := v.(type) {
	case map[string]any:
		for k, x := range e {
			out[k] = str(x)
		}
	case []any:
		for _, x := range e {
			k, val, _ := strings.Cut(str(x), "=")
			out[k] = val
		}
	}
	return out
}

func cutCheckWebEnv(g *guard) {
	env := cutEnvMap(at(g.yamlFile(cutCompose), "services", "web", "environment"))
	g.expect(env["BACKEND_URL"] == "http://traefik:3000", "web BACKEND_URL is %q, want the plane-split router http://traefik:3000", env["BACKEND_URL"])
	_, ok := env["AUTH_URL"]
	g.expect(ok, "web has no AUTH_URL")
}

func cutRunRequired(g *guard, names *string, required ...string) scriptRun {
	file := filepath.Join(cutTemp(g, "names"), "names")
	if names != nil {
		cutWrite(g, file, *names, 0o644)
	}
	return cutExec(g, g.tmp, []string{"PATH=" + os.Getenv("PATH")}, "bash", append([]string{g.path(cutRequired), file}, required...)...)
}

func cutP(s string) *string { return &s }

func cutCheckRequiredPresent(g *guard) {
	r := cutRunRequired(g, cutP("AUTH_URL\nBACKEND_URL\nNODE_ENV\n"), "BACKEND_URL", "AUTH_URL")
	g.expect(r.code == 0, "every required name present must pass, got rc=%d: %s", r.code, r.stderr)
}

func cutCheckRequiredMissing(g *guard) {
	r := cutRunRequired(g, cutP("NODE_ENV\nBACKEND_URL_OLD\n"), "BACKEND_URL", "AUTH_URL")
	g.expect(r.code == 1, "a missing name must fail rc=1, got rc=%d", r.code)
	g.expect(strings.Contains(r.stderr, "BACKEND_URL") && strings.Contains(r.stderr, "AUTH_URL"), "every missing name must be named: %s", r.stderr)
}

func cutCheckRequiredUnread(g *guard) {
	r := cutRunRequired(g, nil, "BACKEND_URL")
	g.expect(r.code == 2, "an unread names file must fail rc=2, got rc=%d: %s", r.code, r.stderr)
	r = cutRunRequired(g, cutP(""), "BACKEND_URL")
	g.expect(r.code == 2, "an empty names file must fail rc=2, got rc=%d: %s", r.code, r.stderr)
}

func cutCheckWebEnvAfterRecreate(g *guard) {
	r := cutRunCut(g, cutOpts{})
	up := cutFind(r.calls, 0, func(c cutCall) bool { return c.compose("up", "go-api", "web") })
	chk := cutFind(r.calls, 0, cutTool("check-web-env-required.sh"))
	if up < 0 || chk < 0 || chk < up {
		g.failf("the web env check (call %d) must run after the recreate it verifies (call %d): %s", chk, up, r.dump())
		return
	}
	g.expect(slices.Equal(r.calls[chk].args[1:], []string{"BACKEND_URL", "AUTH_URL"}), "the web env check must require BACKEND_URL AUTH_URL: %s", r.calls[chk].line())
}

const cutMarker = "plainword-marker-c"

var cutClassDigest = "sha256:" + strings.Repeat("ab", 32)

const cutFakeDocker = `#!/bin/bash
d="$FAKE_DIR"; n=$(( $(cat "$d/n" 2>/dev/null || echo 0) + 1 )); echo "$n" > "$d/n"
printf '%s\0' "$@" > "$d/argv.$n"; env > "$d/env.$n"
cat /dev/stdin > "$d/stdin.$n" 2>/dev/null < /dev/stdin || true
case "$1" in
  buildx) echo '{"digest":"'"$FAKE_DIGEST"'"}'; exit 0 ;;
  compose)
    if printf '%s\n' "$@" | grep -qx 'config'; then echo "ghcr.io/full-chaos/dev-health-go-dho@$FAKE_DIGEST"; exit 0; fi
    if printf '%s\n' "$@" | grep -qx 'ps'; then [ -n "${FAKE_NO_RECONCILER:-}" ] && exit 0; echo "ghcr.io/full-chaos/dev-health-go-operator@$FAKE_DIGEST"; exit 0; fi
    if printf '%s\n' "$@" | grep -qx 'venue-hook'; then
      h=$(( $(cat "$d/hook-n" 2>/dev/null || echo 0) + 1 )); echo "$h" > "$d/hook-n"
      echo '{"msg":"migrate step ok","time":"t"}'; echo '{"state":"clean"}'
      if [ -n "${FAKE_FAIL_HOOK_CALL:-}" ]; then [ "$h" = "$FAKE_FAIL_HOOK_CALL" ] && exit 3; exit 0; fi
      exit "${FAKE_RC:-0}"
    fi
    if printf '%s\n' "$@" | grep -qx 'venue-river'; then echo 'applying postgresql://devhealth:plainword-river-marker@postgres:5432/db then postgresql://other:plainword-second-marker@h:5432/x'; exit "${FAKE_RC:-0}"; fi
    if printf '%s\n' "$@" | grep -qx 'postgres'; then echo 'r|t|SELECT|t|t'; exit "${FAKE_RC:-0}"; fi
    echo 'SELECT	t'; exit "${FAKE_RC:-0}" ;;
  *) echo "unexpected docker verb $1" >&2; exit 99 ;;
esac
`

// The pre-CHAOS-8371 hook check, kept as a planted defect.
const cutOldHook = `#!/usr/bin/env bash
set -euo pipefail
ENVF=$(mktemp); trap 'rm -f "$ENVF"' EXIT
PW=$(docker exec dev-health-api-1 printenv POSTGRES_PASSWORD)
{ echo "MIGRATION_DATABASE_URI=postgresql://devhealth:${PW}@postgres:5432/devhealth?sslmode=disable"; } > "$ENVF"
docker run --rm --network dev-health_dev-health --env-file "$ENVF" op migrate upgrade --river=false || true
`

type cutDockerCall struct {
	argv  []string
	env   map[string]string
	stdin string
}

func (c cutDockerCall) has(a string) bool { return slices.Contains(c.argv, a) }

func (c cutDockerCall) after(flag string) string {
	if i := slices.Index(c.argv, flag); i >= 0 && i+1 < len(c.argv) {
		return c.argv[i+1]
	}
	return ""
}

type cutClass struct {
	scriptRun
	host, fake, scratch string
	calls               []cutDockerCall
}

type cutClassOpts struct {
	args    []string
	fakeRC  string
	env     []string
	kubectl bool
}

// A closed PATH and a sentinel password in the ambient env: a leak shows in what the recording docker received.
func cutRunClass(g *guard, script string, o cutClassOpts) cutClass {
	tmp := cutTemp(g, "class")
	host := filepath.Join(tmp, "host")
	cutWrite(g, filepath.Join(host, "ops", ".env"), "X=1\n", 0o644)
	cutWrite(g, filepath.Join(host, "_records", "images.env"), "BIGBOY_DHO_IMAGE=ghcr.io/full-chaos/dev-health-go-dho@sha256:"+strings.Repeat("cd", 32)+"\n", 0o644)
	cutWrite(g, filepath.Join(host, "rec", "sha.txt"), "1234567abcdef\n", 0o644)
	cutMkdir(g, host, "ops", ".git")
	bin := cutMkdir(g, tmp, "bin")
	for _, tool := range []string{"bash", "cat", "dirname", "grep", "sed", "cut", "head", "tail", "jq", "env", "awk", "wc",
		"printf", "mkdir", "rm", "date", "tr", "mktemp", "sort", "paste", "git"} {
		p, err := exec.LookPath(tool)
		if err != nil {
			g.failf("harness: %s not found", tool)
			continue
		}
		if err := os.Symlink(p, filepath.Join(bin, tool)); err != nil {
			g.failf("harness: %v", err)
		}
	}
	cutWrite(g, filepath.Join(bin, "docker"), cutFakeDocker, 0o755)
	cutStub(g, bin, "python3", "exit 0\n")
	if o.kubectl {
		cutStub(g, bin, "kubectl", `d="$FAKE_DIR"; printf '%s\0' "$@" > "$d/kubectl-argv"; cat > /dev/null; echo 'r|t|SELECT|t|t'`+"\n")
	}
	fake := cutMkdir(g, tmp, "fake")
	scratch := cutMkdir(g, tmp, "scratch")
	env := []string{"PATH=" + bin, "FAKE_DIR=" + fake, "FAKE_DIGEST=" + cutClassDigest, "HOME=" + tmp, "TMPDIR=" + scratch,
		"BIGBOY_ROOT=" + host, "POSTGRES_PASSWORD=" + cutMarker, "CLICKHOUSE_PASSWORD=" + cutMarker}
	if o.fakeRC != "" {
		env = append(env, "FAKE_RC="+o.fakeRC)
	}
	for _, e := range o.env {
		env = append(env, strings.ReplaceAll(e, "{host}", host))
	}
	args := make([]string, 0, len(o.args)+1)
	args = append(args, g.path(script))
	for _, a := range o.args {
		args = append(args, strings.ReplaceAll(a, "{host}", host))
	}
	r := cutExec(g, tmp, env, "bash", args...)
	return cutClass{scriptRun: r, host: host, fake: fake, scratch: scratch, calls: cutDockerCalls(fake)}
}

func cutNul(p string) []string {
	b, err := os.ReadFile(p)
	if err != nil {
		return nil
	}
	parts := strings.Split(string(b), "\x00")
	return parts[:len(parts)-1]
}

func cutDockerCalls(fake string) []cutDockerCall {
	var ns []int
	for _, p := range walkFiles(fake) {
		if n, err := strconv.Atoi(strings.TrimPrefix(filepath.Base(p), "argv.")); err == nil && strings.HasPrefix(filepath.Base(p), "argv.") {
			ns = append(ns, n)
		}
	}
	sort.Ints(ns)
	var out []cutDockerCall
	for _, n := range ns {
		c := cutDockerCall{argv: cutNul(filepath.Join(fake, fmt.Sprintf("argv.%d", n))), env: map[string]string{},
			stdin: cutFileText(filepath.Join(fake, fmt.Sprintf("stdin.%d", n)))}
		for _, ln := range strings.Split(cutFileText(filepath.Join(fake, fmt.Sprintf("env.%d", n))), "\n") {
			if k, v, ok := strings.Cut(ln, "="); ok {
				c.env[k] = v
			}
		}
		out = append(out, c)
	}
	return out
}

var (
	cutEnvFileOK   = regexp.MustCompile(`^(?:/\S*/)?(?:ops/\.env|_records/images\.env)$`)
	cutCredEnvName = regexp.MustCompile(`(PASSWORD|TOKEN|SECRET|_URI)$`)
)

func cutViolations(calls []cutDockerCall) []string {
	var v []string
	for _, c := range calls {
		if len(c.argv) == 0 {
			continue
		}
		if c.argv[0] != "compose" && c.argv[0] != "buildx" {
			v = append(v, fmt.Sprintf("bare docker verb %q", c.argv[0]))
		}
		for i, a := range c.argv {
			if strings.Contains(a, cutMarker) {
				v = append(v, "credential in argv")
			}
			if (strings.HasPrefix(a, "-e") && !strings.HasPrefix(a, "--")) ||
				(strings.HasPrefix(a, "--env") && a != "--env-file" && !strings.HasPrefix(a, "--env-file=")) {
				v = append(v, fmt.Sprintf("env-passing flag %q", a))
			}
			val, isFile := "", false
			if a == "--env-file" && i+1 < len(c.argv) {
				val, isFile = c.argv[i+1], true
			} else if strings.HasPrefix(a, "--env-file=") {
				val, isFile = strings.TrimPrefix(a, "--env-file="), true
			}
			if isFile && !cutEnvFileOK.MatchString(val) {
				v = append(v, fmt.Sprintf("foreign --env-file %q", val))
			}
			if a == "printenv" {
				v = append(v, "printenv out of a container")
			}
		}
		for k, val := range c.env {
			switch k {
			case "POSTGRES_PASSWORD", "CLICKHOUSE_PASSWORD", "FAKE_DIR", "FAKE_DIGEST", "FAKE_RC":
				continue
			}
			if strings.Contains(val, cutMarker) || cutCredEnvName.MatchString(k) {
				v = append(v, "credential-shaped child env "+k)
			}
		}
	}
	return v
}

func cutClassClean(g *guard, r cutClass) {
	for _, v := range cutViolations(r.calls) {
		g.failf("class rule: %s", v)
	}
}

func cutRuns(calls []cutDockerCall, service string) []cutDockerCall {
	var out []cutDockerCall
	for _, c := range calls {
		if len(c.argv) > 0 && c.argv[0] == "compose" && c.has(service) {
			out = append(out, c)
		}
	}
	return out
}

func cutComposeFileArg(c cutDockerCall) bool {
	return slices.ContainsFunc(c.argv, func(a string) bool { return strings.HasSuffix(a, "/compose.bigboy.yml") })
}

func cutCheckHookClassRules(g *guard) {
	r := cutRunClass(g, cutHook, cutClassOpts{args: []string{"{host}/rec"}})
	g.expect(r.code == 0, "bigboy-hook-check.sh failed rc=%d: %s %s", r.code, r.stdout, r.stderr)
	cutClassClean(g, r)
	runs := cutRuns(r.calls, "venue-hook")
	if len(runs) != 2 {
		g.failf("want 2 venue-hook one-offs, got %d", len(runs))
		return
	}
	g.expect(slices.Equal(runs[0].argv[len(runs[0].argv)-3:], []string{"migrate", "upgrade", "--river=false"}), "the first one-off is not `migrate upgrade --river=false`: %v", runs[0].argv)
	g.expect(slices.Equal(runs[1].argv[len(runs[1].argv)-4:], []string{"migrate", "clickhouse", "status", "--check"}), "the second one-off is not `migrate clickhouse status --check`: %v", runs[1].argv)
	for _, c := range runs {
		g.expect(c.has("run"), "a venue-hook call is not a compose run: %v", c.argv)
		_, cf := c.env["COMPOSE_FILE"]
		g.expect(!cf, "COMPOSE_FILE reached the one-off: the one file must be a -f argument")
		g.expect(cutComposeFileArg(c), "the one-off does not name compose.bigboy.yml: %v", c.argv)
		g.expect(strings.HasSuffix(c.env["HOOK_OPERATOR_IMAGE"], "@"+cutClassDigest), "HOOK_OPERATOR_IMAGE is %q, want the derived operator image ref", c.env["HOOK_OPERATOR_IMAGE"])
		g.expect(!strings.Contains(strings.Join(c.argv, " "), filepath.Join(r.host, "rec")), "the record path reached the one-off's argv: %v", c.argv)
	}
	g.expect(len(walkFiles(r.scratch)) == 0, "the hook check left a script-written file in TMPDIR: %v", walkFiles(r.scratch))
	g.expect(strings.Contains(r.stdout, "hook_upgrade_rc=0") && strings.Contains(r.stdout, "ch_status_check_rc=0"), "the step codes are not reported: %s", r.stdout)
}

func cutCheckHookFailingStep(g *guard) {
	r := cutRunClass(g, cutHook, cutClassOpts{args: []string{"{host}/rec"}, fakeRC: "3"})
	g.expect(r.code == 1 && strings.Contains(r.stdout, "hook_upgrade_rc=3"), "a failing hook step must fail the script rc=1 with hook_upgrade_rc=3: rc=%d %s", r.code, r.stdout)
}

func cutCheckHookEachGate(g *guard) {
	for _, c := range []struct{ call, marker string }{{"1", "hook_upgrade_rc=3"}, {"2", "ch_status_check_rc=3"}} {
		r := cutRunClass(g, cutHook, cutClassOpts{args: []string{"{host}/rec"}, env: []string{"FAKE_FAIL_HOOK_CALL=" + c.call}})
		g.expect(r.code == 1 && strings.Contains(r.stdout, c.marker), "only venue-hook call %s failing must fail the script rc=1 with %s: rc=%d %s", c.call, c.marker, r.code, r.stdout)
	}
}

func cutCheckInheritedComposeFile(g *guard) {
	for _, c := range []struct {
		script, profile string
		args            []string
	}{{cutHook, "venue-hook", []string{"{host}/rec"}}, {cutRiver, "venue-river", nil}} {
		r := cutRunClass(g, c.script, cutClassOpts{args: c.args, env: []string{"COMPOSE_FILE=compose.yml:compose/x.yml"}})
		g.expect(r.code == 0, "%s failed rc=%d: %s %s", c.script, r.code, r.stdout, r.stderr)
		if len(r.calls) == 0 {
			g.failf("%s made no docker call", c.script)
			continue
		}
		last := r.calls[len(r.calls)-1]
		_, cf := last.env["COMPOSE_FILE"]
		g.expect(!cf, "%s: an inherited COMPOSE_FILE reached compose", c.script)
		var files []string
		for i, a := range last.argv {
			if a == "-f" && i+1 < len(last.argv) {
				files = append(files, last.argv[i+1])
			}
		}
		g.expect(len(files) == 1 && strings.HasSuffix(files[0], "/compose.bigboy.yml"), "%s: want the one -f compose.bigboy.yml, got %v", c.script, files)
		g.expect(last.has("--profile") && last.has("venue") && last.has(c.profile), "%s: the one-off lacks --profile venue / %s: %v", c.script, c.profile, last.argv)
	}
}

func cutCheckRiverNoReconciler(g *guard) {
	r := cutRunClass(g, cutRiver, cutClassOpts{env: []string{"FAKE_NO_RECONCILER=1"}})
	g.expect(r.code == 0, "the river apply by hand failed rc=%d: %s %s", r.code, r.stdout, r.stderr)
	g.expect(len(cutRuns(r.calls, "venue-river")) > 0, "the river apply never ran venue-river")
	g.expect(strings.Contains(r.stdout, "river_apply_rc=0"), "no river_apply_rc=0: %s", r.stdout)
}

func cutCheckRiverRedacts(g *guard) {
	r := cutRunClass(g, cutRiver, cutClassOpts{})
	out := r.stdout + r.stderr
	g.expect(!strings.Contains(out, "plainword-river-marker"), "the river apply printed the first DSN's password")
	g.expect(!strings.Contains(out, "plainword-second-marker"), "the river apply printed the second DSN's password of the same line")
	g.expect(strings.Contains(r.stdout, "devhealth:<redacted>@"), "no redacted DSN in the output: %s", r.stdout)
}

func cutCheckRiverClassRules(g *guard) {
	r := cutRunClass(g, cutRiver, cutClassOpts{})
	g.expect(r.code == 0, "the river apply failed rc=%d: %s %s", r.code, r.stdout, r.stderr)
	cutClassClean(g, r)
	runs := cutRuns(r.calls, "venue-river")
	if len(runs) != 1 {
		g.failf("want 1 venue-river one-off, got %d", len(runs))
		return
	}
	a := runs[0].argv
	g.expect(slices.Equal(a[len(a)-3:], []string{"migrate", "river", "--apply-and-check"}), "the one-off is not `migrate river --apply-and-check`: %v", a)
	_, cf := runs[0].env["COMPOSE_FILE"]
	g.expect(!cf, "COMPOSE_FILE reached the river one-off")
	g.expect(cutComposeFileArg(runs[0]), "the river one-off does not name compose.bigboy.yml: %v", a)
	g.expect(strings.Contains(r.stdout, "image=sha256:"+strings.Repeat("cd", 32)), "the dho pin of images.env is not the reported image: %s", r.stdout)
	g.expect(strings.Contains(r.stdout, "river_apply_rc=0"), "no river_apply_rc=0: %s", r.stdout)
}

func cutCheckRiverFailingApply(g *guard) {
	r := cutRunClass(g, cutRiver, cutClassOpts{fakeRC: "5"})
	g.expect(strings.Contains(r.stdout, "river_apply_rc=5"), "a failing apply is not reported: %s", r.stdout)
	g.expect(r.code == 5, "the script's own exit status must carry the apply failure: rc=%d", r.code)
}

func cutPGExpect(g *guard, text string) string {
	p := filepath.Join(cutTemp(g, "exp"), "exp.txt")
	cutWrite(g, p, text, 0o644)
	return p
}

func cutNoPassword(argv []string) bool {
	return !slices.ContainsFunc(argv, func(a string) bool {
		return strings.Contains(a, "PGPASSWORD") || strings.Contains(a, "POSTGRES_PASSWORD")
	})
}

func cutPsqlW(argv []string) bool {
	return slices.ContainsFunc(argv, func(a string) bool { return strings.Contains(a, "psql -w -U") })
}

func cutCheckPGGrantsBigboy(g *guard) {
	r := cutRunClass(g, cutPGGrants, cutClassOpts{args: []string{"bigboy", cutPGExpect(g, "r t SELECT\n")}})
	g.expect(r.code == 0 && strings.Contains(r.stdout, "PG_OK expectations=1"), "pg-grants-check bigboy failed rc=%d: %s %s", r.code, r.stdout, r.stderr)
	cutClassClean(g, r)
	i := slices.IndexFunc(r.calls, func(c cutDockerCall) bool { return c.has("exec") })
	if i < 0 {
		g.failf("no compose exec call")
		return
	}
	a := r.calls[i].argv
	g.expect(len(a) >= 3 && slices.Equal(a[:3], []string{"compose", "-p", "dev-health"}) && r.calls[i].after("--env-file") == "ops/.env", "not a compose call of project dev-health on ops/.env: %v", a)
	ex, t, pg := slices.Index(a, "exec"), slices.Index(a, "-T"), slices.Index(a, "postgres")
	g.expect(ex < t && t < pg, "want `exec -T postgres`: %v", a)
	g.expect(cutNoPassword(a), "a password is set or expanded on the psql call: %v", a)
	g.expect(cutPsqlW(a), "psql must run with -w and no password: %v", a)
}

func cutCheckPGGrantsProd(g *guard) {
	r := cutRunClass(g, cutPGGrants, cutClassOpts{args: []string{"prod", cutPGExpect(g, "r t SELECT\n")}, kubectl: true})
	g.expect(r.code == 0 && strings.Contains(r.stdout, "PG_OK expectations=1"), "pg-grants-check prod failed rc=%d: %s %s", r.code, r.stdout, r.stderr)
	a := cutNul(filepath.Join(r.fake, "kubectl-argv"))
	g.expect(slices.Contains(a, "exec") && slices.Contains(a, "-i") && slices.Contains(a, "dev-health-ops-postgresql-0"), "the prod branch is not a kubectl exec -i on the postgres pod: %v", a)
	g.expect(cutPsqlW(a), "the prod psql must run with -w and no password: %v", a)
	g.expect(cutNoPassword(a), "a password is set or expanded on the prod psql call: %v", a)
	g.expect(len(r.calls) == 0, "the prod branch must not call docker")
}

func cutCheckGrantsComposeExec(g *guard) {
	r := cutRunClass(g, cutGrants, cutClassOpts{args: []string{"{host}/rec"}, env: []string{"BIGBOY_OPS_REPO={host}/ops"}})
	if len(r.calls) == 0 {
		g.failf("the grants check made no docker call: %s %s", r.stdout, r.stderr)
		return
	}
	cutClassClean(g, r)
	i := slices.IndexFunc(r.calls, func(c cutDockerCall) bool { return c.has("exec") })
	if i < 0 {
		g.failf("no compose exec call")
		return
	}
	a := r.calls[i].argv
	g.expect(len(a) >= 3 && slices.Equal(a[:3], []string{"compose", "-p", "dev-health"}) && r.calls[i].after("--env-file") == "ops/.env", "not a compose call of project dev-health on ops/.env: %v", a)
	g.expect(slices.Contains(a, "clickhouse") && slices.Contains(a, "clickhouse-client") && slices.Contains(a, "-T"), "want `exec -T clickhouse clickhouse-client`: %v", a)
}

func cutCheckOneOffsYAML(g *guard) {
	d := g.yamlFile(cutCompose)
	uri := regexp.MustCompile(`^postgresql://devhealth:\$\{POSTGRES_PASSWORD:-devhealth\}@postgres:5432/devhealth\?sslmode=disable$`)
	for _, c := range []struct{ svc, prefix string }{{"venue-hook", "${HOOK_OPERATOR_IMAGE:-"}, {"venue-river", "${BIGBOY_DHO_IMAGE:?"}} {
		s := mp(at(d, "services", c.svc))
		g.expect(equal(strs(s["profiles"]), []string{c.svc}), "%s profiles are %v, want [%s]", c.svc, s["profiles"], c.svc)
		var nets []string
		for k := range mp(s["networks"]) {
			nets = append(nets, k)
		}
		g.expect(equal(nets, []string{"dev-health"}), "%s networks are %v, want [dev-health]", c.svc, nets)
		g.expect(strings.HasPrefix(str(s["image"]), c.prefix), "%s image is %q, want a %s reference", c.svc, s["image"], c.prefix)
		g.expect(uri.MatchString(cutEnvMap(s["environment"])["MIGRATION_DATABASE_URI"]), "%s MIGRATION_DATABASE_URI is not the compose-substituted DSN: %q", c.svc, cutEnvMap(s["environment"])["MIGRATION_DATABASE_URI"])
		_, ef := s["env_file"]
		g.expect(!ef, "%s carries env_file: never the whole ops/.env", c.svc)
	}
	river := mp(at(d, "services", "venue-river"))
	g.expect(equal(strs(river["entrypoint"]), []string{"/usr/local/bin/dho"}), "venue-river entrypoint is %v", river["entrypoint"])
	env := cutEnvMap(river["environment"])
	delete(env, "MIGRATION_DATABASE_URI")
	g.expect(equal(env, map[string]string{"RIVER_DATABASE_SCHEMA": "river", "RIVER_DOMAIN_DATABASE_ROLE": "devhealth_domain",
		"RIVER_QUEUE_DATABASE_ROLE": "devhealth_queue", "RIVER_COORDINATOR_DATABASE_ROLE": "devhealth_coordinator", "API_DATABASE_ROLE": "devhealth_api"}),
		"venue-river environment is %v", env)
	g.expect(cutEnvMap(at(d, "services", "venue-hook", "environment"))["CLICKHOUSE_URI"] == "clickhouse://${CLICKHOUSE_USER:-ch}:${CLICKHOUSE_PASSWORD:-ch}@clickhouse:9000/default",
		"venue-hook CLICKHOUSE_URI is not the compose-substituted DSN")
}

var cutBareDocker = regexp.MustCompile(`docker\s+(?:container\s+)?(?:exec|run|cp)\b|\bprintenv\b|\bPGPASSWORD\b`)

// A code-shape rule over every run file, most of which cannot run outside bigboy: a comment-stripped scan.
func cutCheckNoBareDocker(g *guard) {
	entries, err := os.ReadDir(g.path("ci/bigboy"))
	if err != nil {
		g.failf("cannot list ci/bigboy: %v", err)
		return
	}
	for _, e := range entries {
		if ext := filepath.Ext(e.Name()); e.IsDir() || (ext != ".sh" && ext != ".py") {
			continue
		}
		code := strings.Join(cutStripComments(g.read("ci/bigboy/"+e.Name())), "\n")
		if hits := cutBareDocker.FindAllString(code, -1); len(hits) > 0 {
			g.failf("%s: bare docker exec/run/cp, printenv or PGPASSWORD: %v", e.Name(), hits)
		}
	}
}

var (
	cutEFlag   = regexp.MustCompile(`\s-e[A-Za-z_=]*(\s|$)`)
	cutPWExp   = regexp.MustCompile(`\$\{?[A-Za-z_]*PASSWORD`)
	cutPWAssig = regexp.MustCompile(`(?i)\bpw\w*=`)
)

// Every code line of the five scripts, including branches no stub run reaches: a comment-stripped scan.
func cutCheckNoEnvFlagNoPasswordExpansion(g *guard) {
	for _, s := range []string{cutHook, cutRiver, cutPGGrants, cutGrants, cutProve} {
		for _, ln := range cutStripComments(g.read(s)) {
			if strings.Contains(ln, "docker ") || strings.Contains(ln, "HC=(") {
				l := strings.ReplaceAll(ln, " -e PROVE_ORG ", " ")
				g.expect(!cutEFlag.MatchString(l), "%s: env-passing flag -e on a docker line: %s", s, ln)
				for rest := l; ; {
					i := strings.Index(rest, "--env")
					if i < 0 {
						break
					}
					rest = rest[i+len("--env"):]
					g.expect(strings.HasPrefix(rest, "-file"), "%s: env-passing flag --env on a docker line: %s", s, ln)
				}
			}
			g.expect(!cutPWExp.MatchString(ln), "%s: password expansion: %s", s, ln)
			g.expect(!cutPWAssig.MatchString(ln), "%s: password assignment: %s", s, ln)
		}
	}
}

func cutCheckProvePsqlIsComposeExec(g *guard) {
	root := cutTemp(g, "prove-root")
	cutWrite(g, filepath.Join(root, "_records", "images.env"), "BIGBOY_API_TOOLS_IMAGE=ghcr.io/full-chaos/dev-health-go-api-tools@"+cutNewDigest+"\n", 0o644)
	cutWrite(g, filepath.Join(root, "ops", ".env"), "", 0o644)
	work := cutTemp(g, "prove")
	log := filepath.Join(work, "calls.log")
	bin := cutMkdir(g, work, "bin")
	cutStub(g, bin, "docker", cutDockerBody([]cutDigest{{"dev-health-go-api-tools:sha-", cutNewDigest}}))
	r := cutExec(g, work, []string{"PATH=" + bin + ":/usr/bin:/bin", "HOME=" + work, "CUT_LOG=" + log, "BIGBOY_ROOT=" + root},
		"bash", g.path(cutProve), cutNew)
	calls := cutCalls(g, log)
	i := cutFind(calls, 0, func(c cutCall) bool { return c.has("psql") })
	if i < 0 {
		g.failf("bigboy-graphql-prove.sh never ran psql (rc=%d): %s %s", r.code, r.stdout, r.stderr)
		return
	}
	want := []string{"compose", "-p", "dev-health", "--project-directory", root, "--env-file", "ops/.env", "--env-file", "_records/images.env",
		"-f", g.path("ci/bigboy") + "/compose.bigboy.yml", "--profile", "venue", "exec", "-T", "postgres", "psql", "-U", "devhealth"}
	a := calls[i].args
	g.expect(len(a) >= len(want) && slices.Equal(a[:len(want)], want), "the local-org psql is not a compose exec on the stack files:\n got %v\nwant %v ...", a, want)
}

func cutCheckOneOffShape(g *guard) {
	for _, c := range []struct {
		script, service string
		args            []string
	}{{cutHook, "venue-hook", []string{"{host}/rec"}}, {cutRiver, "venue-river", nil}} {
		r := cutRunClass(g, c.script, cutClassOpts{args: c.args})
		g.expect(r.code == 0, "%s failed rc=%d: %s %s", c.script, r.code, r.stdout, r.stderr)
		runs := cutRuns(r.calls, c.service)
		g.expect(len(runs) > 0, "%s ran no %s one-off", c.script, c.service)
		for _, run := range runs {
			a := run.argv
			g.expect(len(a) >= 3 && slices.Equal(a[:3], []string{"compose", "-p", "dev-health"}), "%s: not a compose call of project dev-health: %v", c.script, a)
			g.expect(run.after("--env-file") == "ops/.env", "%s: the first --env-file is not ops/.env: %v", c.script, a)
			for _, need := range []string{"--profile", c.service, "run", "--rm", "--no-deps", "-T"} {
				g.expect(run.has(need), "%s: the one-off lacks %s: %v", c.script, need, a)
			}
			var profiles []string
			for i, x := range a {
				if x == "--profile" && i+1 < len(a) {
					profiles = append(profiles, a[i+1])
				}
			}
			g.expect(slices.Equal(profiles, []string{"venue", c.service}), "%s: profiles are %v, want [venue %s]", c.script, profiles, c.service)
		}
	}
}

func cutCheckPGColumnExpectation(g *guard) {
	r := cutRunClass(g, cutPGGrants, cutClassOpts{args: []string{"bigboy", cutPGExpect(g, "r t SELECT\nr billing_audit_log.actor_id UPDATE\n")}})
	var sql string
	for _, c := range r.calls {
		sql += c.stdin
	}
	g.expect(strings.Contains(sql, "has_column_privilege('r','public.billing_audit_log','actor_id','UPDATE')"), "a column expectation is not read with has_column_privilege: %s %s", sql, r.stdout)
	g.expect(strings.Contains(sql, "has_table_privilege('r','public.t','SELECT')"), "a table expectation is not read with has_table_privilege: %s", sql)
	g.expect(!strings.Contains(sql, "has_table_privilege('r','public.billing_audit_log.actor_id'"), "a column expectation was read as a table: %s", sql)
	cutClassClean(g, r)
}

func TestCutComposeChain(t *testing.T) {
	runReal(t,
		check{"the_cut_runs_the_one_compose_file_and_no_overlay_chain", cutCheckOneComposeFile},
		check{"resolved_web_service_carries_backend_url_and_auth_url", cutCheckWebEnv},
		check{"required_names_present_passes", cutCheckRequiredPresent},
		check{"missing_name_fails_and_is_named", cutCheckRequiredMissing},
		check{"unread_names_file_fails_loudly", cutCheckRequiredUnread},
		check{"cut_checks_web_env_after_the_web_recreate", cutCheckWebEnvAfterRecreate},
		check{"hook_check_obeys_every_class_rule", cutCheckHookClassRules},
		check{"hook_check_failing_step_fails_the_script", cutCheckHookFailingStep},
		check{"each_hook_check_gate_fails_the_script_on_its_own", cutCheckHookEachGate},
		check{"an_inherited_compose_file_never_decides_the_chain", cutCheckInheritedComposeFile},
		check{"river_apply_by_hand_needs_no_running_reconciler", cutCheckRiverNoReconciler},
		check{"river_apply_redacts_the_dsn_it_prints", cutCheckRiverRedacts},
		check{"river_apply_obeys_every_class_rule", cutCheckRiverClassRules},
		check{"river_apply_reports_a_failing_apply", cutCheckRiverFailingApply},
		check{"pg_grants_check_bigboy_uses_compose_exec_only", cutCheckPGGrantsBigboy},
		check{"pg_grants_check_prod_branch_runs_psql_w_without_a_password", cutCheckPGGrantsProd},
		check{"grants_check_uses_compose_exec_only", cutCheckGrantsComposeExec},
		check{"one_offs_carry_no_literal_credential_and_use_compose_substitution", cutCheckOneOffsYAML},
		check{"no_bare_docker_exec_run_or_printenv_in_ci_bigboy_run_files", cutCheckNoBareDocker},
		check{"no_env_passing_flag_on_any_docker_line_and_no_password_expansion", cutCheckNoEnvFlagNoPasswordExpansion},
		check{"graphql_prove_psql_line_is_a_compose_exec_on_the_stack_files", cutCheckProvePsqlIsComposeExec},
		check{"the_one_offs_keep_their_compose_shape", cutCheckOneOffShape},
		check{"pg_grants_check_reads_a_column_expectation_with_has_column_privilege", cutCheckPGColumnExpectation},
	)
}

const cutUnset = `unset $(compgen -v | grep '^COMPOSE_')`

func TestCutComposeChainPlanted(t *testing.T) {
	cp := []string{"ci/bigboy"}
	hookRun := `run --rm --no-deps -T venue-hook`
	riverRun := `run --rm --no-deps -T venue-river`
	runPlanted(t, []planted{
		{
			name: "second -f in the cut", copy: cp, check: cutCheckOneComposeFile,
			mutate: mut(cutScript, `-f "$HERE/compose.bigboy.yml")`, `-f "$R/compose.yml" -f "$HERE/compose.bigboy.yml")`),
			want:   "exactly one -f",
		},
		{
			name: "inherited COMPOSE_FILE kept by the cut", copy: cp, check: cutCheckOneComposeFile,
			mutate: mut(cutScript, cutUnset, "true"),
			want:   "COMPOSE_FILE reached",
		},
		{
			name: "retired stack.sh named", copy: cp, check: cutCheckOneComposeFile,
			mutate: mut(cutScript, "cd \"$R\" || exit\n", "cd \"$R\" || exit\n[ -x \"$HERE/stack.sh\" ] && . \"$HERE/stack.sh\"\n"),
			want:   "retired stack.sh",
		},
		{
			name: "web BACKEND_URL off the router", copy: []string{cutCompose}, check: cutCheckWebEnv,
			mutate: mut(cutCompose, "- BACKEND_URL=http://traefik:3000", "- BACKEND_URL=http://query-api:8090"),
			want:   "web BACKEND_URL",
		},
		{
			name: "web AUTH_URL dropped", copy: []string{cutCompose}, check: cutCheckWebEnv,
			mutate: mut(cutCompose, "- AUTH_URL=https://www.commanderkeen.dev", "- AUTH_URL_OLD=https://www.commanderkeen.dev"),
			want:   "web has no AUTH_URL",
		},
		{
			name: "present names read as missing", copy: []string{cutRequired}, check: cutCheckRequiredPresent,
			mutate: mut(cutRequired, `grep -qx -- "$name"`, `grep -qx -- "X$name"`),
			want:   "must pass",
		},
		{
			name: "missing name not failed", copy: []string{cutRequired}, check: cutCheckRequiredMissing,
			mutate: mut(cutRequired, "missing=1", "missing=0"),
			want:   "must fail rc=1",
		},
		{
			name: "empty names file not refused", copy: []string{cutRequired}, check: cutCheckRequiredUnread,
			mutate: mut(cutRequired, `[ -s "$file" ] ||`, `[ -n "$file" ] ||`),
			want:   "must fail rc=2",
		},
		{
			name: "web env check before the recreate", copy: cp, check: cutCheckWebEnvAfterRecreate,
			mutate: all(
				mut(cutScript, `"$HERE/check-web-env-required.sh" "$REC.web-env-names-after-up" BACKEND_URL AUTH_URL; st web-env-after-up $?`, ":"),
				mut(cutScript, `"${DC[@]}" up -d --no-deps --no-build query-api go-api web`,
					`"$HERE/check-web-env-required.sh" "$REC.web-env-names-after-up" BACKEND_URL AUTH_URL; st web-env-after-up $?`+"\n"+`"${DC[@]}" up -d --no-deps --no-build query-api go-api web`)),
			want: "must run after the recreate",
		},
		{
			name: "web env check drops AUTH_URL", copy: cp, check: cutCheckWebEnvAfterRecreate,
			mutate: mut(cutScript, "BACKEND_URL AUTH_URL; st web-env-after-up", "BACKEND_URL; st web-env-after-up"),
			want:   "must require BACKEND_URL AUTH_URL",
		},
		{
			name: "old hook check: bare docker verb", copy: cp, check: cutCheckHookClassRules,
			mutate: func(t *testing.T, root string) { writeFile(t, root, cutHook, cutOldHook) },
			want:   `bare docker verb "exec"`,
		},
		{
			name: "old hook check: printenv", copy: cp, check: cutCheckHookClassRules,
			mutate: func(t *testing.T, root string) { writeFile(t, root, cutHook, cutOldHook) },
			want:   "printenv out of a container",
		},
		{
			name: "hook one-off passes -e", copy: cp, check: cutCheckHookClassRules,
			mutate: mut(cutHook, hookRun, `run --rm --no-deps -e POSTGRES_PASSWORD -T venue-hook`),
			want:   "env-passing flag",
		},
		{
			name: "hook upgrade status dropped", copy: cp, check: cutCheckHookFailingStep,
			mutate: mut(cutHook, `2>&1) || RC1=$?`, `2>&1) || true`),
			want:   "hook_upgrade_rc=3",
		},
		{
			name: "hook upgrade gate removed", copy: cp, check: cutCheckHookEachGate,
			mutate: mut(cutHook, `[[ ${RC1:-0} -eq 0 ]] || RC=1`, "true"),
			want:   "venue-hook call 1",
		},
		{
			name: "hook clickhouse gate removed", copy: cp, check: cutCheckHookEachGate,
			mutate: mut(cutHook, `[[ ${RC2:-0} -eq 0 ]] || RC=1`, "true"),
			want:   "venue-hook call 2",
		},
		{
			name: "hook check keeps COMPOSE_FILE", copy: cp, check: cutCheckInheritedComposeFile,
			mutate: mut(cutHook, cutUnset, "true"),
			want:   "bigboy-hook-check.sh: an inherited COMPOSE_FILE",
		},
		{
			name: "river apply keeps COMPOSE_FILE", copy: cp, check: cutCheckInheritedComposeFile,
			mutate: mut(cutRiver, cutUnset, "true"),
			want:   "bigboy-river-apply.sh: an inherited COMPOSE_FILE",
		},
		{
			name: "river apply needs the operator image", copy: cp, check: cutCheckRiverNoReconciler,
			mutate: mut(cutRiver, `"${BIGBOY_OPERATOR_IMAGE:-$DHO}"`, `"${BIGBOY_OPERATOR_IMAGE:?no running reconciler}"`),
			want:   "the river apply by hand failed",
		},
		{
			name: "river redaction not global", copy: cp, check: cutCheckRiverRedacts,
			mutate: mut(cutRiver, `<redacted>@#g'`, `<redacted>@#'`),
			want:   "second DSN's password",
		},
		{
			name: "river one-off passes -e", copy: cp, check: cutCheckRiverClassRules,
			mutate: mut(cutRiver, riverRun, `run --rm --no-deps -e POSTGRES_PASSWORD -T venue-river`),
			want:   "env-passing flag",
		},
		{
			name: "river apply exits 0 on failure", copy: cp, check: cutCheckRiverFailingApply,
			mutate: mut(cutRiver, `exit "$rc"`, "exit 0"),
			want:   "must carry the apply failure",
		},
		{
			name: "bigboy psql with PGPASSWORD", copy: cp, check: cutCheckPGGrantsBigboy,
			mutate: mut(cutPGGrants, `'psql -w -U $POSTGRES_USER -d ${POSTGRES_DB`, `'PGPASSWORD=$POSTGRES_PASSWORD psql -U $POSTGRES_USER -d ${POSTGRES_DB`),
			want:   "password is set or expanded",
		},
		{
			name: "prod psql without -w", copy: cp, check: cutCheckPGGrantsProd,
			mutate: mut(cutPGGrants, `sh -c 'psql -w -U "$POSTGRES_USER"`, `sh -c 'psql -U "$POSTGRES_USER"`),
			want:   "must run with -w",
		},
		{
			name: "grants exec without -T", copy: cp, check: cutCheckGrantsComposeExec,
			mutate: mut(cutGrants, "exec -T clickhouse clickhouse-client", "exec clickhouse clickhouse-client"),
			want:   "exec -T clickhouse",
		},
		{
			name: "river one-off reads the whole ops/.env", copy: []string{cutCompose}, check: cutCheckOneOffsYAML,
			mutate: mut(cutCompose, "  venue-river:\n", "  venue-river:\n    env_file: [ops/.env]\n"),
			want:   "venue-river carries env_file",
		},
		{
			name: "hook one-off literal password", copy: []string{cutCompose}, check: cutCheckOneOffsYAML,
			mutate: mut(cutCompose, "      MIGRATION_DATABASE_URI: postgresql://devhealth:${POSTGRES_PASSWORD:-devhealth}@postgres:5432/devhealth?sslmode=disable\n      OPERATIONAL_ORDERING_CONTRACT",
				"      MIGRATION_DATABASE_URI: postgresql://devhealth:hunter2@postgres:5432/devhealth?sslmode=disable\n      OPERATIONAL_ORDERING_CONTRACT"),
			want: "venue-hook MIGRATION_DATABASE_URI",
		},
		{
			name: "bare docker run in a run file", copy: cp, check: cutCheckNoBareDocker,
			mutate: func(t *testing.T, root string) {
				writeFile(t, root, "ci/bigboy/planted-check.sh", "#!/usr/bin/env bash\ndocker run --rm busybox true\n")
			},
			want: "planted-check.sh: bare docker",
		},
		{
			name: "-e on the hook one-off line", copy: cp, check: cutCheckNoEnvFlagNoPasswordExpansion,
			mutate: mut(cutHook, `HC=("${DC[@]}" --profile venue`, `HC=("${DC[@]}" -e CLICKHOUSE_URI --profile venue`),
			want:   "env-passing flag -e",
		},
		{
			name: "password expanded in river apply", copy: cp, check: cutCheckNoEnvFlagNoPasswordExpansion,
			mutate: mut(cutRiver, "set -u\n", "set -u\nX=${POSTGRES_PASSWORD:-}\n"),
			want:   "password expansion",
		},
		{
			name: "prove psql by container name", copy: cp, check: cutCheckProvePsqlIsComposeExec,
			mutate: mut(cutProve, `PROVE_ORG=$("${DC[@]}" --profile venue exec -T postgres psql`, `PROVE_ORG=$(docker exec -i dev-health-postgres-1 psql`),
			want:   "not a compose exec on the stack files",
		},
		{
			name: "hook one-off without its own profile", copy: cp, check: cutCheckOneOffShape,
			mutate: mut(cutHook, "--profile venue --profile venue-hook run", "--profile venue-hook run"),
			want:   "profiles are",
		},
		{
			name: "column expectation read as a table", copy: cp, check: cutCheckPGColumnExpectation,
			mutate: mut(cutPGGrants, "      *.*) #", "      *.*.*) #"),
			want:   "has_column_privilege",
		},
	})
}
