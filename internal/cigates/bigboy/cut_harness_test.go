package bigboy_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

const cutOld8 = "aaaaaaaa"

var (
	cutNew          = strings.Repeat("b", 40)
	cutOldDigest    = "sha256:" + strings.Repeat("1", 64)
	cutNewDigest    = "sha256:" + strings.Repeat("2", 64)
	cutWebSHA       = strings.Repeat("c", 40)
	cutWebOldDigest = "sha256:" + strings.Repeat("3", 64)
	cutWebNewDigest = "sha256:" + strings.Repeat("4", 64)
	cutOperatorRef  = "ghcr.io/full-chaos/dev-health-go-operator@" + cutNewDigest
)

// cutExec runs name with exactly env (nothing inherited: a host COMPOSE_FILE or *_TOKEN must not reach the subject).
func cutExec(g *guard, dir string, env []string, name string, args ...string) scriptRun {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = env
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	code := 0
	if err := cmd.Run(); err != nil {
		var ee *exec.ExitError
		if !errors.As(err, &ee) {
			g.failf("harness: %s %v: %v", name, args, err)
			return scriptRun{code: -1}
		}
		code = ee.ExitCode()
	}
	if ctx.Err() != nil {
		g.failf("harness: %s %v timed out", name, args)
	}
	return scriptRun{stdout: so.String(), stderr: se.String(), code: code}
}

func cutMkdir(g *guard, parts ...string) string {
	p := filepath.Join(parts...)
	if err := os.MkdirAll(p, 0o755); err != nil {
		g.failf("harness: mkdir %s: %v", p, err)
	}
	return p
}

func cutTemp(g *guard, name string) string {
	p := filepath.Join(g.tmp, fmt.Sprintf("%s-%d", name, scratchSeq.Add(1)))
	if err := os.MkdirAll(p, 0o755); err != nil {
		g.failf("harness: %v", err)
	}
	return p
}

func cutWrite(g *guard, p, content string, mode os.FileMode) {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		g.failf("harness: %v", err)
	}
	if err := os.WriteFile(p, []byte(content), mode); err != nil {
		g.failf("harness: %v", err)
	}
}

func cutFileText(p string) string { b, _ := os.ReadFile(p); return string(b) }

func cutStub(g *guard, dir, name, body string) {
	cutWrite(g, filepath.Join(dir, name), "#!/bin/bash\n"+body, 0o755)
}

func cutVar(prefix, name string) string {
	return prefix + "_" + regexp.MustCompile(`[^A-Za-z0-9]`).ReplaceAllString(name, "_")
}

func cutGit(g *guard, repo string, args ...string) string {
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + repo, "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com", "GIT_CONFIG_NOSYSTEM=1"}
	r := cutExec(g, repo, env, "git", append([]string{"-c", "commit.gpgsign=false", "-C", repo}, args...)...)
	if r.code != 0 {
		g.failf("harness: git %v: %s", args, r.stderr)
	}
	return strings.TrimSpace(r.stdout)
}

func cutGitInitMut(t *testing.T, root string) {
	t.Helper()
	g := &guard{root: root, tmp: t.TempDir()}
	cutGit(g, root, "init", "-q")
	cutGit(g, root, "add", "-A")
	cutGit(g, root, "commit", "-q", "-m", "init")
	if len(g.v) > 0 {
		t.Fatal(strings.Join(g.v, "\n"))
	}
}

const cutRecFn = `rec() { local IFS=$'\037'; printf '%s\036%s\036%s\036%s\036%s\n' "$1" "$2" "${BIGBOY_OPERATOR_IMAGE-}" "${COMPOSE_FILE+set}" "${*:3}" >> "$CUT_LOG"; }
`

type cutCall struct {
	kind, name, opImage string
	composeFile         bool
	args                []string
}

func (c cutCall) has(a string) bool { return slices.Contains(c.args, a) }
func (c cutCall) line() string      { return c.name + " " + strings.Join(c.args, " ") }

func (c cutCall) compose(words ...string) bool {
	if c.kind != "docker" || len(c.args) == 0 || c.args[0] != "compose" {
		return false
	}
	for _, w := range words {
		if !c.has(w) {
			return false
		}
	}
	return true
}

func (c cutCall) flagValues(flag string) []string {
	var out []string
	for i, a := range c.args {
		if a == flag && i+1 < len(c.args) {
			out = append(out, c.args[i+1])
		}
	}
	return out
}

func cutCalls(g *guard, log string) []cutCall {
	b, err := os.ReadFile(log)
	if err != nil {
		return nil
	}
	var out []cutCall
	for _, ln := range strings.Split(strings.TrimRight(string(b), "\n"), "\n") {
		if ln == "" {
			continue
		}
		f := strings.SplitN(ln, "\x1e", 5)
		if len(f) != 5 {
			g.failf("harness: bad record %q", ln)
			continue
		}
		c := cutCall{kind: f[0], name: f[1], opImage: f[2], composeFile: f[3] == "set"}
		if f[4] != "" {
			c.args = strings.Split(f[4], "\x1f")
		}
		out = append(out, c)
	}
	return out
}

func cutFind(calls []cutCall, from int, pred func(cutCall) bool) int {
	for i := max(from, 0); i < len(calls); i++ {
		if pred(calls[i]) {
			return i
		}
	}
	return -1
}

func cutTool(name string) func(cutCall) bool {
	return func(c cutCall) bool { return c.kind == "tool" && c.name == name }
}

type cutDigest struct{ ref, digest string }

func cutDefaultDigests() []cutDigest {
	return []cutDigest{
		{"dev-health-go-dho:sha-" + cutOld8[:7], cutOldDigest},
		{"dev-health-go-dho:sha-", cutNewDigest},
		{"dev-health-go-api-tools:sha-", cutNewDigest},
		{"dev-health-go-operator:sha-", cutNewDigest},
		{"dev-health-web:sha-", cutWebNewDigest},
	}
}

func cutWithout(ds []cutDigest, ref string) []cutDigest {
	return slices.DeleteFunc(slices.Clone(ds), func(d cutDigest) bool { return d.ref == ref })
}

func cutDockerBody(ds []cutDigest) string {
	var b strings.Builder
	b.WriteString(cutRecFn + "rec docker docker \"$@\"\nargs=\"$*\"\nif [ \"$1\" = buildx ]; then\n")
	for _, d := range ds {
		raw := `{"manifests":[{"digest":"` + d.digest + `","platform":{"os":"linux","architecture":"arm64"}}]}`
		fmt.Fprintf(&b, "  if [[ $args == *%q* ]]; then if [[ $args == *--raw* ]]; then echo '%s'; else echo '{\"digest\":\"%s\"}'; fi; exit 0; fi\n", d.ref, raw, d.digest)
	}
	b.WriteString("  exit 0\nfi\n[ \"$1\" = compose ] && exit \"${CUT_COMPOSE_RC:-0}\"\nexit 0\n")
	return b.String()
}

// The retired Python helpers the cut still calls are stood in; the flags file names one secret ref so BLANK is decidable.
const cutPython = cutRecFn + `rec tool python3 "$@"
case "$1" in
  *generate-query-api-enabled-flags.py) while [ $# -gt 0 ]; do [ "$1" = --out ] && printf 'GO_API_EDGE_JWT_SECRET: ${JWT_SECRET_KEY}\n' > "$2"; shift; done ;;
esac
exit 0
`

var cutSiblings = []string{
	"bigboy-repin.sh", "bigboy-repin-web.sh", "compose-config-redacted.sh", "container-env-names.sh",
	"check-dho-api-ch-user.sh", "check-web-env-required.sh", "bigboy-log-checks.sh", "bigboy-6889-checks.sh",
	"bigboy-river-apply.sh", "bigboy-hook-check.sh", "bigboy-grants-check.sh", "cut-contents.sh",
	"pg-grants-check.sh", "web-path-smoke.sh",
}

func cutBigboyRoot(g *guard) string {
	root := cutTemp(g, "bigboy-root")
	pins := ""
	for name, v := range map[string]string{
		"dev-health-go-dho": "BIGBOY_DHO_IMAGE", "dev-health-go-api-tools": "BIGBOY_API_TOOLS_IMAGE",
		"dev-health-go-operator": "BIGBOY_OPERATOR_IMAGE",
	} {
		pins += fmt.Sprintf("%s=ghcr.io/full-chaos/%s@%s\n", v, name, cutOldDigest)
	}
	pins += "BIGBOY_WEB_IMAGE=ghcr.io/full-chaos/dev-health-web@" + cutWebOldDigest + "\n"
	cutWrite(g, filepath.Join(root, "_records", "images.env"), pins, 0o644)
	cutWrite(g, filepath.Join(root, "ops", ".env"), "", 0o644)
	cutWrite(g, filepath.Join(root, "_records", "bigboy-"+cutOld8, "bigboy-rest-commands.sh"),
		"#!/usr/bin/env bash\nSTEP=placeholder\nB="+strings.Repeat("0", 40)+"\n", 0o644)
	cutMkdir(g, root, "_records", "bigboy-"+cutNew[:8])
	return root
}

type cutOpts struct {
	root      string      // bigboy root; empty builds cutBigboyRoot
	realTools bool        // the tools are the script's own directory (no BIGBOY_TOOLS_DIR)
	keep      []string    // siblings kept real in the copied tools directory
	digests   []cutDigest // nil: cutDefaultDigests
	env       []string
}

type cutResult struct {
	scriptRun
	calls       []cutCall
	root, tools string
}

// Siblings (but keep) are recorders in a copied tools dir, so the log shows the cut's own call order.
func cutRunCut(g *guard, o cutOpts) cutResult {
	if o.root == "" {
		o.root = cutBigboyRoot(g)
	}
	if o.digests == nil {
		o.digests = cutDefaultDigests()
	}
	work := cutTemp(g, "cut")
	log := filepath.Join(work, "calls.log")
	bin := cutMkdir(g, work, "bin")
	cutStub(g, bin, "docker", cutDockerBody(o.digests))
	cutStub(g, bin, "gh", "echo "+cutWebSHA+"\n")
	cutStub(g, bin, "curl", "echo 200\n")
	cutStub(g, bin, "sleep", "exit 0\n")
	cutStub(g, bin, "python3", cutPython)
	ops := cutMkdir(g, work, "ops-clone", ".git")
	env := []string{"PATH=" + bin + ":/usr/bin:/bin", "HOME=" + work, "CUT_LOG=" + log, "BIGBOY_ROOT=" + o.root,
		"BIGBOY_OPS_REPO=" + filepath.Dir(ops), "WEB_REPIN_WAIT_TRIES=1", "WEB_REPIN_WAIT_SECS=0"}
	tools := g.path("ci/bigboy")
	if !o.realTools {
		tools = cutMkdir(g, work, "tools")
		entries, err := os.ReadDir(g.path("ci/bigboy"))
		if err != nil {
			g.failf("harness: %v", err)
		}
		for _, e := range entries {
			if !e.IsDir() {
				cutWrite(g, filepath.Join(tools, e.Name()), cutFileText(g.path("ci/bigboy/"+e.Name())), 0o755)
			}
		}
		for _, s := range cutSiblings {
			if slices.Contains(o.keep, s) {
				continue
			}
			out, rc := cutVar("CUT_OUT", s), cutVar("CUT_RC", s)
			cutStub(g, tools, s, cutRecFn+"rec tool "+s+" \"$@\"\n[ -z \"${"+out+"-}\" ] || printf '%s\\n' \"$"+out+"\"\nexit \"${"+rc+":-0}\"\n")
		}
		env = append(env, "BIGBOY_TOOLS_DIR="+tools)
	}
	env = append(env, o.env...)
	r := cutExec(g, work, env, "bash", g.path("ci/bigboy/bigboy-cut.sh"), cutOld8, cutNew)
	return cutResult{scriptRun: r, calls: cutCalls(g, log), root: o.root, tools: tools}
}

func (r cutResult) dump() string {
	var lines []string
	for _, c := range r.calls {
		lines = append(lines, c.kind+": "+c.line())
	}
	return fmt.Sprintf("rc=%d\nstdout:\n%s\nstderr:\n%s\ncalls:\n%s", r.code, r.stdout, r.stderr, strings.Join(lines, "\n"))
}

func cutStripComments(text string) []string {
	var out []string
	for _, ln := range strings.Split(text, "\n") {
		if !strings.HasPrefix(strings.TrimLeft(ln, " \t"), "#") {
			out = append(out, ln)
		}
	}
	return out
}
