// Package cachepoison guards .github/workflows against cache poisoning: a job that checks out a ref an outside party chose
// must not write any cache that a privileged run on the default branch can later restore.
//
// A ref counts as untrusted when its expression reads workflow_dispatch/workflow_call inputs, a workflow_run payload, a
// repository_dispatch payload, or (under pull_request_target only) the pull request head. A plain pull_request head checkout is
// allowed: that event runs with a PR-scoped cache that the default branch never restores. A ref copied through a step output, an
// env value or a job output is followed back to what produced it. The guard fails closed: a trace deeper than six hops, a script
// reading $GITHUB_EVENT_PATH and any github.event field outside a short trusted list count as untrusted. It does not look inside
// composite actions or at refs a script fetches from other sources.
//
// A step counts as a cache writer when it is actions/cache or actions/cache/save, sets a non-empty cache-to, sets a setup action's
// cache input, or omits that input on an action whose cache defaults ON (setup-go, setup-uv, setup-gradle). A writer is allowed
// only when it is switched off for a supplied ref by a condition comparing github.event.inputs.ref with the empty string in its own `if`, or in the
// value of cache-to.
package cachepoison_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type step struct {
	ID   string         `yaml:"id"`
	Name string         `yaml:"name"`
	Run  string         `yaml:"run"`
	Env  map[string]any `yaml:"env"`
	Uses string         `yaml:"uses"`
	If   string         `yaml:"if"`
	With map[string]any `yaml:"with"`
}

type job struct {
	Uses    string            `yaml:"uses"`
	Env     map[string]any    `yaml:"env"`
	Outputs map[string]string `yaml:"outputs"`
	With    map[string]any    `yaml:"with"`
	Steps   []step            `yaml:"steps"`
}

type workflow struct {
	On   any            `yaml:"on"`
	Env  map[string]any `yaml:"env"`
	Jobs map[string]job `yaml:"jobs"`
}

const refGate = "github.event.inputs.ref == ''"

var (
	bracketKey = regexp.MustCompile(`\[\s*['"]([^'"]*)['"]\s*\]`)
	// gatedValue is the only value shape that switches a writer off for a supplied ref: GATE && 'literal' || '' (or true/false).
	gatedValue = regexp.MustCompile(`^(?:\$\{\{)?\s*` + regexp.QuoteMeta(refGate) + `\s*&&\s*(?:'[^']*'|true)\s*\|\|\s*(?:''|false)\s*(?:\}\})?$`)
)

// norm lower-cases an expression (property names are case-insensitive), turns bracket keys into dotted ones and folds blanks.
func norm(s string) string {
	return strings.Join(strings.Fields(bracketKey.ReplaceAllString(strings.ToLower(s), ".$1")), " ")
}

// gatedIf holds when the step runs only if no ref was supplied. The condition must be a plain conjunction: no parentheses (so
// no inversion such as `(GATE) == false`), no ||, no status function, and one term that is exactly the gate. A conjunction can
// only narrow when the step runs, so the other terms may be anything else.
func gatedIf(cond string) bool {
	c := strings.TrimSpace(norm(cond))
	c = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(c, "${{"), "}}"))
	if strings.ContainsAny(c, "()") || strings.Contains(c, "||") {
		return false
	}
	for _, term := range strings.Split(c, "&&") {
		if strings.TrimSpace(term) == refGate {
			return true
		}
	}
	return false
}

func gatedWith(v string) bool { return gatedValue.MatchString(norm(v)) }

func events(on any) map[string]bool {
	out := map[string]bool{}
	switch v := on.(type) {
	case string:
		out[v] = true
	case []any:
		for _, e := range v {
			out[fmt.Sprint(e)] = true
		}
	case map[string]any:
		for e := range v {
			out[e] = true
		}
	}
	return out
}

func action(uses string) string {
	name, _, _ := strings.Cut(uses, "@")
	return strings.ToLower(name)
}

func withString(s step, key string) (string, bool) {
	v, ok := s.With[key]
	if !ok {
		return "", false
	}
	return strings.TrimSpace(fmt.Sprint(v)), true
}

// suppliesRef holds when the workflow has a trigger through which an outside party can name a ref.
func suppliesRef(ev map[string]bool) bool {
	for _, e := range []string{"workflow_dispatch", "workflow_call", "workflow_run", "repository_dispatch", "pull_request_target"} {
		if ev[e] {
			return true
		}
	}
	return false
}

func passesUntrustedRef(with map[string]any, wf workflow, ev map[string]bool, jobName string) bool {
	for _, v := range with {
		if (tracer{wf: wf, ev: ev}).refReason(fmt.Sprint(v), jobName, 0) != "" {
			return true
		}
	}
	return false
}

func untrustedRef(ref string, ev map[string]bool) string {
	ref = norm(ref)
	switch {
	case strings.Contains(ref, "inputs."):
		return "dispatch-supplied ref"
	case strings.Contains(ref, "github.event.workflow_run."):
		return "workflow_run-supplied ref"
	case strings.Contains(ref, "github.event.client_payload"):
		return "repository_dispatch-supplied ref"
	case ev["pull_request_target"] && (strings.Contains(ref, "pull_request.head") || strings.Contains(ref, "head_ref")):
		return "pull_request_target head ref"
	}
	return ""
}

func off(v string) bool { return v == "" || v == "false" }

// cacheWriter says whether the step writes a cache and why.
func cacheWriter(s step) (string, bool) {
	if gatedIf(s.If) {
		return "", false
	}
	switch name := action(s.Uses); {
	case name == "actions/cache" || name == "actions/cache/save":
		return s.Uses, true
	case name == "actions/setup-go" || name == "astral-sh/setup-uv" || name == "gradle/actions/setup-gradle":
		key := "cache"
		if name != "actions/setup-go" {
			key = "enable-cache"
		}
		if name == "gradle/actions/setup-gradle" {
			key = "cache-disabled"
			v, set := withString(s, key)
			if set && v == "true" {
				return "", false
			}
			return s.Uses + " (cache defaults ON)", true
		}
		v, set := withString(s, key)
		if !set {
			return s.Uses + " (" + key + " defaults ON)", true
		}
		if !off(v) && !gatedWith(v) {
			return s.Uses + " (" + key + ": " + v + ")", true
		}
		return "", false
	case strings.HasPrefix(name, "actions/setup-") || name == "ruby/setup-ruby":
		for _, key := range []string{"cache", "bundler-cache"} {
			if v, set := withString(s, key); set && !off(v) && !gatedWith(v) {
				return s.Uses + " (" + key + ": " + v + ")", true
			}
		}
	}
	if v, set := withString(s, "cache-to"); set && !off(v) && !gatedWith(v) {
		return s.Uses + " (cache-to: " + v + ")", true
	}
	return "", false
}

func violations(file string, raw []byte) ([]string, error) {
	var wf workflow
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	ev := events(wf.On)
	var out []string
	for name, j := range wf.Jobs {
		if j.Uses != "" && (suppliesRef(ev) || passesUntrustedRef(j.With, wf, ev, name)) {
			out = append(out, fmt.Sprintf("%s: job %s calls the reusable workflow %s under a trigger or input that can supply an untrusted ref, which this guard does not follow", file, name, j.Uses))
		}
		reason := ""
		for _, s := range j.Steps {
			if action(s.Uses) != "actions/checkout" {
				continue
			}
			ref, _ := withString(s, "ref")
			if r := (tracer{wf: wf, ev: ev}).refReason(ref, name, 0); r != "" {
				reason = r
			}
		}
		if reason == "" {
			continue
		}
		for _, s := range j.Steps {
			if strings.HasPrefix(s.Uses, "./") {
				out = append(out, fmt.Sprintf("%s: job %s checks out a %s and runs the local action %s, which this guard does not follow", file, name, reason, s.Uses))
			}
			if why, bad := cacheWriter(s); bad {
				out = append(out, fmt.Sprintf("%s: job %s checks out a %s and writes a cache in step %q: %s", file, name, reason, s.Name, why))
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func TestNoWorkflowWritesACacheAfterAnUntrustedCheckout(t *testing.T) {
	dir := filepath.Join(repoRoot(t), ".github", "workflows")
	yml, err := filepath.Glob(filepath.Join(dir, "*.yml"))
	yaml, err2 := filepath.Glob(filepath.Join(dir, "*.yaml"))
	files := append(yml, yaml...)
	if err == nil {
		err = err2
	}
	if err != nil || len(files) == 0 {
		t.Fatalf("no workflows found: %v", err)
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		got, err := violations(filepath.Base(f), raw)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range got {
			t.Error(v)
		}
	}
}

func wf(on, steps string) []byte {
	return []byte("on: " + on + "\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n" + steps)
}

const dispatchCheckout = `
      - uses: actions/checkout@aaaa
        with:
          ref: ${{ github.event.inputs.ref || github.sha }}
`

func TestPlants(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
		want int
	}{
		{"actions/cache after dispatch ref", wf("workflow_dispatch", dispatchCheckout+"      - uses: actions/cache@bbbb\n        with:\n          path: x\n          key: k\n"), 1},
		{"actions/cache/save after dispatch ref", wf("workflow_dispatch", dispatchCheckout+"      - uses: actions/cache/save@bbbb\n        with:\n          path: x\n          key: k\n"), 1},
		{"actions/cache/restore is read only", wf("workflow_dispatch", dispatchCheckout+"      - uses: actions/cache/restore@bbbb\n        with:\n          path: x\n          key: k\n"), 0},
		{"buildx cache-to literal", wf("workflow_dispatch", dispatchCheckout+"      - uses: docker/build-push-action@cccc\n        with:\n          cache-from: type=gha\n          cache-to: type=gha,mode=max\n"), 1},
		{"buildx cache-to gated on the ref input", wf("workflow_dispatch", dispatchCheckout+"      - uses: docker/build-push-action@cccc\n        with:\n          cache-from: type=gha\n          cache-to: ${{ github.event.inputs.ref == '' && 'type=gha,mode=max' || '' }}\n"), 0},
		{"buildx cache-from only", wf("workflow_dispatch", dispatchCheckout+"      - uses: docker/build-push-action@cccc\n        with:\n          cache-from: type=gha\n"), 0},
		{"setup-go cache default ON", wf("workflow_dispatch", dispatchCheckout+"      - uses: actions/setup-go@dddd\n        with:\n          go-version: '1.24'\n"), 1},
		{"setup-go cache true", wf("workflow_dispatch", dispatchCheckout+"      - uses: actions/setup-go@dddd\n        with:\n          cache: true\n"), 1},
		{"setup-go cache false", wf("workflow_dispatch", dispatchCheckout+"      - uses: actions/setup-go@dddd\n        with:\n          cache: false\n"), 0},
		{"setup-node cache pnpm", wf("workflow_dispatch", dispatchCheckout+"      - uses: actions/setup-node@eeee\n        with:\n          cache: pnpm\n"), 1},
		{"setup-node cache absent is off", wf("workflow_dispatch", dispatchCheckout+"      - uses: actions/setup-node@eeee\n"), 0},
		{"setup-uv cache default ON", wf("workflow_dispatch", dispatchCheckout+"      - uses: astral-sh/setup-uv@ffff\n"), 1},
		{"step gated on the ref input", wf("workflow_dispatch", dispatchCheckout+"      - uses: actions/cache@bbbb\n        if: github.event.inputs.ref == ''\n        with:\n          path: x\n          key: k\n"), 0},
		{"pull_request_target head checkout", wf("pull_request_target", "\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ github.event.pull_request.head.sha }}\n      - uses: actions/cache@bbbb\n        with:\n          path: x\n          key: k\n"), 1},
		{"pull_request_target head_ref checkout", wf("[pull_request_target, push]", "\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ github.head_ref }}\n      - uses: actions/setup-go@dddd\n"), 1},
		{"workflow_run head checkout", wf("workflow_run", "\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ github.event.workflow_run.head_sha }}\n      - uses: actions/cache@bbbb\n        with:\n          path: x\n          key: k\n"), 1},
		{"repository_dispatch payload checkout", wf("repository_dispatch", "\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ github.event.client_payload.sha }}\n      - uses: actions/cache@bbbb\n        with:\n          path: x\n          key: k\n"), 1},
		{"plain pull_request head checkout is allowed", wf("pull_request", "\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ github.event.pull_request.head.sha }}\n      - uses: actions/cache@bbbb\n        with:\n          path: x\n          key: k\n      - uses: actions/setup-go@dddd\n"), 0},
		{"checkout without ref", wf("workflow_dispatch", "\n      - uses: actions/checkout@aaaa\n      - uses: actions/cache@bbbb\n        with:\n          path: x\n          key: k\n"), 0},
		{"derived needs output ref", wf("workflow_dispatch", "\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ needs.build.outputs.sha }}\n      - uses: actions/cache@bbbb\n        with:\n          path: x\n          key: k\n"), 0},
		{"inverted gate", wf("workflow_dispatch", dispatchCheckout+"      - uses: actions/cache@bbbb\n        if: ${{ (github.event.inputs.ref == '') == false }}\n        with:\n          path: x\n          key: k\n"), 1},
		{"gate in parentheses", wf("workflow_dispatch", dispatchCheckout+"      - uses: actions/cache@bbbb\n        if: (github.event.inputs.ref == '')\n        with:\n          path: x\n          key: k\n"), 1},
		{"gate wrapped in expression braces with a second term", wf("workflow_dispatch", dispatchCheckout+"      - uses: actions/cache@bbbb\n        if: ${{ github.event_name == 'push' && github.event.inputs.ref == '' }}\n        with:\n          path: x\n          key: k\n"), 0},
		{"ref through a step output fed by an input", wf("workflow_dispatch", "\n      - id: pick\n        env:\n          REF: ${{ inputs.ref }}\n        run: echo \"ref=$REF\" >> \"$GITHUB_OUTPUT\"\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ steps.pick.outputs.ref }}\n      - uses: actions/cache@bbbb\n        with:\n          path: x\n          key: k\n"), 1},
		{"if with || always() still runs", wf("workflow_dispatch", dispatchCheckout+"      - uses: actions/cache@bbbb\n        if: github.event.inputs.ref == '' || always()\n        with:\n          path: x\n          key: k\n"), 1},
		{"if with always() and the gate still runs", wf("workflow_dispatch", dispatchCheckout+"      - uses: actions/cache@bbbb\n        if: always() && github.event.inputs.ref == ''\n        with:\n          path: x\n          key: k\n"), 1},
		{"if negating the gate", wf("workflow_dispatch", dispatchCheckout+"      - uses: actions/cache@bbbb\n        if: ${{ !(github.event.inputs.ref == '') }}\n        with:\n          path: x\n          key: k\n"), 1},
		{"if with the gate and another term", wf("workflow_dispatch", dispatchCheckout+"      - uses: actions/cache@bbbb\n        if: github.event_name == 'push' && github.event.inputs.ref == ''\n        with:\n          path: x\n          key: k\n"), 0},
		{"cache-to with || fallback that writes", wf("workflow_dispatch", dispatchCheckout+"      - uses: docker/build-push-action@cccc\n        with:\n          cache-to: ${{ github.event.inputs.ref == '' || 'type=gha' }}\n"), 1},
		{"cache-to gate with a non-empty fallback", wf("workflow_dispatch", dispatchCheckout+"      - uses: docker/build-push-action@cccc\n        with:\n          cache-to: ${{ github.event.inputs.ref == '' && 'type=gha' || 'type=gha,mode=min' }}\n"), 1},
		{"bracket form of the ref input", wf("workflow_dispatch", "\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ github.event.inputs['ref'] }}\n      - uses: actions/cache@bbbb\n        with:\n          path: x\n          key: k\n"), 1},
		{"bracket form of the inputs context", wf("workflow_dispatch", "\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ inputs[\"ref\"] }}\n      - uses: actions/cache@bbbb\n        with:\n          path: x\n          key: k\n"), 1},
		{"upper case inputs context", wf("workflow_dispatch", "\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ INPUTS.Ref }}\n      - uses: actions/cache@bbbb\n        with:\n          path: x\n          key: k\n"), 1},
		{"bracket form of the gate", wf("workflow_dispatch", dispatchCheckout+"      - uses: docker/build-push-action@cccc\n        with:\n          cache-to: ${{ github.event.inputs['ref'] == '' && 'type=gha' || '' }}\n"), 0},
		{"local composite action after an untrusted checkout", wf("workflow_dispatch", dispatchCheckout+"      - uses: ./.github/actions/build\n"), 1},
		{"local composite action after a trusted checkout", wf("workflow_dispatch", "\n      - uses: actions/checkout@aaaa\n      - uses: ./.github/actions/build\n"), 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := violations("plant.yml", c.raw)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != c.want {
				t.Fatalf("want %d violations, got %d: %v", c.want, len(got), got)
			}
		})
	}
}

func TestReusableWorkflowCalls(t *testing.T) {
	const call = "jobs:\n  call:\n    uses: ./.github/workflows/other.yml\n"
	cases := []struct {
		name string
		raw  string
		want int
	}{
		{"trusted push-only call", "on: push\n" + call, 0},
		{"trusted pull_request call", "on: pull_request\n" + call, 0},
		{"workflow_dispatch call", "on: workflow_dispatch\n" + call, 1},
		{"pull_request_target call", "on: pull_request_target\n" + call, 1},
		{"workflow_run call", "on: [push, workflow_run]\n" + call, 1},
		{"push call passing a dispatch input as the ref", "on: push\n" + call + "    with:\n      ref: ${{ inputs.ref }}\n", 1},
		{"push call passing a constant", "on: push\n" + call + "    with:\n      ref: main\n", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := violations("plant.yml", []byte(c.raw))
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != c.want {
				t.Fatalf("want %d violations, got %v", c.want, got)
			}
		})
	}
}

var (
	stepOutputRef = regexp.MustCompile(`steps\.([a-z0-9_-]+)\.outputs`)
	envRef        = regexp.MustCompile(`\benv\.([a-z0-9_]+)`)
	needsOutRef   = regexp.MustCompile(`needs\.([a-z0-9_-]+)\.outputs\.([a-z0-9_-]+)`)
)

// tracer follows a ref expression through step outputs, env values and job outputs to the expressions that produced it, so
// that a ref copied from an input into an output is still seen as supplied from outside. Reads of the event payload by a
// script (for example `jq` on $GITHUB_EVENT_PATH) are not followed.
type tracer struct {
	wf workflow
	ev map[string]bool
}

func lowerKeys(m map[string]any, key string) (any, bool) {
	for k, v := range m {
		if strings.ToLower(k) == key {
			return v, true
		}
	}
	return nil, false
}

func (t tracer) refReason(expr, jobName string, depth int) string {
	if depth > maxTrace {
		return "ref trace too deep to follow"
	}
	if r := untrustedRef(expr, t.ev); r != "" {
		return r
	}
	e := norm(expr)
	if strings.Contains(e, "github_event_path") {
		return "event payload read by a script"
	}
	if tok := unclassifiedEvent(e); tok != "" {
		return "unclassified event payload read " + tok
	}
	j := t.wf.Jobs[jobName]
	for _, m := range stepOutputRef.FindAllStringSubmatch(e, -1) {
		for _, s := range j.Steps {
			if strings.ToLower(s.ID) != m[1] {
				continue
			}
			texts := []string{s.Run}
			for _, v := range s.Env {
				texts = append(texts, fmt.Sprint(v))
			}
			for _, v := range s.With {
				texts = append(texts, fmt.Sprint(v))
			}
			for _, text := range texts {
				if r := t.refReason(text, jobName, depth+1); r != "" {
					return r + " (through the output of step " + s.ID + ")"
				}
			}
		}
	}
	for _, m := range envRef.FindAllStringSubmatch(e, -1) {
		scopes := []map[string]any{j.Env, t.wf.Env}
		for _, s := range j.Steps {
			scopes = append(scopes, s.Env)
		}
		for _, scope := range scopes {
			if v, ok := lowerKeys(scope, m[1]); ok {
				if r := t.refReason(fmt.Sprint(v), jobName, depth+1); r != "" {
					return r + " (through env " + m[1] + ")"
				}
			}
		}
	}
	for _, m := range needsOutRef.FindAllStringSubmatch(e, -1) {
		for name, other := range t.wf.Jobs {
			if strings.ToLower(name) != m[1] {
				continue
			}
			for k, v := range other.Outputs {
				if strings.ToLower(k) != m[2] {
					continue
				}
				if r := t.refReason(v, name, depth+1); r != "" {
					return r + " (through the output of job " + name + ")"
				}
			}
		}
	}
	return ""
}

func TestIndirectRefs(t *testing.T) {
	const cache = "      - uses: actions/cache@bbbb\n        with:\n          path: x\n          key: k\n"
	cases := []struct {
		name string
		raw  string
		want int
	}{
		{"ref through workflow env fed by an input", "on: workflow_dispatch\nenv:\n  R: ${{ github.event.inputs.ref }}\njobs:\n  build:\n    runs-on: x\n    steps:\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ env.R }}\n" + cache, 1},
		{"ref through job env fed by an input", "on: workflow_dispatch\njobs:\n  build:\n    runs-on: x\n    env:\n      R: ${{ inputs.ref }}\n    steps:\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ env.r }}\n" + cache, 1},
		{"ref through a job output fed by an input", "on: workflow_dispatch\njobs:\n  pick:\n    runs-on: x\n    outputs:\n      ref: ${{ steps.p.outputs.ref }}\n    steps:\n      - id: p\n        run: echo \"ref=${{ inputs.ref }}\" >> \"$GITHUB_OUTPUT\"\n  build:\n    needs: pick\n    runs-on: x\n    steps:\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ needs.pick.outputs.ref }}\n" + cache, 1},
		{"ref through a step output fed by the default branch name", "on: workflow_dispatch\njobs:\n  build:\n    runs-on: x\n    steps:\n      - id: p\n        env:\n          BRANCH: ${{ github.head_ref || github.ref_name }}\n        run: echo \"ref=$BRANCH\" >> \"$GITHUB_OUTPUT\"\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ steps.p.outputs.ref }}\n" + cache, 0},
		{"ref through a step output under pull_request_target head_ref", "on: pull_request_target\njobs:\n  build:\n    runs-on: x\n    steps:\n      - id: p\n        env:\n          BRANCH: ${{ github.head_ref }}\n        run: echo \"ref=$BRANCH\" >> \"$GITHUB_OUTPUT\"\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ steps.p.outputs.ref }}\n" + cache, 1},
		{"env cycle fails closed", "on: workflow_dispatch\nenv:\n  A: ${{ env.B }}\n  B: ${{ env.A }}\njobs:\n  build:\n    runs-on: x\n    steps:\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ env.A }}\n" + cache, 1},
		{"env chain of 3 ending in a constant", "on: workflow_dispatch\nenv:\n  E0: ${{ env.E1 }}\n  E1: ${{ env.E2 }}\n  E2: main\njobs:\n  build:\n    runs-on: x\n    steps:\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ env.E0 }}\n" + cache, 0},
		{"env chain of 8 fails closed", "on: workflow_dispatch\nenv:\n  E0: ${{ env.E1 }}\n  E1: ${{ env.E2 }}\n  E2: ${{ env.E3 }}\n  E3: ${{ env.E4 }}\n  E4: ${{ env.E5 }}\n  E5: ${{ env.E6 }}\n  E6: ${{ env.E7 }}\n  E7: main\njobs:\n  build:\n    runs-on: x\n    steps:\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ env.E0 }}\n" + cache, 1},
		{"output from a run reading GITHUB_EVENT_PATH", "on: workflow_dispatch\njobs:\n  build:\n    runs-on: x\n    steps:\n      - id: p\n        run: echo \"ref=$(jq -r .ref \"$GITHUB_EVENT_PATH\")\" >> \"$GITHUB_OUTPUT\"\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ steps.p.outputs.ref }}\n" + cache, 1},
		{"env from an unclassified payload field", "on: [workflow_dispatch, release]\nenv:\n  T: ${{ github.event.release.tag_name }}\njobs:\n  build:\n    runs-on: x\n    steps:\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ env.T }}\n" + cache, 1},
		{"output from a trusted payload field", "on: [workflow_dispatch, pull_request]\njobs:\n  build:\n    runs-on: x\n    steps:\n      - id: p\n        env:\n          N: ${{ github.event.pull_request.number }}\n        run: echo \"ref=refs/pull/$N/merge\" >> \"$GITHUB_OUTPUT\"\n      - uses: actions/checkout@aaaa\n        with:\n          ref: ${{ steps.p.outputs.ref }}\n" + cache, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := violations("plant.yml", []byte(c.raw))
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != c.want {
				t.Fatalf("want %d violations, got %v", c.want, got)
			}
		})
	}
}

const maxTrace = 6

var (
	eventRead = regexp.MustCompile(`github\.event\.[a-z0-9_.-]+`)
	// trustedEvent lists the payload fields that cannot name a ref an outside party chose. Everything else read from the
	// payload is unclassified and counts as untrusted.
	trustedEvent = []string{
		"github.event.pull_request.base.", "github.event.pull_request.number", "github.event.number",
		"github.event.repository.", "github.event.merge_group.", "github.event.pull_request.head.sha", "github.event.pull_request.head.ref",
		"github.event.pull_request.draft", "github.event.pull_request.labels", "github.event.pull_request.merged", "github.event.pull_request.head.repo.full_name",
	}
)

// unclassifiedEvent returns the first payload read that is neither a known-untrusted ref source (those are classified by
// untrustedRef) nor in the trusted list.
func unclassifiedEvent(e string) string {
	for _, tok := range eventRead.FindAllString(e, -1) {
		ok := false
		for _, p := range trustedEvent {
			if strings.HasPrefix(tok, p) {
				ok = true
				break
			}
		}
		if !ok {
			return tok
		}
	}
	return ""
}
