// Package cachepoison guards .github/workflows against cache poisoning: a job that checks out a ref an outside party chose
// must not write any cache that a privileged run on the default branch can later restore.
//
// A ref counts as untrusted when its expression reads workflow_dispatch/workflow_call inputs, a workflow_run payload, a
// repository_dispatch payload, or (under pull_request_target only) the pull request head. A plain pull_request head checkout is
// allowed: that event runs with a PR-scoped cache that the default branch never restores.
//
// A step counts as a cache writer when it is actions/cache or actions/cache/save, sets a non-empty cache-to, sets a setup action's
// cache input, or omits that input on an action whose cache defaults ON (setup-go, setup-uv, setup-gradle). A writer is allowed
// only when it is switched off for a supplied ref by the expression `github.event.inputs.ref == ''` in its own `if`, or in the
// value of cache-to.
package cachepoison_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type step struct {
	Name string         `yaml:"name"`
	Uses string         `yaml:"uses"`
	If   string         `yaml:"if"`
	With map[string]any `yaml:"with"`
}

type job struct {
	Steps []step `yaml:"steps"`
}

type workflow struct {
	On   any            `yaml:"on"`
	Jobs map[string]job `yaml:"jobs"`
}

const refGate = "inputs.ref == ''"

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

func untrustedRef(ref string, ev map[string]bool) string {
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
	if strings.Contains(s.If, refGate) {
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
		if !off(v) && !strings.Contains(v, refGate) {
			return s.Uses + " (" + key + ": " + v + ")", true
		}
		return "", false
	case strings.HasPrefix(name, "actions/setup-") || name == "ruby/setup-ruby":
		for _, key := range []string{"cache", "bundler-cache"} {
			if v, set := withString(s, key); set && !off(v) && !strings.Contains(v, refGate) {
				return s.Uses + " (" + key + ": " + v + ")", true
			}
		}
	}
	if v, set := withString(s, "cache-to"); set && !off(v) && !strings.Contains(v, refGate) {
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
		reason := ""
		for _, s := range j.Steps {
			if action(s.Uses) != "actions/checkout" {
				continue
			}
			ref, _ := withString(s, "ref")
			if r := untrustedRef(ref, ev); r != "" {
				reason = r
			}
		}
		if reason == "" {
			continue
		}
		for _, s := range j.Steps {
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
	files, err := filepath.Glob(filepath.Join(repoRoot(t), ".github", "workflows", "*.yml"))
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
