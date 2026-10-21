package docs_test

import (
	"fmt"
	"strings"
	"testing"
)

// The shape of .github/workflows/docs-drift.yml: the docs checks that run when only docs change. The page-content tests of
// internal/cigates/docstools (the investment taxonomy page, the go-worker-runtime page) live in a Go package that no other
// docs job runs, so this workflow must run that package; otherwise a docs-only change that breaks a page assertion keeps
// every docs check green. A step is checked by the COMMAND it runs, not by its name.

const docsDriftPath = ".github/workflows/docs-drift.yml"

const docsDriftJob = "docs-drift"

type driftStep struct{ command, args string }

var docsDriftSteps = []driftStep{
	{"go run -trimpath -mod=readonly ./cmd/cigate docs", "check-investment-drift"},
	{"go run -trimpath -mod=readonly ./cmd/cigate docs", "check-queue-mapping-drift"},
	{"go run -trimpath -mod=readonly ./cmd/cigate docs", "check-candidate-facts"},
	{"go run -trimpath -mod=readonly ./cmd/cigate docs", "check-provider-limits"},
	{"go test -mod=readonly -trimpath -count=1", "./internal/cigates/docstools/..."},
}

// docsDriftTriggerPaths are the paths both triggers must watch: the docs pages and the package that tests them.
var docsDriftTriggerPaths = []string{"docs/**", "internal/cigates/docstools/**", "cmd/cigate/**", ".github/workflows/docs-drift.yml"}

func docsDriftGuard(d docTree) []string {
	var out []string
	wf, err := parseYAML(d, docsDriftPath)
	if err != nil {
		return []string{err.Error()}
	}
	on := asMap(wf["on"])
	for _, trigger := range []string{"push", "pull_request"} {
		paths := asList(asMap(on[trigger])["paths"])
		for _, want := range docsDriftTriggerPaths {
			found := false
			for _, p := range paths {
				found = found || asString(p) == want
			}
			if !found {
				out = append(out, fmt.Sprintf("the %s trigger path filter must watch %s", trigger, want))
			}
		}
	}
	job := asMap(asMap(wf["jobs"])[docsDriftJob])
	if job == nil {
		return append(out, "the workflow must carry the job "+docsDriftJob)
	}
	if _, ok := job["if"]; ok {
		out = append(out, docsDriftJob+" must not carry an if: it must run on every docs change")
	}
	steps := workflowSteps(wf, docsDriftJob)
	out = append(out, maskFindings(docsDriftJob, job, steps)...)
	out = append(out, conditionFindings(docsDriftJob, steps, nil)...)
	goSetup := false
	for _, s := range steps {
		uses := asString(s["uses"])
		goSetup = goSetup || strings.HasPrefix(uses, "actions/setup-go@")
		if strings.HasPrefix(uses, "actions/setup-python@") || strings.Contains(uses, "setup-uv") {
			out = append(out, docsDriftJob+" must install no Python: "+uses)
		}
	}
	if !goSetup {
		out = append(out, docsDriftJob+" must set up Go")
	}
	for _, scope := range []struct {
		name string
		m    map[string]any
	}{{"the workflow", wf}, {docsDriftJob, job}} {
		for _, key := range []string{"env", "defaults"} {
			if _, ok := scope.m[key]; ok {
				out = append(out, scope.name+" must not set "+key+": it could change what the checks run")
			}
		}
	}
	for _, want := range docsDriftSteps {
		ran := false
		for _, s := range steps {
			if strings.TrimSpace(asString(s["run"])) != want.command+" "+want.args {
				continue
			}
			ran = true
			for _, key := range []string{"env", "shell", "working-directory"} {
				if _, ok := s[key]; ok {
					out = append(out, fmt.Sprintf("the step %q must not set %s", want.args, key))
				}
			}
		}
		if !ran {
			out = append(out, fmt.Sprintf("%s runs no step with the command %q and %q (the step run must be exactly that one command line: an echo, a mask, a conditional or an extra flag is refused)", docsDriftJob, want.command, want.args))
		}
	}
	return out
}

func TestDocsDriftWorkflowRunsTheDocsPageGuards(t *testing.T) {
	var plants []plantCase
	for _, s := range docsDriftSteps {
		s := s
		line := s.command + " " + s.args
		plants = append(plants,
			plantCase{"the step `" + line + "` is replaced by an echo", func(t *testing.T, d docTree) docTree {
				return d.edit(t, docsDriftPath, "run: "+line, "run: echo "+line)
			}, "runs no step with the command"},
			plantCase{"the step `" + line + "` carries if: false", func(t *testing.T, d docTree) docTree {
				return d.edit(t, docsDriftPath, "        run: "+line, "        if: false\n        run: "+line)
			}, "must not carry an if"},
			plantCase{"the step `" + line + "` is masked by || true", func(t *testing.T, d docTree) docTree {
				return d.edit(t, docsDriftPath, "run: "+line, "run: "+line+" || true")
			}, "masks a failing command status"},
			plantCase{"the step `" + line + "` sets continue-on-error", func(t *testing.T, d docTree) docTree {
				return d.edit(t, docsDriftPath, "        run: "+line, "        continue-on-error: true\n        run: "+line)
			}, "must not set continue-on-error"},
			plantCase{"the step `" + line + "` is masked by || echo", func(t *testing.T, d docTree) docTree {
				return d.edit(t, docsDriftPath, "run: "+line, "run: "+line+" || echo ignored")
			}, "runs no step with the command"},
			plantCase{"the step `" + line + "` sits in a false shell conditional", func(t *testing.T, d docTree) docTree {
				return d.edit(t, docsDriftPath, "run: "+line, "run: |\n          if false; then\n            "+line+"\n          fi")
			}, "runs no step with the command"},
			plantCase{"the step `" + line + "` gets -run '^$'", func(t *testing.T, d docTree) docTree {
				return d.edit(t, docsDriftPath, "run: "+line, "run: "+line+" -run '^$'")
			}, "runs no step with the command"},
			plantCase{"the step `" + line + "` sets GOFLAGS", func(t *testing.T, d docTree) docTree {
				return d.edit(t, docsDriftPath, "        run: "+line, "        env:\n          GOFLAGS: -run=^$\n        run: "+line)
			}, "must not set env"},
			plantCase{"the step `" + line + "` sets a shell", func(t *testing.T, d docTree) docTree {
				return d.edit(t, docsDriftPath, "        run: "+line, "        shell: bash {0}\n        run: "+line)
			}, "must not set shell"},
		)
	}
	for _, event := range []string{"push", "pull_request"} {
		for _, path := range docsDriftTriggerPaths {
			event, path := event, path
			plants = append(plants, plantCase{"the " + event + " trigger drops " + path, func(t *testing.T, d docTree) docTree {
				text, _ := d.read(docsDriftPath)
				i := strings.Index(text, "\n  "+event+":")
				entry := "      - '" + path + "'\n"
				j := strings.Index(text[i:], entry)
				if i < 0 || j < 0 {
					t.Fatalf("no %s entry in the %s list", path, event)
				}
				return d.added(docsDriftPath, text[:i+j]+text[i+j+len(entry):])
			}, fmt.Sprintf("the %s trigger path filter must watch %s", event, path)})
		}
	}
	plants = append(plants,
		plantCase{"the job sets continue-on-error", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsDriftPath, "  docs-drift:\n    name: docs-drift\n", "  docs-drift:\n    name: docs-drift\n    continue-on-error: true\n")
		}, "must not set continue-on-error"},
		plantCase{"the job carries an if", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsDriftPath, "  docs-drift:\n    name: docs-drift\n", "  docs-drift:\n    name: docs-drift\n    if: false\n")
		}, "must not carry an if"},
		plantCase{"the job sets env", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsDriftPath, "  docs-drift:\n    name: docs-drift\n", "  docs-drift:\n    name: docs-drift\n    env:\n      GOFLAGS: -run=^$\n")
		}, "must not set env"},
		plantCase{"Python is installed", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsDriftPath, "      - name: Set up Go\n", "      - uses: actions/setup-python@v6\n      - name: Set up Go\n")
		}, "must install no Python"},
	)
	assertGuard(t, docsDriftGuard, plants)
}
