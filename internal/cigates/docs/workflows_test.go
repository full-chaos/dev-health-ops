package docs_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// Ports of tests/docs/test_docs_guards_workflow.py (12 tests; the python-version pin is dropped, see the PR) and of the four
// Cloudflare workflow tests of tests/docs/test_guard_failures.py: the shape of .github/workflows/docs-guards.yml and
// .github/workflows/docs-cloudflare.yml, plus the production-confirmation gates executed under bash.
//
// A step is checked by the COMMAND it runs, not by its name: a step whose checker was replaced by `echo` must be refused.

func pyCmd(stem string) string { return "python scripts/" + stem + "." + "py" }

// goCmd is a Go docs check run through the cigate binary (the ported checks).
func goCmd(verb string) string { return "go run -trimpath -mod=readonly ./cmd/cigate docs " + verb }

// expectedCheck is one step of docs-guards.yml: its name, the command line it must run (binary and script) and the
// arguments that command line must carry. A rename of a checker is an edit of this table.
type expectedCheck struct {
	step, command string
	args          []string
}

var docsGuardsChecks = []expectedCheck{
	{"Validate publication inventory and IA placement", goCmd("validate-publication"), nil},
	{"Strict canonical build", "python -m mkdocs build --strict", []string{"--config-file mkdocs.yml"}},
	{"Check rendered links, anchors, and assets", goCmd("check-built-site-links"), []string{"--site-dir .build/docs"}},
	{"Check task-based search acceptance", goCmd("check-docs-search"), []string{"--site-dir .build/docs", "--queries " + searchAcceptance}},
	{"Check structural accessibility invariants", goCmd("check-docs-accessibility"), []string{"--site-dir .build/docs", "--css docs/stylesheets/extra.css"}},
	{"Check code prerequisites", goCmd("check-code-prerequisites"), nil},
	{"Check freshness inventory", goCmd("check-freshness"), nil},
}

const (
	docsGuardsPath = ".github/workflows/docs-guards.yml"
	contentJob     = "docs-content-guards"
)

// docsGuardsTriggerPaths is every path both the push and the pull_request trigger of docs-guards.yml must watch: the docs
// content, the site config, the docs data, the two docs workflows and the checker scripts. A rename of a checker is an edit here.
var docsGuardsTriggerPaths = []string{
	"docs/**", "mkdocs.yml", "docs-data/**", glue("requirements-docs", ".", "txt"),
	".github/workflows/docs-guards.yml", ".github/workflows/docs-cloudflare.yml",
	"scripts/" + "check_freshness_inventory" + "." + "py",
}

var statusMask = regexp.MustCompile(`\|\|\s*(true|:|exit\s+0)(\s|$|;|&)|;\s*(true|:)(\s|$|;|&)|set\s+\+e\b`)

// maskFindings refuses every way a job or step can hide a failing command: `|| true`, `; true`, `set +e`, `continue-on-error`.
func maskFindings(label string, job map[string]any, steps []map[string]any) []string {
	var out []string
	masked := func(v any) bool { return v != nil && fmt.Sprint(v) != "false" }
	if masked(job["continue-on-error"]) {
		out = append(out, label+" must not set continue-on-error: a failing guard would not fail the job")
	}
	for _, s := range steps {
		name := asString(s["name"])
		if masked(s["continue-on-error"]) {
			out = append(out, fmt.Sprintf("%s step %q must not set continue-on-error: a failing guard would not fail the job", label, name))
		}
		for _, line := range commandLines(asString(s["run"])) {
			if m := statusMask.FindString(line); m != "" {
				out = append(out, fmt.Sprintf("%s step %q masks a failing command status (%q)", label, name, strings.TrimSpace(m)))
			}
		}
	}
	return out
}

// conditionFindings refuses an `if` on a guarded step: a condition skips the guard on the runs where it is false. names == nil guards every step.
func conditionFindings(label string, steps []map[string]any, names map[string]bool) []string {
	var out []string
	for _, s := range steps {
		name := asString(s["name"])
		if _, has := s["if"]; has && (names == nil || names[name]) {
			out = append(out, fmt.Sprintf("%s step %q must not carry an if: a condition skips the guard on the runs where it is false", label, name))
		}
	}
	return out
}

func workflowSteps(wf map[string]any, job string) []map[string]any {
	var steps []map[string]any
	for _, s := range asList(asMap(asMap(wf["jobs"])[job])["steps"]) {
		if m := asMap(s); m != nil {
			steps = append(steps, m)
		}
	}
	return steps
}

func stepRuns(steps []map[string]any) map[string]string {
	out := map[string]string{}
	for _, s := range steps {
		out[asString(s["name"])] = asString(s["run"])
	}
	return out
}

// commandLines joins backslash continuations and returns each command line of a run script, comments and blanks dropped.
func commandLines(run string) []string {
	var out []string
	var cur string
	for _, raw := range strings.Split(run, "\n") {
		line := strings.TrimSpace(raw)
		if strings.HasSuffix(line, "\\") {
			cur += strings.TrimSuffix(line, "\\") + " "
			continue
		}
		cur += line
		if cur != "" && !strings.HasPrefix(cur, "#") {
			out = append(out, strings.Join(strings.Fields(cur), " "))
		}
		cur = ""
	}
	return out
}

// runsCommand reports whether some command line of run STARTS with command (an echo of it does not count) and carries every arg.
func runsCommand(run, command string, args ...string) bool {
	for _, line := range commandLines(run) {
		if !strings.HasPrefix(line, command) {
			continue
		}
		ok := true
		for _, a := range args {
			ok = ok && strings.Contains(line, a)
		}
		if ok {
			return true
		}
	}
	return false
}

func docsGuardsGuard(d docTree) []string {
	var out []string
	wf, err := parseYAML(d, docsGuardsPath)
	if err != nil {
		return []string{err.Error()}
	}
	jobs := asMap(wf["jobs"])
	if jobs["docs-guards"] == nil {
		out = append(out, "the workflow must carry the canonical job named docs-guards")
	}
	for _, retired := range []string{"changes", "docs-guards-job"} {
		if jobs[retired] != nil {
			out = append(out, "the workflow must not carry the retired aggregator job "+retired)
		}
	}
	job := asMap(jobs["docs-guards"])
	if _, ok := job["needs"]; ok {
		out = append(out, "docs-guards must not depend on an upstream job (needs)")
	}
	if _, ok := job["strategy"]; ok {
		out = append(out, "docs-guards must not use a matrix (strategy)")
	}
	perms := asMap(wf["permissions"])
	if len(perms) != 1 || asString(perms["contents"]) != "read" {
		out = append(out, fmt.Sprintf("permissions must be exactly contents: read, got %v", perms))
	}
	on := asMap(wf["on"])
	for _, event := range []string{"merge_group", "workflow_dispatch"} {
		if _, ok := on[event]; !ok {
			out = append(out, "the workflow lost its "+event+" trigger")
		}
	}
	for _, trigger := range []string{"push", "pull_request"} {
		paths := asList(asMap(on[trigger])["paths"])
		has := func(want string) bool {
			for _, p := range paths {
				if asString(p) == want {
					return true
				}
			}
			return false
		}
		for _, want := range docsGuardsTriggerPaths {
			if !has(want) {
				out = append(out, fmt.Sprintf("the %s trigger path filter must watch %s", trigger, want))
			}
		}
	}
	steps := workflowSteps(wf, "docs-guards")
	out = append(out, maskFindings("docs-guards", job, steps)...)
	checked := map[string]bool{}
	for _, c := range docsGuardsChecks {
		checked[c.step] = true
	}
	out = append(out, conditionFindings("docs-guards", steps, checked)...)
	runs := stepRuns(steps)
	allRuns := strings.ToLower(strings.Join(func() []string {
		var l []string
		for _, s := range steps {
			l = append(l, asString(s["run"]))
		}
		return l
	}(), "\n"))
	for _, s := range steps {
		if strings.HasPrefix(asString(s["uses"]), "actions/setup-node@") {
			out = append(out, "docs-guards must not set up node")
		}
	}
	for _, forbidden := range []string{"docs-qa", "playwright", "pnpm", "npx", "wrangler", "screenshot", "visual regression", "exact prose", "check-external-links"} {
		if strings.Contains(allRuns, forbidden) {
			out = append(out, "docs-guards must not run "+forbidden)
		}
	}
	goSetup := false
	for _, s := range steps {
		if !strings.HasPrefix(asString(s["uses"]), "actions/setup-go@") {
			continue
		}
		if _, skippable := s["if"]; skippable {
			out = append(out, "docs-guards setup-go step must not carry an if: a skipped setup leaves its Go checkers without Go")
			continue
		}
		goSetup = true
	}
	if !goSetup {
		out = append(out, "docs-guards must set up Go: its Go checkers run through cmd/cigate")
	}
	if len(docsGuardsChecks) == 0 {
		out = append(out, "the expected-check table is empty")
	}
	for _, c := range docsGuardsChecks {
		run, ok := runs[c.step]
		if !ok {
			out = append(out, fmt.Sprintf("missing check step %q: its check would run only from a Makefile target, which no PR executes", c.step))
			continue
		}
		if !runsCommand(run, c.command, c.args...) {
			out = append(out, fmt.Sprintf("step %q does not run the command %q with %v (a step that echoes or replaces its checker is refused)", c.step, c.command, c.args))
		}
		if !strings.Contains(run, "set -o pipefail") || !strings.Contains(run, "| tee ") {
			out = append(out, fmt.Sprintf("step %q must stream through tee under set -o pipefail", c.step))
		}
	}
	var uploads []map[string]any
	for _, s := range steps {
		if strings.HasPrefix(asString(s["uses"]), "actions/upload-artifact@") {
			uploads = append(uploads, s)
		}
	}
	if len(uploads) != 1 {
		out = append(out, fmt.Sprintf("want exactly one evidence upload step, got %d", len(uploads)))
	} else {
		if asString(uploads[0]["if"]) != "always()" {
			out = append(out, "the evidence artifact must be uploaded with if: always()")
		}
		if !strings.Contains(asString(asMap(uploads[0]["with"])["path"]), ".build/docs-*.log") {
			out = append(out, "the evidence artifact must include .build/docs-*.log")
		}
	}
	// The Go guards of the docs content run on every docs change, in a job that installs no Python.
	content := asMap(jobs[contentJob])
	if content == nil {
		return append(out, "missing the Go docs guards job "+contentJob+": a docs-only pull request would run no docs content guard")
	}
	if _, ok := content["if"]; ok {
		out = append(out, contentJob+" must not carry an if: it must run on every docs change")
	}
	if _, ok := content["needs"]; ok {
		out = append(out, contentJob+" must not depend on another job")
	}
	csteps := workflowSteps(wf, contentJob)
	out = append(out, maskFindings(contentJob, content, csteps)...)
	out = append(out, conditionFindings(contentJob, csteps, nil)...)
	goTest := false
	for _, s := range csteps {
		uses := asString(s["uses"])
		if strings.HasPrefix(uses, "actions/setup-python@") || strings.Contains(uses, "setup-uv") {
			out = append(out, contentJob+" must install no Python: "+uses)
		}
		if run := asString(s["run"]); runsCommand(run, "go test", "./internal/cigates/docs") {
			goTest = true
		}
		if run := asString(s["run"]); strings.Contains(run, "uv ") || strings.Contains(run, "pip install") {
			out = append(out, contentJob+" must install no Python: "+run)
		}
	}
	if !goTest {
		out = append(out, contentJob+" does not run `go test` on ./internal/cigates/docs")
	}
	return out
}

func TestDocsGuardsWorkflowContract(t *testing.T) {
	plants := []plantCase{
		{"a second job is added", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "  docs-guards:\n    name: docs-guards\n", "  changes:\n    runs-on: ubuntu-latest\n    steps: []\n  docs-guards:\n    name: docs-guards\n")
		}, "retired aggregator job changes"},
		{"the job gains a needs", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "  docs-guards:\n    name: docs-guards\n", "  docs-guards:\n    name: docs-guards\n    needs: [changes]\n")
		}, "must not depend"},
		{"write permission is requested", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "permissions:\n  contents: read", "permissions:\n  contents: write")
		}, "contents: read"},
		{"the pull_request trigger stops watching docs", func(t *testing.T, d docTree) docTree {
			text, _ := d.read(docsGuardsPath)
			i := strings.Index(text, "  pull_request:")
			j := strings.Index(text[i:], "'docs/**'")
			s := text[:i+j] + "'doc/**'" + text[i+j+len("'docs/**'"):]
			return d.added(docsGuardsPath, s)
		}, "pull_request trigger"},
		{"the Go setup step is removed", func(t *testing.T, d docTree) docTree {
			text, _ := d.read(docsGuardsPath)
			i := strings.Index(text, "      - name: Set up Go\n        uses: actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0\n        with:\n          go-version-file: go.mod\n          cache-dependency-path: '**/go.sum'\n\n      - name: Validate")
			if i < 0 {
				t.Fatal("no Go setup step before the first check of docs-guards")
			}
			return d.added(docsGuardsPath, text[:i]+text[i+strings.Index(text[i:], "      - name: Validate"):])
		}, "must set up Go"},
		{"the Go setup step is skipped by an if", func(t *testing.T, d docTree) docTree {
			text, _ := d.read(docsGuardsPath)
			i := strings.Index(text, "  docs-guards:\n    name: docs-guards")
			anchor := "      - name: Set up Go\n        uses: actions/setup-go@"
			j := strings.Index(text[i:], anchor)
			if i < 0 || j < 0 {
				t.Fatal("no Set up Go step in the docs-guards job")
			}
			return d.added(docsGuardsPath, text[:i+j]+"      - name: Set up Go\n        if: false\n        uses: actions/setup-go@"+text[i+j+len(anchor):])
		}, "docs-guards setup-go step must not carry an if"},
		{"a node runtime is set up", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "      - name: Install documentation dependencies", "      - uses: actions/setup-node@v4\n      - name: Install documentation dependencies")
		}, "set up node"},
		{"a reader-critical check is renamed", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "name: Check structural accessibility invariants", "name: Check accessibility")
		}, `missing check step "Check structural accessibility invariants"`},
		{"a streamed check loses pipefail", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "          set -o pipefail\n          "+goCmd("check-code-prerequisites"), "          "+goCmd("check-code-prerequisites"))
		}, `step "Check code prerequisites" must stream through tee under set -o pipefail`},
		{"the strict build loses --strict (command replaced)", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "mkdocs build --strict", "mkdocs build")
		}, `step "Strict canonical build" does not run the command`},
		{"the search step loses its queries argument", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "--queries "+searchAcceptance, "--queries other-queries")
		}, `step "Check task-based search acceptance" does not run the command`},
		{"the accessibility step loses its css argument", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "--css docs/stylesheets/extra.css", "")
		}, `step "Check structural accessibility invariants" does not run the command`},
		{"the evidence upload is not always", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "        if: always()\n        uses: actions/upload-artifact", "        uses: actions/upload-artifact")
		}, "the evidence artifact must be uploaded with if: always()"},
		{"the external link check joins the gate", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "      - name: Check freshness inventory", "      - name: Extra\n        run: "+goCmd("check-external-links")+" --built-site .build/docs\n      - name: Check freshness inventory")
		}, "docs-guards must not run check-external-links"},
		{"the Go docs guards job is removed", func(t *testing.T, d docTree) docTree {
			text, _ := d.read(docsGuardsPath)
			i := strings.Index(text, "  docs-content-guards:")
			j := strings.Index(text, "  docs-guards:\n    name: docs-guards")
			return d.added(docsGuardsPath, text[:i]+text[j:])
		}, "missing the Go docs guards job"},
		{"the Go docs guards job is gated by an if", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "  docs-content-guards:\n    name: docs-content-guards\n", "  docs-content-guards:\n    name: docs-content-guards\n    if: false\n")
		}, "must not carry an if"},
		{"the Go docs guards job echoes its test command", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "run: go test -mod=readonly", "run: echo go test -mod=readonly")
		}, "does not run `go test`"},
		{"the Go docs guards job installs Python", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "      - name: Run the docs content guards", "      - uses: actions/setup-python@v6\n      - name: Run the docs content guards")
		}, "must install no Python"},
	}
	// One more plant per checker command: the command is replaced by an echo of the same text (the name, pipefail and tee
	// stay), which only a guard of the command line itself refuses.
	for _, c := range docsGuardsChecks {
		c := c
		plants = append(plants, plantCase{"the " + c.step + " command is replaced by an echo", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, c.command, "echo "+c.command)
		}, fmt.Sprintf("step %q does not run the command", c.step)})
	}
	// One plant per (event, path): the path is dropped from that event's list only.
	for _, event := range []string{"push", "pull_request"} {
		for _, path := range docsGuardsTriggerPaths {
			event, path := event, path
			plants = append(plants, plantCase{"the " + event + " trigger drops " + path, func(t *testing.T, d docTree) docTree {
				text, _ := d.read(docsGuardsPath)
				i := strings.Index(text, "\n  "+event+":")
				entry := "      - '" + path + "'\n"
				j := strings.Index(text[i:], entry)
				if i < 0 || j < 0 {
					t.Fatalf("no %s entry %s in the %s list", path, entry, event)
				}
				return d.added(docsGuardsPath, text[:i+j]+text[i+j+len(entry):])
			}, fmt.Sprintf("the %s trigger path filter must watch %s", event, path)})
		}
	}
	for _, event := range []string{"merge_group", "workflow_dispatch"} {
		event := event
		plants = append(plants, plantCase{"the " + event + " trigger is removed", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "\n  "+event+":", "\n  "+event+"_removed:")
		}, "lost its " + event + " trigger"})
	}
	// Status masks: each way to hide a failing command or step is refused, on the Go job, on a Python checker step.
	goTest := "run: go test -mod=readonly -trimpath -count=1 ./internal/cigates/docs/..."
	for _, m := range []struct{ name, repl string }{
		{"|| true", goTest + " || true"}, {"||true", goTest + " ||true"}, {"|| :", "run: \"go test -mod=readonly -trimpath -count=1 ./internal/cigates/docs/... || :\""}, {"|| exit 0", goTest + " || exit 0"},
		{"; true", goTest + "; true"}, {"set +e", "run: set +e; go test -mod=readonly -trimpath -count=1 ./internal/cigates/docs/..."},
	} {
		m := m
		plants = append(plants, plantCase{"the Go docs guards status is masked by " + m.name, func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, goTest, m.repl)
		}, fmt.Sprintf("%s step %q masks a failing command status (%q)", contentJob, "Run the docs content guards", m.name)})
	}
	plants = append(plants,
		plantCase{"the Go docs guards step sets continue-on-error", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "      - name: Run the docs content guards\n", "      - name: Run the docs content guards\n        continue-on-error: true\n")
		}, `docs-content-guards step "Run the docs content guards" must not set continue-on-error`},
		plantCase{"the Go docs guards job sets continue-on-error", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "  docs-content-guards:\n    name: docs-content-guards\n", "  docs-content-guards:\n    name: docs-content-guards\n    continue-on-error: true\n")
		}, "docs-content-guards must not set continue-on-error"},
		plantCase{"a Go checker status is masked by || true", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, goCmd("check-code-prerequisites"), goCmd("check-code-prerequisites")+" || true")
		}, `docs-guards step "Check code prerequisites" masks a failing command status ("|| true")`},
	)
	// The evidence upload is found by its action, never by its name: renaming it and disabling it is refused.
	plants = append(plants,
		plantCase{"the evidence upload is renamed and carries if: false", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "      - name: Upload documentation quality evidence\n        if: always()\n", "      - name: Upload evidence\n        if: false\n")
		}, "must be uploaded with if: always()"},
		plantCase{"a checker step is renamed and carries if: false", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "      - name: Check freshness inventory\n", "      - name: Freshness\n        if: false\n")
		}, `missing check step "Check freshness inventory"`},
	)
	// A condition on a guarded step skips the guard on the runs where it is false: refused on every checker step and on the Go step.
	guarded := []string{"Run the docs content guards"}
	for _, c := range docsGuardsChecks {
		guarded = append(guarded, c.step)
	}
	for _, step := range guarded {
		step := step
		for _, cond := range []string{"false", "github.event_name == 'push'"} {
			cond := cond
			plants = append(plants, plantCase{"the step " + step + " carries if: " + cond, func(t *testing.T, d docTree) docTree {
				return d.edit(t, docsGuardsPath, "      - name: "+step+"\n", "      - name: "+step+"\n        if: "+cond+"\n")
			}, fmt.Sprintf("step %q must not carry an if", step)})
		}
	}
	assertGuard(t, docsGuardsGuard, plants)
}

const cloudflarePath = ".github/workflows/docs-cloudflare.yml"

func cloudflareStep(d docTree, job, step string) (map[string]any, error) {
	wf, err := parseYAML(d, cloudflarePath)
	if err != nil {
		return nil, err
	}
	for _, s := range workflowSteps(wf, job) {
		if asString(s["name"]) == step {
			return s, nil
		}
	}
	return nil, fmt.Errorf("%s: job %q has no step %q", cloudflarePath, job, step)
}

func cloudflareGuard(d docTree) []string {
	var out []string
	raw, rerr := d.read(cloudflarePath)
	if rerr != nil {
		return []string{rerr.Error()}
	}
	for _, forbidden := range []string{"wrangler@", "wrangler deploy", "wrangler versions", "wrangler rollback", "WRANGLER_OUTPUT", "read_wrangler_output", "CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID", "deploy-production", "rollback-production", "docs-production", "pull-requests: write"} {
		mustNotContain(&out, "the Cloudflare docs workflow (Workers Builds owns the deploy)", raw, forbidden)
	}
	if wf, err := parseYAML(d, cloudflarePath); err != nil {
		return append(out, err.Error())
	} else {
		jobs := asMap(wf["jobs"])
		if len(jobs) != 2 || jobs["preview-or-build"] == nil || jobs["smoke-production"] == nil {
			out = append(out, fmt.Sprintf("%s must hold exactly the jobs preview-or-build and smoke-production, found %d jobs", cloudflarePath, len(jobs)))
		}
		smoke := asMap(jobs["smoke-production"])
		mustContain(&out, "the smoke job condition", asString(smoke["if"]), "github.event_name == 'workflow_dispatch'")
		mustContain(&out, "the smoke job condition", asString(smoke["if"]), "inputs.action == 'smoke'")
		if smoke["environment"] != nil || smoke["env"] != nil {
			out = append(out, "the smoke job must not carry an environment or env: it is read-only and holds no secrets")
		}
	}
	smokeStep, err := cloudflareStep(d, "smoke-production", "Smoke-test the canonical domain")
	if err != nil {
		return append(out, err.Error())
	}
	if smokeStep["env"] != nil {
		out = append(out, "the smoke step must not carry env: it is read-only and holds no secrets")
	}
	srun := asString(smokeStep["run"])
	for _, want := range []string{"./cmd/cigate docs host-smoke", "--origin https://docs.fullchaos.dev", "--redirects docs-data/redirects.tsv"} {
		mustContain(&out, "the smoke step", srun, want)
	}
	mustContain(&out, "the smoke step", srun, "exit 0")
	for _, line := range strings.Split(srun, "\n") {
		if strings.Contains(line, "docs host-smoke") && !strings.Contains(line, "go run ") {
			out = append(out, "the smoke step must run host-smoke, not echo it")
		}
	}
	preview, err := cloudflareStep(d, "preview-or-build", "Build and prepare validated preview assets")
	if err != nil {
		return append(out, err.Error())
	}
	prun := asString(preview["run"])
	out = append(out, maskFindings("the preview build", nil, []map[string]any{preview})...)
	out = append(out, conditionFindings("the preview build", []map[string]any{preview}, nil)...)
	mustContain(&out, "the preview build step", prun, "set -o pipefail")
	if !runsCommand(prun, pyCmd("build_docs_cloudflare"), "--mode preview --full-check") {
		out = append(out, "the preview build step does not run "+pyCmd("build_docs_cloudflare")+" --mode preview --full-check (an echo of it is refused)")
	}
	mustContain(&out, "the preview build step", prun, "| tee ")
	if cwf, err := parseYAML(d, cloudflarePath); err != nil {
		out = append(out, err.Error())
	} else {
		goSetups := 0
		for _, s := range workflowSteps(cwf, "preview-or-build") {
			if strings.HasPrefix(asString(s["uses"]), "actions/setup-go@") {
				goSetups++
				if _, skippable := s["if"]; skippable {
					out = append(out, "preview-or-build setup-go step must not carry an if: a skipped setup leaves the build without Go")
				}
			}
		}
		if goSetups != 1 {
			out = append(out, fmt.Sprintf("preview-or-build must set up Go exactly once, found %d: the build runs cmd/cigate, and a second setup-go step is redundant", goSetups))
		}
	}
	return out
}

func TestCloudflareWorkflowGates(t *testing.T) {
	assertGuard(t, cloudflareGuard, []plantCase{
		{"a wrangler deploy step returns to the preview job", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cloudflarePath, "  smoke-production:\n", "  deploy-production:\n    runs-on: ubuntu-latest\n    steps:\n      - run: npx --yes wrangler deploy\n\n  smoke-production:\n")
		}, "wrangler"},
		{"read_wrangler_output returns to the workflow", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cloudflarePath, "          sleep 10\n", "          sleep 10\n          python scripts/read_wrangler_output.py\n")
		}, "read_wrangler_output"},
		{"a Cloudflare secret returns to the smoke job", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cloudflarePath, "      - name: Smoke-test the canonical domain\n", "      - name: Smoke-test the canonical domain\n        env:\n          CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}\n")
		}, "CLOUDFLARE_API_TOKEN"},
		{"the smoke job runs on every trigger", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cloudflarePath, "    if: github.event_name == 'workflow_dispatch' && inputs.action == 'smoke'\n", "")
		}, "the smoke job condition"},
		{"the smoke job gains an environment", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cloudflarePath, "  smoke-production:\n", "  smoke-production:\n    environment: docs-production\n")
		}, "docs-production"},
		{"the smoke check is replaced by an echo", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cloudflarePath, "            if go run -trimpath -mod=readonly ./cmd/cigate docs host-smoke", "            if echo ./cmd/cigate docs host-smoke")
		}, "must run host-smoke"},
		{"a third job is added", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cloudflarePath, "  smoke-production:\n", "  extra:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n\n  smoke-production:\n")
		}, "exactly the jobs"},
		{"the preview build status is masked by || true", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cloudflarePath, pyCmd("build_docs_cloudflare")+" --mode preview --full-check", pyCmd("build_docs_cloudflare")+" --mode preview --full-check || true")
		}, `the preview build step "Build and prepare validated preview assets" masks a failing command status ("|| true")`},
		{"the preview build step sets continue-on-error", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cloudflarePath, "      - name: Build and prepare validated preview assets\n", "      - name: Build and prepare validated preview assets\n        continue-on-error: true\n")
		}, `the preview build step "Build and prepare validated preview assets" must not set continue-on-error`},
		{"the preview build step carries if: false", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cloudflarePath, "      - name: Build and prepare validated preview assets\n", "      - name: Build and prepare validated preview assets\n        if: false\n")
		}, "must not carry an if"},
		{"the preview build is replaced by an echo", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cloudflarePath, pyCmd("build_docs_cloudflare")+" --mode preview --full-check", "echo "+pyCmd("build_docs_cloudflare")+" --mode preview --full-check")
		}, "does not run"},
		{"the preview build loses pipefail", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cloudflarePath, "          set -o pipefail\n          "+pyCmd("build_docs_cloudflare")+" --mode preview", "          "+pyCmd("build_docs_cloudflare")+" --mode preview")
		}, "set -o pipefail"},
		{"preview-or-build gains a second setup-go step", func(t *testing.T, d docTree) docTree {
			text, _ := d.read(cloudflarePath)
			i := strings.Index(text, "      - uses: actions/setup-go@")
			j := strings.Index(text[i:], "      - uses: actions/setup-python@")
			if i < 0 || j < 0 {
				t.Fatal("no setup-go step before setup-python in preview-or-build")
			}
			return d.added(cloudflarePath, text[:i+j]+text[i:i+j]+text[i+j:])
		}, "preview-or-build must set up Go exactly once, found 2"},
		{"the preview setup-go step is skipped by an if", func(t *testing.T, d docTree) docTree {
			return d.editFirst(t, cloudflarePath, "      - uses: actions/setup-go@", "      - if: false\n        uses: actions/setup-go@")
		}, "preview-or-build setup-go step must not carry an if"},
		{"preview-or-build loses its setup-go step", func(t *testing.T, d docTree) docTree {
			text, _ := d.read(cloudflarePath)
			i := strings.Index(text, "      - uses: actions/setup-go@")
			j := strings.Index(text[i:], "      - uses: actions/setup-python@")
			if i < 0 || j < 0 {
				t.Fatal("no setup-go step before setup-python in preview-or-build")
			}
			return d.added(cloudflarePath, text[:i]+text[i+j:])
		}, "preview-or-build must set up Go exactly once, found 0"},
	})
}
