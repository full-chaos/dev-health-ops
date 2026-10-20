package docs_test

import (
	"fmt"
	"strings"
	"testing"
)

// Ports of tests/docs/test_docs_guards_workflow.py (12 tests; the python-version pin is dropped, see the PR) and of the four
// Cloudflare workflow tests of tests/docs/test_guard_failures.py: the shape of .github/workflows/docs-guards.yml and
// .github/workflows/docs-cloudflare.yml, plus the production-confirmation gates executed under bash.

var streamedChecks = []string{
	"Validate publication inventory and IA placement",
	"Strict canonical build",
	"Check rendered links, anchors, and assets",
	"Check task-based search acceptance",
	"Check structural accessibility invariants",
	"Check objective documentation facts",
}

var newlyGatedChecks = []string{"Check code prerequisites", "Check freshness inventory"}

const docsGuardsPath = ".github/workflows/docs-guards.yml"

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

func docsGuardsGuard(d docTree) []string {
	var out []string
	wf, err := parseYAML(d, docsGuardsPath)
	if err != nil {
		return []string{err.Error()}
	}
	jobs := asMap(wf["jobs"])
	if len(jobs) != 1 || jobs["docs-guards"] == nil {
		out = append(out, fmt.Sprintf("the workflow must be one canonical job named docs-guards, got %d job(s)", len(jobs)))
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
	for _, trigger := range []string{"push", "pull_request"} {
		paths := asList(asMap(asMap(wf["on"])[trigger])["paths"])
		has := func(want string) bool {
			for _, p := range paths {
				if asString(p) == want {
					return true
				}
			}
			return false
		}
		if !has("docs/**") || !has("mkdocs.yml") {
			out = append(out, fmt.Sprintf("the %s trigger path filter must watch docs/** and mkdocs.yml", trigger))
		}
	}
	steps := workflowSteps(wf, "docs-guards")
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
	for _, forbidden := range []string{"docs-qa", "playwright", "pnpm", "npx", "wrangler", "screenshot", "visual regression", "exact prose", "check_external_links.py"} {
		if strings.Contains(allRuns, forbidden) {
			out = append(out, "docs-guards must not run "+forbidden)
		}
	}
	for _, name := range streamedChecks {
		run, ok := runs[name]
		if !ok {
			out = append(out, "missing reader-critical check step: "+name)
			continue
		}
		if !strings.Contains(run, "set -o pipefail") || !strings.Contains(run, "| tee ") {
			out = append(out, fmt.Sprintf("step %q must stream through tee under set -o pipefail", name))
		}
	}
	build := runs[streamedChecks[1]]
	mustContain(&out, "the strict build step", build, "mkdocs build --strict")
	mustContain(&out, "the strict build step", build, "--config-file mkdocs.yml")
	mustContain(&out, "the links step", runs[streamedChecks[2]], "--site-dir .build/docs")
	mustContain(&out, "the search step", runs[streamedChecks[3]], "docs-data/search-acceptance.json")
	if len(newlyGatedChecks) == 0 {
		out = append(out, "the gated-check inventory is empty")
	}
	for _, name := range newlyGatedChecks {
		run, ok := runs[name]
		if !ok {
			out = append(out, fmt.Sprintf("%q is no longer a step in docs-guards.yml: its check would run only from a Makefile target, which no PR executes", name))
			continue
		}
		if !strings.Contains(run, "set -o pipefail") || !strings.Contains(run, "tee") {
			out = append(out, fmt.Sprintf("step %q must stream through tee under set -o pipefail", name))
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
	return out
}

func TestDocsGuardsWorkflowContract(t *testing.T) {
	assertGuard(t, docsGuardsGuard, []plantCase{
		{"a second job is added", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "jobs:\n  docs-guards:", "jobs:\n  changes:\n    runs-on: ubuntu-latest\n    steps: []\n  docs-guards:")
		}, "one canonical job"},
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
		{"a node runtime is set up", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "      - name: Install documentation dependencies", "      - uses: actions/setup-node@v4\n      - name: Install documentation dependencies")
		}, "set up node"},
		{"a reader-critical check is dropped", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "name: Check structural accessibility invariants", "name: Check accessibility")
		}, "Check structural accessibility invariants"},
		{"a streamed check loses pipefail", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "          set -o pipefail\n          python scripts/check_docs_candidate_facts.py", "          python scripts/check_docs_candidate_facts.py")
		}, "Check objective documentation facts"},
		{"the strict build loses --strict", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "mkdocs build --strict", "mkdocs build")
		}, "mkdocs build --strict"},
		{"a gated script drops out of the workflow", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "name: Check freshness inventory", "name: Freshness")
		}, "Check freshness inventory"},
		{"the evidence upload is not always", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "        if: always()\n        uses: actions/upload-artifact", "        uses: actions/upload-artifact")
		}, "if: always()"},
		{"the external link check joins the gate", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "      - name: Check freshness inventory", "      - name: Extra\n        run: python scripts/check_external_links.py\n      - name: Check freshness inventory")
		}, "check_external_links.py"},
	})
}

// ---- docs-cloudflare.yml ------------------------------------------------------------------------------------------------

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
	status := func(script string, env ...string) int { return bashStatus(script, env...) }
	deploy, err := cloudflareStep(d, "deploy-production", "Confirm production deployment is enabled")
	if err != nil {
		return []string{err.Error()}
	}
	script := asString(deploy["run"])
	if status(script, "ENABLED=false", "CONFIRMATION=docs.fullchaos.dev") != 1 {
		out = append(out, "the production deploy gate did not refuse a disabled flag")
	}
	if status(script, "ENABLED=true", "CONFIRMATION=nope") != 1 {
		out = append(out, "the production deploy gate did not refuse a wrong confirmation")
	}
	if status(script, "ENABLED=true", "CONFIRMATION=docs.fullchaos.dev") != 0 {
		out = append(out, "the production deploy gate refused a correct confirmation")
	}
	rollback, err := cloudflareStep(d, "rollback-production", "Validate rollback request")
	if err != nil {
		return append(out, err.Error())
	}
	rscript := asString(rollback["run"])
	good := map[string]string{"ENABLED": "true", "CONFIRMATION": "docs.fullchaos.dev", "VERSION_ID": "abc123"}
	envOf := func(skip string) []string {
		var e []string
		for k, v := range good {
			if k == skip {
				v = ""
			}
			e = append(e, k+"="+v)
		}
		return e
	}
	if status(rscript, envOf("")...) != 0 {
		out = append(out, "the rollback gate refused a complete request")
	}
	for _, missing := range []string{"ENABLED", "CONFIRMATION", "VERSION_ID"} {
		if status(rscript, envOf(missing)...) != 1 {
			out = append(out, "the rollback gate accepted a request without "+missing)
		}
	}
	preview, err := cloudflareStep(d, "preview-or-build", "Build and prepare validated preview assets")
	if err != nil {
		return append(out, err.Error())
	}
	prun := asString(preview["run"])
	mustContain(&out, "the preview build step", prun, "set -o pipefail")
	mustContain(&out, "the preview build step", prun, "build_docs_cloudflare.py --mode preview --full-check")
	mustContain(&out, "the preview build step", prun, "| tee ")
	record, err := cloudflareStep(d, "preview-or-build", "Record why a Cloudflare preview was not uploaded")
	if err != nil {
		return append(out, err.Error())
	}
	mustContain(&out, "the preview no-op record condition", asString(record["if"]), "steps.upload.outcome == 'skipped'")
	rrun := asString(record["run"])
	mustNotContain(&out, "the preview no-op record step", rrun, "wrangler")
	mustContain(&out, "the preview no-op record step", rrun, "$GITHUB_STEP_SUMMARY")
	return out
}

func TestCloudflareWorkflowGates(t *testing.T) {
	assertGuard(t, cloudflareGuard, []plantCase{
		{"the production flag check is removed", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cloudflarePath, "          test \"$ENABLED\" = 'true' || {\n            echo 'DOCS_CLOUDFLARE_PRODUCTION_ENABLED is not true.'\n            exit 1\n          }\n          test \"$CONFIRMATION\" = 'docs.fullchaos.dev' || {\n            echo 'Production confirmation", "          test \"$CONFIRMATION\" = 'docs.fullchaos.dev' || {\n            echo 'Production confirmation")
		}, "disabled flag"},
		{"the production confirmation text is not checked", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cloudflarePath, "test \"$CONFIRMATION\" = 'docs.fullchaos.dev' || {\n            echo 'Production confirmation", "test -n \"$CONFIRMATION\" || {\n            echo 'Production confirmation")
		}, "wrong confirmation"},
		{"the rollback version id is not required", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cloudflarePath, "test -n \"$VERSION_ID\"", "test -z \"$VERSION_ID_UNUSED\"")
		}, "VERSION_ID"},
		{"the preview build loses pipefail", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cloudflarePath, "          set -o pipefail\n          python scripts/build_docs_cloudflare.py --mode preview", "          python scripts/build_docs_cloudflare.py --mode preview")
		}, "set -o pipefail"},
		{"the no-op record starts to deploy", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cloudflarePath, "            echo '### Cloudflare preview not uploaded'", "            wrangler deploy\n            echo '### Cloudflare preview not uploaded'")
		}, "wrangler"},
	})
}
