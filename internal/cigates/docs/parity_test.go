package docs_test

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// `make docs:check` claims to be what CI enforces for the docs. This test compares the two check sets and fails when a
// check exists on one side only, or when CI runs the same check twice (the site is built and checked once per revision).

const (
	makefilePath    = "Makefile"
	buildScriptPath = "scripts/build_docs_cloudflare.py"
	mkdocsBuild     = "mkdocs build"
	contentGoTest   = "go test ./internal/cigates/docs/..."
	docsPrepare     = "docs-prepare"
)

// fastLoopOnly are the checks `docs:check-fast` runs that Docs Guards does not: the drift gates run in docs-drift.yml and
// the source-link check is the inner-loop form of the rendered-link check.
var fastLoopOnly = map[string]bool{
	"check-investment-drift": true, "check-queue-mapping-drift": true, "check-docs-links": true,
}

var (
	cigateVerb    = regexp.MustCompile(`cmd/cigate (docs-prepare|docs ([a-z][a-z-]*))`)
	goTestDocs    = regexp.MustCompile(`^go test .*\./internal/cigates/docs/\.\.\.`)
	mkdocsCmd     = regexp.MustCompile(`mkdocs build\b`)
	scriptVerbs   = regexp.MustCompile(`(SITE_CHECK_VERBS|PREVIEW_ONLY_VERBS) = \(([^)]*)\)`)
	quoted        = regexp.MustCompile(`"([a-z][a-z-]*)"`)
	makeSubTarget = regexp.MustCompile(`\$\(MAKE\)[^\n]* (docs\\:[a-z-]+)`)
)

func scriptChecks(script string) (site, previewOnly []string) {
	for _, m := range scriptVerbs.FindAllStringSubmatch(script, -1) {
		var verbs []string
		for _, q := range quoted.FindAllStringSubmatch(m[2], -1) {
			verbs = append(verbs, q[1])
		}
		if m[1] == "SITE_CHECK_VERBS" {
			site = verbs
		} else {
			previewOnly = verbs
		}
	}
	return site, previewOnly
}

// checksOf lists the checks one command line runs. A run of the build script expands to its declared chain.
func checksOf(line, script string) []string {
	site, previewOnly := scriptChecks(script)
	switch {
	case strings.Contains(line, "scripts/build_docs_cloudflare.py"):
		if strings.Contains(line, "--reuse-site") {
			return append(append([]string{}, previewOnly...), docsPrepare)
		}
		if !strings.Contains(line, "--full-check") {
			return nil
		}
		out := append([]string{mkdocsBuild}, site...)
		return append(append(out, previewOnly...), docsPrepare)
	case mkdocsCmd.MatchString(line):
		return []string{mkdocsBuild}
	case goTestDocs.MatchString(line):
		return []string{contentGoTest}
	}
	if m := cigateVerb.FindStringSubmatch(line); m != nil {
		if m[2] == "" {
			return []string{docsPrepare}
		}
		return []string{m[2]}
	}
	return nil
}

func ciChecks(d docTree, script string) ([]string, []string) {
	wf, err := parseYAML(d, docsGuardsPath)
	if err != nil {
		return nil, []string{err.Error()}
	}
	var out []string
	for job := range asMap(wf["jobs"]) {
		for _, s := range workflowSteps(wf, job) {
			for _, line := range commandLines(asString(s["run"])) {
				out = append(out, checksOf(line, script)...)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

func makeRecipe(makefile, target string, seen map[string]bool) []string {
	if seen[target] {
		return nil
	}
	seen[target] = true
	var recipe []string
	in := false
	for _, raw := range strings.Split(makefile, "\n") {
		switch {
		case strings.HasPrefix(raw, target+":"):
			in = true
		case in && strings.HasPrefix(raw, "\t"):
			line := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(raw), "@"))
			if m := makeSubTarget.FindStringSubmatch(line); m != nil {
				recipe = append(recipe, makeRecipe(makefile, m[1], seen)...)
				continue
			}
			recipe = append(recipe, line)
		case in && !strings.HasPrefix(raw, "#") && strings.TrimSpace(raw) != "":
			in = false
		}
	}
	return recipe
}

func localChecks(makefile, script string) []string {
	var out []string
	for _, line := range makeRecipe(makefile, `docs\:check`, map[string]bool{}) {
		line = strings.ReplaceAll(line, "$(PYTHON)", "python")
		out = append(out, checksOf(line, script)...)
	}
	sort.Strings(out)
	return out
}

func parityGuard(d docTree) []string {
	makefile, err := d.read(makefilePath)
	if err != nil {
		return []string{err.Error()}
	}
	script, err := d.read(buildScriptPath)
	if err != nil {
		return []string{err.Error()}
	}
	ci, errs := ciChecks(d, script)
	if errs != nil {
		return errs
	}
	local := localChecks(makefile, script)
	var out []string
	seen := map[string]int{}
	for _, c := range ci {
		seen[c]++
	}
	for c, n := range seen {
		if n > 1 {
			out = append(out, fmt.Sprintf("docs-guards.yml runs %q %d times: the site is built and checked once per revision", c, n))
		}
	}
	have := map[string]bool{}
	for _, c := range local {
		have[c] = true
	}
	inCI := map[string]bool{}
	for _, c := range ci {
		inCI[c] = true
		if !have[c] {
			out = append(out, fmt.Sprintf("CI runs %q but `make docs:check` does not", c))
		}
	}
	for _, c := range local {
		if !inCI[c] && !fastLoopOnly[c] {
			out = append(out, fmt.Sprintf("`make docs:check` runs %q but docs-guards.yml does not", c))
		}
	}
	if len(ci) == 0 || len(local) == 0 {
		out = append(out, "no checks found on one side: the parity test reads nothing")
	}
	return out
}

func TestDocsCheckMatchesCI(t *testing.T) {
	assertGuard(t, parityGuard, []plantCase{
		{"a check is dropped from the build script chain", func(t *testing.T, d docTree) docTree {
			return d.edit(t, buildScriptPath, "    \"check-docs-search\",\n", "")
		}, `CI runs "check-docs-search" but`},
		{"a check is added to the build script chain only", func(t *testing.T, d docTree) docTree {
			return d.edit(t, buildScriptPath, "    \"check-docs-search\",\n", "    \"check-docs-search\",\n    \"check-new-thing\",\n")
		}, `runs "check-new-thing" but docs-guards.yml does not`},
		{"a check is added to docs-guards.yml only", func(t *testing.T, d docTree) docTree {
			return d.edit(t, docsGuardsPath, "      - name: Upload documentation quality evidence\n", "      - name: Check new thing\n        run: go run -trimpath -mod=readonly ./cmd/cigate docs check-new-thing\n\n      - name: Upload documentation quality evidence\n")
		}, `CI runs "check-new-thing" but`},
		{"the content go test is dropped from docs:check", func(t *testing.T, d docTree) docTree {
			return d.edit(t, makefilePath, "\tgo test -mod=readonly -trimpath -count=1 ./internal/cigates/docs/...\n", "")
		}, `CI runs "go test ./internal/cigates/docs/..." but`},
		{"docs:check stops running the build script", func(t *testing.T, d docTree) docTree {
			return d.edit(t, makefilePath, "docs\\:check-fast\n\t$(PYTHON) scripts/build_docs_cloudflare.py --mode preview --full-check\n", "docs\\:check-fast\n")
		}, "CI runs"},
		{"a second job builds the site again", func(t *testing.T, d docTree) docTree {
			return d.appended(t, docsGuardsPath, "  rebuild:\n    runs-on: ubuntu-latest\n    steps:\n      - run: python -m mkdocs build --strict --config-file mkdocs.yml\n")
		}, `runs "mkdocs build" 2 times`},
	})
}
