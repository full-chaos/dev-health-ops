package docs_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"unicode"
)

// Ports of tests/tooling-free docs content contracts: test_audience_navigation.py (4), test_design_tokens.py (4),
// test_ai_user_guide_source_accuracy.py (3), test_user_guides.py (3), test_user_guide_source_accuracy.py (1),
// test_jira_service_management_contract.py (1), the content halves of test_investment_drift.py (1) and
// test_queue_mapping_drift.py (1), and the publication-layout half of test_built_site_links.py (1).

// ---- navigation and home page (test_audience_navigation.py) -----------------------------------------------------------

var (
	productOverview   = []string{"Context Fabric"}
	productOverviewTo = map[string]string{"Context Fabric": "context-fabric/index.md"}
	taskDomains       = []string{"Get started", "Use Dev Health", "Administer Dev Health", "Install and operate", "Integrate and extend", "Reference", "Contribute"}
	firstViewport     = [][2]string{
		{"Use Dev Health", "use/index.md"},
		{"Browse reference", "reference/index.md"},
		{"Investigate where effort appears to be going", "use/investment/investigate-effort.md"},
		{"Diagnose no or incomplete data", "use/troubleshooting/no-or-incomplete-data.md"},
		{"Look up weighting and aggregation", "reference/metrics/weighting-and-aggregation.md"},
	}
	legacyRoutes   = []string{"user-guide/", "product/concepts.md", "contributing/platform-contract.md", "customer-push-ingestion/", "self-hosted-quickstart.md", "api/graphql-overview.md"}
	legacyAudience = []string{"Start here", "Administer & integrate", "Deploy & operate", "Develop & extend", "Reference & history"}
)

func navGuard(d docTree) []string {
	var out []string
	cfg, err := parseYAML(d, "mkdocs.yml")
	if err != nil {
		return []string{err.Error()}
	}
	var labels []string
	var targets []any
	for i, item := range asList(cfg["nav"]) {
		m := asMap(item)
		if len(m) != 1 {
			out = append(out, fmt.Sprintf("nav item %d is not a single-key mapping", i))
			continue
		}
		for k, v := range m {
			labels = append(labels, k)
			targets = append(targets, v)
		}
	}
	if len(labels) == 0 || labels[0] != "Documentation home" || asString(targets[0]) != "index.md" {
		out = append(out, "navigation does not start with the home router: Documentation home -> index.md")
	}
	if len(labels) >= 1+len(productOverview)+len(taskDomains) {
		for i, label := range productOverview {
			if labels[1+i] != label || asString(targets[1+i]) != productOverviewTo[label] {
				out = append(out, fmt.Sprintf("the governed product overview %q is not the exact canonical page %s", label, productOverviewTo[label]))
			}
		}
		rest := labels[1+len(productOverview):]
		if strings.Join(rest, "|") != strings.Join(taskDomains, "|") {
			out = append(out, fmt.Sprintf("top-level task domains are %v, want exactly %v in order", rest, taskDomains))
		}
	} else {
		out = append(out, fmt.Sprintf("navigation has %d top-level entries, want the home router, the product overview and %d task domains", len(labels), len(taskDomains)))
	}
	for _, legacy := range legacyAudience {
		for _, l := range labels {
			if l == legacy {
				out = append(out, "a superseded audience bucket returned to the navigation: "+legacy)
			}
		}
	}
	navText, _ := d.read("mkdocs.yml")
	index, ok := readOrNote(&out, d, "docs/index.md")
	for _, legacy := range legacyRoutes {
		mustNotContain(&out, "mkdocs.yml", navText, legacy)
		if ok {
			mustNotContain(&out, "docs/index.md", index, legacy)
		}
	}
	if ok {
		head, _, _ := strings.Cut(index, "\n## ")
		for _, task := range firstViewport {
			mustContain(&out, "home first viewport", head, fmt.Sprintf("[%s](%s)", task[0], task[1]))
			if !d.exists("docs/" + task[1]) {
				out = append(out, "first-viewport destination is not a canonical page: "+task[1])
			}
		}
	}
	return out
}

func TestNavigationContract(t *testing.T) {
	assertGuard(t, navGuard, []plantCase{
		{"an audience bucket returns", func(t *testing.T, d docTree) docTree {
			return d.editFirst(t, "mkdocs.yml", "  - Get started:", "  - Start here: index.md\n  - Get started:")
		}, "Start here"},
		{"a task domain is renamed", func(t *testing.T, d docTree) docTree {
			return d.edit(t, "mkdocs.yml", "  - Contribute:", "  - Contributing:")
		}, "top-level task domains"},
		{"a legacy route returns", func(t *testing.T, d docTree) docTree {
			return d.edit(t, "mkdocs.yml", "  - Contribute:", "  - Old: user-guide/index.md\n  - Contribute:")
		}, "user-guide/"},
		{"the home router loses a first-viewport task", func(t *testing.T, d docTree) docTree {
			return d.edit(t, "docs/index.md", "[Browse reference](reference/index.md)", "Browse reference")
		}, "Browse reference"},
		{"a first-viewport destination is removed", func(t *testing.T, d docTree) docTree {
			return d.removed("docs/use/investment/investigate-effort.md")
		}, "investigate-effort.md"},
	})
}

// ---- design tokens (test_design_tokens.py) ------------------------------------------------------------------------------

var (
	rawCSSValue  = regexp.MustCompile(`(#[0-9a-fA-F]{3,8})(?:[^\p{L}\p{N}_]|$)`)
	cssToken     = regexp.MustCompile(`(--[A-Za-z0-9_-]+)[\s\p{Z}\x{0085}]*:[\s\p{Z}\x{0085}]*([^;]+);`)
	requiredPals = [][2]string{
		{"--fc-void", "#08080A"}, {"--fc-ink", "#15171A"}, {"--fc-graphite", "#23252A"}, {"--fc-silver", "#D1D1D4"}, {"--fc-flame", "#FE7501"},
		{"--fc-crimson", "#A30A06"}, {"--fc-aqua", "#04B7C4"}, {"--fc-ocean", "#037493"}, {"--fc-glacier", "#6CE0E1"}, {"--fc-gold", "#FFDB4B"},
	}
	reviewOnlyPaths = []string{"docs/design-directions.md", "docs/reference/primitive-showcase.md", qaPackage, qaPlaywright}
)

func paletteTokens(css string) map[string]string {
	tokens := map[string]string{}
	for _, m := range cssToken.FindAllStringSubmatch(css, -1) {
		tokens[m[1]] = strings.TrimSpace(m[2])
	}
	return tokens
}

// undeclaredRawCSSValue is the first raw colour value outside a `:root { --fc-... }` token declaration, or "".
func undeclaredRawCSSValue(css string) string {
	inRoot := false
	for _, line := range splitLines(css) {
		if line == ":root {" {
			inRoot = true
		} else if inRoot && line == "}" {
			inRoot = false
		}
		if m := rawCSSValue.FindStringSubmatch(line); m != nil && !(inRoot && strings.HasPrefix(strings.TrimLeftFunc(line, unicode.IsSpace), "--fc-")) {
			return m[1]
		}
	}
	return ""
}

func designGuard(d docTree) []string {
	var out []string
	palette, ok := readOrNote(&out, d, "docs/stylesheets/logo-palette.css")
	if ok {
		tokens := paletteTokens(palette)
		for _, p := range requiredPals {
			if tokens[p[0]] != p[1] {
				out = append(out, fmt.Sprintf("palette token %s is %q, want %s", p[0], tokens[p[0]], p[1]))
			}
		}
	}
	extra, ok1 := readOrNote(&out, d, "docs/stylesheets/extra.css")
	home, ok2 := readOrNote(&out, d, "docs/stylesheets/home.css")
	cf, ok3 := readOrNote(&out, d, "docs/stylesheets/context-fabric.css")
	if ok1 {
		mustContain(&out, "extra.css", extra, `@import url("logo-palette.css");`)
		mustContain(&out, "extra.css", extra, ":focus-visible")
		mustContain(&out, "extra.css", extra, "prefers-reduced-motion")
	}
	if ok2 {
		mustContain(&out, "home.css", home, "fc-home-hero")
		mustContain(&out, "home.css", home, "fc-home-card")
	}
	if ok3 {
		mustContain(&out, "context-fabric.css", cf, "fc-cf-hero")
		mustContain(&out, "context-fabric.css", cf, "fc-cf-use-card")
		mustContain(&out, "context-fabric.css", cf, "prefers-reduced-motion")
		if v := undeclaredRawCSSValue(cf); v != "" {
			out = append(out, "context-fabric.css: undeclared raw CSS value "+v)
		}
	}
	for _, p := range reviewOnlyPaths {
		if d.exists(p) {
			out = append(out, "review-only material is in the public tree: "+p)
		}
	}
	return out
}

func TestDesignTokens(t *testing.T) {
	assertGuard(t, designGuard, []plantCase{
		{"a palette value changes", func(t *testing.T, d docTree) docTree {
			return d.editFirst(t, "docs/stylesheets/logo-palette.css", "#FE7501", "#FE7502")
		}, "--fc-flame"},
		{"the palette import is dropped", func(t *testing.T, d docTree) docTree {
			return d.edit(t, "docs/stylesheets/extra.css", `@import url("logo-palette.css");`, "")
		}, "logo-palette.css"},
		{"a raw colour outside the tokens", func(t *testing.T, d docTree) docTree {
			return d.appended(t, "docs/stylesheets/context-fabric.css", "\n.fixture { color: #badc0d; }\n")
		}, "undeclared raw CSS value #badc0d"},
		{"a design-directions page is published", func(t *testing.T, d docTree) docTree {
			return d.added("docs/design-directions.md", "# x\n")
		}, "design-directions.md"},
		{"the design-QA harness returns", func(t *testing.T, d docTree) docTree {
			return d.added(qaPackage, "{}")
		}, qaPackage},
	})
}

func TestUndeclaredRawCSSValueIsRejected(t *testing.T) {
	if got := undeclaredRawCSSValue(".fixture { color: #badc0d; }"); got != "#badc0d" {
		t.Fatalf("undeclared raw value not found: %q", got)
	}
	if got := undeclaredRawCSSValue(":root {\n  --fc-void: #08080A;\n}\n.a { color: var(--fc-void); }"); got != "" {
		t.Fatalf("a declared token was rejected: %q", got)
	}
}

// ---- AI workflow and user guide language (test_ai_user_guide_source_accuracy.py, test_user_guides.py, test_user_guide_source_accuracy.py) ----

var (
	aiGuidePages = []string{"impact.md", "review-load.md", "risk.md"}
	aiPhrases    = map[string][]string{
		"impact.md":      {"an association can appear", "not a productivity label, forecast, or causal experiment"},
		"review-load.md": {"does not establish that ai caused review pressure", "rather than a person-level judgment"},
		"risk.md":        {"not proof of policy violation", "derived or model-assisted risk estimates"},
	}
	aiForbidden       = []string{"determined", "detected", "verdict"}
	canonicalNavRoute = []string{
		"get-started/index.md", "use/delivery-flow/pr-flow.md", "use/ai-workflows/impact.md", "use/ai-workflows/review-load.md",
		"use/ai-workflows/risk.md", "use/reports/index.md", "reference/taxonomies/investment.md",
	}
	prFlowRemovedView = []string{"state flow", "state-transition", "sankey"}
	prFlowUnsupported = []string{"first review", "merge timing", "PR stages"}
)

func guidesGuard(d docTree) []string {
	var out []string
	for _, page := range aiGuidePages {
		text, ok := readOrNote(&out, d, "docs/use/ai-workflows/"+page)
		if !ok {
			continue
		}
		n := normalized(text)
		for _, phrase := range aiPhrases[page] {
			mustContain(&out, page, n, phrase)
		}
		for _, f := range aiForbidden {
			mustNotContain(&out, page, n, f)
		}
	}
	for _, withheld := range []string{"docs/use/ai-workflows/attribution.md", "docs/use/ai-workflows/ai-attribution.md"} {
		if d.exists(withheld) {
			out = append(out, "AI Attribution must not be republished as a public page: "+withheld)
		}
	}
	nav, _ := d.read("mkdocs.yml")
	for _, route := range canonicalNavRoute {
		if !strings.Contains(nav, route) {
			out = append(out, "canonical route not wired in mkdocs.yml: "+route)
		}
		if !d.exists("docs/" + route) {
			out = append(out, "missing canonical page: "+route)
		}
	}
	mustNotContain(&out, "mkdocs.yml", nav, "user-guide/")
	if pr, ok := readOrNote(&out, d, "docs/use/delivery-flow/pr-flow.md"); ok {
		n := normalized(pr)
		for _, want := range []string{"evidence", "not a measure of individual performance", "cycle time × throughput", "metric tiles", "likely associations", "primary contributors", "individual performance"} {
			mustContain(&out, "pr-flow.md", n, want)
		}
		for _, f := range []string{"detected", "determined"} {
			mustNotContain(&out, "pr-flow.md", n, f)
		}
		for _, c := range prFlowRemovedView {
			mustNotContain(&out, "pr-flow.md (a removed view)", n, c)
		}
		for _, c := range prFlowUnsupported {
			mustNotContain(&out, "pr-flow.md (an unsupported claim)", n, fold(c))
		}
	}
	return out
}

func TestUserGuideLanguageAndRoutes(t *testing.T) {
	assertGuard(t, guidesGuard, []plantCase{
		{"an AI guide loses a calibrated phrase", func(t *testing.T, d docTree) docTree {
			return d.edit(t, "docs/use/ai-workflows/risk.md", "not proof of policy violation", "proof of policy violation")
		}, "not proof of policy violation"},
		{"an AI guide uses definitive language", func(t *testing.T, d docTree) docTree {
			return d.appended(t, "docs/use/ai-workflows/impact.md", "\nThe verdict is clear.\n")
		}, "verdict"},
		{"AI Attribution is republished", func(t *testing.T, d docTree) docTree {
			return d.added("docs/use/ai-workflows/attribution.md", "# x\n")
		}, "attribution.md"},
		{"a canonical route leaves the navigation", func(t *testing.T, d docTree) docTree {
			return d.edit(t, "mkdocs.yml", "use/reports/index.md", "use/reports/other.md")
		}, "use/reports/index.md"},
		{"the legacy user-guide route returns", func(t *testing.T, d docTree) docTree {
			return d.edit(t, "mkdocs.yml", "  - Contribute:", "  - Old: user-guide/index.md\n  - Contribute:")
		}, "user-guide/"},
		{"pr-flow describes a removed view", func(t *testing.T, d docTree) docTree {
			return d.appended(t, "docs/use/delivery-flow/pr-flow.md", "\nA sankey shows the flow.\n")
		}, "sankey"},
	})
}

// ---- single-page content guards ---------------------------------------------------------------------------------------

func jiraGuard(d docTree) []string {
	var out []string
	text, ok := readOrNote(&out, d, "docs/admin/data-sources/jira-atlassian.md")
	if !ok {
		return out
	}
	mustContain(&out, "the public Jira guide", text, "page_id: admin-jira")
	mustContain(&out, "the public Jira guide", text, "Connect and verify Jira")
	mustContain(&out, "the public Jira guide", text, "not yet a supported\nadministrator workflow")
	mustNotContain(&out, "the public Jira guide", text, "Jira Service Management provider contract")
	mustNotContain(&out, "the public Jira guide", text, "api.atlassian.com/jsm/incidents")
	return out
}

func TestPublicJiraGuideOmitsTheInternalContract(t *testing.T) {
	assertGuard(t, jiraGuard, []plantCase{
		{"the internal provider contract leaks", func(t *testing.T, d docTree) docTree {
			return d.edit(t, "docs/admin/data-sources/jira-atlassian.md", "Connect and verify Jira", "Connect and verify Jira\n\nJira Service Management provider contract")
		}, "provider contract"},
		{"an internal API host leaks", func(t *testing.T, d docTree) docTree {
			return d.edit(t, "docs/admin/data-sources/jira-atlassian.md", "Connect and verify Jira", "Connect and verify Jira api.atlassian.com/jsm/incidents")
		}, "api.atlassian.com/jsm/incidents"},
	})
}

func investmentGuard(d docTree) []string {
	var out []string
	text, ok := readOrNote(&out, d, "docs/reference/taxonomies/investment.md")
	if !ok {
		return out
	}
	n := fold(text)
	for _, want := range []string{"canonical", "not workspace-configurable", "must not define a competing vocabulary"} {
		mustContain(&out, "the taxonomy page", n, want)
	}
	mustContain(&out, "the taxonomy page", text, taxonomySource)
	for _, theme := range []string{"feature_delivery", "operational", "maintenance", "quality", "risk"} {
		mustContain(&out, "the taxonomy page (a canonical theme key)", text, "`"+theme+"`")
	}
	return out
}

func TestCanonicalTaxonomyPageDocumentsTheVocabulary(t *testing.T) {
	assertGuard(t, investmentGuard, []plantCase{
		{"the page stops forbidding a competing vocabulary", func(t *testing.T, d docTree) docTree {
			return d.edit(t, "docs/reference/taxonomies/investment.md", "must not define a competing vocabulary", "may define a vocabulary")
		}, "competing vocabulary"},
		{"the page stops naming its source", func(t *testing.T, d docTree) docTree {
			text, _ := d.read("docs/reference/taxonomies/investment.md")
			s := strings.ReplaceAll(text, taxonomySource, "the registry")
			return d.added("docs/reference/taxonomies/investment.md", s)
		}, taxonomySource},
	})
}

func workerRuntimeGuard(d docTree) []string {
	var out []string
	text, ok := readOrNote(&out, d, "docs/contribute/architecture/go-worker-runtime.md")
	if !ok {
		return out
	}
	mustContain(&out, "go-worker-runtime.md", text, "CHAOS-4054")
	if !strings.Contains(text, "are **deleted**") && !strings.Contains(text, "surface is deleted") {
		out = append(out, "go-worker-runtime.md: must state that the WORKER_*_ENABLED surface is deleted")
	}
	for _, phrase := range []string{"being retired", "is dying", "on its way out"} {
		mustNotContain(&out, "go-worker-runtime.md (in-flight wording)", text, phrase)
	}
	if !strings.Contains(text, "IntegrationDataset.is_enabled") || !strings.Contains(text, "-Q") {
		out = append(out, "go-worker-runtime.md: must name both replacement planes: intent (IntegrationDataset.is_enabled) and serving (-Q topology)")
	}
	return out
}

func TestGoWorkerRuntimeDoesNotEnshrineWorkerEnabledSwitches(t *testing.T) {
	assertGuard(t, workerRuntimeGuard, []plantCase{
		{"the switch surface is described as in flight", func(t *testing.T, d docTree) docTree {
			return d.appended(t, "docs/contribute/architecture/go-worker-runtime.md", "\nThis surface is being retired.\n")
		}, "being retired"},
		{"a replacement plane is no longer named", func(t *testing.T, d docTree) docTree {
			return d.editAll(t, "docs/contribute/architecture/go-worker-runtime.md", "IntegrationDataset.is_enabled", "the dataset flag")
		}, "replacement planes"},
	})
}

// publicationLayoutGuard is the file half of test_canonical_publication_gate_is_not_driven_by_docs_publication_yml: the
// canonical gate is driven by mkdocs.yml and the docs-data IA and redirects, not by a docs/publication.yml manifest. The
// validator itself runs in the docs-guards workflow.
func publicationLayoutGuard(d docTree) []string {
	var out []string
	if d.exists(docPublicationManifest) {
		out = append(out, "docs/publication.yml must not exist: the canonical gate is driven by mkdocs.yml and docs-data")
	}
	for _, rel := range []string{"mkdocs.yml", redirectsTable} {
		if !d.exists(rel) {
			out = append(out, "missing "+rel)
		}
	}
	if !d.dirHas("docs-data/ia", glue(".", "tsv")) {
		out = append(out, "docs-data/ia holds no table")
	}
	for _, rel := range []string{"use/index.md", "reference/index.md", "use/investment/investigate-effort.md", "reference/metrics/weighting-and-aggregation.md", "reference/taxonomies/investment.md", "reference/cli/index.md"} {
		if !d.exists("docs/" + rel) {
			out = append(out, "navigated canonical page is missing: docs/"+rel)
		}
	}
	return out
}

func TestCanonicalPublicationLayout(t *testing.T) {
	assertGuard(t, publicationLayoutGuard, []plantCase{
		{"a publication manifest returns", func(t *testing.T, d docTree) docTree { return d.added(docPublicationManifest, "x: 1\n") }, docPublicationManifest},
		{"a canonical page is removed", func(t *testing.T, d docTree) docTree { return d.removed("docs/reference/cli/index.md") }, "reference/cli/index.md"},
		{"the redirects table is removed", func(t *testing.T, d docTree) docTree { return d.removed(redirectsTable) }, "docs-data/redir"},
	})
}
