package docstools_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cigates/docstools"
)

// Tests of check-docs-links, check-freshness and check-external-links that the recorded pairs cannot hold: a fixed
// date.today(), an injected fetcher, and the cases of the deleted tests/docs pytest files (test_freshness_inventory.py,
// test_external_links.py, the matching rows of test_guard_failures.py), one Go test per pytest case.

var linkToday = time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)

func writeFiles(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		writeTree(t, filepath.Join(root, rel), content, 0o644)
	}
}

func freshness(t *testing.T, inventory string) (int, string, string) {
	t.Helper()
	work := t.TempDir()
	writeFiles(t, work, map[string]string{"docs/guide.md": "# Guide\n", "docs/replacement.md": "# Replacement\n"})
	if inventory != "" {
		writeFiles(t, work, map[string]string{"docs-data/freshness-inventory.yml": inventory})
	}
	t.Chdir(work)
	var stdout, stderr bytes.Buffer
	code := docstools.FreshnessRunForTest(nil, &stdout, &stderr, linkToday)
	return code, stdout.String(), stderr.String()
}

func TestLoadInventoryRaisesWhenTheFileIsMissing(t *testing.T) {
	code, stdout, stderr := freshness(t, "")
	if code != 1 || stdout != "" || !strings.Contains(stderr, "ERROR: missing freshness inventory") {
		t.Errorf("exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
}

func TestCheckFreshnessInventoryAcceptsAFreshRefreshEntry(t *testing.T) {
	code, stdout, stderr := freshness(t, "- page: guide.md\n  disposition: refresh\n  owner: docs-team\n  last-reviewed: '2026-07-01'\n")
	if code != 0 || stdout != "Documentation freshness inventory check passed\n" || stderr != "" {
		t.Errorf("exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
}

func TestCheckFreshnessInventoryRejectsAStaleReviewDate(t *testing.T) {
	code, stdout, stderr := freshness(t, "- page: guide.md\n  disposition: refresh\n  owner: docs-team\n  last-reviewed: '2025-01-01'\n")
	want := "ERROR: guide.md: last-reviewed 2025-01-01 is 561 days old, exceeding the 180-day review cadence\n"
	if code != 1 || stdout != want || stderr != "" {
		t.Errorf("exit %d, stdout %q, stderr %q; want stdout %q", code, stdout, stderr, want)
	}
}

func TestCheckFreshnessInventoryCadenceBoundary(t *testing.T) {
	for _, c := range []struct {
		reviewed string
		stale    bool
	}{{"2026-01-17", false}, {"2026-01-16", true}, {"0001-01-01", true}, {"9999-12-31", false}} {
		code, stdout, _ := freshness(t, "- page: guide.md\n  disposition: refresh\n  owner: a\n  last-reviewed: '"+c.reviewed+"'\n")
		if (code == 1) != c.stale || c.stale != strings.Contains(stdout, "exceeding the 180-day review cadence") {
			t.Errorf("last-reviewed %s: exit %d, stdout %q; want stale=%v", c.reviewed, code, stdout, c.stale)
		}
	}
}

func TestCheckFreshnessInventoryRequiresAReplacementForRetiredPages(t *testing.T) {
	code, stdout, _ := freshness(t, "- page: guide.md\n  disposition: retire\n  owner: docs-team\n  last-reviewed: '2026-07-01'\n")
	if code != 1 || stdout != "ERROR: guide.md: retired pages must name a replacement page\n" {
		t.Errorf("exit %d, stdout %q", code, stdout)
	}
}

func TestCheckFreshnessInventoryAcceptsARetiredPageWithAnExistingReplacement(t *testing.T) {
	code, stdout, _ := freshness(t, "- page: guide.md\n  disposition: retire\n  owner: docs-team\n  last-reviewed: '2026-07-01'\n  replacement: replacement.md\n")
	if code != 0 || stdout != "Documentation freshness inventory check passed\n" {
		t.Errorf("exit %d, stdout %q", code, stdout)
	}
}

func TestLoadInventoryRejectsAnInvalidDisposition(t *testing.T) {
	code, stdout, stderr := freshness(t, "- page: guide.md\n  disposition: archive\n  owner: docs-team\n  last-reviewed: '2026-07-01'\n")
	if code != 1 || stdout != "" || !strings.Contains(stderr, "invalid disposition 'archive'") {
		t.Errorf("exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
}

func TestStaleReviewDateFails(t *testing.T) {
	code, stdout, _ := freshness(t, "- page: guide.md\n  disposition: refresh\n  owner: docs-team\n  last-reviewed: '2025-01-01'\n")
	if code != 1 || !strings.Contains(stdout, "guide.md") || !strings.Contains(stdout, "exceeding the 180-day review cadence") {
		t.Errorf("exit %d, stdout %q", code, stdout)
	}
}

// external runs check-external-links on a site of the given files with --allowlist allowlist.yml.
func external(t *testing.T, files map[string]string, fetch func(string) (bool, string, error), extra ...string) (int, string, string) {
	t.Helper()
	work := t.TempDir()
	writeFiles(t, work, files)
	if err := os.MkdirAll(filepath.Join(work, "site"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Chdir(work)
	var stdout, stderr bytes.Buffer
	args := append([]string{"--built-site", "site", "--allowlist", "allowlist.yml"}, extra...)
	code := docstools.ExternalLinksRunForTest(args, &stdout, &stderr, linkToday, fetch)
	return code, stdout.String(), stderr.String()
}

func alwaysOK(string) (bool, string, error)     { return true, "status 200", nil }
func alwaysBroken(string) (bool, string, error) { return false, "status 404", nil }

func noFetch(t *testing.T) func(string) (bool, string, error) {
	return func(u string) (bool, string, error) {
		t.Errorf("unexpected fetch of %s", u)
		return false, "", nil
	}
}

func TestCheckExternalLinksAcceptsAReachableLink(t *testing.T) {
	code, stdout, _ := external(t, map[string]string{"site/index.html": `<a href="https://example.com/">example</a>`}, alwaysOK)
	if code != 0 || stdout != "External link check passed\n" {
		t.Errorf("exit %d, stdout %q", code, stdout)
	}
}

func TestCheckExternalLinksRejectsABrokenUnlistedLink(t *testing.T) {
	code, stdout, _ := external(t, map[string]string{"site/index.html": `<a href="https://broken.example.com/">broken</a>`}, alwaysBroken)
	if code != 1 || stdout != "ERROR: index.html: broken external link https://broken.example.com/: status 404\n" {
		t.Errorf("exit %d, stdout %q", code, stdout)
	}
}

const expiredAllowlist = "- url: https://expired.example.com/\n  owner: docs-content\n  reason: temporary vendor outage\n  expires: '2026-01-01'\n"

func TestCheckExternalLinksRejectsAnExpiredAllowlistEntryEvenWhenReachable(t *testing.T) {
	code, stdout, _ := external(t, map[string]string{"site/index.html": `<a href="https://expired.example.com/">expired</a>`, "allowlist.yml": expiredAllowlist}, alwaysOK)
	if code != 1 || !strings.Contains(stdout, "allowlist entry for https://expired.example.com/ expired") {
		t.Errorf("exit %d, stdout %q", code, stdout)
	}
}

func TestExpiredAllowlistFails(t *testing.T) {
	code, stdout, _ := external(t, map[string]string{"site/index.html": `<a href="https://expired.example.com/">expired</a>`, "allowlist.yml": expiredAllowlist}, alwaysOK)
	want := "ERROR: index.html: allowlist entry for https://expired.example.com/ expired on 2026-01-01 (owner: docs-content, reason: temporary vendor outage)\n"
	if code != 1 || stdout != want {
		t.Errorf("exit %d, stdout %q, want %q", code, stdout, want)
	}
}

func TestCheckExternalLinksSkipsALinkCoveredByAnUnexpiredAllowlistEntry(t *testing.T) {
	allow := "- url: https://flaky.example.com/\n  owner: docs-content\n  reason: known flaky vendor endpoint\n  expires: '2027-01-01'\n"
	code, stdout, _ := external(t, map[string]string{"site/index.html": `<a href="https://flaky.example.com/">flaky</a>`, "allowlist.yml": allow}, noFetch(t))
	if code != 0 || stdout != "External link check passed\n" {
		t.Errorf("exit %d, stdout %q", code, stdout)
	}
}

func TestCheckExternalLinksAllowlistExpiresStrictlyBeforeToday(t *testing.T) {
	allow := "- url: https://flaky.example.com/\n  owner: a\n  reason: b\n  expires: '2026-07-16'\n"
	code, stdout, _ := external(t, map[string]string{"site/index.html": `<a href="https://flaky.example.com/">flaky</a>`, "allowlist.yml": allow}, noFetch(t))
	if code != 0 {
		t.Errorf("an entry expiring today is still valid: exit %d, stdout %q", code, stdout)
	}
}

func TestCheckExternalLinksRetriesBeforeFailing(t *testing.T) {
	var calls []string
	fetch := func(u string) (bool, string, error) {
		calls = append(calls, u)
		if len(calls) >= 2 {
			return true, "status 200", nil
		}
		return false, "status 503", nil
	}
	code, _, _ := external(t, map[string]string{"site/index.html": `<a href="https://flaky.example.com/">flaky</a>`}, fetch)
	if code != 0 || len(calls) != 2 {
		t.Errorf("exit %d, %d calls; want 0 and 2", code, len(calls))
	}
	calls = nil
	fetch = func(u string) (bool, string, error) { calls = append(calls, u); return false, "status 503", nil }
	code, stdout, _ := external(t, map[string]string{"site/index.html": `<a href="https://flaky.example.com/">flaky</a>`}, fetch)
	if code != 1 || len(calls) != 3 || !strings.HasSuffix(stdout, ": status 503\n") {
		t.Errorf("exit %d, %d calls, stdout %q; want 1, 3 calls and the last detail", code, len(calls), stdout)
	}
}

func TestCheckExternalLinksDeduplicatesRepeatedUrls(t *testing.T) {
	var calls []string
	fetch := func(u string) (bool, string, error) { calls = append(calls, u); return true, "status 200", nil }
	external(t, map[string]string{
		"site/index.html": `<a href="https://example.com/">one</a><a href="https://example.com/">two</a>`,
		"site/other.html": `<a href="https://example.com/">three</a>`,
	}, fetch)
	if len(calls) != 1 || calls[0] != "https://example.com/" {
		t.Errorf("calls %v", calls)
	}
}

func TestCheckExternalLinksResolvesSameSiteCanonicalAnchorLocally(t *testing.T) {
	code, stdout, _ := external(t, map[string]string{
		"site/index.html":       `<a href="https://docs.fullchaos.dev/guide/#details">guide</a>`,
		"site/guide/index.html": `<section id="details"></section>`,
	}, noFetch(t), "--site-url", "https://docs.fullchaos.dev")
	if code != 0 || stdout != "External link check passed\n" {
		t.Errorf("exit %d, stdout %q", code, stdout)
	}
}

func TestCheckExternalLinksRejectsMissingSameSiteCanonicalPath(t *testing.T) {
	code, stdout, _ := external(t, map[string]string{"site/index.html": `<a href="https://docs.fullchaos.dev/missing/">missing</a>`}, alwaysBroken, "--site-url", "https://docs.fullchaos.dev")
	if code != 1 || stdout != "ERROR: index.html: missing built page for https://docs.fullchaos.dev/missing/\n" {
		t.Errorf("exit %d, stdout %q", code, stdout)
	}
}

func TestCheckExternalLinksRejectsMissingSameSiteCanonicalAnchor(t *testing.T) {
	code, stdout, _ := external(t, map[string]string{
		"site/index.html":       `<a href="https://docs.fullchaos.dev/guide/#missing">guide</a>`,
		"site/guide/index.html": `<section id="details"></section>`,
	}, alwaysBroken, "--site-url", "https://docs.fullchaos.dev")
	if code != 1 || stdout != "ERROR: index.html: missing anchor 'missing' for https://docs.fullchaos.dev/guide/#missing\n" {
		t.Errorf("exit %d, stdout %q", code, stdout)
	}
}

func TestCheckExternalLinksMatchesEquivalentSameSiteOrigins(t *testing.T) {
	code, stdout, _ := external(t, map[string]string{
		"site/index.html":       `<a href="https://DOCS.FULLCHAOS.DEV:443/guide/#details">guide</a>`,
		"site/guide/index.html": `<section id="details"></section>`,
	}, alwaysBroken, "--site-url", "https://docs.fullchaos.dev")
	if code != 0 {
		t.Errorf("exit %d, stdout %q", code, stdout)
	}
}

func TestMainAcceptsLegacyExternalLinkCheckerArguments(t *testing.T) {
	work := t.TempDir()
	writeFiles(t, work, map[string]string{"site/index.html": ""})
	t.Chdir(work)
	var stdout, stderr bytes.Buffer
	if code := docstools.ExternalLinksRunForTest([]string{"--built-site", "site"}, &stdout, &stderr, linkToday, noFetch(t)); code != 0 {
		t.Errorf("exit %d, stderr %q", code, stderr.String())
	}
}

func TestCheckExternalLinksRejectsExpiredSameSiteAllowlistEntry(t *testing.T) {
	allow := "- url: https://docs.fullchaos.dev/guide/\n  owner: docs-content\n  reason: temporary canonical migration\n  expires: '2026-01-01'\n"
	code, stdout, _ := external(t, map[string]string{
		"site/index.html":       `<a href="https://docs.fullchaos.dev/guide/">guide</a>`,
		"site/guide/index.html": "",
		"allowlist.yml":         allow,
	}, alwaysBroken, "--site-url", "https://docs.fullchaos.dev")
	want := "ERROR: index.html: allowlist entry for https://docs.fullchaos.dev/guide/ expired on 2026-01-01 (owner: docs-content, reason: temporary canonical migration)\n"
	if code != 1 || stdout != want {
		t.Errorf("exit %d, stdout %q", code, stdout)
	}
}

func TestCheckExternalLinksFetcherErrorStopsTheRun(t *testing.T) {
	fetch := func(string) (bool, string, error) { return false, "", os.ErrInvalid }
	code, stdout, stderr := external(t, map[string]string{"site/index.html": `<a href="https://example.com/">x</a>`}, fetch)
	if code != 1 || stdout != "" || strings.Count(stderr, "\n") != 1 {
		t.Errorf("exit %d, stdout %q, stderr %q; want 1, no stdout, one stderr line", code, stdout, stderr)
	}
}

func TestBrokenSourceLinkFails(t *testing.T) {
	work := t.TempDir()
	writeFiles(t, work, map[string]string{"docs/index.md": "# Home\n\nSee the [missing page](does-not-exist.md) for details.\n"})
	t.Chdir(work)
	code, stdout, _ := runDocs("check-docs-links")
	if code != 1 || stdout != "ERROR: docs/index.md -> does-not-exist.md: missing file\n" {
		t.Errorf("exit %d, stdout %q", code, stdout)
	}
}

func TestCheckDocsLinksRefusesArguments(t *testing.T) {
	t.Chdir(t.TempDir())
	code, stdout, stderr := runDocs("check-docs-links", "--help")
	if code != 2 || stdout != "" || !strings.Contains(stderr, "takes no arguments") {
		t.Errorf("exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
}

// The reference slugs are markdown.extensions.toc.slugify(heading, "-") of python-markdown 3.11.2 (unicode=False),
// what mkdocs renders; the last row is the script's own HTML-tag and backtick strip before it.
func TestSlugifyMatchesTheRealMkdocsTocAnchor(t *testing.T) {
	for heading, want := range map[string]string{
		"Plain Heading": "plain-heading",
		"What a surface ISSUES: the `issued_credential` field": "what-a-surface-issues-the-issued_credential-field",
		"snake_case_identifier stays snake_case":               "snake_case_identifier-stays-snake_case",
		"Trailing punctuation!?":                               "trailing-punctuation",
		"Multiple   spaces  collapse":                          "multiple-spaces-collapse",
		"Mixed_Underscore-and-Hyphen Runs":                     "mixed_underscore-and-hyphen-runs",
		"Café Résumé — unicode gets folded to ASCII":           "cafe-resume-unicode-gets-folded-to-ascii",
		"  leading and trailing whitespace  ":                  "leading-and-trailing-whitespace",
		" nbsp  inside":                                        "nbsp-inside",
		"x\x1cy":                                               "x-y",
		"Ǆemo ﬁne ①":                                           "dzemo-fine-1",
	} {
		if got := docstools.SlugifyForTest(heading); got != want {
			t.Errorf("slug of %q = %q, want %q", heading, got, want)
		}
	}
}

func TestSlugifyHtmlAndBacktickStrippingMatchesTheRenderedText(t *testing.T) {
	if got := docstools.SlugifyForTest("<code>HTML</code> tags and `backticks` are stripped"); got != "html-tags-and-backticks-are-stripped" {
		t.Errorf("slug %q", got)
	}
}

// argparseCase is one row of testdata/argparse-cases.json: the outcome of the Python script for those arguments,
// recorded in a work directory that holds an empty site/ and nothing else.
type argparseCase struct {
	Verb   string   `json:"verb"`
	Args   []string `json:"args"`
	RC     int      `json:"rc"`
	Error  string   `json:"error"`
	Help   bool     `json:"help"`
	Stdout *string  `json:"stdout"`
	Stderr *string  `json:"stderr"`
}

func TestArgumentParsingMatchesPythonArgparse(t *testing.T) {
	var cases []argparseCase
	if err := json.Unmarshal([]byte(mustRead(t, filepath.Join("testdata", "argparse-cases.json"))), &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range cases {
		work := t.TempDir()
		if err := os.MkdirAll(filepath.Join(work, "site"), 0o755); err != nil {
			t.Fatal(err)
		}
		t.Chdir(work)
		code, stdout, stderr := runDocs(append([]string{c.Verb}, c.Args...)...)
		label := c.Verb + " " + strings.Join(c.Args, " | ")
		if code != c.RC {
			t.Errorf("%s: exit %d, Python %d (stderr %q)", label, code, c.RC, stderr)
			continue
		}
		switch {
		case c.RC == 2:
			lines := strings.Split(strings.TrimSuffix(stderr, "\n"), "\n")
			if stdout != "" || !strings.HasSuffix(lines[len(lines)-1], ": error: "+c.Error) {
				t.Errorf("%s: stdout %q, stderr %q; want the argparse error %q", label, stdout, stderr, c.Error)
			}
		case c.Help:
			if !strings.HasPrefix(stdout, "usage: ") || stderr != "" {
				t.Errorf("%s: stdout %q, stderr %q; want help", label, stdout, stderr)
			}
		default:
			sub := func(s *string) string { return strings.ReplaceAll(*s, "{WORK}", work) }
			if stdout != sub(c.Stdout) || stderr != sub(c.Stderr) {
				t.Errorf("%s: stdout %q, stderr %q; Python %q, %q", label, stdout, stderr, sub(c.Stdout), sub(c.Stderr))
			}
		}
	}
}
