package docs_test

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The published docs state how the product works today. Tracker ids, rulings, plan ids, PR numbers, agent and
// people names and internal hosts or paths belong in the tracker, not on the public site.

type internalTextClass struct {
	name string
	re   *regexp.Regexp
}

var internalTextClasses = []internalTextClass{
	{"tracker id", regexp.MustCompile(`CHAOS-\d+`)},
	{"cutover story", regexp.MustCompile(`(?i)cutover`)},
	{"wave or plan id", regexp.MustCompile(`\bW\d+-\d+\b|(?i:\bwave[ -]\d+)`)},
	{"lane", regexp.MustCompile(`(?i)\blane\b`)},
	{"PR number", regexp.MustCompile(`(?:^|[^&\w])#\d{2,}`)},
	{"ruling D", regexp.MustCompile(`\bD\d{4}\b`)},
	{"ruling R", regexp.MustCompile(`\bR\d{3}\b`)},
	{"person name", regexp.MustCompile(`(?i)\bchris\b`)},
	{"internal host", regexp.MustCompile(`(?i)bigboy`)},
	{"internal path", regexp.MustCompile(`/home/ubuntu|/var/lib/oci-cache|lane-scratch|\.remember`)},
	{"agent name", regexp.MustCompile(`\b(?:fcsup|fcc|fcs|cag|acp)\b`)},
	{"trap number", regexp.MustCompile(`Trap #\d+`)},
	{"job number", regexp.MustCompile(`\bJOB \d+`)},
}

// internalTextAllowed are names a reader types: they stay until the code behind them changes.
var internalTextAllowed = regexp.MustCompile(strings.Join([]string{
	`go-cutover`,
	`cutover-replay\.sh`,
	`celeryRiverCutover`,
	`check_session_cutover_rehearsal\.sh`,
	`DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER`,
	`lane_<ticket>`,
	`CUTOVER-RUNBOOK\.md`,
	"`acp(?:-[a-z0-9-]+)?`",
	`ghpr:[^\s]*#\d+`,
	`pending lane`,
}, "|"))

func publishedMarkdown(d docTree) []string {
	excluded := map[string]bool{}
	if cfg, err := parseYAML(d, "mkdocs.yml"); err == nil {
		if s, ok := cfg["exclude_docs"].(string); ok {
			for _, line := range strings.Split(s, "\n") {
				if line = strings.TrimSpace(line); line != "" {
					excluded["docs/"+line] = true
				}
			}
		}
	}
	seen := map[string]bool{}
	var out []string
	add := func(p string) {
		if strings.HasPrefix(p, "docs/") && strings.HasSuffix(p, ".md") && !excluded[p] && !seen[p] && d.exists(p) {
			seen[p] = true
			out = append(out, p)
		}
	}
	for _, p := range d.tracked {
		add(p)
	}
	for p := range d.over {
		add(p)
	}
	sort.Strings(out)
	return out
}

func stripFrontMatter(text string) string {
	if !strings.HasPrefix(text, "---\n") {
		return text
	}
	if end := strings.Index(text[4:], "\n---\n"); end >= 0 {
		return text[4+end+5:]
	}
	return text
}

func internalTextGuard(d docTree) []string {
	var out []string
	for _, page := range publishedMarkdown(d) {
		text, err := d.read(page)
		if err != nil {
			out = append(out, err.Error())
			continue
		}
		for _, line := range strings.Split(stripFrontMatter(text), "\n") {
			line = internalTextAllowed.ReplaceAllString(line, "")
			for _, c := range internalTextClasses {
				if m := c.re.FindString(line); m != "" {
					out = append(out, fmt.Sprintf("%s: internal text (%s) %q in the published page", page, c.name, strings.TrimSpace(m)))
				}
			}
		}
	}
	return out
}

func TestPublishedPagesCarryNoInternalText(t *testing.T) {
	const page = "docs/operate/run/workers-and-jobs.md"
	plant := func(repl string) func(*testing.T, docTree) docTree {
		return func(t *testing.T, d docTree) docTree { return d.appended(t, page, "\n"+repl+"\n") }
	}
	assertGuard(t, internalTextGuard, []plantCase{
		{"a tracker id", plant("Fixed in CHAOS-1234."), "tracker id"},
		{"a cutover story", plant("After the cutover the worker runs in Go."), "cutover story"},
		{"a wave id", plant("Shipped in W3-2."), "wave or plan id"},
		{"a wave word", plant("Wave 4 adds the route."), "wave or plan id"},
		{"an agent lane", plant("The lane runs the check."), "lane"},
		{"a PR number", plant("See #5821 for details."), "PR number"},
		{"a ruling D", plant("Ruled in D6660."), "ruling D"},
		{"a ruling R", plant("Ruled in R123."), "ruling R"},
		{"a person name", plant("chris wants this."), "person name"},
		{"an internal host", plant("Runs on bigboy."), "internal host"},
		{"a host path", plant("Files are in /var/lib/oci-cache/x."), "internal path"},
		{"an agent name", plant("Ask cag first."), "agent name"},
		{"a trap number", plant("This is Trap #174."), "trap number"},
		{"a job number", plant("This is JOB 12."), "job number"},
		{"a new page", func(t *testing.T, d docTree) docTree { return d.added("docs/use/new-page.md", "# x\n\nCHAOS-99\n") }, "docs/use/new-page.md"},
		{"front matter is not scanned but the body is", func(t *testing.T, d docTree) docTree {
			return d.added("docs/use/fm.md", "---\nsummary: CHAOS-5\n---\n# x\n\nbody D1234\n")
		}, "ruling D"},
	})
}

func TestInternalTextAllowlistAndExclusion(t *testing.T) {
	d := realTree(t)
	ok := d.added("docs/use/ok.md", "# x\n\nUse `--profile go-cutover` and `DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER`; an &#35; entity.\n")
	for _, line := range internalTextGuard(ok) {
		if strings.Contains(line, "docs/use/ok.md") {
			t.Fatalf("an allowlisted name was reported: %s", line)
		}
	}
	bad := d.added("docs/use/bad.md", "# x\n\nThe go-cutover profile and a cutover story.\n")
	if got := strings.Join(internalTextGuard(bad), "\n"); !strings.Contains(got, "docs/use/bad.md") {
		t.Fatalf("a cutover story next to an allowlisted name was not reported: %q", got)
	}
	fm := d.added("docs/use/fm-only.md", "---\nsummary: CHAOS-5\nsource_of_truth:\n  - x (D1234)\n---\n# x\n\nclean body\n")
	for _, line := range internalTextGuard(fm) {
		if strings.Contains(line, "docs/use/fm-only.md") {
			t.Fatalf("front matter is not published text but was scanned: %s", line)
		}
	}
	excluded := d.added("mkdocs.yml", "docs_dir: docs\nexclude_docs: |\n  use/zz-excluded.md\n").added("docs/use/zz-excluded.md", "# x\n\nCHAOS-1\n")
	for _, line := range internalTextGuard(excluded) {
		if strings.Contains(line, "zz-excluded") {
			t.Fatalf("a page excluded from the site was scanned: %s", line)
		}
	}
}
