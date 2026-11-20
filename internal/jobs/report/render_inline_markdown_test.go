package report

import (
	"context"
	"html"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/text"
)

// shape parses a report with a GFM parser (the web renderer is GFM) and returns the multiset of node kinds and the
// visible text with whitespace removed.
func shape(t *testing.T, markdown string) (string, string) {
	t.Helper()
	source := []byte(markdown)
	doc := goldmark.New(goldmark.WithExtensions(extension.GFM)).Parser().Parse(text.NewReader(source))
	kinds := map[string]int{}
	var visible strings.Builder
	_ = ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			if n.Type() == ast.TypeBlock {
				visible.WriteByte('\n')
			}
			return ast.WalkContinue, nil
		}
		if n.Kind() != ast.KindText && n.Kind() != ast.KindString {
			kinds[n.Kind().String()]++
		}
		switch v := n.(type) {
		case *ast.Text:
			visible.WriteString(decodeInline(string(v.Segment.Value(source))))
			if v.SoftLineBreak() || v.HardLineBreak() {
				visible.WriteByte(' ')
			}
		case *ast.String:
			visible.Write(v.Value)
		}
		return ast.WalkContinue, nil
	})
	var names []string
	for k, n := range kinds {
		names = append(names, k+"="+strings.Repeat("x", n))
	}
	sort.Strings(names)
	return strings.Join(names, ","), strings.Join(strings.Fields(visible.String()), "")
}

// inlineSite is one free-text field; forms are the shapes the value takes in the report (a section name shows as a
// title-cased heading and as the raw provenance id), marker a value that survives each form.
var entityPattern = regexp.MustCompile(`^&(#[0-9]{1,7}|#[xX][0-9a-fA-F]{1,6}|[A-Za-z][A-Za-z0-9]*);`)

// decodeInline reads text as a CommonMark reader does: a backslash before ASCII punctuation is that character, and an
// entity is the character it names.
func decodeInline(raw string) string {
	var b strings.Builder
	for i := 0; i < len(raw); i++ {
		if raw[i] == '\\' && i+1 < len(raw) && strings.IndexByte("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~", raw[i+1]) >= 0 {
			i++
			b.WriteByte(raw[i])
		} else if m := entityPattern.FindString(raw[i:]); raw[i] == '&' && m != "" {
			b.WriteString(html.UnescapeString(m))
			i += len(m) - 1
		} else {
			b.WriteByte(raw[i])
		}
	}
	return b.String()
}

type inlineSite struct {
	name   string
	marker string
	set    func(plan *Plan, chart *ChartResult, value string)
	forms  []func(value string) string
}

func plainFold(value string) string { return strings.TrimSpace(markdownLineBreaks.Replace(value)) }
func titled(value string) string    { return titleCase(plainFold(value)) }

func inlineSites() []inlineSite {
	raw := []func(string) string{plainFold}
	title := []func(string) string{titled}
	both := []func(string) string{titled, plainFold}
	return []inlineSite{
		{"report type", "Mark0", func(p *Plan, _ *ChartResult, v string) { p.ReportType = v }, title},
		{"window", "Mark0", func(p *Plan, _ *ChartResult, v string) { p.TimeRangeStart, p.TimeRangeEnd = v, v }, raw},
		{"section", "mark0", func(p *Plan, _ *ChartResult, v string) { p.Sections = append(p.Sections, v) }, both},
		{"chart title", "Mark0", func(_ *Plan, c *ChartResult, v string) { c.Title = v }, raw},
		{"not applicable", "Mark0", func(_ *Plan, c *ChartResult, v string) { c.NotApplicable = v }, raw},
		{"note", "Mark0", func(_ *Plan, c *ChartResult, v string) { c.Note = v }, raw},
		{"uncharted label", "Mark0", func(p *Plan, _ *ChartResult, v string) {
			p.UnchartedMetrics = []UnchartedMetric{{Label: v, Reason: "why"}}
		}, raw},
		{"uncharted reason", "Mark0", func(p *Plan, _ *ChartResult, v string) {
			p.UnchartedMetrics = []UnchartedMetric{{Label: "lbl", Reason: v}}
		}, raw},
		{"provenance artifact id", "Mark0", func(_ *Plan, c *ChartResult, v string) { c.Spec.ChartID = v }, raw},
		{"provenance source", "Mark0", func(_ *Plan, c *ChartResult, v string) { c.SourceTable = v }, raw},
		{"metric name", "mark0", func(_ *Plan, c *ChartResult, v string) { c.Spec.Metric = v }, both},
		{"team scope", "Mark0", func(p *Plan, _ *ChartResult, v string) { p.ScopeTeams = []string{v} }, raw},
		{"repo scope", "Mark0", func(p *Plan, _ *ChartResult, v string) { p.ScopeRepos = []string{v} }, raw},
		{"service scope", "Mark0", func(p *Plan, _ *ChartResult, v string) { p.ScopeServices = []string{v} }, raw},
		{"footer confidence", "Mark0", func(p *Plan, _ *ChartResult, v string) { p.ConfidenceThreshold = v }, raw},
		{"table x", "Mark0", func(_ *Plan, c *ChartResult, v string) { c.DataPoints[0].X = v }, raw},
		{"table group", "Mark0", func(_ *Plan, c *ChartResult, v string) { c.DataPoints[0].Group = v }, raw},
	}
}

func renderInline(t *testing.T, set func(*Plan, *ChartResult, string), value string) string {
	t.Helper()
	plan := Plan{
		PlanID: "plan-1", ReportType: "weekly_health", Audience: "team_lead",
		TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-07",
		Sections: []string{"summary", "quality"}, ConfidenceThreshold: "direct_fact",
		CreatedAt: time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC), OrganizationID: "org-1",
		UnchartedMetrics: []UnchartedMetric{{Label: "lbl", Reason: "why"}},
	}
	chart := ChartResult{
		Spec: ChartSpec{
			ChartID: "chart-1", PlanID: "plan-1", ChartType: "bar", Metric: "failure_rate",
			GroupBy: "team", Title: "Failure rate", OrganizationID: "org-1",
		},
		DataPoints: []DataPoint{{X: "2026-01-01", Y: 0.5, Group: "team-a"}},
		Title:      "Failure rate", SourceTable: "t", Unit: "ratio", Note: "n",
	}
	set(&plan, &chart, value)
	artifact, err := NewDeterministicRenderer().Render(context.Background(), QueryResult{
		Plan: plan, Charts: []ChartResult{chart}, Metadata: map[string]string{"renderer_version": "reports.v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return artifact.Markdown
}

// A free-text value shows as written: a GFM parser reads the report with the same node kinds as one holding a plain
// word, and the visible text is the value (line breaks as spaces).
func TestRenderFreeTextShowsAsWritten(t *testing.T) {
	const marker = "Mark0"
	values := []string{
		"*x*", "**x**", "_x_", "__x__", "a *b* c", "a _b_ c", "`code`", "``a`b``", "[a](b)", "[a]: b", "![i](u)", "<b>x</b>", "<!-- c -->",
		"~~gone~~", "~x~", "&amp;", "&#35;", "&copy;", `a\*b`, `\`, `end\`, "# h", "## Provenance", "- a", "+ a", "1. a", "> q", "=== ", "---", "***",
		"a_b_c", "snake_case_name", "_a", "a_", "a _ b", "a\nb", "a\r\nb", "a\rb", "x\n## Provenance\ny", " lead", "trail ", "|", "a | b",
		"```", "~~~", "    code", "\ttab",
		"https://example.com", "HTTPS://EXAMPLE.COM", "http://a.b/c", "ftp://a.b", "www.example.com", "WWW.EXAMPLE.COM", "a@b.co", "mailto:a@b.co",
		"see https://x.co now", "a @ b", "@handle", "50% @ noon", "user@host", "x@", "https://", "www.", "a.b@c",
	}
	rng := rand.New(rand.NewSource(14274))
	alphabet := []string{"\\", "`", "*", "_", "[", "]", "(", ")", "<", ">", "!", "|", "~", "&", "#", "-", "+", "=", ".", "/", ";", `"`, "'",
		"a", "b", "x", "1", "2", " ", "\t", "\n", "\r", "&amp;", "<b>", "[a](b)", "![i](u)", "~~", "***", "`x`", "## Provenance", "_x_", "1. ", "https://x.co", "HTTP://X.CO", "www.x.co", "WWW.X.CO", "ftp://x.co", "a@b.co", "mailto:", "@", ":", "/", "w", ".co"}
	for len(values) < 1600 {
		var b strings.Builder
		for n := 1 + rng.Intn(10); n > 0; n-- {
			b.WriteString(alphabet[rng.Intn(len(alphabet))])
		}
		values = append(values, b.String())
	}
	for _, site := range inlineSites() {
		t.Run(site.name, func(t *testing.T) {
			baseKinds, baseText := shape(t, renderInline(t, site.set, site.marker))
			failures := 0
			for _, value := range values {
				markdown := renderInline(t, site.set, value)
				kinds, visible := shape(t, markdown)
				wantText := baseText
				for _, form := range site.forms {
					wantText = strings.ReplaceAll(wantText, strings.Join(strings.Fields(form(site.marker)), ""), strings.Join(strings.Fields(form(value)), ""))
				}
				if kinds != baseKinds || visible != wantText {
					failures++
					if failures <= 3 {
						t.Errorf("value %q does not show as written\nkinds %s\nwant  %s\nvisible %q\nwant    %q\n%s", value, kinds, baseKinds, visible, wantText, markdown)
					}
				}
			}
			if failures > 3 {
				t.Errorf("%d more values fail", failures-3)
			}
		})
	}
}

// An "_" inside a word stays plain, any other "_" and every "*" is escaped.
func TestMarkdownTextUnderscoreRule(t *testing.T) {
	for value, want := range map[string]string{
		"testops_pipeline_metrics_daily": "testops_pipeline_metrics_daily", "a_b": "a_b", "_a": `\_a`, "a_": `a\_`, "a _ b": `a \_ b`,
		"a_*b": `a\_\*b`, "a*b": `a\*b`, "é_a": `é\_a`, "_": `\_`,
	} {
		if got := markdownText(value); got != want {
			t.Errorf("markdownText(%q) = %q, want %q", value, got, want)
		}
	}
}

// Only the exact name collides with the fallback heading: a literal "#" suffix is part of the title, not a closing
// sequence, so such a section keeps its name.
func TestOtherChartsCollisionIsExactName(t *testing.T) {
	for name, want := range map[string]string{
		"Other Charts#": `## Other Charts\#` + "\n", "Other Charts ###": `## Other Charts \#\#\#` + "\n", "Other Charts #1": `## Other Charts \#1` + "\n",
		"Other Charts": "## Other Charts (plan section)\n", "other charts": "## Other Charts (plan section)\n",
	} {
		markdown := renderInline(t, func(p *Plan, _ *ChartResult, v string) { p.Sections = append(p.Sections, v) }, name)
		if !strings.Contains(markdown, "\n"+want) {
			t.Errorf("section %q: heading %q missing:\n%s", name, want, markdown)
		}
		if name != "Other Charts" && name != "other charts" && strings.Contains(markdown, "(plan section)") {
			t.Errorf("section %q was renamed:\n%s", name, markdown)
		}
	}
}

// A heading, list-line, wrapped or footer value shows trimmed of edge space and tabs (and a trailing line break), not as
// the raw value. A table cell keeps its edge space: the GFM table drops it.
func TestRenderFreeTextShowsEdgeTrimmed(t *testing.T) {
	const padded = " \tReview \t\r\n"
	want := map[string]string{
		"report type": "\n# Review Report — ", "window": "— Review → Review\n", "section": "\n## Review\n", "chart title": "\n#### Review\n",
		"not applicable": "_Not applicable: Review_", "note": "_Note: Review_", "uncharted label": "\n- Review: why\n", "uncharted reason": "\n- lbl: Review\n",
		"provenance artifact id": "**chart:Review** —", "provenance source": "sources: Review; metrics", "metric name": "metrics: Review\n",
		"team scope": "Scope: teams=Review\n", "repo scope": "Scope: repos=Review\n", "service scope": "Scope: services=Review\n",
		"footer confidence": "Confidence: Review |",
	}
	for _, site := range inlineSites() {
		fragment, ok := want[site.name]
		if !ok {
			continue
		}
		if markdown := "\n" + renderInline(t, site.set, padded); !strings.Contains(markdown, fragment) {
			t.Errorf("%s: %q missing in:\n%s", site.name, fragment, markdown)
		}
	}
}

// The title-cased fields (custom report type, section name, fallback metric name) show title-case(value): the escaper
// makes that text literal, and the existing title-case rule (lower case, "_" as a space) still applies first.
func TestRenderTitleCasedFieldsShowTitleCaseOfTheValue(t *testing.T) {
	const value = "Release _Candidate_ *x*"
	want := map[string]string{
		"report type": "\n# Release Candidate \\*x\\* Report — ", "section": "\n## Release Candidate \\*x\\*\n", "metric name": "Release Candidate \\*x\\* appears near",
	}
	for _, site := range inlineSites() {
		fragment, ok := want[site.name]
		if !ok {
			continue
		}
		if markdown := "\n" + renderInline(t, site.set, value); !strings.Contains(markdown, fragment) {
			t.Errorf("%s: %q missing in:\n%s", site.name, fragment, markdown)
		}
	}
}

// The collision check reads the title as a reader does: edge space is dropped, case is ignored, a "#" suffix is text.
func TestOtherChartsCollisionIgnoresEdgeSpace(t *testing.T) {
	plan := Plan{PlanID: "plan-1", ReportType: "weekly_health", CreatedAt: time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC)}
	chart := ChartResult{Spec: ChartSpec{ChartID: "c1", Metric: "failure_rate", GroupBy: "team"}, Title: "Failure rate", Unit: "ratio",
		DataPoints: []DataPoint{{X: "2026-01-01", Y: 0.5}}}
	for title, renamed := range map[string]bool{" Other Charts\t": true, "other charts ": true, "Other Charts#": false} {
		markdown := renderMarkdown(plan, []ChartResult{chart}, []narrative{{sectionType: "x", title: title}}, nil)
		if got := strings.Contains(markdown, "## Other Charts (plan section)\n"); got != renamed {
			t.Errorf("title %q renamed = %v, want %v:\n%s", title, got, renamed, markdown)
		}
		if n := strings.Count(markdown, "\n## Other Charts\n"); n != 1 {
			t.Errorf("title %q: %d plain Other Charts headings, want 1 (the fallback):\n%s", title, n, markdown)
		}
	}
}

// A value that only looks like a URL, a host or an address (no GFM autolink would form) shows unchanged: the autolink
// escape does not turn ordinary text into noise, and every "@" is escaped without guessing what an address looks like.
func TestAutolinkEscapeKeepsOrdinaryTextAndEscapesEveryAt(t *testing.T) {
	for value, want := range map[string]string{
		"http": "http", "www": "www", "a.b": "a.b", "x://": "x://", "a:b": "a:b", "https:/x": "https:/x", "user at b.co": "user at b.co",
		"w.w.w.x": "w.w.w.x", "https://x.co": `https\://x.co`, "HTTP://X.CO": `HTTP\://X.CO`, "ftp://a.b": `ftp\://a.b`,
		"www.x.co": `www\.x.co`, "WWW.x": `WWW\.x`, "a@b.co": `a\@b.co`, "a @ b": `a \@ b`, "@": `\@`, `www\.x`: `www\\.x`, `\@`: `\\\@`,
	} {
		if got := markdownText(value); got != want {
			t.Errorf("markdownText(%q) = %q, want %q", value, got, want)
		}
		if got := markdownTableCell(value); got != strings.ReplaceAll(want, "|", `\|`) {
			t.Errorf("markdownTableCell(%q) = %q, want %q", value, got, want)
		}
	}
}

// A custom report type and a custom section name are title-cased, then escaped: a URL, a www host or an address in
// them shows as text (the heading reads title-case(value), and no autolink character is left bare).
func TestCustomNamesNeverAutolink(t *testing.T) {
	for value, want := range map[string]string{
		"https://example.com": `Https\://example.com`, "HTTPS://EXAMPLE.COM": `Https\://example.com`, "www.example.com": `Www\.example.com`,
		"a@b.co": `A\@b.co`, "see www.x.co now": `See Www\.x.co Now`, "ftp://a.b": `Ftp\://a.b`, "mailto:a@b.co": `Mailto:a\@b.co`,
	} {
		for site, fragment := range map[string]string{"report type": "\n# " + want + " Report — ", "section": "\n## " + want + "\n"} {
			var set func(*Plan, *ChartResult, string)
			for _, s := range inlineSites() {
				if s.name == site {
					set = s.set
				}
			}
			markdown := "\n" + renderInline(t, set, value)
			if !strings.Contains(markdown, fragment) {
				t.Errorf("%s %q: %q missing in:\n%s", site, value, fragment, markdown)
			}
			doc := goldmark.New(goldmark.WithExtensions(extension.GFM)).Parser().Parse(text.NewReader([]byte(markdown)))
			_ = ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
				if entering && (n.Kind() == ast.KindAutoLink || n.Kind() == ast.KindLink) {
					t.Errorf("%s %q renders a link:\n%s", site, value, markdown)
				}
				return ast.WalkContinue, nil
			})
		}
	}
}
