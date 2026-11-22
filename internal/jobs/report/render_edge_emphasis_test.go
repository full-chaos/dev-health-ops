package report

import (
	"bytes"
	"strings"
	"testing"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"
)

func gfmHTML(t *testing.T, markdown string) string {
	t.Helper()
	var out bytes.Buffer
	if err := goldmark.New(goldmark.WithExtensions(extension.GFM)).Convert([]byte(markdown), &out); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

var kept = []string{" ", " ", " ", "　", "  "}

// A kept Unicode edge of a value inside an emphasis span is written outside the delimiters, so a GFM reader still
// makes <strong>/<em> and shows the space. The web Markdown renderer itself is NOT pinned by these cells.
func TestKeptUnicodeEdgeKeepsEmphasisAndFooter(t *testing.T) {
	t.Parallel()
	type family struct {
		name    string
		set     func(*Plan, *ChartResult, string)
		open    string
		prefix  string
		closing string
	}
	families := []family{
		{"section prov id", func(p *Plan, _ *ChartResult, v string) { p.Sections = append(p.Sections, v) }, "<strong>", "narrative:", "</strong>"},
		{"chart prov id", func(_ *Plan, c *ChartResult, v string) { c.Spec.ChartID = v }, "<strong>", "chart:", "</strong>"},
		{"plan prov id", func(p *Plan, _ *ChartResult, v string) { p.PlanID = v }, "<strong>", "report:", "</strong>"},
		{"note", func(_ *Plan, c *ChartResult, v string) { c.Note = v }, "<em>", "Note: ", "</em>"},
		{"not applicable", func(_ *Plan, c *ChartResult, v string) { c.NotApplicable = v }, "<em>", "Not applicable: ", "</em>"},
	}
	for _, f := range families {
		for _, u := range kept {
			for _, edge := range []string{"lead", "trail", "both", "only"} {
				var value, want string
				switch edge {
				case "lead":
					value, want = u+"Release", f.open+f.prefix+u+"Release"+f.closing
				case "trail":
					value, want = "Release"+u, f.open+f.prefix+"Release"+f.closing+u
				case "both":
					value, want = u+"Release"+u, f.open+f.prefix+u+"Release"+f.closing+u
				default:
					value, want = u, f.open+strings.TrimRight(f.prefix, " ")+f.closing
					if f.open == "<strong>" {
						want = f.open + f.prefix + f.closing + u
					} else {
						want += " " + u
					}
				}
				if html := gfmHTML(t, renderInline(t, f.set, value)); !strings.Contains(html, want) {
					t.Errorf("%s/%s/%q: %q missing:\n%s", f.name, edge, value, want, html)
				}
			}
		}
	}
}

func TestWrapEmphasisMovesBothEdgesOutside(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"\u00a0a": "\u00a0**a**", "a\u202f": "**a**\u202f", "\u00a0a\u202f": "\u00a0**a**\u202f", "a b": "**a b**",
	} {
		if got := wrapEmphasis("**", in); got != want {
			t.Errorf("wrapEmphasis(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestKeptUnicodeEdgeKeepsFooterScope(t *testing.T) {
	t.Parallel()
	for _, s := range []struct {
		name string
		set  func(*Plan, *ChartResult, string)
		pre  string
	}{
		{"team", func(p *Plan, _ *ChartResult, v string) { p.ScopeTeams = []string{v} }, "teams="},
		{"repo", func(p *Plan, _ *ChartResult, v string) { p.ScopeRepos = []string{v} }, "repos="},
		{"service", func(p *Plan, _ *ChartResult, v string) { p.ScopeServices = []string{v} }, "services="},
	} {
		for _, u := range kept {
			value := "Release" + u
			md := renderInline(t, s.set, value)
			if !strings.HasSuffix(md, s.pre+value+"\n") {
				t.Errorf("%s scope %q: document end lost the trailing edge: %q", s.name, value, md[len(md)-40:])
			}
		}
	}
}
