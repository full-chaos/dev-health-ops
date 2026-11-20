package report

import (
	"strings"
	"testing"
)

func visibleHeadingCount(markdown, title string) int {
	count := 0
	for _, line := range strings.Split(markdown, "\n") {
		if strings.HasPrefix(line, "## ") && strings.TrimSpace(strings.TrimPrefix(line, "## ")) == title {
			count++
		}
	}
	return count
}

// A line break in a section title shows as a space, so the collision check folds it the way markdownText does.
func TestOtherChartsCollisionFoldsLineBreaks(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"Other\nCharts", "Other\r\nCharts", "Other\rCharts", " \tOther Charts\n"} {
		unlisted := cicdCharts("team")
		markdown := renderPlan(t, []string{"summary", name}, unlisted)
		if got := visibleHeadingCount(markdown, "Other Charts"); got != 1 {
			t.Errorf("section %q with a fallback: %d visible Other Charts headings, want 1:\n%s", name, got, markdown)
		}
		if !strings.Contains(markdown, "## Other Charts (plan section)\n") {
			t.Errorf("section %q was not renamed:\n%s", name, markdown)
		}
	}
}

// With no fallback section a custom name is not rewritten.
func TestOtherChartsNameKeptWithoutFallback(t *testing.T) {
	t.Parallel()
	covered := cicdCharts("team")
	markdown := renderPlan(t, []string{"summary", "other charts", "cicd"}, covered)
	if !strings.Contains(markdown, "\n## other charts\n") || strings.Contains(markdown, "(plan section)") {
		t.Errorf("name not kept as written:\n%s", markdown)
	}
	markdown = renderPlan(t, []string{"summary", "other charts"}, nil)
	if !strings.Contains(markdown, "\n## other charts\n") || strings.Contains(markdown, "(plan section)") {
		t.Errorf("name not kept as written with no charts:\n%s", markdown)
	}
}

// NBSP and narrow NBSP are shown by a GFM reader: edge trim drops only ASCII space, tab, CR, LF.
func TestEdgeTrimKeepsUnicodeSpace(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"\u00a0Release\u00a0", "\u202fRelease\u202f", "\u00a0", "\u202f", " \u00a0Release \u00a0\n"} {
		markdown := renderPlan(t, []string{"summary", name}, nil)
		want := strings.Trim(name, " \t\r\n")
		if !strings.Contains(markdown, "\n## "+want+"\n") {
			t.Errorf("name %q: heading edge %q not kept:\n%q", name, want, markdown)
		}
	}
	if got := trimMarkdownEdge("\u00a0 a \u202f"); got != "\u00a0 a \u202f" {
		t.Errorf("trimMarkdownEdge = %q", got)
	}
	if got := markdownText("\u00a0"); got != "\u00a0" {
		t.Errorf("NBSP-only markdownText = %q", got)
	}
}
