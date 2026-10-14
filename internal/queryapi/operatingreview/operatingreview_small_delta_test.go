package operatingreview

import (
	"strings"
	"testing"
)

func TestFormatDeltaAmount_SmallNumbers(t *testing.T) {
	for _, tc := range []struct {
		v    float64
		want string
	}{
		{0.04, "<0.1"},
		{-0.04, ">-0.1"},
		{0.1, "+0.1"},
		{-0.1, "-0.1"},
		{0.05, "+0.1"},
		{0, "+0.0"},
	} {
		if got := formatDeltaAmount(tc.v, "score"); got != tc.want {
			t.Errorf("formatDeltaAmount(%v) = %q, want %q", tc.v, got, tc.want)
		}
	}
}

func TestBuildMetric_StatusForSmallDelta(t *testing.T) {
	for _, tc := range []struct {
		name      string
		value     float64
		prior     float64
		direction string
		want      string
	}{
		{"+0.04 lower-is-better", 1.04, 1, lowerIsBetter, "unchanged"},
		{"-0.04 lower-is-better", 0.96, 1, lowerIsBetter, "unchanged"},
		{"+0.04 higher-is-better", 1.04, 1, higherIsBetter, "unchanged"},
		{"-0.04 higher-is-better", 0.96, 1, higherIsBetter, "unchanged"},
		{"+0.04 no direction", 1.04, 1, "", "unchanged"},
		{"+0.1 lower-is-better", 1.1, 1, lowerIsBetter, "worsened"},
		{"-0.1 lower-is-better", 0.9, 1, lowerIsBetter, "improved"},
		{"+0.1 higher-is-better", 1.1, 1, higherIsBetter, "improved"},
		{"-0.1 higher-is-better", 0.9, 1, higherIsBetter, "worsened"},
		{"0", 1, 1, lowerIsBetter, "unchanged"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := buildMetric("k", "L", tc.value, tc.prior, "score", tc.direction).delta.status; got != tc.want {
				t.Errorf("status = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSmallDeltaSentencesAndRecommendations(t *testing.T) {
	small := buildMetric("k", "Hotspot risk", 1.04, 1, "score", lowerIsBetter)
	big := buildMetric("j", "Ownership concentration", 1.1, 1, "ratio", lowerIsBetter)
	sec := buildSection("risk", "Risk", []reviewMetric{small, big})
	if len(sec.worsened) != 1 || sec.worsened[0] != "Ownership concentration worsened by +0.10 ratio" {
		t.Errorf("worsened = %q", sec.worsened)
	}
	recs := recommendationsFromSections([]reviewSection{sec})
	if len(recs) != 1 || recs[0] != "Review Ownership concentration: worsened by +0.10 ratio week-over-week." {
		t.Errorf("recommendations = %q", recs)
	}
	// A changed-status metric whose delta cannot be shown is forced to the
	// "<0.1" text by deltaSummary, never "+0.0".
	forced := small
	forced.delta.status = "worsened"
	if got := deltaSummary(forced); got != "Hotspot risk worsened by <0.1 score" {
		t.Errorf("deltaSummary = %q", got)
	}
	forced.delta.absolute = -0.04
	forced.delta.status = "improved"
	if got := deltaSummary(forced); got != "Hotspot risk improved by >-0.1 score" {
		t.Errorf("deltaSummary = %q", got)
	}
	forced.delta.status = "worsened"
	if r := recommendationsFromSections([]reviewSection{{metrics: []reviewMetric{forced}}}); len(r) != 1 || strings.Contains(r[0], "0.0") {
		t.Errorf("recommendation = %q", r)
	}
}

func TestRatioKeepsTwoDecimals(t *testing.T) {
	for _, tc := range []struct {
		v    float64
		want string
	}{
		{0.02, "+0.02"},
		{-0.02, "-0.02"},
		{0.004, "<0.01"},
		{-0.004, ">-0.01"},
		{0, "+0.00"},
	} {
		if got := formatDeltaAmount(tc.v, "ratio"); got != tc.want {
			t.Errorf("formatDeltaAmount(%v, ratio) = %q, want %q", tc.v, got, tc.want)
		}
	}
	for _, tc := range []struct {
		name         string
		value, prior float64
		want         string
	}{
		{"revert 0.10 to 0.12", 0.12, 0.10, "worsened"},
		{"revert 0.12 to 0.10", 0.10, 0.12, "improved"},
		{"0.004 move", 0.104, 0.10, "unchanged"},
	} {
		if got := buildMetric("k", "Revert rate", tc.value, tc.prior, "ratio", lowerIsBetter).delta.status; got != tc.want {
			t.Errorf("%s: status = %q, want %q", tc.name, got, tc.want)
		}
	}
	m := buildMetric("k", "Revert rate", 0.12, 0.10, "ratio", lowerIsBetter)
	if got := deltaSummary(m); got != "Revert rate worsened by +0.02 ratio" {
		t.Errorf("deltaSummary = %q", got)
	}
}
