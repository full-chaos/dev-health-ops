package investmentflow

import (
	"reflect"
	"testing"
)

// Reference values below were captured live against dev_health_ops.core.
// taxonomy's own functions (uncommitted one-off, .venv/bin/python3 -c
// "from dev_health_ops.core.taxonomy import format_theme_label, format_
// subcategory_label, normalize_theme_key, split_category_filters; ..."),
// not re-derived from reading the source a second time.

func TestFormatThemeLabel(t *testing.T) {
	cases := map[string]string{
		"feature_delivery": "Feature Delivery",
		"operational":      "Operational / Support",
		"maintenance":      "Maintenance / Tech Debt",
		"quality":          "Quality / Reliability",
		"risk":             "Risk / Security",
		"unknown_theme":    "Unknown Theme",
		"custom-name":      "Custom Name",
		"":                 "",
	}
	for in, want := range cases {
		if got := formatThemeLabel(in); got != want {
			t.Errorf("formatThemeLabel(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFormatSubcategoryLabel(t *testing.T) {
	cases := map[string]string{
		"feature_delivery.customer": "Feature Delivery · Customer",
		"maintenance.debt":          "Maintenance · Debt",
		"no_dot_value":              "No Dot Value",
		"risk.security":             "Risk · Security",
		"":                          "",
	}
	for in, want := range cases {
		if got := formatSubcategoryLabel(in); got != want {
			t.Errorf("formatSubcategoryLabel(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalizeThemeKey(t *testing.T) {
	cases := []struct {
		in      *string
		wantKey string
		wantOK  bool
	}{
		{nil, "", false},
		{strPtr(""), "", false},
		{strPtr("  "), "", false},
		{strPtr("Feature Delivery"), "feature_delivery", true},
		{strPtr("FEATURE_DELIVERY"), "feature_delivery", true},
		{strPtr("risk"), "risk", true},
		{strPtr("Risk / Security"), "risk", true},
		{strPtr("bogus"), "", false},
	}
	for _, c := range cases {
		key, ok := normalizeThemeKey(c.in)
		if key != c.wantKey || ok != c.wantOK {
			in := "nil"
			if c.in != nil {
				in = *c.in
			}
			t.Errorf("normalizeThemeKey(%q) = (%q, %v), want (%q, %v)", in, key, ok, c.wantKey, c.wantOK)
		}
	}
}

func TestSplitCategoryFilters(t *testing.T) {
	in := []string{"feature_delivery.customer", "maintenance", "feature_delivery.customer", "", "risk.security"}
	wantThemes := []string{"feature_delivery", "maintenance", "risk"}
	wantSubcategories := []string{"feature_delivery.customer", "risk.security"}

	gotThemes, gotSubcategories := splitCategoryFilters(in)
	if !reflect.DeepEqual(gotThemes, wantThemes) {
		t.Errorf("themes = %v, want %v", gotThemes, wantThemes)
	}
	if !reflect.DeepEqual(gotSubcategories, wantSubcategories) {
		t.Errorf("subcategories = %v, want %v", gotSubcategories, wantSubcategories)
	}
}
