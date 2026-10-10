// The theme labels of the Home tile "Investment by theme" (CHAOS-9195). The themes themselves
// are the canonical five of internal/jobs/workgraph/units (the vocabulary the Jev categorization
// writes); the legacy raw-key mapping (canonical_investment_theme_sql over investment_area) is
// removed with the rule classifier it served.
package home

// investmentThemeLabels ports _INVESTMENT_THEME_LABELS
// (api/queries/metrics.py:251-257) verbatim.
var investmentThemeLabels = map[string]string{
	"feature_delivery": "Feature Delivery",
	"operational":      "Operational / Support",
	"maintenance":      "Maintenance / Tech Debt",
	"quality":          "Quality / Reliability",
	"risk":             "Risk / Security",
}
