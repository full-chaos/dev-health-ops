// canonicalInvestmentThemeSQL ports canonical_investment_theme_sql
// (api/queries/metrics.py:260-273) -- the multiIf(...) expression
// mapping a raw investment_area/subcategory/leaf value to its canonical
// theme, case-insensitively. THEMES/SUBCATEGORIES/theme_of reuse
// internal/jobs/workgraph/units' own taxonomy port
// (dev_health_ops/investment_taxonomy.py) rather than a second copy of
// that registry, matching cmd/query-api/internal/investmentflow/
// taxonomy.go's own precedent.
package home

import (
	"fmt"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// investmentThemeLabels ports _INVESTMENT_THEME_LABELS
// (api/queries/metrics.py:251-257) verbatim.
var investmentThemeLabels = map[string]string{
	"feature_delivery": "Feature Delivery",
	"operational":      "Operational / Support",
	"maintenance":      "Maintenance / Tech Debt",
	"quality":          "Quality / Reliability",
	"risk":             "Risk / Security",
}

func canonicalInvestmentThemeSQL(column string) string {
	mappings := map[string]string{}
	for _, theme := range units.SortedThemes {
		mappings[theme] = theme
	}
	for _, sub := range units.SortedSubcategories {
		mappings[sub] = units.ThemeOf(sub)
	}
	for _, sub := range units.SortedSubcategories {
		parts := strings.Split(sub, ".")
		leaf := parts[len(parts)-1]
		if _, exists := mappings[leaf]; !exists {
			mappings[leaf] = units.ThemeOf(sub)
		}
	}
	keys := make([]string, 0, len(mappings))
	for k := range mappings {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	clauses := make([]string, 0, len(keys)*2)
	for _, k := range keys {
		clauses = append(clauses, fmt.Sprintf("lowerUTF8(%s) = '%s'", column, k))
		clauses = append(clauses, fmt.Sprintf("'%s'", mappings[k]))
	}
	return fmt.Sprintf("multiIf(%s, '')", strings.Join(clauses, ", "))
}
