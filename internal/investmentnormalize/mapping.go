// Package investmentnormalize is `dho backfill normalize-investment` (CHAOS-9206, D5939 / D5941): history
// that holds only the deprecated rule classifier's keys (investment_classifications_daily: investment_area,
// project_stream) is normalized by a DETERMINISTIC script to the canonical themes and sub-themes of the Jev
// taxonomy, and the result is kept in work_unit_investments with its provenance (categorization_status
// normalized_legacy). It is an ESTIMATE: low evidence quality, the rule that matched is named in the evidence,
// never presented as an LLM result. No LLM pass runs over history.
package investmentnormalize

import "strings"

// MappingVersion is stamped in the model version of every normalized row. Change it when a rule changes.
const MappingVersion = "mapping_v1"

// Provenance values.
const (
	Status       = "normalized_legacy"
	EffortMetric = "legacy_delivery_units"
	// QualityScore / QualityBand: a deterministic label with no validated evidence quote (named, low).
	QualityScore = 0.2
	QualityBand  = "low"
)

// specificMap is the mapping of every legacy (investment_area, project_stream) the rule file can emit, with a
// specific stream (D5939 table; the "needs chris" rows as he ruled them, D5941). product/general is not here:
// it means "no rule matched" and is ESTIMATED from the item's text (estimate.go).
var specificMap = map[string]map[string]string{
	"security": {
		"vulnerability": "risk.vulnerability", "compliance": "risk.compliance", "audit": "risk.compliance",
		"auth": "risk.security", "encryption": "risk.security", "access-control": "risk.security", "general": "risk.security",
	},
	"infra": {
		"k8s": "maintenance.upgrade", "terraform": "maintenance.upgrade", "network": "maintenance.upgrade",
		"database": "maintenance.upgrade", "platform": "maintenance.upgrade", "ci-cd": "maintenance.upgrade",
		"monitoring": "quality.reliability", "cost": "maintenance.debt",
	},
	"quality": {
		"testing": "quality.testing", "reliability": "quality.reliability",
		"performance": "quality.reliability", "automation": "quality.testing",
	},
	"docs": {
		"user-guide": "feature_delivery.enablement", "tutorial": "feature_delivery.enablement",
		"release-notes": "feature_delivery.enablement", "readme": "feature_delivery.enablement",
		"api-docs": "feature_delivery.enablement", "documentation": "feature_delivery.enablement",
	},
	"data": {
		"warehouse": "feature_delivery.enablement", "visualization": "feature_delivery.enablement",
		"schema": "feature_delivery.enablement", "pipeline": "feature_delivery.enablement",
		"etl": "feature_delivery.enablement", "analytics": "feature_delivery.enablement",
		"platform": "feature_delivery.enablement",
	},
	"product": {
		"feature": "feature_delivery.roadmap", "ux": "feature_delivery.roadmap", "onboarding": "feature_delivery.roadmap",
		"mobile": "feature_delivery.roadmap", "growth": "feature_delivery.roadmap", "monetization": "feature_delivery.roadmap",
		"api": "feature_delivery.enablement",
	},
}

// IsDefaultFallback is the legacy "no rule matched" key (product_default of investment_areas.yaml).
func IsDefaultFallback(area, stream string) bool { return area == "product" && stream == "general" }

// MapSpecific returns the subcategory of a legacy key with a specific stream. ok is false for a key the table
// does not hold (the script refuses it: no default bucket, never "unknown").
func MapSpecific(area, stream string) (subcategory string, ok bool) {
	streams, found := specificMap[area]
	if !found {
		return "", false
	}
	subcategory, ok = streams[stream]
	return subcategory, ok
}

// Theme is the theme of a subcategory key (the part before the dot).
func Theme(subcategory string) string {
	if i := strings.IndexByte(subcategory, '.'); i > 0 {
		return subcategory[:i]
	}
	return ""
}

// KnownKeys lists every (area, stream) of the table, for the census test against investment_areas.yaml.
func KnownKeys() [][2]string {
	var out [][2]string
	for area, streams := range specificMap {
		for stream := range streams {
			out = append(out, [2]string{area, stream})
		}
	}
	out = append(out, [2]string{"product", "general"})
	return out
}
