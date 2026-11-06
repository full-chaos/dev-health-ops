package pdp

import (
	"sort"

	"github.com/full-chaos/dev-health-go/pep"
)

// The route-class registry is the one gate point of an allowlist decision (CHAOS-13082 Q9). Each REST row has a
// class, the label its handler passes to authenticateRESTRequest. A class is N (narrowable: every response read is
// keyed by repository) or M (mixed: some response read has no repository column; the whole route stays M).
const (
	KindNarrowable = "N"
	KindMixed      = "M"
)

// ReasonAllowlistRouteUnsupported is the 503 of an allowlist decision on an M route.
const ReasonAllowlistRouteUnsupported pep.Reason = "allowlist_route_unsupported"

// routeClassKind is every class and its kind.
var routeClassKind = map[string]string{
	"heatmap": KindNarrowable, "investment": KindNarrowable, "investment_sunburst": KindNarrowable,
	"investment_explain": KindNarrowable, "drilldown_prs": KindNarrowable, "work_units": KindNarrowable,
	"work_unit_explain": KindNarrowable, "people_drilldown_prs": KindNarrowable,
	"drilldown_issues": KindNarrowable, "people_drilldown_issues": KindNarrowable, "flame": KindNarrowable,
	"home": KindNarrowable, "opportunities": KindNarrowable,
	"quadrant": KindNarrowable, "people": KindNarrowable, "people_summary": KindNarrowable, "people_metric": KindNarrowable,
	"sankey": KindNarrowable, "filteroptions": KindNarrowable, "explain": KindNarrowable, "flame_aggregated": KindNarrowable,
}

// routeClassOf maps a row profile ("METHOD /route", as the server builds it) to its class.
var routeClassOf = map[string]string{
	"GET /api/v1/quadrant":                            "quadrant",
	"GET /api/v1/heatmap":                             "heatmap",
	"GET /api/v1/sankey":                              "sankey",
	"POST /api/v1/sankey":                             "sankey",
	"GET /api/v1/home":                                "home",
	"POST /api/v1/home":                               "home",
	"GET /api/v1/opportunities":                       "opportunities",
	"POST /api/v1/opportunities":                      "opportunities",
	"GET /api/v1/filters/options":                     "filteroptions",
	"GET /api/v1/investment":                          "investment",
	"POST /api/v1/investment":                         "investment",
	"GET /api/v1/investment/sunburst":                 "investment_sunburst",
	"POST /api/v1/investment/explain":                 "investment_explain",
	"GET /api/v1/drilldown/prs":                       "drilldown_prs",
	"POST /api/v1/drilldown/prs":                      "drilldown_prs",
	"GET /api/v1/drilldown/issues":                    "drilldown_issues",
	"POST /api/v1/drilldown/issues":                   "drilldown_issues",
	"GET /api/v1/work-units":                          "work_units",
	"POST /api/v1/work-units":                         "work_units",
	"POST /api/v1/work-units/{work_unit_id}/explain":  "work_unit_explain",
	"GET /api/v1/people":                              "people",
	"GET /api/v1/people/{person_id}/summary":          "people_summary",
	"GET /api/v1/people/{person_id}/metric":           "people_metric",
	"GET /api/v1/people/{person_id}/drilldown/prs":    "people_drilldown_prs",
	"GET /api/v1/people/{person_id}/drilldown/issues": "people_drilldown_issues",
	"GET /api/v1/explain":                             "explain",
	"POST /api/v1/explain":                            "explain",
	"GET /api/v1/flame":                               "flame",
	"GET /api/v1/flame/aggregated":                    "flame_aggregated",
}

// allowlistNarrowed is the classes whose handlers narrow every read to a repository ceiling: the eight narrowable
// classes of CHAOS-13090 and the item-grain classes of CHAOS-13085, whose work-item reads are narrowed by the link of
// an item to an allowed repository (internal/queryapi/linkscope). CHAOS-13083 adds a mixed class here only with its
// code and its census entry, and moves it to narrowable in the same change.
var allowlistNarrowed = map[string]bool{
	"heatmap": true, "investment": true, "investment_sunburst": true, "investment_explain": true,
	"drilldown_prs": true, "work_units": true, "work_unit_explain": true, "people_drilldown_prs": true,
	"drilldown_issues": true, "people_drilldown_issues": true, "flame": true,
	"home": true, "opportunities": true,
	"quadrant": true, "people": true, "people_summary": true, "people_metric": true,
	"sankey": true, "filteroptions": true, "explain": true, "flame_aggregated": true,
}

// allowlistCutOver is the cut-over registry: the row profiles ("METHOD /route") whose class is narrowed. An
// allowlist decision on a route not listed is not carried out (503), so a caller with a repository ceiling never
// reaches an unfiltered read.
var allowlistCutOver = deriveCutOver()

func deriveCutOver() map[string]bool {
	cut := map[string]bool{}
	for profile, class := range routeClassOf {
		if allowlistNarrowed[class] {
			cut[profile] = true
		}
	}
	return cut
}

// RouteClassOf is the class of a row profile.
func RouteClassOf(profile string) (string, bool) {
	class, ok := routeClassOf[profile]
	return class, ok
}

// ClassKind is KindNarrowable or KindMixed.
func ClassKind(class string) (string, bool) {
	kind, ok := routeClassKind[class]
	return kind, ok
}

// RouteClassNames is every class, sorted.
func RouteClassNames() []string {
	names := make([]string, 0, len(routeClassKind))
	for class := range routeClassKind {
		names = append(names, class)
	}
	sort.Strings(names)
	return names
}

// AllowlistNarrowed reports whether the class narrows every read to a repository ceiling.
func AllowlistNarrowed(class string) bool { return allowlistNarrowed[class] }

// LocalReasons is the closed set of reasons of a 503 the gate makes itself.
func LocalReasons() []pep.Reason {
	return []pep.Reason{ReasonNoDecisionPoint, ReasonNotCarriedOut, ReasonTenantMismatch, ReasonNoRepositoryRule,
		ReasonEnforceCellOff, ReasonAllowlistRouteUnsupported}
}

// allowlistRefusal is the reason of an allowlist decision on a route that does not carry it, and the route's class.
func allowlistRefusal(profile string) (pep.Reason, string) {
	class, known := routeClassOf[profile]
	if known && routeClassKind[class] == KindMixed {
		return ReasonAllowlistRouteUnsupported, class
	}
	return ReasonNotCarriedOut, class
}
