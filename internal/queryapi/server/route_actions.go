package server

// routeActions maps every protected query-api route to the catalog action a
// decision request for it names. The key is the profile row's "METHOD route
// @listeners", as endpoint-profiles.query-api.json spells its id.
var routeActions = map[string]string{
	"GET /api/v1/drilldown/issues @query,internal":                    "work_items.read",
	"GET /api/v1/drilldown/prs @query,internal":                       "git.read",
	"GET /api/v1/explain @query,internal":                             "metrics.read",
	"GET /api/v1/filters/options @query,internal":                     "analytics.read",
	"GET /api/v1/flame @query,internal":                               "metrics.read",
	"GET /api/v1/flame/aggregated @query,internal":                    "metrics.read",
	"GET /api/v1/heatmap @query,internal":                             "metrics.read",
	"GET /api/v1/home @query,internal":                                "analytics.read",
	"GET /api/v1/investment @query,internal":                          "analytics.read",
	"GET /api/v1/investment/sunburst @query,internal":                 "analytics.read",
	"GET /api/v1/opportunities @query,internal":                       "analytics.read",
	"GET /api/v1/people @query,internal":                              "metrics.read",
	"GET /api/v1/people/{person_id}/drilldown/issues @query,internal": "work_items.read",
	"GET /api/v1/people/{person_id}/drilldown/prs @query,internal":    "git.read",
	"GET /api/v1/people/{person_id}/metric @query,internal":           "metrics.read",
	"GET /api/v1/people/{person_id}/summary @query,internal":          "metrics.read",
	"GET /api/v1/quadrant @query,internal":                            "metrics.read",
	"GET /api/v1/sankey @query,internal":                              "analytics.read",
	"GET /api/v1/work-units @query,internal":                          "analytics.read",
	"GET /buildinfo @query,internal":                                  "org.read",
	"GET /graphql @query,internal":                                    "analytics.read",
	"POST /api/v1/drilldown/issues @query,internal":                   "work_items.read",
	"POST /api/v1/drilldown/prs @query,internal":                      "git.read",
	"POST /api/v1/explain @query,internal":                            "metrics.read",
	"POST /api/v1/home @query,internal":                               "analytics.read",
	"POST /api/v1/investment @query,internal":                         "analytics.read",
	"POST /api/v1/investment/explain @query,internal":                 "analytics.read",
	"POST /api/v1/opportunities @query,internal":                      "analytics.read",
	"POST /api/v1/sankey @query,internal":                             "analytics.read",
	"POST /api/v1/work-units @query,internal":                         "analytics.read",
	"POST /api/v1/work-units/{work_unit_id}/explain @query,internal":  "analytics.read",
	"POST /graphql @query,internal":                                   "analytics.read",
	"POST /query @mcp":                                                "analytics.read",
	"POST /query @query,internal":                                     "analytics.read",
	"POST /query/proof @query,internal":                               "analytics.read",
	"POST /query/proof-write @internal":                               "analytics.read",
	"POST /query/run-operation @internal":                             "analytics.read",
}

// RouteActionTable returns a copy of the route action table.
func RouteActionTable() map[string]string {
	out := make(map[string]string, len(routeActions))
	for key, action := range routeActions {
		out[key] = action
	}
	return out
}
