package goapiproof

// workGraphEdgesVariants are the workGraphEdges document's comparing cases on
// pages the request limit does not cut. The base request is one page cut at the
// default limit, where a duplicate-collapsing difference is absorbed by the
// per-edge shape (CHAOS-5791); these two requests fall under the whole-list
// declaration (CHAOS-6114), which admits a length or count difference only when
// the candidate's ids equal the baseline's distinct ids, so a dropped or
// invented edge cannot hide. Both are measured only when a leg lists at least
// one edge, and both must echo the supplied value on every listed edge.

// workGraphEdgesSourceTypeSmallVariant filters edges by a run-supplied source
// type whose whole edge set is below the page limit (production: a feature
// flag node type with 9 edges, an incident type with 6).
func workGraphEdgesSourceTypeSmallVariant(name, kind string) Variant {
	return Variant{
		Name: name,
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "filters": map[string]any{"sourceType": "FEATURE_FLAG", "limit": 200}}
		},
		Parity: Options{
			BaselineDefects: workGraphEdgesParity.BaselineDefects,
			RequireNonEmpty: []string{"data.workGraphEdges.edges"},
		},
		Instance: &VariantInstance{
			Kind: kind,
			Bind: func(vars map[string]any, value string) {
				vars["filters"].(map[string]any)["sourceType"] = value
			},
			EchoFor: func(value string) []ScopeEcho {
				return []ScopeEcho{{List: "data.workGraphEdges.edges", Fields: []string{"sourceType"}, Value: value}}
			},
		},
	}
}

// workGraphEdgesNodeVariant filters edges by a run-supplied node id that is the
// source or target of edges, on a page below the limit.
func workGraphEdgesNodeVariant(name, kind string) Variant {
	return Variant{
		Name: name,
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "filters": map[string]any{"nodeId": "", "limit": 200}}
		},
		Parity: Options{
			BaselineDefects: workGraphEdgesParity.BaselineDefects,
			RequireNonEmpty: []string{"data.workGraphEdges.edges"},
		},
		Instance: &VariantInstance{
			Kind: kind,
			Bind: func(vars map[string]any, value string) {
				vars["filters"].(map[string]any)["nodeId"] = value
			},
			EchoFor: func(value string) []ScopeEcho {
				return []ScopeEcho{{List: "data.workGraphEdges.edges", Fields: []string{"sourceId", "targetId"}, Value: value}}
			},
		},
	}
}
