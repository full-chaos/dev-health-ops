package goapiproof

// releaseImpactVariables is the release impact page's request for every
// release: an empty node id, the RELEASE source type and the page limit.
func releaseImpactVariables(orgID string, _ Window) map[string]any {
	return map[string]any{"orgId": orgID, "filters": map[string]any{"nodeId": "", "sourceType": "RELEASE", "limit": 200}}
}

func releaseImpactVariant(name string, filters map[string]any) Variant {
	return Variant{
		Name: name,
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "filters": filters}
		},
	}
}

// The base request and LIMIT_ONE send the RELEASE source type the page sends.
// A production organisation can hold no release node at all, in which case both
// legs answer an empty edge list and those two requests compare nothing; the
// populated requests below carry the non-empty comparisons, through the same
// document, because the whole filter set is one document variable.

// releaseImpactNodeVariant filters edges by a run-supplied node id (the source
// or target of at least one edge), without a source type, measured only when
// the answer lists at least one edge on a leg.
func releaseImpactNodeVariant(name, kind string) Variant {
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

// releaseImpactSourceTypeVariant filters edges by a run-supplied source type
// that has edges (for example PR), measured only when the answer lists at
// least one edge on a leg and every listed edge has that source type.
func releaseImpactSourceTypeVariant(name, kind string) Variant {
	return Variant{
		Name: name,
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "filters": map[string]any{"nodeId": "", "sourceType": "PR", "limit": 50}}
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
