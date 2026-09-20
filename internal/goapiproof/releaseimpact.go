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

// releaseImpactNodeVariant filters release edges by a run-supplied node id,
// measured only when the answer lists at least one edge on a leg.
func releaseImpactNodeVariant(name, kind string) Variant {
	return Variant{
		Name: name,
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "filters": map[string]any{"nodeId": "", "sourceType": "RELEASE", "limit": 200}}
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
