package goapiproof

// The work unit team attribution requests are the ones the investment view
// sends: a sorted, de-duplicated list of work unit ids and an optional team
// id. The base request names a work unit that does not exist, so both planes
// answer an empty list. The variants prove the branches a request can reach:
// a team filter over unknown units, a blank team (no filter on both planes),
// one real work unit, and one real team over every unit.

// workUnitTeamAttributionsUnknownUnit is a work unit id no organisation holds.
const workUnitTeamAttributionsUnknownUnit = "wu-unknown-1"

func workUnitTeamAttributionsVariables(orgID string, _ Window) map[string]any {
	return map[string]any{"orgId": orgID, "workUnitIds": []any{workUnitTeamAttributionsUnknownUnit}, "teamId": nil}
}

func workUnitTeamAttributionsTeamVariant(name string, teamID string) Variant {
	return Variant{
		Name: name,
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "workUnitIds": []any{workUnitTeamAttributionsUnknownUnit}, "teamId": teamID}
		},
	}
}

// workUnitTeamAttributionsUnitVariant asks for one run-supplied work unit,
// measured only when the answer lists at least one attribution on a leg.
func workUnitTeamAttributionsUnitVariant(name, kind string) Variant {
	const list = "data.workUnitTeamAttributions"
	return Variant{
		Name: name,
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "workUnitIds": []any{}, "teamId": nil}
		},
		Parity: Options{RequireNonEmpty: []string{list}},
		Instance: &VariantInstance{
			Kind: kind,
			Bind: func(vars map[string]any, value string) { vars["workUnitIds"] = []any{value} },
			EchoFor: func(value string) []ScopeEcho {
				return []ScopeEcho{{List: list, Fields: []string{"workUnitId"}, Value: value}}
			},
		},
	}
}

// workUnitTeamAttributionsTeamOwnedVariant asks for every unit one
// run-supplied team owns, measured only when the answer lists at least one
// attribution on a leg.
func workUnitTeamAttributionsTeamOwnedVariant(name, kind string) Variant {
	const list = "data.workUnitTeamAttributions"
	return Variant{
		Name: name,
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "workUnitIds": []any{}, "teamId": nil}
		},
		Parity: Options{RequireNonEmpty: []string{list}},
		Instance: &VariantInstance{
			Kind: kind,
			Bind: func(vars map[string]any, value string) { vars["teamId"] = value },
			EchoFor: func(value string) []ScopeEcho {
				return []ScopeEcho{{List: list, Fields: []string{"teamId"}, Value: value}}
			},
		},
	}
}
