package teamsidentity

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// parseTeamImportRequest mirrors TeamImportRequest (schemas_flat.py:
// 597-599): `teams: list[DiscoveredTeam]` (required, each element itself
// validated against DiscoveredTeam's own required/optional fields,
// schemas_flat.py:578-585) and `on_conflict: str = Field(default="skip",
// pattern="^(skip|merge)$")`.
func parseTeamImportRequest(object *pyjson.Object) (teams []discoveredTeam, onConflict string, problems pybody.Errors) {
	rawTeams, ok := object.Get("teams")
	if !ok {
		problems = append(problems, pybody.Error{Type: "missing", Loc: []pyjson.Value{"body", "teams"}, Msg: "Field required", Input: object})
	} else {
		list, isList := rawTeams.([]pyjson.Value)
		if !isList {
			problems = append(problems, pybody.Error{Type: "list_type", Loc: []pyjson.Value{"body", "teams"}, Msg: "Input should be a valid list", Input: rawTeams})
		} else {
			for index, item := range list {
				loc := []pyjson.Value{"body", "teams", int64(index)}
				team, itemProblems := parseDiscoveredTeam(item, loc)
				problems = append(problems, itemProblems...)
				if len(itemProblems) == 0 {
					teams = append(teams, team)
				}
			}
		}
	}

	onConflict = "skip"
	if raw, ok := object.Get("on_conflict"); ok {
		text, isString := raw.(string)
		if !isString || (text != "skip" && text != "merge") {
			ctx := pyjson.NewObject()
			ctx.Set("pattern", "^(skip|merge)$")
			problems = append(problems, pybody.Error{
				Type: "string_pattern_mismatch", Loc: []pyjson.Value{"body", "on_conflict"},
				Msg: "String should match pattern '^(skip|merge)$'", Input: raw, Ctx: ctx,
			})
		} else {
			onConflict = text
		}
	}
	return teams, onConflict, problems
}

// parseDiscoveredTeam validates one DiscoveredTeam element: provider_type
// and provider_team_id and name are required non-empty strings;
// description is an optional nullable string; member_count is an
// optional nullable int; associations is an optional dict[str, Any]
// (default {}).
func parseDiscoveredTeam(raw pyjson.Value, loc []pyjson.Value) (discoveredTeam, pybody.Errors) {
	var problems pybody.Errors
	object, isObject := raw.(*pyjson.Object)
	if !isObject {
		problems = append(problems, pybody.Error{Type: "dict_type", Loc: loc, Msg: "Input should be a valid dictionary", Input: raw})
		return discoveredTeam{}, problems
	}
	var element pybody.Errors
	providerType, _ := element.RequiredString(object, "provider_type", 1, 0)
	providerTeamID, _ := element.RequiredString(object, "provider_team_id", 1, 0)
	name, _ := element.RequiredString(object, "name", 1, 0)
	description, hasDescription := element.OptionalString(object, "description", 0, 0)
	memberCountValue, hasMemberCount := element.OptionalLaxInt(object, "member_count")
	// associations: `dict[str, Any] = Field(default_factory=dict)` --
	// absent is {}, an explicit null (or any non-dict) is a dict_type
	// error, never silently replaced by {} (schemas_flat.py:578-585).
	associations := pyjson.NewObject()
	if nested, ok := element.DefaultedAnyDict(object, "associations"); ok {
		associations = nested
	}
	// The pybody helpers report loc ["body", <field>]; nest each under this
	// element's own loc ("body", "teams", <index>, <field>).
	for _, e := range element {
		e.Loc = append(append([]pyjson.Value(nil), loc...), e.Loc[1:]...)
		problems = append(problems, e)
	}
	if len(problems) > 0 {
		return discoveredTeam{}, problems
	}
	team := discoveredTeam{
		ProviderType: providerType, ProviderTeamID: providerTeamID, Name: name,
		Associations: associations,
	}
	if hasDescription {
		team.Description = &description
	}
	if hasMemberCount {
		// member_count is only echoed by the discover routes; an import
		// never reads it back, so a value past int64 (which pydantic
		// accepts) is kept as its low 64 bits rather than refused.
		memberCount := memberCountValue.Int64()
		team.MemberCount = &memberCount
	}
	return team, nil
}

// importTeamsJSON mirrors TeamImportResponse (schemas_flat.py:602-606).
func importTeamsJSON(imported, skipped, merged int, details []projectTeamResult) *pyjson.Object {
	detailValues := make([]pyjson.Value, len(details))
	for index, detail := range details {
		row := pyjson.NewObject()
		row.Set("team_id", detail.TeamID)
		row.Set("provider_team_id", detail.ProviderTeamID)
		row.Set("action", detail.Action)
		detailValues[index] = row
	}
	out := pyjson.NewObject()
	out.Set("imported", int64(imported))
	out.Set("skipped", int64(skipped))
	out.Set("merged", int64(merged))
	out.Set("details", detailValues)
	return out
}

// importTeams is POST /api/v1/admin/teams/import, dispatched from inside
// the {team_id} wildcard's own POST handler (see the route-table comment
// on Routes -- the same ServeMux-literal-vs-wildcard conflict CHAOS-6310
// r1 finding #7 hit for discover applies here identically: a literal
// registration for this path panics construction, so it lives inside the
// wildcard's own handler set instead).
func (h handlers) importTeams(w http.ResponseWriter, r *http.Request) {
	body := bodyFromContext(r.Context())
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	teams, onConflict, parseProblems := parseTeamImportRequest(object)
	if len(parseProblems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(parseProblems), nil)
		return
	}

	ctx := r.Context()
	orgID := orgIDOf(ctx)
	var (
		imported, skipped, merged int
		details                   []projectTeamResult
	)
	for _, team := range teams {
		result, err := h.store.projectTeam(ctx, orgID, team, onConflict)
		if err != nil {
			h.internal(w, r, "import teams", err)
			return
		}
		switch result.Action {
		case "imported":
			imported++
		case "skipped":
			skipped++
		case "merged":
			merged++
		}
		details = append(details, result)
	}
	policy.WriteModel(w, http.StatusOK, importTeamsJSON(imported, skipped, merged, details), nil)
}
