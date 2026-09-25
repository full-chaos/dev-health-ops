package goapiproof

import "net/url"

// The org-admin team routes that need a team: admin/teams/{team_id} and the two
// member-discovery routes, registered into the dho-api corpus by init below.
// Same rules as batch 1 (dhoapi_admin_access_corpus.go): the org-admin token on
// both legs, statuses from a live capture of BOTH planes on bigboy (ops
// a97b97ed, with and without an Origin header; status, body and headers
// identical on these rows; record
// _records/bigboy-a97b97ed/corpus-baseline-admin3), bigboy is the leg of record
// until the org-admin proof principal exists on prod.
//
// SAFETY: discover-members and infer-members are sent a team that does not
// exist ONLY. Python answers 404 for a missing team before it looks up any
// credential or calls any provider, and the Go plane must too. With a real team
// AND a credential in the org they reach a provider API (GitHub, GitLab, Jira,
// Linear), so no case with a real team is ever added here; a test pins that
// each has exactly the missing-team request and no id binding.
//
// HELD OUT until the timestamp render fix (a corpus row known red on main fails
// every STEP run): teams (list), identities (list) and admin/teams/{team_id}
// for a REAL team, whose bodies differ on the capture only by the datetime
// render (Go trims trailing zeros and appends Z, Python emits six digits without
// Z). The id of a real team is items[0].team_id of the list, NOT items[0].id.

var dhoAPIAdminTeamsRunOrder = []string{
	"REST:GET:/api/v1/admin/teams/{team_id}",
	"REST:GET:/api/v1/admin/teams/{team_id}/discover-members",
	"REST:GET:/api/v1/admin/teams/{team_id}/infer-members",
}

// withQuery sets the query string of an entry's first request.
func withQuery(spec RESTEndpointSpec, query url.Values) RESTEndpointSpec {
	spec.Requests[0].Query = query
	return spec
}

var dhoAPIAdminTeamsEndpointSpecs = map[string]RESTEndpointSpec{
	"REST:GET:/api/v1/admin/teams/{team_id}": adminGET("/api/v1/admin/teams/{team_id}", "missing", 404, map[string]string{"team_id": adminMissingID}),
	"REST:GET:/api/v1/admin/teams/{team_id}/discover-members": withQuery(
		adminGET("/api/v1/admin/teams/{team_id}/discover-members", "missing_team", 404, map[string]string{"team_id": adminMissingID}),
		url.Values{"provider": {"github"}}),
	"REST:GET:/api/v1/admin/teams/{team_id}/infer-members": adminGET("/api/v1/admin/teams/{team_id}/infer-members", "missing_team", 404, map[string]string{"team_id": adminMissingID}),
}

func init() {
	for operation, spec := range dhoAPIAdminTeamsEndpointSpecs {
		restEndpointSpecs[operation] = spec
	}
	restRunOrder = append(restRunOrder, dhoAPIAdminTeamsRunOrder...)
}
