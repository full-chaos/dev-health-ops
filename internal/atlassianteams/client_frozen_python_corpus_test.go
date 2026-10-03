package atlassianteams

import (
	"fmt"

	"atlassian/atlassian/graph/gen"
)

// The scenarios below widen the frozen-Python oracle from the happy reads to
// every branch of the client the sync leans on: the call arguments, the HTTP
// answers a gateway can give (status, body, a hung-up connection, a rate
// limit), the page shapes, and the rows the two mappers accept or refuse. Each
// scenario is one fake-gateway tenant; both clients read it and the outcome,
// its error family, the requests sent and the waits asked for are compared.

type column2 = gen.GraphStoreCypherQueryV2Column

func ptr[T any](v T) *T { return &v }

// typedNode is an ARI node that carries its __typename in data, as the
// gateway sends it when the query selects the type.
func typedNode(id, typename string) *gen.GraphStoreCypherQueryV2Value {
	return cypherNode(id, typename, &gen.GraphStoreCypherQueryV2AriNodeData{})
}

// miniScenario is one team with one member and one project.
func miniScenario(name string, strict bool) (scenario, string) {
	t := teamARI(name, "1")
	return scenario{name: name, strict: strict, teamIDs: []string{t},
		teams: [][]byte{teamPage([]gen.TeamNode{{ID: t, DisplayName: str("Mini"), State: str("ACTIVE")}}, false, nil, false)},
		graph: map[string][][]byte{
			usersField + " " + t: {graphPage(usersField, [][]column2{{
				column("user", cypherNode(userARI("acc-1"), "AtlassianAccountUser", nil)), column("team", cypherNode(t, "TeamV2", nil))}}, false, nil, false)},
			projectsField + " " + t: {graphPage(projectsField, [][]column2{{
				column("team", cypherNode(t, "TeamV2", nil)),
				column("project", cypherNode("ari:cloud:jira:site:project/1", "JiraProject", &gen.GraphStoreCypherQueryV2AriNodeData{Key: str("K"), Name: str("N")}))}}, false, nil, false)},
		}}, t
}

// everyKey applies one fault to the first request of the teams read and of
// both graph reads of the scenario.
func everyKey(s scenario, t string, faults ...fault) scenario {
	s.faults = map[string][]fault{"teams": faults, usersField + " " + t: faults, projectsField + " " + t: faults}
	return s
}

func rateLimited(header string) fault {
	return fault{status: 429, header: map[string]string{"Retry-After": header}, body: `{"errors":[{"message":"slow down"}]}`}
}

// extraCorpusA is the gateway side of the client: the HTTP answers, the
// bodies, the rate limits, the client configuration, a cursor the guard refuses.
func extraCorpusA() []scenario {
	var out []scenario
	add := func(s scenario) { out = append(out, s) }

	// ---- HTTP answers
	for _, status := range []int{400, 401, 403, 404, 409, 500, 502, 503} {
		s, t := miniScenario(fmt.Sprintf("http-%d", status), true)
		add(everyKey(s, t, fault{status: status, body: `{"errors":[{"message":"no"}]}`}))
	}
	for _, c := range []struct{ name, body string }{
		{"body-empty", ""},
		{"body-not-json", "{"},
		{"body-array", "[]"},
		{"body-string", `"x"`},
		{"body-data-null", `{"data":null}`},
		{"body-no-data", `{}`},
		{"body-errors-only", `{"errors":[{"message":"x"}]}`},
		{"body-data-null-errors", `{"data":null,"errors":[{"message":"x"}]}`},
		{"body-data-wrong-type", `{"data":{"team":"x","teamworkGraph_teamUsers":"x","teamworkGraph_teamActiveProjects":"x"}}`},
		{"body-data-wrong-type-errors", `{"data":{"team":"x","teamworkGraph_teamUsers":"x","teamworkGraph_teamActiveProjects":"x"},"errors":[{"message":"m"}]}`},
		{"body-data-array", `{"data":[]}`},
		{"body-extensions", `{"data":{"team":null},"extensions":{"requestId":"r1"}}`},
	} {
		for _, strict := range []bool{true, false} {
			name := c.name
			if !strict {
				name += "-lenient"
			}
			s, t := miniScenario(name, strict)
			add(everyKey(s, t, fault{status: 200, body: c.body}))
		}
	}
	{
		// GraphQL errors next to valid data, with the client not strict.
		s, _ := miniScenario("lenient-errors", false)
		t := s.teamIDs[0]
		s.teams = [][]byte{teamPage([]gen.TeamNode{{ID: t, DisplayName: str("Ops"), State: str("ACTIVE")}}, false, nil, true)}
		s.graph[usersField+" "+t] = [][]byte{graphPage(usersField, [][]column2{{
			column("user", cypherNode(userARI("acc-9"), "AtlassianAccountUser", nil)), column("team", cypherNode(t, "TeamV2", nil))}}, false, nil, true)}
		s.graph[projectsField+" "+t] = [][]byte{graphPage(projectsField, nil, false, nil, true)}
		add(s)
	}
	{
		s, t := miniScenario("conn-closed", true)
		add(everyKey(s, t, fault{closeConn: true}))
	}
	{
		s, t := miniScenario("body-truncated", true)
		add(everyKey(s, t, fault{status: 200, body: `{"data":{"team":`, truncate: true}))
	}
	{
		s, _ := miniScenario("oversized-answer", true)
		s.faults = map[string][]fault{"teams": {{status: 200, padTo: 33 << 20}}}
		add(s)
	}

	// ---- rate limits: the retry budget, the Retry-After forms, the cap
	for _, c := range []struct {
		name   string
		cfg    clientConfig
		faults []fault
	}{
		{"429-retry-ok", clientConfig{retries: 2}, []fault{rateLimited("2026-01-01T00:00:02Z")}},
		{"429-retry-twice", clientConfig{retries: 2}, []fault{rateLimited("2026-01-01T00:00:01Z"), rateLimited("2026-01-01T00:00:03Z")}},
		{"429-exhausted", clientConfig{retries: 1}, []fault{rateLimited("2026-01-01T00:00:01Z"), rateLimited("2026-01-01T00:00:01Z")}},
		{"429-no-retry", clientConfig{}, []fault{rateLimited("2026-01-01T00:00:01Z")}},
		{"429-default-two", clientConfig{defaultRetries: true}, []fault{rateLimited("2026-01-01T00:00:01Z"), rateLimited("2026-01-01T00:00:01Z")}},
		{"429-default-three", clientConfig{defaultRetries: true}, []fault{rateLimited("2026-01-01T00:00:01Z"), rateLimited("2026-01-01T00:00:01Z"), rateLimited("2026-01-01T00:00:01Z")}},
		{"429-now", clientConfig{retries: 1}, []fault{rateLimited("2026-01-01T00:00:00Z")}},
		{"429-past", clientConfig{retries: 1}, []fault{rateLimited("2025-12-31T23:59:50Z")}},
		{"429-fraction", clientConfig{retries: 1}, []fault{rateLimited("2026-01-01T00:00:01.5Z")}},
		{"429-minute", clientConfig{retries: 1}, []fault{rateLimited("2026-01-01T00:01Z")}},
		{"429-offset", clientConfig{retries: 1}, []fault{rateLimited("2026-01-01T01:00:05+01:00")}},
		{"429-http-date", clientConfig{retries: 1}, []fault{rateLimited("Thu, 01 Jan 2026 00:00:03 GMT")}},
		{"429-numeric", clientConfig{retries: 1}, []fault{rateLimited("5")}},
		{"429-garbage", clientConfig{retries: 1}, []fault{rateLimited("soon")}},
		{"429-blank", clientConfig{retries: 1}, []fault{rateLimited("   ")}},
		{"429-missing", clientConfig{retries: 1}, []fault{{status: 429, body: "{}"}}},
		{"429-cap-edge", clientConfig{retries: 1}, []fault{rateLimited("2026-01-01T00:01:00Z")}},
		{"429-over-cap", clientConfig{retries: 1}, []fault{rateLimited("2026-01-01T00:01:01Z")}},
		{"429-over-cap-no-retry", clientConfig{}, []fault{rateLimited("2026-01-01T00:05:00Z")}},
		{"429-wider-cap", clientConfig{retries: 1, maxWait: 600}, []fault{rateLimited("2026-01-01T00:05:00Z")}},
		{"429-narrow-cap", clientConfig{retries: 1, maxWait: 5}, []fault{rateLimited("2026-01-01T00:00:06Z")}},
		{"429-narrow-cap-edge", clientConfig{retries: 1, maxWait: 5}, []fault{rateLimited("2026-01-01T00:00:05Z")}},
		{"429-request-id", clientConfig{retries: 1}, []fault{{status: 429, header: map[string]string{"Retry-After": "2026-01-01T00:00:01Z"}, body: `{"extensions":{"requestId":"r-9"}}`}}},
		{"429-body-not-json", clientConfig{retries: 1}, []fault{{status: 429, header: map[string]string{"Retry-After": "2026-01-01T00:00:01Z"}, body: "slow"}}},
	} {
		s, t := miniScenario(c.name, true)
		s.config = c.cfg
		add(everyKey(s, t, c.faults...))
	}
	// A 429 on one request and none on the next, and two reads one after the
	// other on a client that has already waited.
	{
		s, t := miniScenario("429-second-page-only", true)
		s.config = clientConfig{retries: 1}
		s.teams = [][]byte{
			teamPage([]gen.TeamNode{{ID: t, DisplayName: str("A"), State: str("ACTIVE")}}, true, str("c1"), false),
			teamPage([]gen.TeamNode{{ID: teamARI(s.name, "2"), DisplayName: str("B"), State: str("ACTIVE")}}, false, nil, false),
		}
		s.faults = map[string][]fault{"teams": {{}, rateLimited("2026-01-01T00:00:02Z")}}
		add(s)
	}

	// ---- client configuration
	for _, c := range []struct {
		name string
		cfg  clientConfig
	}{
		{"base-slash", clientConfig{baseSuffix: "/"}},
		{"base-double-slash", clientConfig{baseSuffix: "//"}},
		{"base-graphql", clientConfig{baseSuffix: "/graphql"}},
		{"base-graphql-slash", clientConfig{baseSuffix: "/graphql/"}},
		{"base-trailing-space", clientConfig{baseSuffix: " "}},
		{"custom-user-agent", clientConfig{userAgent: "oracle-agent/1.2"}},
		{"no-auth", clientConfig{noAuth: true}},
		{"throttled", clientConfig{throttling: true}},
		{"throttled-retry", clientConfig{throttling: true, retries: 1}},
	} {
		s, _ := miniScenario(c.name, true)
		s.config = c.cfg
		add(s)
	}

	{
		// A cursor of only whitespace after hasNextPage: Python follows it,
		// the Go guard refuses the answer.
		s, t := miniScenario("cursor-blank", true)
		s.teams = [][]byte{teamPage([]gen.TeamNode{{ID: t, DisplayName: str("A"), State: str("ACTIVE")}}, true, str("  "), false),
			teamPage([]gen.TeamNode{{ID: teamARI(s.name, "2"), DisplayName: str("B"), State: str("ACTIVE")}}, false, nil, false)}
		s.graph[usersField+" "+t] = [][]byte{graphPage(usersField, nil, true, str("  "), false), graphPage(usersField, nil, false, nil, false)}
		s.graph[projectsField+" "+t] = [][]byte{graphPage(projectsField, nil, true, str("  "), false), graphPage(projectsField, nil, false, nil, false)}
		add(s)
	}
	return out
}
