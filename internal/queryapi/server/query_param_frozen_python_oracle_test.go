package server

import (
	"encoding/json"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pydanticecho"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// serverGoldens is the set of this package's frozen Python answers. The
// producers import the Python api and its ClickHouse reader module as they
// are, so the route signatures and the reader SQL are the real ones.
var serverGoldens = programoracle.Set{
	Build:    "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity: "python 3.14.7\nunicodedata 16.0.0",
	Pins: map[string]string{
		"dedup-sources.golden.json": "550db38800dd3d6c124e5fde250699042faf6ae7ce450badfb4527a12d6c9a4c",
		"query-params.golden.json":  "6ae1b2cdc65971de9cb655418eedb5c7ae0b60f25b3827c5659be9c75b35af2a",
	},
}

// TestQueryParamValidationMatchesFrozenPython answers each request with the
// frozen answer of the real Python api's own request validation and with the
// Go handler, and requires the same status and the same body text. The cases
// are the scalars a request can send present-but-empty or repeated: FastAPI
// validates a present-but-empty value like any other (a `date | None` and a
// `datetime | None` answer "input is too short", an int answers
// int_parsing) and reads the LAST of a repeated key. Every case here must be
// a 422 on both sides: the Go handler is built on a reader that is never
// reached, so a request that PASSED validation would answer something else
// and differ.
func TestQueryParamValidationMatchesFrozenPython(t *testing.T) {
	prs := newDrilldownPRsGetHandler(newEmptyRowsDrilldownReader(t))
	peoplePRs := newPeopleDrilldownPRsHandler(newPeopleDetailFailingReader(t))
	peopleIssues := newPeopleDrilldownIssuesHandler(newPeopleDetailFailingReader(t))
	peopleSearch := newPeopleSearchHandler(newPeopleDetailFailingReader(t))
	sunburst := newInvestmentSunburstGetHandler(newEmptyRowsInvestmentReader(t))
	cases := []struct {
		name    string
		path    string
		person  string
		query   string
		handler http.HandlerFunc
	}{
		{"drilldown prs empty start_date", "/api/v1/drilldown/prs", "", "scope_type=repo&scope_id=r&start_date=", prs},
		{"drilldown prs empty end_date", "/api/v1/drilldown/prs", "", "scope_type=repo&scope_id=r&end_date=", prs},
		{"drilldown prs valid then empty start_date", "/api/v1/drilldown/prs", "", "scope_type=repo&scope_id=r&start_date=2024-01-01&start_date=", prs},
		{"drilldown prs empty then invalid start_date", "/api/v1/drilldown/prs", "", "scope_type=repo&scope_id=r&start_date=&start_date=nope", prs},
		{"people drilldown prs empty cursor", "/api/v1/people/{person_id}/drilldown/prs", "p1", "cursor=", peoplePRs},
		{"people drilldown prs empty limit", "/api/v1/people/{person_id}/drilldown/prs", "p1", "limit=", peoplePRs},
		{"people drilldown issues empty cursor", "/api/v1/people/{person_id}/drilldown/issues", "p1", "cursor=", peopleIssues},
		{"people drilldown issues empty limit", "/api/v1/people/{person_id}/drilldown/issues", "p1", "limit=", peopleIssues},
		{"people search empty limit", "/api/v1/people", "", "limit=", peopleSearch},
		{"people search valid then empty limit", "/api/v1/people", "", "limit=5&limit=", peopleSearch},
		{"investment sunburst empty limit", "/api/v1/investment/sunburst", "", "limit=", sunburst},
	}

	type pythonCase struct {
		Path  string `json:"path"`
		Query string `json:"query"`
	}
	requests := make([]pythonCase, len(cases))
	for i, c := range cases {
		requests[i] = pythonCase{Path: strings.ReplaceAll(c.path, "{person_id}", firstNonEmpty(c.person, "x")), Query: c.query}
	}
	payload, err := json.Marshal(requests)
	if err != nil {
		t.Fatal(err)
	}
	output := serverGoldens.Outputs(t, "query-params.golden.json", programoracle.Program{
		Name:  "query params",
		Stdin: payload,
	})[0]
	var answers []struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
	}
	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, "RESULT ") {
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "RESULT ")), &answers); err != nil {
				t.Fatalf("decode python answers: %v", err)
			}
		}
	}
	if len(answers) != len(cases) {
		t.Fatalf("python answered %d of %d cases: %s", len(answers), len(cases), output)
	}

	for i, c := range cases {
		request := httptest.NewRequest(http.MethodGet, strings.ReplaceAll(c.path, "{person_id}", firstNonEmpty(c.person, "x"))+"?"+c.query, nil)
		if c.person != "" {
			request.SetPathValue("person_id", c.person)
		}
		request = request.WithContext(authctx.WithClaims(request.Context(), authctx.Claims{OrgID: "org-1"}))
		recorder := httptest.NewRecorder()
		c.handler(recorder, request)
		if answers[i].Status != http.StatusUnprocessableEntity {
			t.Errorf("%s: python answered %d, want 422 (the case must be a validation failure): %s", c.name, answers[i].Status, answers[i].Body)
		}
		if recorder.Code != answers[i].Status || strings.TrimSpace(recorder.Body.String()) != pydanticecho.Strip(answers[i].Body) {
			t.Errorf("%s: DIFF\n  go     %d %s\n  python %d %s", c.name, recorder.Code, strings.TrimSpace(recorder.Body.String()), answers[i].Status, answers[i].Body)
		}
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
