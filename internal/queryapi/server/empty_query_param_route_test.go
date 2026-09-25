package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// TestPresentButEmptyScalarQueryParamIs422 (CHAOS-6604): FastAPI validates a
// present-but-empty query value like any other -- a `date | None` and a
// `datetime | None` answer "input is too short", an int answers int_parsing
// -- and only a key that is not in the query at all is absent. The Go
// handlers used to treat "?start_date=" as absent (200). The live-Python
// comparison of the same requests is TestQueryParamValidationVenueOracle;
// this is the fast form that runs in the unit tier.
func TestPresentButEmptyScalarQueryParamIs422(t *testing.T) {
	prs := newDrilldownPRsGetHandler(newEmptyRowsDrilldownReader(t))
	peoplePRs := newPeopleDrilldownPRsHandler(newPeopleDetailFailingReader(t))
	peopleSearch := newPeopleSearchHandler(newPeopleDetailFailingReader(t))
	cases := []struct {
		name    string
		target  string
		person  string
		handler http.HandlerFunc
		want    string
	}{
		{"empty start_date", "/api/v1/drilldown/prs?scope_type=repo&scope_id=r&start_date=", "", prs, `"loc":["query","start_date"],"msg":"Input should be a valid date or datetime, input is too short","input":""`},
		{"valid then empty start_date (last wins)", "/api/v1/drilldown/prs?scope_type=repo&scope_id=r&start_date=2024-01-01&start_date=", "", prs, `"loc":["query","start_date"],"msg":"Input should be a valid date or datetime, input is too short","input":""`},
		{"empty cursor", "/api/v1/people/p1/drilldown/prs?cursor=", "p1", peoplePRs, `"loc":["query","cursor"],"msg":"Input should be a valid datetime or date, input is too short","input":""`},
		{"empty limit", "/api/v1/people?limit=", "", peopleSearch, `"loc":["query","limit"],"msg":"Input should be a valid integer, unable to parse string as an integer","input":""`},
	}
	for _, c := range cases {
		request := httptest.NewRequest(http.MethodGet, c.target, nil)
		if c.person != "" {
			request.SetPathValue("person_id", c.person)
		}
		request = request.WithContext(authctx.WithClaims(request.Context(), authctx.Claims{OrgID: "org-1"}))
		recorder := httptest.NewRecorder()
		c.handler(recorder, request)
		if recorder.Code != http.StatusUnprocessableEntity || !strings.Contains(recorder.Body.String(), c.want) {
			t.Errorf("%s: got %d %s, want 422 containing %s", c.name, recorder.Code, recorder.Body.String(), c.want)
		}
	}
	// A key that is not in the query at all is still absent.
	request := httptest.NewRequest(http.MethodGet, "/api/v1/drilldown/prs?scope_type=repo&scope_id=r", nil)
	request = request.WithContext(authctx.WithClaims(request.Context(), authctx.Claims{OrgID: "org-1"}))
	recorder := httptest.NewRecorder()
	prs(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Errorf("absent dates: got %d %s, want 200", recorder.Code, recorder.Body.String())
	}
}
