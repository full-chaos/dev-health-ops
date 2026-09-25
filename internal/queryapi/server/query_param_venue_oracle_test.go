//go:build integration

package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestQueryParamValidationVenueOracle answers each request with the real
// Python api's own request validation (testdata/venue_oracle_query_params.py
// imports the app as it is, so the route signatures are the real ones) and
// with the Go handler, and requires the same status and the same body text.
// The cases are the scalars a request can send present-but-empty or
// repeated: FastAPI validates a present-but-empty value like any other (a
// `date | None` and a `datetime | None` answer "input is too short", an int
// answers int_parsing) and reads the LAST of a repeated key. Every case
// here must be a 422 on both sides: the Go handler is built on a reader
// that is never reached, so a request that PASSED validation would answer
// something else and differ.
func TestQueryParamValidationVenueOracle(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh")
	}
	_, file, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(file)
	root := filepath.Clean(filepath.Join(packageDir, "..", "..", ".."))
	python := pyoracle.Resolve(t, root)

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
	command := exec.Command(python, filepath.Join(packageDir, "testdata", "venue_oracle_query_params.py"), string(payload))
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "OTEL_SDK_DISABLED=true")
	output, err := command.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			t.Fatalf("python oracle: %v: %s", err, exitErr.Stderr)
		}
		t.Fatal(err)
	}
	var answers []struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
	}
	for _, line := range strings.Split(string(output), "\n") {
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
		if recorder.Code != answers[i].Status || strings.TrimSpace(recorder.Body.String()) != answers[i].Body {
			t.Errorf("%s: DIFF\n  go     %d %s\n  python %d %s", c.name, recorder.Code, strings.TrimSpace(recorder.Body.String()), answers[i].Status, answers[i].Body)
		}
	}
	venueoracle.WriteProof(t)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
