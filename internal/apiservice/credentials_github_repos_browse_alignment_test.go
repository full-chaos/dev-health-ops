package apiservice

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

func browseAlignmentFixture() ([]venueoracle.Request, []string, map[string][]string, map[string]int) {
	requests := []venueoracle.Request{
		{Name: "github repos: user max_repos=1"},
		{Name: "github repos: org max_repos=1"}, // retired
		{Name: "github repos: search max_repos=1"},
	}
	user := "GET /user/repos?per_page=100 auth=token t accept=a"
	orgs := "GET /orgs/other/repos?per_page=100 auth=token t accept=a"
	search := "GET /search/repositories?per_page=100&q=o+org%3Aother auth=token t accept=a"
	python := []string{user, orgs, search}
	goCalls := map[string][]string{
		requests[0].Name: {user},
		requests[1].Name: {user, orgs},
		requests[2].Name: {search},
	}
	goStatus := map[string]int{requests[0].Name: 200, requests[1].Name: 200, requests[2].Name: 200}
	return requests, python, goCalls, goStatus
}

// The traffic of a retired request is set aside by that request's own calls: the added /user/repos before its owner
// listing goes, the owner listing stays and is counted against Python's sequence.
func TestBrowseProviderAlignmentAcceptsTheRulingsAddedCalls(t *testing.T) {
	requests, python, goCalls, goStatus := browseAlignmentFixture()

	if aligned, detail := alignBrowseProviderCalls(requests, goCalls, goStatus, python); !aligned {
		t.Fatalf("the honest traffic is refused: %s", detail)
	}
}

// A request that is not retired may not lose a call Python made, even when a retired request follows: the retired
// request used to skip up to twelve of Python's calls, so it absorbed the missing /user/repos of the request before it.
func TestBrowseProviderAlignmentRefusesAMissingCallBeforeARetiredRequest(t *testing.T) {
	requests, python, goCalls, goStatus := browseAlignmentFixture()
	goCalls[requests[0].Name] = nil

	if aligned, _ := alignBrowseProviderCalls(requests, goCalls, goStatus, python); aligned {
		t.Fatal("Go sent no /user/repos call for the first request and the alignment still passed: a retired request absorbed it")
	}
}

// And an extra call of a request that is not retired, other than the two the ruling adds, is refused.
func TestBrowseProviderAlignmentRefusesAnExtraCallOfAnUnretiredRequest(t *testing.T) {
	requests, python, goCalls, goStatus := browseAlignmentFixture()
	goCalls[requests[2].Name] = append(goCalls[requests[2].Name], "GET /repos/other/x?per_page=100 auth=token t accept=a")

	if aligned, _ := alignBrowseProviderCalls(requests, goCalls, goStatus, python); aligned {
		t.Fatal("an extra provider call of a request that is not retired was accepted")
	}
}
