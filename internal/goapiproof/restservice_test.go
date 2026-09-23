package goapiproof

import (
	"slices"
	"testing"
)

// withDHOAPISpec registers one dho-api corpus entry for the test's duration
// and restores both tables exactly.
func withDHOAPISpec(t *testing.T, inOrder bool) string {
	t.Helper()
	const operation = "REST:GET:/health"
	restEndpointSpecs[operation] = RESTEndpointSpec{
		Method: "GET", Path: "/health", Service: RESTServiceDHOAPI, PublicNoAuth: true,
		Requests: []RESTRequest{{Name: "probe", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: RESTBodyModeJSON}},
	}
	order := restRunOrder
	if inOrder {
		restRunOrder = append(slices.Clone(order), operation)
	}
	t.Cleanup(func() {
		delete(restEndpointSpecs, operation)
		restRunOrder = order
	})
	return operation
}

func TestParseRESTService(t *testing.T) {
	for value, want := range map[string]RESTService{"": RESTServiceQueryAPI, "query-api": RESTServiceQueryAPI, "dho-api": RESTServiceDHOAPI} {
		got, err := ParseRESTService(value)
		if err != nil || got != want {
			t.Fatalf("ParseRESTService(%q) = %q, %v; want %q", value, got, err, want)
		}
	}
	if _, err := ParseRESTService("query_api"); err == nil {
		t.Fatal("an unknown service must be refused, not defaulted")
	}
}

func TestServiceScopedViewsNeverMixServices(t *testing.T) {
	queryBefore := slices.Clone(RESTRunOrderFor(RESTServiceQueryAPI))
	pathsBefore := slices.Clone(KnownRESTPaths())
	operation := withDHOAPISpec(t, true)

	if got := RESTRunOrderFor(RESTServiceDHOAPI); !slices.Equal(got, []string{operation}) {
		t.Fatalf("dho-api run order = %v, want just %s", got, operation)
	}
	if got := RESTRunOrderFor(RESTServiceQueryAPI); !slices.Equal(got, queryBefore) {
		t.Fatalf("a dho-api entry leaked into query-api's run order: %v", got)
	}
	if got := KnownRESTPaths(); !slices.Equal(got, pathsBefore) {
		t.Fatalf("a dho-api path leaked into query-api's coverage paths: %v", got)
	}
	if got := KnownRESTPathsFor(RESTServiceDHOAPI); !slices.Equal(got, []string{"/health"}) {
		t.Fatalf("dho-api paths = %v", got)
	}
	if err := AssertRESTPathCoverage(MountedRESTPaths()); err != nil {
		t.Fatalf("query-api coverage must be unaffected by a dho-api entry: %v", err)
	}
}

func TestValidateRESTCorpusRefusesADHOAPIEntryNoRunWouldSend(t *testing.T) {
	withDHOAPISpec(t, false)
	if err := ValidateRESTCorpus(); err == nil {
		t.Fatal("a dho-api entry absent from restRunOrder reads as covered while never being sent; it must be refused")
	}
}

func TestValidateRESTCorpusRefusesAnUnknownService(t *testing.T) {
	operation := withDHOAPISpec(t, true)
	spec := restEndpointSpecs[operation]
	spec.Service = "dho_api"
	restEndpointSpecs[operation] = spec
	if err := ValidateRESTCorpus(); err == nil {
		t.Fatal("an unknown service must be refused")
	}
}
