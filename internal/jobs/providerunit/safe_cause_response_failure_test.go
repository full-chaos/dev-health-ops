package providerunit

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

func TestSafeCauseDoesNotLabelAPostResponseFailureWithAnHTTPClass(t *testing.T) {
	claim := providersync.Claim{Unit: providersync.Unit{Provider: "github", Dataset: "cicd"}}
	response := &http.Response{Body: http.NoBody}
	postResponse := providerfoundation.ResponseInvalid(response, providerfoundation.ReasonBodyDecode)
	got := safeCause("category", claim, nil, postResponse)
	if strings.Contains(got, "provider_error_class=") || !strings.Contains(got, "response_failure=body_decode") {
		t.Fatalf("cause=%q: a post-2xx failure must not read as an HTTP verdict and must name its reason", got)
	}
	if strings.Contains(got, "provider_status=") {
		t.Fatalf("cause=%q: a post-2xx failure carries no status", got)
	}
	transport := &providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient, Reason: providerfoundation.TransportReasonConnRefused}
	got = safeCause("category", claim, nil, transport)
	if !strings.Contains(got, "provider_error_class=transient") || strings.Contains(got, "provider_status=") || strings.Contains(got, "response_failure=") {
		t.Fatalf("cause=%q: a transport failure keeps its class and carries no status or response_failure", got)
	}
	httpFailure := &providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient, StatusCode: 503}
	got = safeCause("category", claim, nil, errors.Join(httpFailure))
	if !strings.Contains(got, "provider_error_class=transient") || !strings.Contains(got, "provider_status=503") {
		t.Fatalf("cause=%q lost the HTTP class", got)
	}
}

func TestSafeCauseLabelsAGraphQLErrorsResponseFailure(t *testing.T) {
	claim := providersync.Claim{Unit: providersync.Unit{Provider: "github", Dataset: "work-items"}}
	err := providerfoundation.ResponseGraphQL(&http.Response{Body: http.NoBody}, providerfoundation.ErrGraphQLResponse)
	got := safeCause("category", claim, nil, err)
	if !strings.Contains(got, "response_failure=graphql_errors") || strings.Contains(got, "provider_error_class=") {
		t.Fatalf("cause=%q: a GraphQL errors body must name response_failure=graphql_errors and no HTTP class", got)
	}
}

func TestGraphQLErrorsResponseKeepsTheRetryAnswerOfABareSentinel(t *testing.T) {
	response := &http.Response{Body: http.NoBody}
	for _, sentinel := range []error{providerfoundation.ErrGraphQLResponse, providerfoundation.ErrGraphQLComplexity} {
		for name, wrapped := range map[string]error{
			"response":   providerfoundation.ResponseGraphQL(response, sentinel),
			"shape only": providerfoundation.GraphQLShapeFailure("/graphql", sentinel),
		} {
			for _, timedOut := range []bool{false, true} {
				if got, want := causeClass(wrapped, timedOut), causeClass(sentinel, timedOut); got != want || (!timedOut && got != providersync.CauseClassUnclassified) {
					t.Errorf("%s %v timedOut=%v: causeClass=%q, bare sentinel=%q (want %q)", name, sentinel, timedOut, got, want, providersync.CauseClassUnclassified)
				}
			}
			if got, want := retryMaxAttempts(wrapped), retryMaxAttempts(sentinel); got != want || got != unclassifiedMaxAttempts {
				t.Errorf("%s %v: retryMaxAttempts=%d, bare sentinel=%d (want %d)", name, sentinel, got, want, unclassifiedMaxAttempts)
			}
			gotCategory, gotDeterministic := deterministicTerminalCategory(wrapped)
			wantCategory, wantDeterministic := deterministicTerminalCategory(sentinel)
			if gotCategory != wantCategory || gotDeterministic != wantDeterministic {
				t.Errorf("%s %v: deterministic category=%q,%v, bare sentinel=%q,%v", name, sentinel, gotCategory, gotDeterministic, wantCategory, wantDeterministic)
			}
			if _, limited := providerRateLimitDelay(wrapped); limited {
				t.Errorf("%s %v: a GraphQL errors body must not read as a provider rate limit", name, sentinel)
			}
		}
	}
}
