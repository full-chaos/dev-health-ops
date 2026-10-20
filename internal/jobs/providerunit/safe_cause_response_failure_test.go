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
