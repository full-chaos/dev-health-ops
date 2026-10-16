//go:build integration

package apiservice

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The repository browse answers a provider failure with a fixed sentence per status (CHAOS-8438); the frozen Python
// reference still answers str(exc), the provider's own text. That difference is the leak the change removes, so it is
// ruled here and never compared: both planes are reduced to one placeholder, and the raw Go body is asserted to be
// exactly the fixed sentence for its status, so the reduction cannot hide a regression.
var providerFailureSentence = map[int]string{
	401: "The provider rejected the credential.",
	429: "The provider rate limit was reached. Try again later.",
	502: "The provider request failed.",
}

var pythonProviderFailure = regexp.MustCompile(`^((github|gitlab) (authentication failed|rate limit exceeded|forbidden|API error) on |Unexpected paginated response for |GitHub rate limit \(403\) on )`)

const providerFailurePlaceholder = `{"detail":"<provider failure>"}`

func providerFailureRequest(request venueoracle.Request) bool {
	return strings.Contains(request.Name, "provider answer") || strings.Contains(request.Name, "app appdenied")
}

// reduceProviderFailure replaces a provider failure detail, Go's fixed sentence or Python's provider text, by the placeholder.
func reduceProviderFailure(request venueoracle.Request, body string) string {
	if !providerFailureRequest(request) {
		return body
	}
	var decoded struct {
		Detail string `json:"detail"`
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(body), &fields) != nil || len(fields) != 1 || json.Unmarshal([]byte(body), &decoded) != nil || decoded.Detail == "" {
		return body
	}
	for _, sentence := range providerFailureSentence {
		if decoded.Detail == sentence {
			return providerFailurePlaceholder
		}
	}
	if pythonProviderFailure.MatchString(decoded.Detail) {
		return providerFailurePlaceholder
	}
	return body
}

// assertProviderFailureSentence checks the raw Go body of a provider failure row and counts it.
func assertProviderFailureSentence(t *testing.T, request venueoracle.Request, response venueoracle.Response, checked *int) {
	t.Helper()
	if !providerFailureRequest(request) {
		return
	}
	want, failure := providerFailureSentence[response.Status]
	if !failure {
		return
	}
	var decoded map[string]string
	if json.Unmarshal([]byte(response.Body), &decoded) != nil || len(decoded) != 1 || decoded["detail"] != want {
		t.Errorf("%s: go answered %d %s, want the fixed sentence %q alone", request.Name, response.Status, response.Body, want)
	}
	*checked++
}
