package providersync

import (
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestClassifyLookupFailureRefusesAnAuthStepError(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusNotFound, http.StatusForbidden, http.StatusBadRequest} {
		err := &providerfoundation.AuthStepError{Err: &providerfoundation.ProviderError{Class: providerfoundation.ErrorNotFound, StatusCode: status}}
		if verdict, _ := classifyLookupFailure(err, nil); verdict != lookupFails {
			t.Errorf("status %d: verdict=%d, want lookupFails", status, verdict)
		}
	}
}
