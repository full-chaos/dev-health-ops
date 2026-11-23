package providersync

import (
	"errors"
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// lookupSourceAbsent reports a provider answer that says the source is not
// there: a 404, or a 403 whose body is one of the provider's documented
// feature-off messages (featureOff, matched case-insensitively). Only such an
// answer may be served as an empty source. Every other answer is returned by
// the caller, so the unit defers or fails and the window is read again: a rate
// limit, any other 4xx (400, 401, 409, 422, a 403 with another body), a 5xx
// that outlasted the retries, a lease, budget or context error, and a failure
// after the response arrived.
func lookupSourceAbsent(err error, featureOff ...string) bool {
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr == nil || providerErr.AfterResponse() {
		return false
	}
	switch {
	case providerErr.Class == providerfoundation.ErrorNotFound && providerErr.StatusCode == http.StatusNotFound:
		return true
	case providerErr.Class == providerfoundation.ErrorAuthentication && providerErr.StatusCode == http.StatusForbidden:
		body := strings.ToLower(providerErr.Body)
		for _, text := range featureOff {
			if text != "" && strings.Contains(body, strings.ToLower(text)) {
				return true
			}
		}
	}
	return false
}
