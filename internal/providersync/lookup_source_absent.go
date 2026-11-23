package providersync

import (
	"errors"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// lookupSourceAbsent reports a provider HTTP answer that says the source is not
// there for this credential: 404, 409, another non-auth 4xx, or a 403 that is
// not a rate limit (the client classes rate-limit 403s as rate_limited). Only
// such an answer may be served as an empty source. A rate limit, a 5xx that
// outlasted the retries, a 401, a lease, budget or context error, and a failure
// after the response arrived are not absence: the caller returns them, so the
// unit defers or retries and the window is read again.
func lookupSourceAbsent(err error) bool {
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr == nil || providerErr.AfterResponse() {
		return false
	}
	switch providerErr.Class {
	case providerfoundation.ErrorNotFound, providerfoundation.ErrorConflict, providerfoundation.ErrorPermanent:
		return true
	case providerfoundation.ErrorAuthentication:
		return providerErr.StatusCode == http.StatusForbidden
	}
	return false
}
