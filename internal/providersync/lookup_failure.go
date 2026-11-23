package providersync

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type lookupVerdict int

const (
	lookupFails lookupVerdict = iota
	lookupAbsent
	lookupRecorded
)

func classifyLookupFailure(err error, featureOff []string) (lookupVerdict, string) {
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr == nil || providerErr.AfterResponse() {
		return lookupFails, ""
	}
	switch providerErr.Class {
	case providerfoundation.ErrorNotFound:
		if providerErr.StatusCode == http.StatusNotFound {
			return lookupAbsent, ""
		}
		return lookupRecorded, "other_4xx"
	case providerfoundation.ErrorAuthentication:
		if providerErr.StatusCode != http.StatusForbidden {
			return lookupFails, ""
		}
		body := strings.ToLower(providerErr.Body)
		for _, text := range featureOff {
			if text != "" && strings.Contains(body, strings.ToLower(text)) {
				return lookupAbsent, ""
			}
		}
		return lookupRecorded, "forbidden"
	case providerfoundation.ErrorPermanent:
		if providerErr.StatusCode == http.StatusBadRequest || providerErr.StatusCode == http.StatusUnprocessableEntity {
			return lookupRecorded, "bad_request"
		}
		return lookupRecorded, "other_4xx"
	case providerfoundation.ErrorConflict:
		return lookupRecorded, "other_4xx"
	}
	return lookupFails, ""
}

// serveLookupFailure decides what a failed core lookup becomes. It returns nil
// when the lookup is served as an empty source: a 404 or a documented
// feature-off 403 (silent), or any other 4xx (counted and logged with its
// reason, never silent). Every other error is returned: a rate limit defers the
// unit, a 5xx after retries, a 401, and lease, budget, context and
// after-response errors fail it, so the window is read again.
func serveLookupFailure(
	ctx context.Context, client *providerfoundation.HTTPClient, claim Claim, lookup string, err error, featureOff ...string,
) error {
	verdict, reason := classifyLookupFailure(err, featureOff)
	switch verdict {
	case lookupAbsent:
		return nil
	case lookupRecorded:
		client.Metrics.RecordLookupUnavailable(claim.Provider, claim.Dataset, reason)
		slog.WarnContext(ctx, "provider_lookup_unavailable",
			"provider", claim.Provider, "dataset", claim.Dataset, "lookup", lookup, "reason", reason, "status_class", "4xx")
		return nil
	}
	return err
}
