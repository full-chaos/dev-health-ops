package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type lookupVerdict int

const (
	lookupFails lookupVerdict = iota
	lookupAbsent
	lookupRecorded
)

func joinedError(err error) bool {
	for ; err != nil; err = errors.Unwrap(err) {
		if _, joined := err.(interface{ Unwrap() []error }); joined {
			return true
		}
	}
	return false
}

func featureOffMessage(body string, featureOff []string) bool {
	decoder := json.NewDecoder(strings.NewReader(body))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return false
	}
	message, seen := "", false
	for decoder.More() {
		token, err := decoder.Token()
		key, isKey := token.(string)
		if err != nil || !isKey {
			return false
		}
		if !strings.EqualFold(key, "message") {
			var skipped json.RawMessage
			if decoder.Decode(&skipped) != nil {
				return false
			}
			continue
		}
		if key != "message" || seen || decoder.Decode(&message) != nil {
			return false
		}
		seen = true
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return false
	}
	for _, text := range featureOff {
		if seen && text != "" && message == text {
			return true
		}
	}
	return false
}

func classifyLookupFailure(err error, featureOff []string) (lookupVerdict, string) {
	var providerErr *providerfoundation.ProviderError
	var authStep *providerfoundation.AuthStepError
	if !errors.As(err, &providerErr) || providerErr == nil || providerErr.AfterResponse() ||
		errors.As(err, &authStep) || joinedError(err) {
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
		if featureOffMessage(providerErr.Body, featureOff) {
			return lookupAbsent, ""
		}
		return lookupRecorded, "forbidden"
	case providerfoundation.ErrorPermanent:
		// A permanent error with no 4xx status got no answer (a dial-time egress refusal): the lookup failed.
		if providerErr.StatusCode < 400 || providerErr.StatusCode > 499 {
			return lookupFails, ""
		}
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

// lookupStatusClass names a failed lookup's HTTP status class for a log line: "5xx", "4xx", "transport" (no HTTP
// answer) or "none" (the request was never sent, e.g. a refused budget or a failed token refresh).
func lookupStatusClass(err error) string {
	var providerErr *providerfoundation.ProviderError
	var authStep *providerfoundation.AuthStepError
	if errors.As(err, &authStep) || !errors.As(err, &providerErr) || providerErr == nil {
		return "none"
	}
	if providerErr.StatusCode == 0 {
		return "transport"
	}
	return strconv.Itoa(providerErr.StatusCode/100) + "xx"
}

// lookupReadFailed reports whether a lookup failed before it got a usable answer: classifyLookupFailure fails it and
// it is not an answered response that could not be used (a body the caller could not read or decode).
func lookupReadFailed(err error) bool {
	var providerErr *providerfoundation.ProviderError
	if errors.As(err, &providerErr) && providerErr.AfterResponse() {
		return false
	}
	verdict, _ := classifyLookupFailure(err, nil)
	return verdict == lookupFails
}
