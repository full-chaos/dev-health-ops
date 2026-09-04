package categorize

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// httpStatusError wraps a non-2xx HTTP response so classifyProviderError can
// read both the status code and the response body/headers (needed for
// Retry-After on a 429).
type httpStatusError struct {
	statusCode int
	header     http.Header
	body       string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("http %d: %s", e.statusCode, e.body)
}

// httpTransportError wraps a failure where no HTTP response was ever
// received (DNS, TLS, connection refused, timeout at the transport level).
type httpTransportError struct {
	cause error
}

func (e *httpTransportError) Error() string { return e.cause.Error() }
func (e *httpTransportError) Unwrap() error { return e.cause }

// statusCodeOf and headerOf extract classification inputs from an error
// produced by executeResponsesRequest/executeChatCompletionRequest -- 0 and
// nil respectively for a transport-level failure, matching
// classify_provider_error's own status_code=None case.
func statusCodeOf(err error) int {
	var statusErr *httpStatusError
	if errors.As(err, &statusErr) {
		return statusErr.statusCode
	}
	return 0
}

func headerOf(err error) http.Header {
	var statusErr *httpStatusError
	if errors.As(err, &statusErr) {
		return statusErr.header
	}
	return nil
}

// sleepForRetry waits out a retry backoff, returning false if the context
// was cancelled first (the caller should abandon the retry loop, not sleep
// past a caller-requested cancellation).
func sleepForRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// retryDelayFor honours a rate limit's own Retry-After over the generic
// exponential backoff, matching every Python retry site's
// `delay = retry_after if isinstance(exc, LLMRateLimitError) and exc.retry_after else retry_delay(attempt)`.
func retryDelayFor(err *llmError, attempt int) time.Duration {
	if err.kind == llmErrorRateLimit && err.retryAfter > 0 {
		return err.retryAfter
	}
	return retryDelay(attempt)
}
