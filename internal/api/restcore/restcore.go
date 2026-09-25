// Package restcore ports providers/_http.py's InstrumentedRESTCore.request:
// one logical GET retried in place, then classified into the exception
// Python raises, for the api routes that call a provider through the same
// code clients. Every failure's Error() is str(exc) of that exception, because
// the routes write it into a response detail.
//
// The provider differences are hooks (which statuses retry, how long to wait,
// what a terminal 403 means); the loop and the default classification are
// written once.
package restcore

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const (
	// DefaultMaxRetries is _DEFAULT_MAX_RETRIES: attempts per logical request.
	DefaultMaxRetries = 5
	// DefaultInitialBackoff and DefaultMaxBackoff are the core's backoff
	// bounds: the delay doubles from the first up to the second.
	DefaultInitialBackoff = time.Second
	DefaultMaxBackoff     = 60 * time.Second
	// DefaultTimeout is _DEFAULT_TIMEOUT_SECONDS.
	DefaultTimeout = 30 * time.Second
)

// Error is a Python exception the core raises: Class names it
// (AuthenticationException, APIException, ...), Error() is str(exc).
type Error struct {
	Class   string
	Message string
}

func (e *Error) Error() string { return e.Message }

// Response is one physical response.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
	// URL is the URL the request was sent to (str(response.url)).
	URL string
}

// Text is httpx's response.text for a body without a charset parameter:
// UTF-8 decoded with errors="replace". A charset parameter other than UTF-8
// is not ported (named limit).
func (r Response) Text() string { return pythonparity.DecodeUTF8Replace(string(r.Body)) }

// HeaderText is one header's value as httpx reads it: repeated values joined
// with ", ", ok false when absent.
func (r Response) HeaderText(name string) (string, bool) {
	values := r.Header.Values(name)
	if len(values) == 0 {
		return "", false
	}
	return strings.Join(values, ", "), true
}

// Core is one provider's request loop.
type Core struct {
	// Provider is the label in every message ("github", "gitlab").
	Provider string
	// HTTP defaults to a client with the core's timeout and no redirects
	// followed. Headers are set on every request.
	HTTP    *http.Client
	Headers map[string]string
	// Sleep defaults to a context-aware sleep.
	Sleep func(context.Context, time.Duration) error
	// MaxRetries, InitialBackoff and MaxBackoff default to the core's.
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	// IsRetryable is is_retryable_status (default: 429 and 5xx except 501).
	IsRetryable func(Response) bool
	// RetryAfter is resolve_retry_after: the wait a retryable response asks
	// for, zero for none (the running backoff is used then). Required.
	RetryAfter func(Response) time.Duration
	// Classify is classify_error, run on a terminal non-2xx response before
	// the default classification; a non-nil result is raised.
	Classify func(Response, string) *Error
}

// DefaultRetryable is _default_is_retryable_status.
func DefaultRetryable(r Response) bool {
	switch r.Status {
	case 429, 500, 502, 503, 504:
		return true
	}
	return false
}

// Get is InstrumentedRESTCore.request for one GET of target: retried in
// place on a timeout or refused connection and on a retryable status, a 3xx
// an APIException, then classified by _raise_for_status.
func (c Core) Get(ctx context.Context, target, operation string) (Response, error) {
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = &http.Client{Timeout: DefaultTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	sleep := c.Sleep
	if sleep == nil {
		sleep = sleepContext
	}
	retries := c.MaxRetries
	if retries == 0 {
		retries = DefaultMaxRetries
	}
	delay := c.InitialBackoff
	if delay == 0 {
		delay = DefaultInitialBackoff
	}
	maxBackoff := c.MaxBackoff
	if maxBackoff == 0 {
		maxBackoff = DefaultMaxBackoff
	}
	retryable := c.IsRetryable
	if retryable == nil {
		retryable = DefaultRetryable
	}
	for attempt := 0; attempt < retries; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return Response{}, &Error{"APIException", err.Error()}
		}
		for name, value := range c.Headers {
			request.Header.Set(name, value)
		}
		response, err := httpClient.Do(request)
		if err != nil {
			if attempt < retries-1 {
				if err := sleep(ctx, delay); err != nil {
					return Response{}, err
				}
				delay = min(delay*2, maxBackoff)
				continue
			}
			// httpx's own exception text differs from Go's (named limit).
			return Response{}, &Error{"APIException", fmt.Sprintf("%s request failed on %s: %v", c.Provider, operation, err)}
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return Response{}, &Error{"APIException", fmt.Sprintf("%s request failed on %s: %v", c.Provider, operation, err)}
		}
		got := Response{Status: response.StatusCode, Header: response.Header, Body: body, URL: target}
		if got.Status < 300 {
			return got, nil
		}
		if got.Status < 400 {
			location, present := got.HeaderText("Location")
			if !present {
				location = "<no Location header>"
			}
			return Response{}, &Error{"APIException", fmt.Sprintf("%s unexpected redirect on %s: HTTP %d -> %s; the instrumented core does not follow redirects (pass raw_redirect=True to receive the redirect response and handle Location manually)", c.Provider, operation, got.Status, location)}
		}
		if retryable(got) && attempt < retries-1 {
			wait := c.RetryAfter(got)
			if wait <= 0 {
				wait = delay
			}
			if err := sleep(ctx, wait); err != nil {
				return Response{}, err
			}
			delay = min(delay*2, maxBackoff)
			continue
		}
		return Response{}, c.raise(got, operation)
	}
	return Response{}, &Error{"APIException", fmt.Sprintf("%s request failed on %s: unknown error", c.Provider, operation)}
}

// raise is _raise_for_status: classify_error first, then the generic
// 401/403/404/429/5xx classification.
func (c Core) raise(r Response, operation string) error {
	if c.Classify != nil {
		if err := c.Classify(r, operation); err != nil {
			return err
		}
	}
	switch {
	case r.Status == 401:
		return &Error{"AuthenticationException", c.Provider + " authentication failed on " + operation}
	case r.Status == 403:
		return &Error{"AuthenticationException", fmt.Sprintf("%s forbidden on %s: %s", c.Provider, operation, r.Text())}
	case r.Status == 404:
		return &Error{"NotFoundException", fmt.Sprintf("%s resource not found on %s: %s", c.Provider, operation, r.URL)}
	case r.Status == 429:
		return &Error{"RateLimitException", fmt.Sprintf("%s rate limit exceeded on %s", c.Provider, operation)}
	case r.Status >= 500:
		return &Error{"APIException", fmt.Sprintf("%s server error on %s: %d - %s", c.Provider, operation, r.Status, r.Text())}
	}
	return &Error{"APIException", fmt.Sprintf("%s API error on %s: %d - %s", c.Provider, operation, r.Status, r.Text())}
}

func sleepContext(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
