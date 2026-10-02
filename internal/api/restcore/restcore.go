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
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
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
	// The core never follows a redirect (httpx's default: follow_redirects=False): a supplied client gets the same policy on a
	// copy, so a client with the default policy cannot follow a Location and quote it in a later error (CHAOS-7927 r1).
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = &http.Client{Timeout: DefaultTimeout}
	} else {
		clone := *httpClient
		httpClient = &clone
	}
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
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
			// The text of a URL error quotes the whole target, query included (CHAOS-7935).
			return Response{}, &Error{"APIException", fmt.Sprintf("%s request failed on %s: invalid request URL", c.Provider, operation)}
		}
		for name, value := range c.Headers {
			request.Header.Set(name, value)
		}
		response, err := httpClient.Do(request)
		var body []byte
		if err == nil {
			body, err = io.ReadAll(response.Body)
			response.Body.Close()
		}
		if err != nil && isLocationParseFailure(err) {
			// net/http refused the redirect itself and its error text quotes the whole Location (CHAOS-7927): say that
			// the redirect was refused, not where to.
			return Response{}, &Error{"APIException", fmt.Sprintf("%s unexpected redirect on %s: the Location header is not a valid URL (%s); the instrumented core does not follow redirects", c.Provider, operation, redirectUnparsable)}
		}
		if err != nil {
			// Only a timeout and a connection failure are retried
			// (httpx.TimeoutException, httpx.ConnectError); any other
			// transport error is raised at once as the httpx exception
			// itself, which no route handles.
			if !retryableTransport(err) {
				// net/http's text quotes the whole request URL, query included: the text is the operation and the class only
				// (logging.TransportFailure). The cause is NOT kept: Error holds only Class and Message and has no Unwrap, so
				// errors.Is / errors.As do not reach it (pinned by TestAnErrorOfTheCoreHasNoCauseToUnwrap); the class names it.
				return Response{}, &Error{Class: "TransportError", Message: logging.TransportFailure(err).Error()}
			}
			if attempt < retries-1 {
				if err := sleep(ctx, delay); err != nil {
					return Response{}, err
				}
				delay = min(delay*2, maxBackoff)
				continue
			}
			// httpx's own exception text differs from Go's (named limit); Go's quotes the request URL, so the text carries
			// the class of the failure only (CHAOS-7935).
			return Response{}, &Error{Class: "APIException", Message: fmt.Sprintf("%s request failed on %s: %s", c.Provider, operation, logging.TransportClass(err))}
		}
		got := Response{Status: response.StatusCode, Header: response.Header, Body: body, URL: target}
		if got.Status < 300 {
			return got, nil
		}
		if got.Status < 400 {
			target := "<no Location header>"
			if location, present := got.HeaderText("Location"); present {
				target = RedirectTarget(location)
			}
			return Response{}, &Error{"APIException", fmt.Sprintf("%s unexpected redirect on %s: HTTP %d -> %s; the instrumented core does not follow redirects (pass raw_redirect=True to receive the redirect response and handle Location manually)", c.Provider, operation, got.Status, target)}
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

const (
	redirectRelative   = "<relative Location>"
	redirectUnparsable = "<unparsable Location>"
)

// RedirectTarget is what an error text may say of a redirect's Location: the scheme and the host (with its port), never
// the userinfo, the path, the query or the fragment, which can carry a credential or an id (CHAOS-7927). A Location with no
// host is "<relative Location>"; one that does not parse is "<unparsable Location>". Python's text carries the raw header;
// this is a named difference.
func RedirectTarget(location string) string {
	parsed, err := url.Parse(strings.TrimSpace(location))
	if err != nil {
		return redirectUnparsable
	}
	if parsed.Host == "" {
		return redirectRelative
	}
	if parsed.Scheme == "" {
		return "//" + parsed.Host
	}
	return parsed.Scheme + "://" + parsed.Host
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
		return &Error{"NotFoundException", fmt.Sprintf("%s resource not found on %s: %s", c.Provider, operation, redactRequestURL(r.URL))}
	case r.Status == 429:
		return &Error{"RateLimitException", fmt.Sprintf("%s rate limit exceeded on %s", c.Provider, operation)}
	case r.Status >= 500:
		return &Error{"APIException", fmt.Sprintf("%s server error on %s: %d - %s", c.Provider, operation, r.Status, r.Text())}
	}
	return &Error{"APIException", fmt.Sprintf("%s API error on %s: %d - %s", c.Provider, operation, r.Status, r.Text())}
}

// retryableTransport is `except (httpx.TimeoutException, httpx.ConnectError)`:
// a timeout of any phase, or a failure to connect (a refused connection, an
// unreachable host, a name that does not resolve).
func retryableTransport(err error) bool {
	var timeout interface{ Timeout() bool }
	if errors.As(err, &timeout) && timeout.Timeout() {
		return true
	}
	var operation *net.OpError
	return errors.As(err, &operation) && operation.Op == "dial"
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

// isLocationParseFailure: net/http refused a redirect whose Location does not parse. Its error is a *url.Error wrapping an
// unwrappable error whose text STARTS with the phrase; a transport error that merely mentions the phrase, or wraps a cause,
// is an ordinary transport error and keeps its class (CHAOS-7927 r1).
func isLocationParseFailure(err error) bool {
	var urlErr *url.Error
	if !errors.As(err, &urlErr) || urlErr.Err == nil || errors.Unwrap(urlErr.Err) != nil {
		return false
	}
	return strings.HasPrefix(urlErr.Err.Error(), "failed to parse Location header ")
}

// redactRequestURL is the request URL as it appears in the text of a NotFound error, with what can hold a credential taken out:
// the userinfo, the value of every query parameter whose NAME is a protected key (token, secret, password, key, ...) and the
// fragment. Every other byte is kept as it is, so the answer stays byte-identical to Python's for a URL without them (the
// frozen venue oracles of the sync admin routes pin that text). Both providers authenticate by header (Authorization,
// PRIVATE-TOKEN), so the URL the callers build holds no credential today; this is the guard for a base URL an operator
// configured with one (CHAOS-7935).
func redactRequestURL(raw string) string {
	rest, _, _ := strings.Cut(raw, "#")
	if scheme := strings.Index(rest, "://"); scheme >= 0 {
		authorityEnd := len(rest)
		if cut := strings.IndexAny(rest[scheme+3:], "/?"); cut >= 0 {
			authorityEnd = scheme + 3 + cut
		}
		if at := strings.LastIndex(rest[scheme+3:authorityEnd], "@"); at >= 0 {
			rest = rest[:scheme+3] + "[REDACTED]@" + rest[scheme+3+at+1:]
		}
	}
	if path, query, hasQuery := strings.Cut(rest, "?"); hasQuery {
		pairs := strings.Split(query, "&")
		for index, pair := range pairs {
			name, _, hasValue := strings.Cut(pair, "=")
			if unescaped, err := url.QueryUnescape(name); err == nil {
				name = unescaped
			}
			if hasValue && logging.ProtectedKey(name) {
				pairs[index] = pair[:strings.Index(pair, "=")+1] + "[REDACTED]"
			}
		}
		rest = path + "?" + strings.Join(pairs, "&")
	}
	return rest
}
