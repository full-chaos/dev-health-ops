package providerfoundation

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const maxProviderRequestBody = 1 << 20
const maxProviderErrorBody = 64 << 10

// RetryPolicy is an attempt budget, not a nested retry multiplier. A request
// gets at most MaxAttempts calls including the initial call.
type RetryPolicy struct {
	MaxAttempts int
	InitialWait time.Duration
	MaxWait     time.Duration
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, InitialWait: time.Second, MaxWait: 30 * time.Second}
}

func (p RetryPolicy) valid() bool {
	return p.MaxAttempts > 0 && p.InitialWait > 0 && p.MaxWait >= p.InitialWait
}

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// Auth is constructed from a typed credential. It is the only place that may
// reveal a secret for an outbound request.
type Auth func(*http.Request) error

func TokenAuth(header, prefix string, token secrets.Value) Auth {
	return func(request *http.Request) error {
		if !token.Configured() {
			return ErrCredentialInvalid
		}
		request.Header.Set(header, prefix+token.Reveal())
		return nil
	}
}

type HTTPClient struct {
	Provider  string
	BaseURL   *url.URL
	Doer      HTTPDoer
	Auth      Auth
	Retry     RetryPolicy
	Lease     LeaseGuard
	Gate      BackoffGate
	Budget    BudgetStore
	BudgetKey BudgetKey
	Metrics   *Metrics
}

func NewHTTPClient(provider, base string, doer HTTPDoer, auth Auth, retry RetryPolicy, lease LeaseGuard) (*HTTPClient, error) {
	parsed, err := url.Parse(base)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || doer == nil || auth == nil || lease == nil || !retry.valid() {
		return nil, ErrCredentialInvalid
	}
	return &HTTPClient{Provider: strings.ToLower(provider), BaseURL: parsed, Doer: doer, Auth: auth, Retry: retry, Lease: lease}, nil
}

func (c *HTTPClient) Do(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	if c == nil || c.BaseURL == nil {
		return nil, ErrCredentialInvalid
	}
	if err := c.Lease.Assert(ctx); err != nil {
		return nil, err
	}
	if c.Gate != nil {
		if wait, err := c.Gate.Wait(ctx); err != nil {
			return nil, err
		} else if wait > 0 {
			return nil, &ProviderError{Class: ErrorRateLimited, RetryAfter: wait}
		}
	}
	var requestBody []byte
	if body != nil {
		var err error
		requestBody, err = io.ReadAll(io.LimitReader(body, maxProviderRequestBody+1))
		if err != nil || len(requestBody) > maxProviderRequestBody {
			return nil, ErrCredentialInvalid
		}
	}
	var reservation Reservation
	var err error
	if c.Budget != nil {
		reservation, err = c.Budget.Acquire(ctx, c.BudgetKey)
		if err != nil {
			if c.Metrics != nil {
				c.Metrics.RecordBudgetDenied(c.Provider)
			}
			return nil, err
		}
		defer reservation.Release(context.Background())
	}
	target, err := c.BaseURL.Parse(path)
	if err != nil || target.Host != c.BaseURL.Host {
		return nil, ErrCredentialInvalid
	}
	var last *ProviderError
	for attempt := 1; attempt <= c.Retry.MaxAttempts; attempt++ {
		if err := c.Lease.Assert(ctx); err != nil {
			return nil, err
		}
		request, err := http.NewRequestWithContext(ctx, method, target.String(), bytes.NewReader(requestBody))
		if err != nil {
			return nil, ErrCredentialInvalid
		}
		if err := c.Auth(request); err != nil {
			return nil, err
		}
		response, requestErr := c.Doer.Do(request)
		if requestErr != nil {
			last = &ProviderError{Class: ErrorTransient}
			c.observe(last.Class)
			if err := c.wait(ctx, attempt, 0); err != nil {
				return nil, err
			}
			continue
		}
		message, _ := io.ReadAll(io.LimitReader(response.Body, maxProviderErrorBody))
		classification := ClassifyHTTPWithMessage(c.Provider, response.StatusCode, response.Header, string(message))
		if classification == nil {
			c.observe("")
			return response, nil
		}
		if classification.Class == ErrorRateLimited && c.Gate != nil {
			_ = c.Gate.Penalize(ctx, classification.RetryAfter)
		}
		_ = response.Body.Close()
		last = classification
		c.observe(classification.Class)
		if !classification.Retryable() || attempt == c.Retry.MaxAttempts {
			return nil, classification
		}
		if err := c.wait(ctx, attempt, classification.RetryAfter); err != nil {
			return nil, err
		}
	}
	if last == nil {
		last = &ProviderError{Class: ErrorTransient}
	}
	return nil, last
}

func (c *HTTPClient) wait(ctx context.Context, attempt int, retryAfter time.Duration) error {
	wait := retryAfter
	if wait <= 0 {
		wait = c.Retry.InitialWait << (attempt - 1)
		if wait > c.Retry.MaxWait {
			wait = c.Retry.MaxWait
		}
		wait += time.Duration(rand.Int64N(int64(wait/5 + 1)))
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *HTTPClient) observe(class ErrorClass) {
	if c.Metrics != nil {
		c.Metrics.RecordRequest(c.Provider, class)
	}
}

// ClassifyHTTP mirrors the current Python provider taxonomy without retaining
// response bodies or headers. GitHub's 403 rate-limit vocabulary is special;
// all other 403s remain authentication/permission failures.
func ClassifyHTTP(provider string, status int, headers http.Header) *ProviderError {
	return ClassifyHTTPWithMessage(provider, status, headers, "")
}

func ClassifyHTTPWithMessage(provider string, status int, headers http.Header, message string) *ProviderError {
	retryAfter := ParseRetryAfter(headerValue(headers, "retry-after"), time.Now())
	message = strings.ToLower(message)
	if provider == "github" && status == http.StatusForbidden && (headerValue(headers, "x-ratelimit-remaining") == "0" || headerValue(headers, "retry-after") != "" || strings.Contains(message, "rate limit") || strings.Contains(message, "abuse") || strings.Contains(message, "secondary")) {
		return &ProviderError{Class: ErrorRateLimited, StatusCode: status, RetryAfter: retryAfter}
	}
	switch status {
	case 0:
		return &ProviderError{Class: ErrorTransient}
	case http.StatusUnauthorized, http.StatusForbidden:
		return &ProviderError{Class: ErrorAuthentication, StatusCode: status}
	case http.StatusNotFound:
		return &ProviderError{Class: ErrorNotFound, StatusCode: status}
	case http.StatusConflict:
		return &ProviderError{Class: ErrorConflict, StatusCode: status}
	case http.StatusTooManyRequests:
		return &ProviderError{Class: ErrorRateLimited, StatusCode: status, RetryAfter: retryAfter}
	}
	if status >= 500 && status <= 599 {
		return &ProviderError{Class: ErrorTransient, StatusCode: status, RetryAfter: retryAfter}
	}
	if status >= 400 {
		return &ProviderError{Class: ErrorPermanent, StatusCode: status}
	}
	return nil
}

func headerValue(headers http.Header, wanted string) string {
	for key, values := range headers {
		if strings.EqualFold(key, wanted) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

func ParseRetryAfter(raw string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseFloat(strings.TrimSpace(raw), 64); err == nil {
		if seconds < 0 {
			return 0
		}
		return time.Duration(seconds * float64(time.Second))
	}
	if date, err := http.ParseTime(raw); err == nil && date.After(now) {
		return date.Sub(now)
	}
	return 0
}

// PageFunc follows an opaque next cursor. Its source envelope is already
// normalized; the helper only enforces bounded pagination and cancellation.
type PageFunc[T any] func(context.Context, string) (items []T, next string, err error)

func CollectPages[T any](ctx context.Context, maxPages int, fetch PageFunc[T]) ([]T, error) {
	if maxPages < 1 || fetch == nil {
		return nil, ErrCredentialInvalid
	}
	var values []T
	cursor := ""
	for page := 0; page < maxPages; page++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		items, next, err := fetch(ctx, cursor)
		if err != nil {
			return nil, err
		}
		values = append(values, items...)
		if next == "" {
			return values, nil
		}
		cursor = next
	}
	return nil, errors.New("provider pagination budget exhausted")
}
