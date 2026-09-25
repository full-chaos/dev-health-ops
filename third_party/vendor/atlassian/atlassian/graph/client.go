package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"log/slog"

	"atlassian/atlassian"
)

const defaultUserAgent = "atlassian-go/0.1.0"
const defaultTimeout = 30 * time.Second
const defaultRetries429 = 2
const defaultMaxWait = 60 * time.Second

func buildGraphQLURL(baseURL string) (string, error) {
	trimmed := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if trimmed == "" {
		return "", errors.New("BaseURL is required")
	}
	if strings.HasSuffix(trimmed, "/graphql") {
		return trimmed, nil
	}
	return trimmed + "/graphql", nil
}

type Client struct {
	BaseURL               string
	HTTPClient            *http.Client
	Auth                  atlassian.AuthProvider
	Strict                bool
	ExperimentalAPIs      []string
	Logger                *slog.Logger
	MaxRetries429         int
	MaxWait               time.Duration
	EnableLocalThrottling bool
	UserAgent             string
	Now                   func() time.Time
	Sleep                 func(time.Duration)

	localBucket *tokenBucket
}

func (c *Client) Execute(ctx context.Context, query string, variables map[string]any, operationName string, experimentalAPIs []string, estimatedCost int) (*atlassian.Result, error) {
	if strings.TrimSpace(query) == "" {
		return nil, errors.New("query must be provided")
	}
	graphQLURL, err := buildGraphQLURL(c.BaseURL)
	if err != nil {
		return nil, err
	}

	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}

	nowFn := c.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	sleepFn := c.Sleep
	if sleepFn == nil {
		sleepFn = time.Sleep
	}

	maxRetries := c.MaxRetries429
	if maxRetries == 0 {
		maxRetries = defaultRetries429
	}
	if maxRetries < 0 {
		maxRetries = 0
	}
	maxWait := c.MaxWait
	if maxWait <= 0 {
		maxWait = defaultMaxWait
	}
	cost := estimatedCost
	if cost <= 0 {
		cost = 1
	}

	var bucket *tokenBucket
	if c.EnableLocalThrottling {
		if c.localBucket == nil {
			c.localBucket = newTokenBucket(nowFn, sleepFn)
		} else {
			c.localBucket.now = nowFn
			c.localBucket.sleep = sleepFn
		}
		bucket = c.localBucket
	}

	payload := atlassian.GraphQLRequest{
		Query:         query,
		Variables:     variables,
		OperationName: operationName,
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	attempt := 0
	for {
		attempt++
		if bucket != nil {
			waited, err := bucket.consume(float64(cost), maxWait)
			if err != nil {
				return nil, err
			}
			if c.Logger != nil {
				c.Logger.Debug(
					"local throttling applied",
					slog.Float64("estimated_cost", float64(cost)),
					slog.Duration("wait", waited),
					slog.String("endpoint", graphQLURL),
				)
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, graphQLURL, bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}

		headers := req.Header
		headers.Set("Content-Type", "application/json")
		headers.Set("Accept", "application/json")
		ua := c.UserAgent
		if ua == "" {
			ua = defaultUserAgent
		}
		headers.Set("User-Agent", ua)
		for _, beta := range experimentalAPIs {
			if strings.TrimSpace(beta) != "" {
				headers.Add("X-ExperimentalApi", beta)
			}
		}

		if c.Auth != nil {
			if err := c.Auth.Apply(req); err != nil {
				return nil, fmt.Errorf("apply auth: %w", err)
			}
		}

		start := nowFn()
		resp, err := httpClient.Do(req)
		duration := nowFn().Sub(start)
		if c.Logger != nil {
			c.Logger.Debug(
				"graphql request",
				slog.String("operationName", operationName),
				slog.Int("attempt", attempt),
				slog.Duration("duration", duration),
				slog.Any("headers", atlassian.SanitizeHeaders(req.Header)),
			)
		}
		if err != nil {
			return nil, fmt.Errorf("execute request: %w", err)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read response: %w", err)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			retryHeader := resp.Header.Get("Retry-After")
			requestID := extractRequestID(body)
			retryAt, parserUsed, parseErr := atlassian.ParseRetryAfter(retryHeader)
			if parseErr != nil {
				if c.Logger != nil {
					c.Logger.Debug(
						"Retry-After parsing failed",
						slog.String("retryAfter", retryHeader),
						slog.String("parser", "none"),
						slog.String("operationName", operationName),
					)
				}
				return nil, &atlassian.RateLimitError{
					RetryAfter:  time.Time{},
					Attempts:    attempt,
					HeaderValue: retryHeader,
				}
			}
			if c.Logger != nil {
				c.Logger.Debug(
					"parsed Retry-After header",
					slog.String("retryAfter", retryHeader),
					slog.String("parser", parserUsed),
					slog.String("retryAt", retryAt.UTC().Format(time.RFC3339)),
					slog.String("operationName", operationName),
				)
			}

			computedWait := retryAt.Sub(nowFn()).Seconds()
			waitSeconds := computedWait
			if waitSeconds < 0 {
				waitSeconds = 0
			}

			retryAllowed := (attempt - 1) < maxRetries
			overCap := computedWait > maxWait.Seconds()

			if c.Logger != nil {
				c.Logger.Warn(
					"rate limited on GraphQL request",
					slog.Int("attempt", attempt),
					slog.String("endpoint", graphQLURL),
					slog.String("operationName", operationName),
					slog.String("retryAt", retryAt.UTC().Format(time.RFC3339)),
					slog.Float64("computedWaitSeconds", computedWait),
					slog.Float64("waitSeconds", waitSeconds),
					slog.Bool("retrying", retryAllowed && !overCap),
					slog.String("request_id", requestID),
				)
			}

			if overCap {
				return nil, &atlassian.RateLimitError{
					RetryAfter:     retryAt,
					Attempts:       attempt,
					HeaderValue:    retryHeader,
					WaitSeconds:    computedWait,
					MaxWaitSeconds: maxWait.Seconds(),
				}
			}
			if !retryAllowed {
				return nil, &atlassian.RateLimitError{
					RetryAfter:  retryAt,
					Attempts:    attempt,
					HeaderValue: retryHeader,
					WaitSeconds: computedWait,
				}
			}

			if waitSeconds > 0 {
				sleepFn(time.Duration(waitSeconds * float64(time.Second)))
			}
			continue
		}

		if resp.StatusCode >= http.StatusInternalServerError {
			return nil, &atlassian.TransportError{
				StatusCode:  resp.StatusCode,
				BodySnippet: string(body),
			}
		}
		if resp.StatusCode >= http.StatusBadRequest {
			return nil, &atlassian.TransportError{
				StatusCode:  resp.StatusCode,
				BodySnippet: string(body),
			}
		}

		var result atlassian.Result
		if len(body) > 0 {
			if err := json.Unmarshal(body, &result); err != nil {
				return nil, &atlassian.JSONError{Err: err}
			}
		}

		if c.Strict && len(result.Errors) > 0 {
			return nil, &atlassian.GraphQLOperationError{
				Errors:      result.Errors,
				PartialData: result.Data,
			}
		}

		return &result, nil
	}
}

// ExecuteWithExtraHeaders is identical to Execute but also sets the provided
// extraHeaders on the HTTP request. This is used for APIs that require headers
// beyond the standard ExperimentalApi set (e.g. X-Force-Dynamo for Teamwork Graph).
func (c *Client) ExecuteWithExtraHeaders(ctx context.Context, query string, variables map[string]any, operationName string, experimentalAPIs []string, estimatedCost int, extraHeaders map[string]string) (*atlassian.Result, error) {
	if len(extraHeaders) == 0 {
		return c.Execute(ctx, query, variables, operationName, experimentalAPIs, estimatedCost)
	}

	if strings.TrimSpace(query) == "" {
		return nil, errors.New("query must be provided")
	}
	graphQLURL, err := buildGraphQLURL(c.BaseURL)
	if err != nil {
		return nil, err
	}

	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}

	nowFn := c.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	sleepFn := c.Sleep
	if sleepFn == nil {
		sleepFn = time.Sleep
	}

	maxRetries := c.MaxRetries429
	if maxRetries == 0 {
		maxRetries = defaultRetries429
	}
	if maxRetries < 0 {
		maxRetries = 0
	}
	maxWait := c.MaxWait
	if maxWait <= 0 {
		maxWait = defaultMaxWait
	}
	cost := estimatedCost
	if cost <= 0 {
		cost = 1
	}

	var bucket *tokenBucket
	if c.EnableLocalThrottling {
		if c.localBucket == nil {
			c.localBucket = newTokenBucket(nowFn, sleepFn)
		} else {
			c.localBucket.now = nowFn
			c.localBucket.sleep = sleepFn
		}
		bucket = c.localBucket
	}

	payload := atlassian.GraphQLRequest{
		Query:         query,
		Variables:     variables,
		OperationName: operationName,
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	attempt := 0
	for {
		attempt++
		if bucket != nil {
			_, lerr := bucket.consume(float64(cost), maxWait)
			if lerr != nil {
				return nil, lerr
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, graphQLURL, bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}

		h := req.Header
		h.Set("Content-Type", "application/json")
		h.Set("Accept", "application/json")
		ua := c.UserAgent
		if ua == "" {
			ua = defaultUserAgent
		}
		h.Set("User-Agent", ua)
		for _, beta := range experimentalAPIs {
			if strings.TrimSpace(beta) != "" {
				h.Add("X-ExperimentalApi", beta)
			}
		}
		for k, v := range extraHeaders {
			if k != "" && v != "" {
				h.Set(k, v)
			}
		}

		if c.Auth != nil {
			if err := c.Auth.Apply(req); err != nil {
				return nil, fmt.Errorf("apply auth: %w", err)
			}
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("execute request: %w", err)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read response: %w", err)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			if attempt > maxRetries {
				return nil, &atlassian.RateLimitError{
					Attempts: attempt,
				}
			}
			sleepFn(defaultMaxWait / 10)
			continue
		}

		if resp.StatusCode >= http.StatusInternalServerError {
			return nil, &atlassian.TransportError{StatusCode: resp.StatusCode, BodySnippet: string(body)}
		}
		if resp.StatusCode >= http.StatusBadRequest {
			return nil, &atlassian.TransportError{StatusCode: resp.StatusCode, BodySnippet: string(body)}
		}

		var result atlassian.Result
		if len(body) > 0 {
			if err := json.Unmarshal(body, &result); err != nil {
				return nil, &atlassian.JSONError{Err: err}
			}
		}

		if c.Strict && len(result.Errors) > 0 {
			return nil, &atlassian.GraphQLOperationError{
				Errors:      result.Errors,
				PartialData: result.Data,
			}
		}

		return &result, nil
	}
}

func extractRequestID(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	extensions, ok := payload["extensions"]
	if !ok {
		return ""
	}
	extMap, ok := extensions.(map[string]any)
	if !ok {
		return ""
	}
	for _, key := range []string{"requestId", "request_id", "requestid"} {
		if val, ok := extMap[key]; ok {
			if s, ok := val.(string); ok {
				return s
			}
		}
	}
	return ""
}
