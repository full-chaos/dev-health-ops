package rest

import (
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

const defaultTimeout = 30 * time.Second
const defaultRetries429 = 2
const defaultMaxWait = 60 * time.Second
const defaultJiraRESTUserAgent = "atlassian-jira-rest-go/0.1.0"

type JiraRESTClient struct {
	BaseURL       string
	HTTPClient    *http.Client
	Auth          atlassian.AuthProvider
	Logger        *slog.Logger
	MaxRetries429 int
	MaxWait       time.Duration
	UserAgent     string
	Now           func() time.Time
	Sleep         func(time.Duration)
}

func (c *JiraRESTClient) GetJSON(ctx context.Context, path string, query map[string]string) (map[string]any, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if baseURL == "" {
		return nil, errors.New("BaseURL is required")
	}
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("path is required")
	}
	cleanedPath := path
	if !strings.HasPrefix(cleanedPath, "/") {
		cleanedPath = "/" + cleanedPath
	}
	url := baseURL + cleanedPath

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

	ua := strings.TrimSpace(c.UserAgent)
	if ua == "" {
		ua = defaultJiraRESTUserAgent
	}

	attempt := 0
	for {
		attempt++
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}

		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", ua)

		if len(query) > 0 {
			q := req.URL.Query()
			for k, v := range query {
				if strings.TrimSpace(k) != "" {
					q.Set(k, v)
				}
			}
			req.URL.RawQuery = q.Encode()
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
				"jira rest request",
				slog.String("method", http.MethodGet),
				slog.String("path", cleanedPath),
				slog.Int("attempt", attempt),
				slog.Duration("duration", duration),
				slog.Any("headers", atlassian.SanitizeHeaders(req.Header)),
			)
		}
		if err != nil {
			return nil, fmt.Errorf("execute request: %w", err)
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read response: %w", readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			retryHeader := resp.Header.Get("Retry-After")
			retryAt, parserUsed, parseErr := atlassian.ParseRetryAfterAny(retryHeader, nowFn())
			if parseErr != nil {
				if c.Logger != nil {
					c.Logger.Debug(
						"Retry-After parsing failed",
						slog.String("retryAfter", retryHeader),
						slog.String("parser", "none"),
						slog.String("path", cleanedPath),
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
					slog.String("path", cleanedPath),
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
					"rate limited on Jira REST request",
					slog.Int("attempt", attempt),
					slog.String("path", cleanedPath),
					slog.String("retryAt", retryAt.UTC().Format(time.RFC3339)),
					slog.Float64("computedWaitSeconds", computedWait),
					slog.Float64("waitSeconds", waitSeconds),
					slog.Bool("retrying", retryAllowed && !overCap),
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

		var out map[string]any
		if len(body) > 0 {
			if err := json.Unmarshal(body, &out); err != nil {
				return nil, &atlassian.JSONError{Err: err}
			}
		} else {
			out = map[string]any{}
		}
		return out, nil
	}
}

func (c *JiraRESTClient) PostJSON(ctx context.Context, path string, data any) (map[string]any, error) {
	return c.requestJSON(ctx, http.MethodPost, path, data)
}

func (c *JiraRESTClient) PutJSON(ctx context.Context, path string, data any) (map[string]any, error) {
	return c.requestJSON(ctx, http.MethodPut, path, data)
}

func (c *JiraRESTClient) Delete(ctx context.Context, path string) error {
	baseURL := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if baseURL == "" {
		return errors.New("BaseURL is required")
	}
	cleanedPath := path
	if !strings.HasPrefix(cleanedPath, "/") {
		cleanedPath = "/" + cleanedPath
	}
	url := baseURL + cleanedPath

	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	ua := strings.TrimSpace(c.UserAgent)
	if ua == "" {
		ua = defaultJiraRESTUserAgent
	}
	req.Header.Set("User-Agent", ua)

	if c.Auth != nil {
		if err := c.Auth.Apply(req); err != nil {
			return fmt.Errorf("apply auth: %w", err)
		}
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return &atlassian.TransportError{
			StatusCode:  resp.StatusCode,
			BodySnippet: string(body),
		}
	}

	return nil
}

func (c *JiraRESTClient) requestJSON(ctx context.Context, method, path string, data any) (map[string]any, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if baseURL == "" {
		return nil, errors.New("BaseURL is required")
	}
	cleanedPath := path
	if !strings.HasPrefix(cleanedPath, "/") {
		cleanedPath = "/" + cleanedPath
	}
	url := baseURL + cleanedPath

	var bodyReader io.Reader
	if data != nil {
		b, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("marshal data: %w", err)
		}
		bodyReader = strings.NewReader(string(b))
	}

	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	if data != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	ua := strings.TrimSpace(c.UserAgent)
	if ua == "" {
		ua = defaultJiraRESTUserAgent
	}
	req.Header.Set("User-Agent", ua)

	if c.Auth != nil {
		if err := c.Auth.Apply(req); err != nil {
			return nil, fmt.Errorf("apply auth: %w", err)
		}
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("read response: %w", readErr)
	}

	if resp.StatusCode >= 400 {
		return nil, &atlassian.TransportError{
			StatusCode:  resp.StatusCode,
			BodySnippet: string(body),
		}
	}

	if resp.StatusCode == http.StatusNoContent || len(body) == 0 {
		return map[string]any{}, nil
	}

	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, &atlassian.JSONError{Err: err}
	}
	return out, nil
}
