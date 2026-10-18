// Package orgllmconfig is the worker-side client of the api's internal
// "resolved categorize config" route. The heavy worker does not hold the
// settings encryption key, so it asks the api (which does) for the org's
// resolved config: the categorize override, else the default row. The key
// stays in the api process (CHAOS-3033 key boundary).
package orgllmconfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// Environment names of the client.
const (
	EnvURL   = "ORG_LLM_CONFIG_URL"
	EnvToken = "ORG_LLM_CONFIG_TOKEN"
)

const (
	// noConfigHeader and noConfigValue mirror acr.NoConfigHeader/NoConfigValue:
	// the api marks the 404 that means "this org has no BYO config".
	noConfigHeader = "X-Org-Llm-Config"
	noConfigValue  = "none"

	schemaVersion = "acr_llm_config.v1"
	maxBody       = 64 << 10
)

// Config is the org's resolved config. APIKey is Hidden: no formatter,
// logger or JSON encoder of this struct can print it.
type Config struct {
	Provider string
	Model    string
	BaseURL  string
	APIKey   secrets.Hidden
	// Version is the api's digest over the effective source rows.
	Version string
}

// ErrNotConfigured: the org has no BYO config (the platform setting applies).
var ErrNotConfigured = errors.New("orgllmconfig: org has no byo llm config")

// ErrRefused: the api refused the worker (401/403) or the stored config is
// unreadable (409). It will not heal by retrying.
var ErrRefused = errors.New("orgllmconfig: the api refused the config request")

// ErrUnavailable: the api could not answer (network, timeout, 5xx). A retry may heal it.
var ErrUnavailable = errors.New("orgllmconfig: the api is unavailable")

// Client reads the resolved categorize config from the api.
type Client struct {
	base  string
	token secrets.Hidden
	http  *http.Client
}

// New builds a client. baseURL is the api internal listener's origin.
func New(baseURL string, token secrets.Hidden, httpClient *http.Client) (*Client, error) {
	parsed, err := url.Parse(strings.TrimRight(strings.TrimSpace(baseURL), "/"))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("%s is not an http(s) origin", EnvURL)
	}
	if !token.Configured() {
		return nil, fmt.Errorf("%s is empty", EnvToken)
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	// The worker token must never follow a redirect to another origin.
	return &Client{base: parsed.String(), token: token, http: httpguard.NoRedirects(httpClient)}, nil
}

// FromEnv builds a client from ORG_LLM_CONFIG_URL and ORG_LLM_CONFIG_TOKEN.
// Both unset is (nil, nil): the feature is off and categorize reads the
// platform environment only. One set without the other is an error.
func FromEnv(lookup func(string) string) (*Client, error) {
	baseURL, token := strings.TrimSpace(lookup(EnvURL)), lookup(EnvToken)
	if baseURL == "" && token == "" {
		return nil, nil
	}
	return New(baseURL, secrets.NewHidden(token), nil)
}

// Fetch returns the org's resolved categorize config, or ErrNotConfigured.
func (c *Client) Fetch(ctx context.Context, orgID string) (Config, error) {
	endpoint := c.base + "/api/v1/internal/workers/orgs/" + url.PathEscape(orgID) + "/llm-config/categorize"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return Config{}, fmt.Errorf("%w: build request", ErrUnavailable)
	}
	req.Header.Set("Authorization", "Bearer "+c.token.Reveal())
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		// A transport error carries the URL; say only whether it was a timeout.
		if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
			return Config{}, fmt.Errorf("%w: timed out", ErrUnavailable)
		}
		return Config{}, fmt.Errorf("%w: request failed", ErrUnavailable)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound && resp.Header.Get(noConfigHeader) == noConfigValue:
		return Config{}, ErrNotConfigured
	case resp.StatusCode == http.StatusNotFound:
		// A bare 404 is a missing route (an old api replica during a rollout, a
		// wrong target), not "no config": never a license to use the platform key.
		return Config{}, fmt.Errorf("%w: route not found", ErrUnavailable)
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden, resp.StatusCode == http.StatusConflict:
		return Config{}, fmt.Errorf("%w: status %d", ErrRefused, resp.StatusCode)
	case resp.StatusCode != http.StatusOK:
		return Config{}, fmt.Errorf("%w: status %d", ErrUnavailable, resp.StatusCode)
	}
	var body struct {
		SchemaVersion string `json:"schema_version"`
		OrgID         string `json:"org_id"`
		Provider      string `json:"provider"`
		Model         string `json:"model"`
		BaseURL       string `json:"base_url"`
		APIKey        string `json:"api_key"`
		Version       string `json:"version"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxBody)).Decode(&body); err != nil {
		return Config{}, fmt.Errorf("%w: undecodable body", ErrUnavailable)
	}
	if body.SchemaVersion != schemaVersion || body.OrgID != orgID || body.Provider == "" {
		return Config{}, fmt.Errorf("%w: unexpected response", ErrRefused)
	}
	return Config{
		Provider: strings.ToLower(strings.TrimSpace(body.Provider)), Model: body.Model, BaseURL: body.BaseURL,
		APIKey: secrets.NewHidden(body.APIKey), Version: body.Version,
	}, nil
}
