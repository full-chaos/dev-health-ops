package oidc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/oauth2"

	"github.com/full-chaos/dev-health-ops/internal/api/externalurl"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

const maxIdPResponseBytes = 1 << 20

// defaultHTTPClient reaches only addresses the SSRF guard allows and follows
// no redirect: the issuer and its endpoints are admin-configured URLs.
func defaultHTTPClient() *http.Client {
	return &http.Client{Timeout: 30 * time.Second, Transport: externalurl.GuardedTransport(),
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

type metadata struct{ Issuer, TokenEndpoint, UserinfoEndpoint, JWKSURI string }

var errDiscovery = errors.New("oidc: discovery failed")

// resolveMetadata is go-api resolveOIDCMetadata: the configured endpoints
// when both token_endpoint and jwks_uri are set, else the discovery document
// over HTTPS. Unlike go-api, the document's issuer must equal the configured
// issuer (OpenID Connect Discovery 1.0 section 4.3).
func resolveMetadata(ctx context.Context, client *http.Client, cfg ProviderConfig) (metadata, error) {
	if cfg.TokenEndpoint != "" && cfg.JWKSURI != "" {
		return metadata{Issuer: cfg.Issuer, TokenEndpoint: cfg.TokenEndpoint,
			UserinfoEndpoint: cfg.UserinfoEndpoint, JWKSURI: cfg.JWKSURI}, nil
	}
	if cfg.Issuer == "" {
		return metadata{}, fmt.Errorf("%w: no issuer", errDiscovery)
	}
	parsed, err := url.Parse(cfg.Issuer)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
		return metadata{}, fmt.Errorf("%w: issuer is not an https URL", errDiscovery)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimRight(cfg.Issuer, "/")+"/.well-known/openid-configuration", nil)
	if err != nil {
		return metadata{}, fmt.Errorf("%w: %v", errDiscovery, err)
	}
	var doc map[string]any
	if err := getJSON(client, req, &doc); err != nil {
		return metadata{}, fmt.Errorf("%w: %v", errDiscovery, err)
	}
	issuer, _ := doc["issuer"].(string)
	if issuer != cfg.Issuer {
		return metadata{}, fmt.Errorf("%w: document issuer differs from the configured issuer", errDiscovery)
	}
	tokenEndpoint, _ := doc["token_endpoint"].(string)
	userinfoEndpoint, _ := doc["userinfo_endpoint"].(string)
	jwksURI, _ := doc["jwks_uri"].(string)
	if tokenEndpoint == "" || jwksURI == "" {
		return metadata{}, fmt.Errorf("%w: document is missing required endpoints", errDiscovery)
	}
	return metadata{Issuer: issuer, TokenEndpoint: tokenEndpoint, UserinfoEndpoint: userinfoEndpoint, JWKSURI: jwksURI}, nil
}

func getJSON(client *http.Client, req *http.Request, out any) error {
	resp, err := httpguard.NoRedirects(client).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxIdPResponseBytes+1))
	if err != nil {
		return err
	}
	if len(body) > maxIdPResponseBytes {
		return errors.New("response too large")
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("data after the JSON document")
	}
	return nil
}

// exchangeCode calls the token endpoint. The client secret and the code ride
// the body, so no redirect is followed. An empty verifier sends no PKCE.
func exchangeCode(ctx context.Context, client *http.Client, cfg oauth2.Config, code, verifier string) (*oauth2.Token, error) {
	ctx = context.WithValue(ctx, oauth2.HTTPClient, httpguard.NoRedirects(client))
	if verifier == "" {
		return cfg.Exchange(ctx, code)
	}
	return cfg.Exchange(ctx, code, oauth2.VerifierOption(verifier))
}

func fetchUserinfo(ctx context.Context, client *http.Client, endpoint, accessToken string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	var out map[string]any
	if err := getJSON(client, req, &out); err != nil {
		return nil, err
	}
	return out, nil
}
