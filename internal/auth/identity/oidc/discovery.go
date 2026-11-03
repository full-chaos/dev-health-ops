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
		return metadata{}, recordAs(msgIssuerRequired, fmt.Errorf("%w: no issuer", errDiscovery))
	}
	notHTTPS := fmt.Errorf("%w: issuer is not an https URL", errDiscovery)
	parsed, err := url.Parse(cfg.Issuer)
	switch {
	case err != nil:
		return metadata{}, recordAs(msgIssuerInvalid, notHTTPS)
	case parsed.Scheme != "https":
		return metadata{}, recordAs(msgIssuerNotHTTPS, notHTTPS)
	case parsed.Host == "":
		return metadata{}, recordAs(msgIssuerNoHost, notHTTPS)
	case parsed.User != nil:
		return metadata{}, notHTTPS
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimRight(cfg.Issuer, "/")+"/.well-known/openid-configuration", nil)
	if err != nil {
		return metadata{}, recordAs(msgDiscoveryFetch, fmt.Errorf("%w: %v", errDiscovery, err))
	}
	var doc map[string]any
	if err := getJSON(client, req, &doc); err != nil {
		failed := fmt.Errorf("%w: %w", errDiscovery, err)
		if errors.Is(err, errFetchFailed) {
			return metadata{}, recordAs(msgDiscoveryFetch, failed)
		}
		return metadata{}, failed
	}
	issuer, _ := doc["issuer"].(string)
	if issuer != cfg.Issuer {
		return metadata{}, fmt.Errorf("%w: document issuer differs from the configured issuer", errDiscovery)
	}
	tokenEndpoint, _ := doc["token_endpoint"].(string)
	userinfoEndpoint, _ := doc["userinfo_endpoint"].(string)
	jwksURI, _ := doc["jwks_uri"].(string)
	if tokenEndpoint == "" || jwksURI == "" {
		return metadata{}, recordAs(msgDiscoveryEndpoints, fmt.Errorf("%w: document is missing required endpoints", errDiscovery))
	}
	return metadata{Issuer: issuer, TokenEndpoint: tokenEndpoint, UserinfoEndpoint: userinfoEndpoint, JWKSURI: jwksURI}, nil
}

// errFetchFailed is a request that failed in transport or answered a status of 400 or above. errDocumentRefused is a
// size or framing rule of this package that go-api does not have.
var (
	errFetchFailed     = errors.New("oidc: request failed")
	errDocumentRefused = errors.New("oidc: document refused")
)

func getJSON(client *http.Client, req *http.Request, out any) error {
	resp, err := httpguard.NoRedirects(client).Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", errFetchFailed, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("%w: status %d", errFetchFailed, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxIdPResponseBytes+1))
	if err != nil {
		return err
	}
	if len(body) > maxIdPResponseBytes {
		return fmt.Errorf("%w: response too large", errDocumentRefused)
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return fmt.Errorf("%w: data after the JSON document", errDocumentRefused)
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
		return nil, recordAs(msgOIDCUserinfo, err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	var out map[string]any
	if err := getJSON(client, req, &out); err != nil {
		if errors.Is(err, errDocumentRefused) {
			return nil, err
		}
		return nil, recordAs(msgOIDCUserinfo, err)
	}
	return out, nil
}
