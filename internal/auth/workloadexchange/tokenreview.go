package workloadexchange

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

// TokenReview transport bounds. ReviewCallTimeout bounds the whole call.
const (
	reviewDialTimeout           = 2 * time.Second
	reviewTLSHandshakeTimeout   = 2 * time.Second
	reviewResponseHeaderTimeout = 3 * time.Second
	maxReviewerTokenBytes       = 16 << 10
)

// ReviewConfig configures the TokenReview validator.
type ReviewConfig struct {
	// APIServerURL is the Kubernetes API server origin (https).
	APIServerURL string
	// CAFile is the PEM bundle the API server certificate must chain to. No
	// other root is trusted.
	CAFile string
	// ReviewerTokenFile holds auth-service's own token, allowed to create
	// tokenreviews. It is read on every review (the kubelet rotates it).
	ReviewerTokenFile string
	// TrustDomain is this cluster's configured trust domain.
	TrustDomain string
	Now         func() time.Time
}

// NewReviewValidator builds authverify's TokenReview validator over a client
// that trusts only CAFile, follows no redirect, uses no proxy and is bounded
// at every step. The subject token is authenticated by the API server alone;
// nothing here decodes it.
func NewReviewValidator(cfg ReviewConfig) (authverify.SubjectTokenValidator, error) {
	if !isHTTPSOrigin(cfg.APIServerURL) {
		return nil, errors.New("workloadexchange: the API server URL must be an https origin")
	}
	if cfg.CAFile == "" || cfg.ReviewerTokenFile == "" || strings.TrimSpace(cfg.TrustDomain) == "" {
		return nil, errors.New("workloadexchange: the CA file, the reviewer token file and the trust domain are required")
	}
	bundle, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, errors.New("workloadexchange: the CA file cannot be read")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(bundle) {
		return nil, errors.New("workloadexchange: the CA file holds no PEM certificate")
	}
	client := reviewHTTPClient(roots)
	path := cfg.ReviewerTokenFile
	return authverify.NewKubernetesTokenReviewValidator(authverify.KubernetesTokenReviewOptions{
		HTTPClient:    client,
		APIServerURL:  cfg.APIServerURL,
		ReviewerToken: func() (string, error) { return readReviewerToken(path) },
		Audience:      ReviewAudience,
		TrustDomain:   cfg.TrustDomain,
		Now:           cfg.Now,
	})
}

// reviewHTTPClient is the TokenReview client: its own transport (only the
// configured cluster CA pool, never the system roots; no proxy, never one from
// the environment, so the ServiceAccount tokens reach only the API server;
// bounded dial, TLS and header waits) and a ReviewCallTimeout bound, guarded by
// httpguard.NoRedirects so no redirect is followed.
func reviewHTTPClient(roots *x509.CertPool) *http.Client {
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: reviewDialTimeout}).DialContext,
		TLSClientConfig:       &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout:   reviewTLSHandshakeTimeout,
		ResponseHeaderTimeout: reviewResponseHeaderTimeout,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          MaxInFlightReviews,
		MaxIdleConnsPerHost:   MaxInFlightReviews,
		IdleConnTimeout:       90 * time.Second,
	}
	return httpguard.NoRedirects(&http.Client{Transport: transport, Timeout: ReviewCallTimeout})
}

// isHTTPSOrigin accepts only "https://host[:port]" with at most one trailing
// slash: the URL must equal its own scheme and host, so no path, query (a
// bare "?" included), fragment, userinfo or empty port ("host:") can move the
// TokenReview path.
func isHTTPSOrigin(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || strings.HasSuffix(parsed.Host, ":") {
		return false
	}
	origin := "https://" + parsed.Host
	return raw == origin || raw == origin+"/"
}

var errReviewerToken = errors.New("workloadexchange: the reviewer token is unavailable")

// readReviewerToken reads the reviewer token file, bounded. The error never
// carries the path or the content.
func readReviewerToken(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", errReviewerToken
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, maxReviewerTokenBytes+1))
	if err != nil || len(raw) > maxReviewerTokenBytes {
		return "", errReviewerToken
	}
	value := strings.TrimSpace(string(raw))
	if value == "" {
		return "", errReviewerToken
	}
	return value, nil
}
