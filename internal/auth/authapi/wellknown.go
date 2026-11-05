package authapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// Well-known route patterns. The endpoint-profile rows name these exact strings.
const (
	PatternJWKS      = contracts.DiscoveryJWKSPath
	PatternDiscovery = "/.well-known/openid-configuration"
)

// wellKnownMaxAge bounds how long a verifier or an intermediary may reuse a
// key set or discovery document. It is far below the overlap a rotation
// leaves a retiring key published for, so a verifier never holds a document
// that omits a key still signing live tokens. The rotation floors are built
// on the same constant.
const wellKnownMaxAge = int(token.JWKSCacheTTL / time.Second)

// KeySetSource reads the current signing key set (*token.Signer).
type KeySetSource interface {
	KeySet(ctx context.Context) (token.KeySet, error)
}

// WellKnownDeps are the discovery and JWKS handlers' collaborators.
type WellKnownDeps struct {
	// JWKS is the last-known key set document the JWKS route serves; a
	// request reads it in memory only. Nil answers 503.
	JWKS *JWKSDocument
	// IssuerURL is the https issuer the access tokens carry as iss. Empty
	// mounts the JWKS route only: a discovery document cannot conform without
	// a pinned https issuer, and the issuer is never derived from a request.
	IssuerURL string
	Logger    *slog.Logger
}

type jwk struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	Kid string `json:"kid"`
	X   string `json:"x"`
}

type jwksDocument struct {
	Keys []jwk `json:"keys"`
}

type discoveryDocument struct {
	Issuer  string   `json:"issuer"`
	JWKSURI string   `json:"jwks_uri"`
	Algs    []string `json:"id_token_signing_alg_values_supported"`
}

// WellKnownRoutes returns GET and HEAD of the JWKS and, when an issuer is
// configured, of the discovery document. Each registration is on a line of
// its own: the endpoint-profile rows anchor to these lines.
func WellKnownRoutes(deps WellKnownDeps) []httpapi.Route {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	routes := []httpapi.Route{
		{Method: http.MethodGet, Pattern: PatternJWKS, Handler: http.HandlerFunc(deps.serveJWKS)},
		{Method: http.MethodHead, Pattern: PatternJWKS, Handler: http.HandlerFunc(deps.serveJWKS)},
	}
	if deps.IssuerURL == "" {
		return markPublicClass(routes)
	}
	return markPublicClass(append(routes,
		httpapi.Route{Method: http.MethodGet, Pattern: PatternDiscovery, Handler: http.HandlerFunc(deps.serveDiscovery)},
		httpapi.Route{Method: http.MethodHead, Pattern: PatternDiscovery, Handler: http.HandlerFunc(deps.serveDiscovery)},
	))
}

// markPublicClass puts every route in the public-document admission class:
// none of them does dependency I/O on a request.
func markPublicClass(routes []httpapi.Route) []httpapi.Route {
	for i := range routes {
		routes[i].PublicClass = true
	}
	return routes
}

func (d WellKnownDeps) serveJWKS(w http.ResponseWriter, r *http.Request) {
	snapshot := d.JWKS.load()
	if snapshot == nil {
		writeKeySetUnavailable(w, r)
		return
	}
	writeDocument(w, r, snapshot.raw, snapshot.etag)
}

func (d WellKnownDeps) serveDiscovery(w http.ResponseWriter, r *http.Request) {
	writeCacheable(w, r, discoveryDocument{
		Issuer: d.IssuerURL, JWKSURI: d.IssuerURL + contracts.DiscoveryJWKSPath, Algs: []string{"EdDSA"},
	})
}

// writeCacheable writes a public, bounded-cache JSON body with a strong ETag
// and answers a matching If-None-Match with 304. HEAD carries the headers only.
func writeCacheable(w http.ResponseWriter, r *http.Request, body any) {
	raw, err := json.Marshal(body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	writeDocument(w, r, raw, etagOf(raw))
}

// etagOf is the strong ETag of a rendered document.
func etagOf(raw []byte) string {
	sum := sha256.Sum256(raw)
	return `"` + hex.EncodeToString(sum[:16]) + `"`
}

// writeDocument writes raw as writeCacheable does.
func writeDocument(w http.ResponseWriter, r *http.Request, raw []byte, etag string) {
	h := w.Header()
	h.Set("Cache-Control", "public, max-age="+strconv.Itoa(wellKnownMaxAge))
	h.Set("ETag", etag)
	if etagMatches(r.Header.Values("If-None-Match"), etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(raw)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = io.Copy(w, bytes.NewReader(raw))
	}
}

// etagMatches is the weak comparison RFC 9110 section 13.1.2 prescribes for
// If-None-Match: a W/ prefix on a presented tag is ignored.
func etagMatches(values []string, etag string) bool {
	for _, value := range values {
		for _, candidate := range strings.Split(value, ",") {
			candidate = strings.TrimPrefix(strings.TrimSpace(candidate), "W/")
			if candidate == "*" || candidate == etag {
				return true
			}
		}
	}
	return false
}

// writeKeySetUnavailable is the 503 error.v1 answer: no key material, never
// cached, retryable.
func writeKeySetUnavailable(w http.ResponseWriter, r *http.Request) {
	retry := retryAfterSeconds
	w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
	writeJSON(w, http.StatusServiceUnavailable, errorDocument{
		SchemaVersion: contracts.ErrorSurface, Status: http.StatusServiceUnavailable, ReasonCode: reasonDependency,
		RequestID: requestid.Wire(r.Context()), OccurredAt: contracts.Timestamp{Time: time.Now().UTC()},
		RetryAfterSeconds: &retry,
	})
}
