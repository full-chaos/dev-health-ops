package authapi

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-go/authverify"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

const (
	// AccessService is the service name of this service's endpoint-profile rows.
	AccessService = "dev-health-auth-service"
	// AccessTokenAudience is the audience an EdDSA access token carries to be
	// accepted at the session routes (access-token.v1 scope auth:session).
	AccessTokenAudience = "auth-service"

	// keyReadBound bounds one read of the key set on the verify path.
	keyReadBound = 2 * time.Second
)

var errAccessUnavailable = errors.New("authapi: access token validator unavailable")

// AuthServiceAccessRows is the access-token row table of the auth-service
// routes: every dev-health-auth-service row of the endpoint-profile inventory.
func AuthServiceAccessRows() ([]accessverify.Row, error) {
	return parseAccessRows(authv1.AuthServiceEndpointProfiles)
}

func parseAccessRows(raw []byte) ([]accessverify.Row, error) {
	var document struct {
		Rows []struct {
			Service               string   `json:"service"`
			Method                string   `json:"method"`
			Route                 string   `json:"route"`
			AcceptedClasses       []string `json:"accepted_credential_classes"`
			AccessTokenValidation *struct {
				LegacyHS256 bool `json:"legacy_hs256"`
			} `json:"access_token_validation"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("auth-service endpoint profiles: %w", err)
	}
	seen := map[string]bool{}
	var rows []accessverify.Row
	for _, entry := range document.Rows {
		if entry.Service != AccessService || entry.AccessTokenValidation == nil {
			continue
		}
		if entry.Method == "" || entry.Route == "" {
			return nil, errors.New("auth-service endpoint profiles: a row has no method or route")
		}
		profile := entry.Method + " " + entry.Route
		if seen[profile] {
			return nil, fmt.Errorf("auth-service endpoint profiles: %s has two rows", profile)
		}
		seen[profile] = true
		rows = append(rows, accessverify.Row{
			Service:     AccessService,
			Profile:     profile,
			AcceptsUser: slices.Contains(entry.AcceptedClasses, accessverify.ClassUser),
			LegacyHS256: entry.AccessTokenValidation.LegacyHS256,
		})
	}
	if len(rows) == 0 {
		return nil, errors.New("auth-service endpoint profiles: no access-token row")
	}
	return rows, nil
}

// publishedKeys resolves a kid against the key set the JWKS publishes: the
// same token.KeySet.Published() the JWKS handler serves, so a key the JWKS
// does not publish (retired, compromised) never verifies a token here.
type publishedKeys struct{ source KeySetSource }

func (p publishedKeys) ResolveKey(kid string) (ed25519.PublicKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), keyReadBound)
	defer cancel()
	set, err := p.source.KeySet(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", authverify.ErrKeySourceUnavailable, err)
	}
	for _, key := range set.Published() {
		if key.KeyID == kid {
			return key.PublicKey, nil
		}
	}
	return nil, nil
}

func (p publishedKeys) PublicKey(kid string) (ed25519.PublicKey, bool) {
	key, err := p.ResolveKey(kid)
	return key, err == nil && key != nil
}

// AccessCheck validates the bearer of a session route by the route's
// endpoint-profile row: an EdDSA user access token (audience auth-service)
// where the row accepts it, the HS256 compatibility token where the row opts in.
type AccessCheck struct {
	validator *accessverify.Validator
	eddsa     accessverify.EdDSAVerifier
	now       func() time.Time
}

// AccessCheckConfig builds an AccessCheck.
type AccessCheckConfig struct {
	// IssuerURL is the https iss the EdDSA tokens carry.
	IssuerURL string
	// Keys is the key set the JWKS publishes (*token.Signer).
	Keys KeySetSource
	// Legacy verifies the HS256 compatibility token.
	Legacy *Issuer
	// Rows is the row table; nil reads the auth-service inventory.
	Rows   []accessverify.Row
	Meter  metric.Meter
	Logger *slog.Logger
	Now    func() time.Time
}

type legacyOutcomeKey struct{}

// legacyOutcome carries the auth-outcome reason of a refused HS256 token out
// of the validator, which reports every refusal as one error.
type legacyOutcome struct{ reason string }

// NewAccessCheck builds the check over cfg.
func NewAccessCheck(cfg AccessCheckConfig) (*AccessCheck, error) {
	if cfg.Keys == nil || cfg.Legacy == nil {
		return nil, errors.New("authapi: the access check needs a key set and the legacy verifier")
	}
	rows := cfg.Rows
	if rows == nil {
		var err error
		if rows, err = AuthServiceAccessRows(); err != nil {
			return nil, err
		}
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/accessverify")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: cfg.IssuerURL, Audience: AccessTokenAudience, Keys: publishedKeys{source: cfg.Keys}})
	if err != nil {
		return nil, err
	}
	legacy := cfg.Legacy
	validator, err := accessverify.New(accessverify.Config{
		Rows:  rows,
		EdDSA: verifier,
		Legacy: func(ctx context.Context, tok string) (any, error) {
			bearer, reason, err := legacy.Verify(tok)
			if out, ok := ctx.Value(legacyOutcomeKey{}).(*legacyOutcome); ok {
				out.reason = reason
			}
			if err != nil {
				return nil, err
			}
			return bearer, nil
		},
		Meter: meter, Logger: cfg.Logger, Now: now,
	})
	if err != nil {
		return nil, err
	}
	return &AccessCheck{validator: validator, eddsa: verifier, now: now}, nil
}

// WithAccessCheck makes the Issuer's Verify path for the session routes go
// through check. Without one the routes verify the HS256 token only.
func (i *Issuer) WithAccessCheck(check *AccessCheck) *Issuer {
	i.access = check
	return i
}

// bearer verifies token for the route profile (the mux pattern, "METHOD /path").
func (i *Issuer) bearer(ctx context.Context, profile, token string) (Bearer, string, error) {
	if i.access == nil {
		return i.Verify(token)
	}
	return i.access.bearer(ctx, profile, token)
}

func (c *AccessCheck) bearer(ctx context.Context, profile, token string) (Bearer, string, error) {
	outcome := &legacyOutcome{}
	result, err := c.validator.Validate(context.WithValue(ctx, legacyOutcomeKey{}, outcome), AccessService, profile, token)
	switch {
	case errors.Is(err, accessverify.ErrUnavailable):
		return Bearer{}, reasonDependency, fmt.Errorf("%w: %w", errAccessUnavailable, err)
	case err != nil && outcome.reason != "":
		return Bearer{}, outcome.reason, err
	case err != nil:
		return Bearer{}, c.refusalReason(token), err
	}
	switch result.Class {
	case accessverify.ClassLegacy:
		if bearer, ok := result.Legacy.(Bearer); ok {
			return bearer, "", nil
		}
	case accessverify.ClassUser:
		return userBearer(result.Claims)
	}
	return Bearer{}, reasonMalformed, errBearerShape
}

// refusalReason names why the EdDSA verifier refuses token, in the
// auth-outcome vocabulary. Any other refusal (a row that does not accept the
// token's class, an unknown row, a malformed token) is malformed.
func (c *AccessCheck) refusalReason(token string) string {
	_, err := c.eddsa.Verify(token, c.now())
	switch authverify.ReasonOf(err) {
	case authverify.ReasonUnknownKid, authverify.ReasonSignature:
		return "bad_signature"
	case authverify.ReasonIssuer:
		return "issuer"
	case authverify.ReasonAudience:
		return "audience"
	case authverify.ReasonNotYetValid:
		return "not_yet_valid"
	case authverify.ReasonExpired:
		return "expired"
	}
	return reasonMalformed
}

// userBearer reads the subject and the session of an accepted EdDSA token.
// The session id is required: a token without a valid sid is malformed.
func userBearer(claims *authverify.AccessClaims) (Bearer, string, error) {
	if claims == nil {
		return Bearer{}, reasonMalformed, errBearerShape
	}
	principal, ok := parseWirePrincipal(claims.Subject)
	if !ok {
		return Bearer{}, reasonMalformed, errBearerShape
	}
	sessionID, err := session.ParseWireSessionID(claims.SessionID)
	if err != nil {
		return Bearer{}, reasonMalformed, errBearerShape
	}
	return Bearer{PrincipalID: principal, SessionID: sessionID, sessionPinned: true}, "", nil
}

func parseWirePrincipal(subject string) (uuid.UUID, bool) {
	encoded, ok := strings.CutPrefix(subject, "prn_")
	if !ok {
		return uuid.UUID{}, false
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil || len(raw) != len(uuid.UUID{}) {
		return uuid.UUID{}, false
	}
	var id uuid.UUID
	copy(id[:], raw)
	return id, id != uuid.Nil
}

// revisionStale reports a token whose revision no longer matches the user. An
// HS256 token carries tv. An EdDSA token has no tv: its session's
// legacy_token_version pin stands for it, and a session without a pin is stale.
func (b Bearer) revisionStale(record session.Record, user UserState) bool {
	if !b.sessionPinned {
		return user.TokenVersion != b.TokenVersion
	}
	return record.LegacyTokenVersion == nil || *record.LegacyTokenVersion != user.TokenVersion
}

// bearerRefusal is the answer to a bearer that did not verify, and the error
// to log with it: a validator outage is a retryable 503, anything else a 401.
func bearerRefusal(reason string, err error) (refusal, error) {
	if errors.Is(err, errAccessUnavailable) {
		return unavailable(reasonDependency), err
	}
	return unauthorized(reason), nil
}
