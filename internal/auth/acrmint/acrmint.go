// Package acrmint mints the EdDSA user access token for the two ACR audiences,
// acr-api and acr-mcp (CHAOS-12250, plan step O2 of CHAOS-12082), beside the
// session token class and apart from it (ACP-ADR-13 decision 3).
//
// It signs with the auth-service signer and issuer the workload exchange
// already uses (internal/auth/authruntime/runtime.go builds the signer;
// AUTH_TOKEN_ISSUER_URL is the issuer). It mints an access token only: no
// refresh token and no session credential (G-43 condition 2).
//
// The package is dark. No production binary constructs a Minter: O3's token
// endpoint is its first caller. Until then only tests build one, and the
// census test in this package fails if a production file imports it or if the
// user_access_token class row stops being unconsumed.
package acrmint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// DefaultLifetime is the life of a minted token when a request names none:
// the design's 10 minutes, inside contracts.AccessTokenLifetimeCeiling.
const DefaultLifetime = 10 * time.Minute

// tokenTyp is the JWS typ of an access-token.v1 token.
const tokenTyp = "at+jwt"

// MintsMetric counts every Mint by audience and outcome.
const MintsMetric = "auth.acr.access_mints"

// The fixed outcome set of MintsMetric. No other value is ever recorded.
const (
	OutcomeMinted                   = "minted"
	OutcomeRefusedAudience          = "refused_audience"
	OutcomeRefusedOrganization      = "refused_org"
	OutcomeRefusedLifetime          = "refused_lifetime"
	OutcomeRefusedSubject           = "refused_subject"
	OutcomeRefusedImpersonating     = "refused_impersonating"
	OutcomeImpersonationUnavailable = "impersonation_unavailable"
	OutcomeSignerUnavailable        = "signer_unavailable"
)

// issuerPattern is the discovery.v1 issuer shape, the one access-token.v1's
// iss takes (a test pins it to the schema): an https origin with an optional
// path prefix, no trailing slash, userinfo, query or fragment.
var issuerPattern = regexp.MustCompile(`^https://[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?(:[0-9]{1,5})?(/[A-Za-z0-9_~-][A-Za-z0-9._~-]{0,63}){0,8}$`)

// otherAudience is the metric label of an audience that is not an ACR one, so
// a caller's value never becomes a label.
const otherAudience = "other"

// The refusals of Mint, one per outcome; errors.Is tells them apart.
var (
	ErrAudience                 = errors.New("acrmint: the audience is not an ACR audience")
	ErrOrganization             = errors.New("acrmint: an ACR access token needs an organization")
	ErrLifetime                 = errors.New("acrmint: the lifetime is not inside the access token ceiling")
	ErrSubject                  = errors.New("acrmint: the principal or the session is missing")
	ErrImpersonating            = errors.New("acrmint: the principal holds an active impersonation")
	ErrImpersonationUnavailable = errors.New("acrmint: the impersonation state could not be read")
	ErrSignerUnavailable        = errors.New("acrmint: the signer could not sign")
)

// Signer signs a JSON payload as a compact JWS with the protected header
// {alg EdDSA, kid, typ}; *token.Signer is the production one.
type Signer interface {
	Sign(ctx context.Context, typ string, payload []byte) (token, kid string, err error)
}

// Impersonation reads one side of an impersonation: whether principal is the
// impersonating ACTOR (the admin) of an active impersonation session, an
// impersonation_sessions row with admin_user_id = principal that has not
// ended and has not expired. The target of an impersonation is not that side,
// and this check does not refuse it (acp's ruling on the O2 plan, condition 2).
// A read error is returned as an error, never as false.
type Impersonation interface {
	ImpersonatingActor(ctx context.Context, principal uuid.UUID) (bool, error)
}

// Config builds a Minter. Signer, IssuerURL and Impersonation are required.
type Config struct {
	Signer        Signer
	IssuerURL     string
	Impersonation Impersonation
	// Meter records MintsMetric; nil records nothing.
	Meter metric.Meter
	// Logger writes one Warn line per refusal; nil discards it, so the caller
	// that wires the mint (O3) passes its service logger.
	Logger *slog.Logger
	// Now is the clock; nil is time.Now.
	Now func() time.Time
}

// Request is one token to mint.
type Request struct {
	// Audience is acr-api or acr-mcp; any other value is refused.
	Audience string
	// PrincipalID is the human principal (sub, as session.WirePrincipalID).
	PrincipalID uuid.UUID
	// SessionID is the client's own ACP session (sid), never the web session's.
	SessionID uuid.UUID
	// OrganizationID is required for an ACR audience.
	OrganizationID uuid.UUID
	// Lifetime is exp - iat; zero is DefaultLifetime. A value over
	// contracts.AccessTokenLifetimeCeiling, or below one second, is refused,
	// never clamped.
	Lifetime time.Duration
}

// Minter mints ACR-audience access tokens.
type Minter struct {
	signer        Signer
	issuer        string
	impersonation Impersonation
	mints         metric.Int64Counter
	logger        *slog.Logger
	now           func() time.Time
}

// New validates cfg and returns a Minter.
func New(cfg Config) (*Minter, error) {
	if cfg.Signer == nil {
		return nil, errors.New("acrmint: a signer is required")
	}
	if !issuerPattern.MatchString(cfg.IssuerURL) {
		return nil, errors.New("acrmint: the issuer URL is not an access-token.v1 issuer (https, lowercase host, no trailing slash)")
	}
	if cfg.Impersonation == nil {
		return nil, errors.New("acrmint: an impersonation reader is required")
	}
	m := &Minter{signer: cfg.Signer, issuer: cfg.IssuerURL, impersonation: cfg.Impersonation, logger: cfg.Logger, now: cfg.Now}
	if m.logger == nil {
		m.logger = slog.New(slog.DiscardHandler)
	}
	if m.now == nil {
		m.now = time.Now
	}
	if cfg.Meter != nil {
		mints, err := cfg.Meter.Int64Counter(MintsMetric,
			metric.WithDescription("ACR-audience access tokens minted or refused, by audience and outcome."))
		if err != nil {
			return nil, fmt.Errorf("acrmint: %w", err)
		}
		m.mints = mints
	}
	return m, nil
}

// Mint returns the compact token and the kid that signed it.
func (m *Minter) Mint(ctx context.Context, r Request) (string, string, error) {
	label := otherAudience
	scope, acr := contracts.ACRAccessTokenScope(r.Audience)
	if acr {
		label = r.Audience
	}
	if !acr {
		return m.refuse(ctx, label, r, OutcomeRefusedAudience, ErrAudience)
	}
	if r.OrganizationID == uuid.Nil {
		return m.refuse(ctx, label, r, OutcomeRefusedOrganization, ErrOrganization)
	}
	if r.PrincipalID == uuid.Nil || r.SessionID == uuid.Nil {
		return m.refuse(ctx, label, r, OutcomeRefusedSubject, ErrSubject)
	}
	lifetime := r.Lifetime
	if lifetime == 0 {
		lifetime = DefaultLifetime
	}
	if lifetime < time.Second || lifetime > contracts.AccessTokenLifetimeCeiling || lifetime%time.Second != 0 {
		return m.refuse(ctx, label, r, OutcomeRefusedLifetime, ErrLifetime)
	}
	impersonating, err := m.impersonation.ImpersonatingActor(ctx, r.PrincipalID)
	if err != nil {
		return m.refuse(ctx, label, r, OutcomeImpersonationUnavailable, ErrImpersonationUnavailable)
	}
	if impersonating {
		return m.refuse(ctx, label, r, OutcomeRefusedImpersonating, ErrImpersonating)
	}
	now := m.now()
	org := r.OrganizationID.String()
	claims := contracts.AccessTokenClaims{
		Iss:           m.issuer,
		Aud:           r.Audience,
		Sub:           session.WirePrincipalID(r.PrincipalID),
		PrincipalType: "human",
		Sid:           session.WireSessionID(r.SessionID),
		Jti:           uuid.NewString(),
		Iat:           now.Unix(),
		Nbf:           now.Unix(),
		Exp:           now.Add(lifetime).Unix(),
		OrgID:         &org,
		Scope:         scope,
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return m.refuse(ctx, label, r, OutcomeSignerUnavailable, ErrSignerUnavailable)
	}
	token, kid, err := m.signer.Sign(ctx, tokenTyp, payload)
	if err != nil {
		return m.refuse(ctx, label, r, OutcomeSignerUnavailable, ErrSignerUnavailable)
	}
	m.count(ctx, label, OutcomeMinted)
	return token, kid, nil
}

// refuse counts and logs one refusal. The Warn line's message is the fixed
// text of err, and its fields are the organization and the audience label
// only: no principal, no session, no token and no kid (acp condition 4).
func (m *Minter) refuse(ctx context.Context, label string, r Request, outcome string, err error) (string, string, error) {
	m.count(ctx, label, outcome)
	org := ""
	if r.OrganizationID != uuid.Nil {
		org = r.OrganizationID.String()
	}
	m.logger.WarnContext(ctx, err.Error(), slog.String("org_id", org), slog.String("audience", label))
	return "", "", err
}

func (m *Minter) count(ctx context.Context, label, outcome string) {
	if m.mints == nil {
		return
	}
	m.mints.Add(ctx, 1, metric.WithAttributes(attribute.String("audience", label), attribute.String("outcome", outcome)))
}
