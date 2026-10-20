package oidc

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The flow is not implemented yet: every entry point refuses.

var errNotImplemented = errors.New("oidc: not implemented")

const (
	stateTTL        = 10 * time.Minute
	maxStateBytes   = 4096
	maxSubjectBytes = 1024
	clockSkew       = 60 * time.Second
	ProviderKind    = "oidc"
)

var AMR = []string{"fed"}

type flowState struct {
	ProviderID, OrgID, Nonce, CodeVerifier, RedirectURI, NonceHash, ID string
	IssuedAt, ExpiresAt                                                int64
}

var (
	errStateInvalid   = errors.New("oidc: state does not authenticate")
	errStateExpired   = errors.New("oidc: state expired")
	errMalformedToken = errors.New("oidc: malformed ID token")
	errAlgNotAllowed  = errors.New("oidc: ID token algorithm not allowed")
)

func sealState(string, flowState, time.Time) (string, flowState, error) {
	return "", flowState{}, errNotImplemented
}
func openState(string, string, string, time.Time) (flowState, error) {
	return flowState{}, errStateInvalid
}
func hashLoginNonce(string) string { return "" }

type joseHeader struct{ Alg, Kid string }

func parseJOSEHeader(string) (joseHeader, error) { return joseHeader{}, errNotImplemented }
func algAllowed(string) bool                     { return false }
func checkJSONObject([]byte) error               { return errNotImplemented }

type claimRules struct {
	Issuer, ClientID, Nonce string
	Now                     time.Time
}

type idClaims struct {
	Issuer, Subject string
	All             map[string]any
}

func checkClaims([]byte, claimRules) (idClaims, error) { return idClaims{}, errNotImplemented }
func reasonOf(error) Reason                            { return ReasonMalformed }
func stringOrStrings(any) ([]string, bool)             { return nil, false }
func contains([]string, string) bool                   { return false }
func validSubject(string) bool                         { return false }

type profile struct {
	Email         string
	EmailVerified bool
	FullName      string
}

func readProfile(map[string]any, map[string]string) profile { return profile{} }

type fieldError struct{ Type, Msg string }

type callbackBody struct{ Code, State string }

func parseCallbackBody([]byte) (callbackBody, []fieldError) {
	return callbackBody{}, []fieldError{{Type: "not_implemented"}}
}

func validateHTTPSOrLoopback(string) error { return errNotImplemented }

type ExternalKey struct{ Issuer, Provider, Subject string }

type Principal struct {
	ID     uuid.UUID
	Active bool
}

type LegacyMatch struct {
	AuthProvider  string
	MembershipOrg uuid.UUID
}

type FederatedAccount struct {
	Key             ExternalKey
	Email, FullName string
	RequestID       *string
}

type IdentityStore interface {
	IdentityBySubject(ctx context.Context, key ExternalKey, legacy LegacyMatch) (Principal, error)
	EmailInUse(ctx context.Context, email string) (bool, error)
	ProvisionFederated(ctx context.Context, account FederatedAccount) (Principal, error)
}

type MembershipReader interface {
	Role(ctx context.Context, principalID, orgID uuid.UUID) (string, bool, error)
}

type MembershipJIT interface {
	Join(ctx context.Context, principalID, orgID uuid.UUID) (string, error)
}

type Entitlements interface {
	SSOAllowed(ctx context.Context, orgID uuid.UUID) (bool, error)
	DeniedDetail() any
}

type SessionRequest struct {
	PrincipalID uuid.UUID
	OrgID       *uuid.UUID
	Role        string
	AMR         []string
}

type Tokens struct {
	AccessToken, RefreshToken string
	ExpiresIn                 int64
}

type SessionIssuer interface {
	Issue(ctx context.Context, req SessionRequest) (Tokens, error)
}

type Config struct {
	Providers     ProviderSource
	Flows         FlowStore
	Identities    IdentityStore
	Memberships   MembershipReader
	MembershipJIT MembershipJIT
	Entitlements  Entitlements
	Sessions      SessionIssuer
	Secrets       SecretDecrypter
	StateSecret   contracts.SecretString
	AppBaseURL    string
	HTTPClient    *http.Client
	Now           func() time.Time
	Logger        *slog.Logger
	Meter         metric.Meter
}

type Service struct{ cfg Config }

func New(cfg Config) (*Service, error) { return &Service{cfg: cfg}, nil }

type AuthorizeResult struct {
	AuthorizationURL, State string
	Cookie                  *http.Cookie
}

func (s *Service) Authorize(context.Context, uuid.UUID, string) (AuthorizeResult, error) {
	return AuthorizeResult{}, errNotImplemented
}

type CallbackInput struct {
	Code, State string
	RequestID   *string
	Cookie      func(name string) (string, bool)
}

type Login struct {
	PrincipalID uuid.UUID
	Email       string
	OrgID       *uuid.UUID
	Role        string
	Tokens      Tokens
}

func (s *Service) Callback(context.Context, uuid.UUID, CallbackInput) (Login, *http.Cookie, error) {
	return Login{}, nil, errNotImplemented
}

func (s *Service) AuthorizeHandler() http.Handler { return http.NotFoundHandler() }
func (s *Service) CallbackHandler() http.Handler  { return http.NotFoundHandler() }

type StoreIdentities struct{ Store *identity.Store }

func (StoreIdentities) IdentityBySubject(context.Context, ExternalKey, LegacyMatch) (Principal, error) {
	return Principal{}, errNotImplemented
}
func (StoreIdentities) EmailInUse(context.Context, string) (bool, error) {
	return false, errNotImplemented
}
func (StoreIdentities) ProvisionFederated(context.Context, FederatedAccount) (Principal, error) {
	return Principal{}, errNotImplemented
}

type LicensingEntitlements struct{ Pool *pgxpool.Pool }

func (LicensingEntitlements) SSOAllowed(context.Context, uuid.UUID) (bool, error) { return true, nil }
func (LicensingEntitlements) DeniedDetail() any                                   { return "not licensed" }
