package social

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// DefaultLinkFreshness is how recently a principal must have signed in to link
// or unlink an identity.
const DefaultLinkFreshness = 10 * time.Minute

// Domain errors. Each maps to one response; none carries provider text.
var (
	ErrProviderRefused          = errors.New("social: the provider token was refused")
	ErrEmailUnverified          = errors.New("social: the provider has not verified the email")
	ErrPrincipalDisabled        = errors.New("social: the principal is disabled")
	ErrReauthenticationRequired = errors.New("social: a recent sign-in is required")
	ErrAccountConflict          = errors.New("social: an existing account holds this email")
	ErrUnavailable              = errors.New("social: identity store unavailable")
	ErrProviderLinked           = errors.New("social: the principal already has an identity at this provider")
)

// ConflictError is ErrAccountConflict with the existing account's sign-in
// method. Nothing was written.
type ConflictError struct{ ExistingMethod string }

func (e *ConflictError) Error() string   { return ErrAccountConflict.Error() }
func (e *ConflictError) Is(t error) bool { return t == ErrAccountConflict }

// ProfileFetcher fetches a provider profile after checking that the token was issued
// to the configured client; *ProviderClient is the real one.
type ProfileFetcher interface {
	Configured(Provider) bool
	// Issuer is the issuer of the identities Fetch returns for p.
	Issuer(Provider) string
	Fetch(ctx context.Context, p Provider, token contracts.SecretString) (Profile, error)
}

// Config builds a Service.
type Config struct {
	Store     Store
	Providers ProfileFetcher
	// LinkFreshness bounds the age of the sign-in behind Link and Unlink;
	// 0 means DefaultLinkFreshness.
	LinkFreshness time.Duration
	Now           func() time.Time
	Logger        *slog.Logger
	Meter         metric.Meter
}

// Service is the social sign-in domain.
type Service struct {
	store     Store
	providers ProfileFetcher
	freshness time.Duration
	now       func() time.Time
	logger    *slog.Logger
	outcomes  metric.Int64Counter
}

// New validates cfg and builds a Service.
func New(cfg Config) (*Service, error) {
	if cfg.Store == nil || cfg.Providers == nil {
		return nil, errors.New("social: a store and a provider client are required")
	}
	s := &Service{
		store: cfg.Store, providers: cfg.Providers,
		freshness: cfg.LinkFreshness, now: cfg.Now, logger: cfg.Logger,
	}
	if s.freshness <= 0 {
		s.freshness = DefaultLinkFreshness
	}
	if s.now == nil {
		s.now = time.Now
	}
	if s.logger == nil {
		s.logger = slog.Default()
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/identity/social")
	}
	var err error
	if s.outcomes, err = meter.Int64Counter("auth.social.outcomes",
		metric.WithDescription("Social exchange, link and unlink outcomes by operation, provider and reason code.")); err != nil {
		return nil, fmt.Errorf("social: registering the outcome counter: %w", err)
	}
	return s, nil
}

// ExchangeResult is a signed-in principal.
type ExchangeResult struct {
	PrincipalID uuid.UUID
	Created     bool
}

// SignedIn is the authenticated session behind Link and Unlink. RequestID is
// the server-minted audit request id (requestid.Audit), never a client value.
type SignedIn struct {
	PrincipalID uuid.UUID
	AuthTime    time.Time
	RequestID   *string
}

// Exchange signs in the owner of a provider token. Only an identity already
// bound by (issuer, provider, subject) signs in to an existing principal. A
// verified email that an existing account holds is a *ConflictError and
// writes nothing. requestID is the server-minted audit request id, or nil.
func (s *Service) Exchange(ctx context.Context, provider Provider, token contracts.SecretString, requestID *string) (res ExchangeResult, err error) {
	reason := ReasonAccepted
	defer func() { s.record(ctx, OperationExchange, provider, reason, err) }()

	profile, reason, err := s.fetch(ctx, provider, token)
	if err != nil {
		return ExchangeResult{}, err
	}
	principal, found, err := s.lookup(ctx, profile.ExternalKey)
	if err != nil {
		reason = ReasonStoreUnavailable
		return ExchangeResult{}, err
	}
	if found {
		reason, err = signInTo(principal)
		return ExchangeResult{PrincipalID: principal.ID}, err
	}
	if !profile.EmailVerified {
		reason = ReasonEmailUnverified
		return ExchangeResult{}, ErrEmailUnverified
	}
	email, err := identity.NormalizeEmail(profile.Email)
	if err != nil {
		reason = ReasonEmailUnverified
		return ExchangeResult{}, ErrEmailUnverified
	}
	if method, taken, err := s.emailOwner(ctx, email); err != nil {
		reason = ReasonStoreUnavailable
		return ExchangeResult{}, err
	} else if taken {
		res, reason, err = s.settleTaken(ctx, profile.ExternalKey, method)
		return res, err
	}
	name := profile.FullName
	if name == "" {
		name = profile.Username
	}
	id, err := s.store.ProvisionFederated(ctx, identity.ProvisionRequest{Key: profile.ExternalKey, Email: email, FullName: name, Actor: identity.SelfSignUp(requestID)})
	switch {
	case err == nil:
		return ExchangeResult{PrincipalID: id, Created: true}, nil
	case errors.Is(err, identity.ErrSignupNotAvailable):
		reason = ReasonSignupUnavailable
		return ExchangeResult{}, err
	case errors.Is(err, identity.ErrIdentityTaken), errors.Is(err, identity.ErrEmailTaken):
		method, _, lookupErr := s.emailOwner(ctx, email)
		if lookupErr != nil {
			reason = ReasonStoreUnavailable
			return ExchangeResult{}, errors.Join(err, lookupErr)
		}
		res, reason, err = s.settleTaken(ctx, profile.ExternalKey, method)
		return res, err
	default:
		reason = ReasonStoreUnavailable
		return ExchangeResult{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
}

// lookup resolves key. Before the users cutover the identity store also finds
// a go-api account by the provider subject it stored, never by email.
func (s *Service) lookup(ctx context.Context, key identity.ExternalKey) (identity.Principal, bool, error) {
	legacy := &identity.LegacyMatch{AuthProvider: string(key.Provider)}
	p, found, err := s.store.IdentityBySubject(ctx, key.Issuer, key.Provider, key.Subject, legacy)
	if err != nil {
		return identity.Principal{}, false, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return p, found, nil
}

// emailOwner reports whether an account holds email, and the sign-in method
// to name in the conflict, so the user knows how to sign in before linking:
// go-api's auth_provider before the users cutover; after it "local" for an
// account with a password, else the provider of its oldest linked identity
// ("sso" for an enterprise SSO provider row), else "". More than one account
// holding the email is a conflict too.
func (s *Service) emailOwner(ctx context.Context, email string) (string, bool, error) {
	owner, err := s.store.ByEmail(ctx, email)
	switch {
	case errors.Is(err, identity.ErrMultipleUsers):
		return "", true, nil
	case err != nil:
		return "", false, fmt.Errorf("%w: %w", ErrUnavailable, err)
	case owner == nil:
		return "", false, nil
	case owner.AuthProvider != nil:
		return *owner.AuthProvider, true, nil
	}
	hasPassword, err := s.store.HasPassword(ctx, owner.ID)
	if err != nil {
		return "", false, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if hasPassword {
		return "local", true, nil
	}
	linked, err := s.store.ListIdentities(ctx, owner.ID)
	if err != nil {
		return "", false, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if len(linked) == 0 {
		return "", true, nil
	}
	if p := linked[0].Provider; p.IsSocial() {
		return string(p), true, nil
	}
	return "sso", true, nil
}

// settleTaken answers an exchange whose email is held by an account. A
// concurrent first exchange of the same identity may be that account; only a
// binding of this identity signs in, otherwise it is a conflict.
func (s *Service) settleTaken(ctx context.Context, key identity.ExternalKey, method string) (ExchangeResult, Reason, error) {
	principal, found, err := s.lookup(ctx, key)
	if err != nil {
		return ExchangeResult{}, ReasonStoreUnavailable, err
	}
	if found {
		reason, err := signInTo(principal)
		return ExchangeResult{PrincipalID: principal.ID}, reason, err
	}
	return ExchangeResult{}, ReasonEmailCollision, &ConflictError{ExistingMethod: method}
}

func signInTo(p identity.Principal) (Reason, error) {
	if p.DisabledAt != nil {
		return ReasonPrincipalDeactivated, ErrPrincipalDisabled
	}
	return ReasonAccepted, nil
}

// Link binds the token owner's provider identity to the signed-in principal.
// Linking the principal's own identity again is a no-op. One identity per
// provider per principal.
func (s *Service) Link(ctx context.Context, who SignedIn, provider Provider, token contracts.SecretString) (linked identity.Identity, err error) {
	reason := ReasonAccepted
	defer func() { s.record(ctx, OperationLink, provider, reason, err) }()

	if reason, err = s.fresh(who); err != nil {
		return identity.Identity{}, err
	}
	profile, reason, err := s.fetch(ctx, provider, token)
	if err != nil {
		return identity.Identity{}, err
	}
	key := profile.ExternalKey
	principal, found, err := s.lookup(ctx, key)
	if err != nil {
		reason = ReasonStoreUnavailable
		return identity.Identity{}, err
	}
	if found && principal.ID != who.PrincipalID {
		reason = ReasonEmailCollision
		return identity.Identity{}, identity.ErrIdentityConflict
	}
	all, err := s.store.ListIdentities(ctx, who.PrincipalID)
	if err != nil {
		reason = ReasonStoreUnavailable
		return identity.Identity{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	for _, have := range all {
		if have.Issuer == key.Issuer && have.Provider == key.Provider {
			if have.Subject == key.Subject {
				return have, nil
			}
			reason = ReasonEmailCollision
			return identity.Identity{}, ErrProviderLinked
		}
	}
	actor := identity.Actor{PrincipalID: &who.PrincipalID, RequestID: who.RequestID}
	id, err := s.store.LinkIdentity(ctx, key, who.PrincipalID, actor)
	switch {
	case err == nil:
		return identity.Identity{ID: id, Issuer: key.Issuer, Provider: key.Provider, Subject: key.Subject}, nil
	case errors.Is(err, identity.ErrIdentityConflict):
		reason = ReasonEmailCollision
		return identity.Identity{}, err
	case errors.Is(err, identity.ErrPrincipalUnavailable):
		reason = ReasonPrincipalDeactivated
		return identity.Identity{}, ErrPrincipalDisabled
	default:
		reason = ReasonStoreUnavailable
		return identity.Identity{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
}

// Unlink removes one of the signed-in principal's identities by its id. An id
// that is not the principal's is identity.ErrNotFound.
func (s *Service) Unlink(ctx context.Context, who SignedIn, identityID uuid.UUID) (err error) {
	reason := ReasonAccepted
	var provider Provider
	defer func() { s.record(ctx, OperationUnlink, provider, reason, err) }()

	if reason, err = s.fresh(who); err != nil {
		return err
	}
	all, err := s.store.ListIdentities(ctx, who.PrincipalID)
	if err != nil {
		reason = ReasonStoreUnavailable
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	var key *identity.ExternalKey
	for _, have := range all {
		if have.ID == identityID {
			key = &identity.ExternalKey{Issuer: have.Issuer, Provider: have.Provider, Subject: have.Subject}
		}
	}
	if key == nil {
		reason = ReasonPrincipalNotFound
		return identity.ErrNotFound
	}
	provider = key.Provider
	err = s.store.UnlinkIdentity(ctx, *key, who.PrincipalID, identity.Actor{PrincipalID: &who.PrincipalID, RequestID: who.RequestID})
	switch {
	case err == nil:
		return nil
	case errors.Is(err, identity.ErrNotFound), errors.Is(err, identity.ErrNotOwner):
		reason = ReasonPrincipalNotFound
		return identity.ErrNotFound
	case errors.Is(err, identity.ErrLastSignInMethod):
		reason = ReasonInvalidCredentials
		return err
	default:
		reason = ReasonStoreUnavailable
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
}

// fresh refuses a sign-in older than the freshness bound, or one with no time.
func (s *Service) fresh(who SignedIn) (Reason, error) {
	if who.PrincipalID == uuid.Nil || who.AuthTime.IsZero() {
		return ReasonMalformed, ErrReauthenticationRequired
	}
	age := s.now().Sub(who.AuthTime)
	if age > s.freshness || age < -time.Minute {
		return ReasonExpired, ErrReauthenticationRequired
	}
	return ReasonAccepted, nil
}

func (s *Service) fetch(ctx context.Context, provider Provider, token contracts.SecretString) (Profile, Reason, error) {
	if _, ok := ParseProvider(string(provider)); !ok || !s.providers.Configured(provider) {
		return Profile{}, ReasonDependency, ErrProviderNotConfigured
	}
	profile, err := s.providers.Fetch(ctx, provider, token)
	var perr *ProviderError
	switch {
	case err == nil:
	case errors.As(err, &perr):
		return Profile{}, perr.Reason, fmt.Errorf("%w: %s", ErrProviderRefused, perr.Step)
	case errors.Is(err, ErrProviderNotConfigured):
		return Profile{}, ReasonDependency, err
	default:
		return Profile{}, ReasonDependency, ErrProviderRefused
	}
	if profile.Issuer != s.providers.Issuer(provider) || profile.Provider != provider || profile.Validate() != nil {
		return Profile{}, ReasonAudience, fmt.Errorf("%w: profile identity does not match the provider", ErrProviderRefused)
	}
	return profile, ReasonAccepted, nil
}

func (s *Service) record(ctx context.Context, op Operation, provider Provider, reason Reason, err error) {
	if provider != "" && !provider.IsSocial() {
		provider = "invalid"
	}
	s.outcomes.Add(ctx, 1, metric.WithAttributes(
		attribute.String("operation", string(op)),
		attribute.String("provider", string(provider)),
		attribute.String("reason_code", string(reason)),
	))
	attrs := []any{"operation", string(op), "provider", string(provider), "reason_code", string(reason)}
	switch {
	case err == nil:
		s.logger.InfoContext(ctx, "social outcome", attrs...)
	case reason == ReasonStoreUnavailable || reason == ReasonDependency:
		s.logger.ErrorContext(ctx, "social outcome failed", append(attrs, logging.ErrorAttr(err))...)
	default:
		s.logger.InfoContext(ctx, "social outcome refused", attrs...)
	}
}
