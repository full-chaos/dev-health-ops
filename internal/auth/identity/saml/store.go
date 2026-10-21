package saml

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// FlowTTL bounds an SP-initiated sign-in from AuthnRequest to response.
const FlowTTL = 10 * time.Minute

// Flow is one stored AuthnRequest. Only hashes are stored.
type Flow struct {
	ProviderID  uuid.UUID
	RequestID   string
	BindingHash [32]byte
	ExpiresAt   time.Time
}

var (
	// ErrFlowNotFound: no unconsumed, unexpired flow for that request id,
	// provider and browser binding.
	ErrFlowNotFound = errors.New("saml: no open sign-in request matches the response")
	// ErrFlowConsumed: the request this response answers was already used.
	ErrFlowConsumed = errors.New("saml: the sign-in request was already used")
	// ErrAssertionReplayed: the assertion id was already used.
	ErrAssertionReplayed = errors.New("saml: assertion was already used")
	// ErrProviderNotFound: no sso_providers row with that id.
	ErrProviderNotFound = errors.New("saml: provider not found")
)

// FlowStore keeps SP-initiated requests (auth sso_flow_states, protocol
// "saml"). A flow is consumed at most once: ConsumeFlow returns
// ErrFlowConsumed for a used flow of this provider and browser, and
// ErrFlowNotFound for an unknown, expired, other-provider or other-browser one.
type FlowStore interface {
	SaveFlow(ctx context.Context, f Flow) error
	ConsumeFlow(ctx context.Context, providerID uuid.UUID, requestID string, bindingHash [32]byte, now time.Time) error
}

// AssertionUses refuses a second use of an assertion id within its validity
// window, across auth-service and the go-api replay table.
type AssertionUses interface {
	UseAssertion(ctx context.Context, providerID uuid.UUID, issuer, assertionID string, expiresAt time.Time) error
}

// ProviderReader reads sso_providers rows (go-api owns them).
type ProviderReader interface {
	Provider(ctx context.Context, id uuid.UUID) (Provider, error)
}

// Entitlements answers the organization feature gate for SSO.
type Entitlements interface {
	OrgHasSSO(ctx context.Context, orgID uuid.UUID) (bool, error)
}

// Identity is the key of an external identity: issuer = IdP entity ID,
// provider = the sso_providers row id, subject = Subject(NameID).
type Identity struct {
	Issuer   string
	Provider string
	Subject  string
}

// LegacyMatch lets the identity store find an account that go-api's SAML
// routes wrote before the users cutover: public.users with auth_provider =
// AuthProvider and auth_provider_id = Subject (the raw NameID value), accepted
// only when exactly one row matches and that user is a member of
// MembershipOrg.
type LegacyMatch struct {
	AuthProvider  string
	Subject       string
	MembershipOrg uuid.UUID
}

// Principal is what an identity lookup resolves to.
type Principal struct {
	ID       uuid.UUID
	Disabled bool
}

// EmailOwner is an existing account that holds an email.
type EmailOwner struct {
	PrincipalID uuid.UUID
	Method      string
}

// NewFederatedUser is the account ProvisionFederated creates. AuditRequestID
// is the server-issued request id for the audit actor (nil without one).
type NewFederatedUser struct {
	Email          string
	DisplayName    string
	Identity       Identity
	AuditRequestID *string
}

var (
	// ErrSignupNotAvailable: new accounts are not created by auth-service until
	// it owns the users aggregate. The identity store returns it (wrapped or not).
	ErrSignupNotAvailable = identity.ErrSignupNotAvailable
	// ErrIdentityTaken: the identity belongs to another principal.
	ErrIdentityTaken = identity.ErrIdentityTaken
	// ErrEmailTaken: the email belongs to an existing account.
	ErrEmailTaken = identity.ErrEmailTaken
	// ErrInvalidEmailAddress: the asserted email is not an address.
	ErrInvalidEmailAddress = identity.ErrInvalidEmail
	// ErrNotOrgMember: the one go-api account carrying the NameID is not a
	// member of the provider's organization.
	ErrNotOrgMember = errors.New("saml: the account is not a member of the provider organization")
	// ErrIdentityAmbiguous: more than one go-api account, in any
	// organization, carries the NameID.
	ErrIdentityAmbiguous = errors.New("saml: more than one account carries the NameID")
)

// Store is the identity store this package needs (internal/auth/identity).
// Every write runs in audit.Commit inside the implementation.
type Store interface {
	// IdentityBySubject resolves the identity key only. With legacy set, a
	// miss also tries the go-api account and, on a hit, projects the identity
	// under audit.Commit. It never matches by email. A go-api account outside
	// the provider's organization is ErrNotOrgMember; more than one go-api
	// account carrying the NameID is ErrIdentityAmbiguous.
	IdentityBySubject(ctx context.Context, id Identity, legacy *LegacyMatch) (Principal, bool, error)
	NormalizeEmail(raw string) (string, error)
	PrincipalByEmail(ctx context.Context, email string) (EmailOwner, bool, error)
	// ProvisionFederated creates the principal, user and identity in one
	// audited transaction, or returns ErrSignupNotAvailable.
	ProvisionFederated(ctx context.Context, user NewFederatedUser) (uuid.UUID, error)
}

// Memberships reads organization membership (go-api writes it until Wave 4).
type Memberships interface {
	Member(ctx context.Context, principalID, orgID uuid.UUID) (bool, error)
}

// MembershipJIT is the seam for organization membership on first sign-in.
// go-api owns membership writes until Wave 4; a nil MembershipJIT adds none.
type MembershipJIT interface {
	JoinOnSignIn(ctx context.Context, principalID uuid.UUID, p Provider) error
}
