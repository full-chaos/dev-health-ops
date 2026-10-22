package local

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// Purpose is a link credential purpose (link_credentials.purpose, 0010).
type Purpose string

const (
	PurposeEmailVerification Purpose = "email_verification"
	PurposePasswordReset     Purpose = "password_reset"
)

// Link credential lifetimes are go-api's: create_email_verification_token
// ttl_hours=24, create_password_reset_token ttl_hours=1.
const (
	VerificationTTL = 24 * time.Hour
	ResetTTL        = time.Hour
)

// ProvisionLocalRequest creates a local account. PasswordHash is a finished
// bcrypt hash, never the password.
type ProvisionLocalRequest struct {
	Email        string
	FullName     *string
	PasswordHash string
}

// Accounts is the B2 identity store as the link flows use it. ByEmail and
// ByID read the live user. The three writes are D3-gated inside B2
// (identity.ErrSignupNotAvailable while Config.UsersOwnedByAuth is off) and
// each commits through audit.Commit.
type Accounts interface {
	ByEmail(ctx context.Context, normalizedEmail string) (*User, error)
	ByID(ctx context.Context, id uuid.UUID) (*User, error)
	// ProvisionLocal returns identity.ErrEmailTaken for a taken email.
	ProvisionLocal(ctx context.Context, req ProvisionLocalRequest) (uuid.UUID, error)
	SetEmailVerified(ctx context.Context, id uuid.UUID) error
	// SetPasswordEndingSessions sets the password and ends every other
	// credential of the principal (auth sessions, go-api token version and
	// refresh tokens) in one transaction: an error leaves the password as it
	// was.
	SetPasswordEndingSessions(ctx context.Context, id uuid.UUID, hash string) error
}

// SessionAdmin is the B1 session store as the flows use it.
type SessionAdmin interface {
	Sessions
}

// IssueRequest asks Links for one new credential. A zero PrincipalID is a
// decoy: the store runs the same statements but writes nothing, so a refused
// address and an accepted one make the same call.
type IssueRequest struct {
	PrincipalID uuid.UUID
	Purpose     Purpose
	TokenHash   [32]byte
	TTL         time.Duration
}

// RedeemRequest consumes one credential. ReasonCode and EventType name the
// audit row the consume commits with.
type RedeemRequest struct {
	Purpose   Purpose
	TokenHash [32]byte
}

// Links is the link_credentials store (0010). Redeem returns ok=false for an
// unknown, expired, consumed, replaced or wrong-purpose credential, all the
// same refusal; the database clock decides expiry.
type Links interface {
	Issue(ctx context.Context, req IssueRequest) error
	Redeem(ctx context.Context, req RedeemRequest) (principalID uuid.UUID, ok bool, err error)
}

// Message is one link e-mail. Deliver false is a decoy: the sender does the
// same work and sends nothing.
type Message struct {
	Purpose  Purpose
	To       string
	FullName *string
	Secret   contracts.SecretString
	Deliver  bool
}

// Mailer sends link e-mail, best effort.
type Mailer interface {
	Send(ctx context.Context, msg Message) error
}

// Locker runs fn while it holds a per-principal lock, so the flows that set a
// password for one principal run one at a time. fn's error is returned after
// the lock is released.
type Locker interface {
	WithPrincipalLock(ctx context.Context, principalID uuid.UUID, fn func(ctx context.Context) error) error
}
