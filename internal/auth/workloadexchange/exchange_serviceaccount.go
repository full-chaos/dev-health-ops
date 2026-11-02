package workloadexchange

import (
	"context"
	"crypto/sha256"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// TokenTypeServiceAccountKey is the subject_token_type of a service account
// key (fcsak_).
const TokenTypeServiceAccountKey = "urn:dev-health:params:oauth:token-type:service-account-key"

// ServiceAccountKeyRegistration is a live service account key as the exchange
// reads it. It holds no audience and no scope: the exchange ignores both.
type ServiceAccountKeyRegistration struct {
	CredentialID uuid.UUID
	PrincipalID  uuid.UUID
	OrgID        uuid.UUID
	ExpiresAt    time.Time
	Revision     int64
}

// ServiceAccountKeyRegistry resolves a service account key by the SHA-256 digest
// of the presented value. It never sees the value.
type ServiceAccountKeyRegistry interface {
	ResolveServiceAccountKey(ctx context.Context, digest [sha256.Size]byte, now time.Time) (ServiceAccountKeyRegistration, error)
}

// KeyRefusal is a registry refusal of a key; Reason is for the log and the
// counter only. Any other registry error is a store failure.
type KeyRefusal struct{ Reason string }

func (e *KeyRefusal) Error() string { return "workloadexchange: key refused: " + e.Reason }

func (h *handler) serveServiceAccountKey(w http.ResponseWriter, r *http.Request, req exchangeRequest) {
}
