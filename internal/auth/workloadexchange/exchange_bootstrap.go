package workloadexchange

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// TokenTypeWorkloadBootstrap is the subject_token_type of a Compose bootstrap
// credential. The type alone selects the bootstrap path: the value never
// reaches TokenReview, and a JWT-typed subject never reaches the bootstrap
// registry.
const TokenTypeWorkloadBootstrap = "urn:dev-health:params:oauth:token-type:workload-bootstrap"

// oauthError is the RFC 6749 error answer; the value is one fixed code.
type oauthError struct {
	Error string `json:"error"`
}

// BootstrapRegistration is a resolved bootstrap credential. Audiences is the
// workload's audiences narrowed to the credential's; Narrow is the registry's.
type BootstrapRegistration struct {
	Registration
	CredentialID uuid.UUID
	// ExpiresAt is the credential's expiry; zero when it has none.
	ExpiresAt time.Time
}

// BootstrapRegistry resolves a bootstrap credential by the SHA-256 digest of
// the presented value (the workload store). It never sees the value.
type BootstrapRegistry interface {
	ResolveBootstrap(ctx context.Context, digest [sha256.Size]byte, now time.Time) (BootstrapRegistration, error)
}

// check refuses a Deps missing a collaborator.
func (d Deps) check() error {
	if d.Validator == nil || d.Registry == nil || d.RefusalReason == nil || d.Signer == nil || d.Audit == nil {
		return errors.New("workloadexchange: validator, registry, refusal reason, signer and audit are required")
	}
	if strings.TrimSpace(d.ClusterIssuer) == "" {
		return errors.New("workloadexchange: the cluster issuer is required")
	}
	return nil
}

// subjectOn reports whether the subject_token_type names a subject this
// route serves.
func (h *handler) subjectOn(subjectType string) bool { return subjectType == TokenTypeJWT }

// serveBootstrap is not implemented yet.
func (h *handler) serveBootstrap(w http.ResponseWriter, r *http.Request, req exchangeRequest) {
	h.answerAs(w, r, subjectBootstrap, outcomeRefused, reasonMalformed, audienceLabel(req.audience))
}
