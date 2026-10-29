package workloadexchange

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
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

// check refuses a Deps with neither subject on, or with a subject missing a
// collaborator.
func (d Deps) check() error {
	if d.RefusalReason == nil || d.Signer == nil || d.Audit == nil {
		return errors.New("workloadexchange: refusal reason, signer and audit are required")
	}
	if d.Validator == nil && d.Bootstrap == nil {
		return errors.New("workloadexchange: a TokenReview validator or a bootstrap registry is required")
	}
	if d.Validator != nil && d.Registry == nil {
		return errors.New("workloadexchange: the TokenReview subject needs the workload registry")
	}
	if d.Validator != nil && strings.TrimSpace(d.ClusterIssuer) == "" {
		return errors.New("workloadexchange: the cluster issuer is required")
	}
	return nil
}

// subjectOn reports whether the subject_token_type names a subject this
// route serves.
func (h *handler) subjectOn(subjectType string) bool {
	switch subjectType {
	case TokenTypeJWT:
		return h.Validator != nil
	case TokenTypeWorkloadBootstrap:
		return h.Bootstrap != nil
	}
	return false
}

// serveBootstrap is the bootstrap subject: a value outside the credential's
// shape is refused before any store read, the registry resolves the digest of
// the rest, and the answers are the Kubernetes path's answers.
func (h *handler) serveBootstrap(w http.ResponseWriter, r *http.Request, req exchangeRequest) {
	ctx := r.Context()
	audience := audienceLabel(req.audience)
	refuse := func(reason string) { h.answerAs(w, r, subjectBootstrap, outcomeRefused, reason, audience) }
	if !workload.ValidBootstrapShape(req.subjectToken) {
		refuse(reasonMalformed)
		return
	}
	now := h.Now()
	reg, err := h.Bootstrap.ResolveBootstrap(ctx, workload.BootstrapDigest(req.subjectToken), now)
	if err != nil {
		refuse(bootstrapResolveReason(h.RefusalReason(err)))
		return
	}
	if reg.Narrow == nil {
		refuse(reasonResolveRefused)
		return
	}
	if !isWorkloadAudience(req.audience) {
		refuse(reasonAudienceRefused)
		return
	}
	actions, err := reg.Narrow(req.audience, req.scope)
	if err != nil {
		reason := reasonScopeRefused
		if !slices.Contains(reg.Audiences, req.audience) {
			reason = reasonAudienceRefused
		}
		refuse(reason)
		return
	}
	lifetime := int64(contracts.WorkloadTokenLifetimeCeiling / time.Second)
	if !reg.ExpiresAt.IsZero() {
		if left := int64(reg.ExpiresAt.Sub(now) / time.Second); left < lifetime {
			lifetime = left
		}
	}
	if lifetime < 1 {
		refuse(reasonLifetimeTooShort)
		return
	}
	issued, jti, reason := h.mint(ctx, reg.Registration, req.audience, now, lifetime)
	if reason != "" {
		h.answerAs(w, r, subjectBootstrap, outcomeUnavailable, reason, audience)
		return
	}
	principal := reg.PrincipalID.String()
	if actions == nil {
		actions = []string{}
	}
	if err := h.Audit(ctx, audit.AuditEvent{
		EventType: audit.EventWorkloadTokenIssued, Outcome: audit.OutcomeAllowed,
		SubjectPrincipalID: &principal, RequestID: requestid.Audit(ctx),
		Attributes: map[string]any{
			"jti": jti, "principal_id": principal, "credential_id": reg.CredentialID.String(),
			"revision": reg.Revision, "audience": req.audience, "actions": actions,
		},
	}); err != nil {
		h.answerAs(w, r, subjectBootstrap, outcomeUnavailable, reasonAuditUnavailable, audience)
		return
	}
	h.rec.record(ctx, subjectBootstrap, outcomeIssued, reasonIssued, audience)
	writeJSON(w, http.StatusOK, issuedAnswer{
		AccessToken: issued, IssuedTokenType: TokenTypeJWT, TokenType: "Bearer", ExpiresIn: lifetime,
	})
}
