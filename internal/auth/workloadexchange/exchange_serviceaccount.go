package workloadexchange

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/serviceaccount"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
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

// subjectServed reports whether the subject_token_type names a subject this
// route serves: the two of subjectOn, or a service account key.
func (h *handler) subjectServed(subjectType string) bool {
	return h.subjectOn(subjectType) || subjectType == TokenTypeServiceAccountKey && h.ServiceAccountKeys != nil
}

// serveStoreSubject serves the subjects whose value the store resolves by
// digest and reports whether it did.
func (h *handler) serveStoreSubject(w http.ResponseWriter, r *http.Request, req exchangeRequest) bool {
	switch req.subjectType {
	case TokenTypeWorkloadBootstrap:
		h.serveBootstrap(w, r, req)
	case TokenTypeServiceAccountKey:
		h.serveServiceAccountKey(w, r, req)
	default:
		return false
	}
	return true
}

// serveServiceAccountKey is the key subject. A value outside the key's shape,
// an audience other than dev-health-api and a scope are refused before any
// store read: a key holds no scope, and the exchange ignores whatever
// audiences and scopes the key row holds. The registry resolves the digest of
// the value; the answers are the other subjects' answers.
func (h *handler) serveServiceAccountKey(w http.ResponseWriter, r *http.Request, req exchangeRequest) {
	ctx := r.Context()
	audience := audienceLabel(req.audience)
	refuse := func(reason string) { h.answerAs(w, r, subjectServiceAccountKey, outcomeRefused, reason, audience) }
	switch {
	case !serviceaccount.ValidKeyShape(req.subjectToken):
		refuse(reasonMalformed)
		return
	case req.audience != contracts.ServiceAccountTokenAudience:
		refuse(reasonAudienceRefused)
		return
	case req.scopePresent:
		refuse(reasonScopeRefused)
		return
	}
	now := h.Now()
	reg, err := h.ServiceAccountKeys.ResolveServiceAccountKey(ctx, serviceaccount.KeyDigest(req.subjectToken), now)
	if err != nil {
		var refusal *KeyRefusal
		if errors.As(err, &refusal) {
			refuse(keyResolveReason(refusal.Reason))
			return
		}
		h.answerAs(w, r, subjectServiceAccountKey, outcomeUnavailable, reasonStoreError, audience)
		return
	}
	lifetime := int64(contracts.ServiceAccountTokenLifetime(now, reg.ExpiresAt) / time.Second)
	if lifetime < 1 {
		refuse(reasonLifetimeTooShort)
		return
	}
	issued, jti, reason := h.mintServiceAccount(ctx, reg, now, lifetime)
	if reason != "" {
		h.answerAs(w, r, subjectServiceAccountKey, outcomeUnavailable, reason, audience)
		return
	}
	principal, org := reg.PrincipalID.String(), reg.OrgID.String()
	if err := h.Audit(ctx, audit.AuditEvent{
		EventType: audit.EventServiceAccountTokenIssued, Outcome: audit.OutcomeAllowed,
		SubjectPrincipalID: &principal, OrganizationID: &org, RequestID: requestid.Audit(ctx),
		Attributes: map[string]any{
			"jti": jti, "principal_id": principal, "credential_id": reg.CredentialID.String(),
			"revision": reg.Revision, "audience": req.audience, "credential_class": contracts.ServiceAccountTokenClass,
		},
	}); err != nil {
		h.answerAs(w, r, subjectServiceAccountKey, outcomeUnavailable, reasonAuditUnavailable, audience)
		return
	}
	h.rec.record(ctx, subjectServiceAccountKey, outcomeIssued, reasonIssued, audience)
	writeJSON(w, http.StatusOK, issuedAnswer{
		AccessToken: issued, IssuedTokenType: TokenTypeJWT, TokenType: "Bearer", ExpiresIn: lifetime,
	})
}

// mintServiceAccount signs a service-account-token.v1 token for reg, valid for
// lifetime seconds from now, and returns it with its jti. Before it is
// returned, the canonical service account verifier must accept it under the
// signer's published keys; a refusal is an unavailable reason and no token
// leaves.
func (h *handler) mintServiceAccount(ctx context.Context, reg ServiceAccountKeyRegistration, now time.Time, lifetime int64) (string, string, string) {
	claims := contracts.ServiceAccountTokenClaims{
		Iss:           h.IssuerURL,
		Aud:           contracts.ServiceAccountTokenAudience,
		Sub:           session.WirePrincipalID(reg.PrincipalID),
		PrincipalType: contracts.ServiceAccountTokenPrincipalType,
		OrgID:         reg.OrgID.String(),
		Cid:           "crd_" + reg.CredentialID.String(),
		Jti:           uuid.NewString(),
		Iat:           now.Unix(),
		Nbf:           now.Unix(),
		Exp:           now.Unix() + lifetime,
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", "", reasonSignerUnavailable
	}
	compact, _, err := h.Signer.Sign(ctx, contracts.ServiceAccountTokenTyp, payload)
	if err != nil {
		return "", "", reasonSignerUnavailable
	}
	set, err := h.Signer.KeySet(ctx)
	if err != nil {
		return "", "", reasonSignerUnavailable
	}
	keys := authverify.StaticKeys{}
	for _, key := range set.Published() {
		keys[key.KeyID] = key.PublicKey
	}
	verifier, err := authverify.NewServiceAccountVerifier(authverify.ServiceAccountVerifierConfig{Issuer: h.IssuerURL, Keys: keys})
	if err != nil {
		return "", "", selfCheckPrefix + string(authverify.ReasonContract)
	}
	verified, err := verifier.Verify(compact, now)
	if err != nil {
		return "", "", selfCheckReason(err)
	}
	if verified.Subject != claims.Sub || verified.TokenID != claims.Jti || verified.ExpiresAt != claims.Exp ||
		verified.OrgID != claims.OrgID || verified.CredentialID != claims.Cid {
		return "", "", selfCheckPrefix + string(authverify.ReasonContract)
	}
	return compact, claims.Jti, ""
}
