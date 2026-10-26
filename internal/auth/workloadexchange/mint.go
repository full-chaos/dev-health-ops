package workloadexchange

import (
	"context"
	"encoding/json"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// mint signs a workload-token.v1 token for reg at audience, valid for
// lifetime seconds from now, and returns it with its jti. Before it is
// returned, the canonical workload verifier must accept it under the signer's
// published keys; a refusal is an unavailable reason and no token leaves.
func (h *handler) mint(ctx context.Context, reg Registration, audience string, now time.Time, lifetime int64) (string, string, string) {
	claims := contracts.WorkloadTokenClaims{
		Iss:           h.IssuerURL,
		Aud:           audience,
		Sub:           session.WirePrincipalID(reg.PrincipalID),
		PrincipalType: "workload",
		Jti:           uuid.NewString(),
		Iat:           now.Unix(),
		Nbf:           now.Unix(),
		Exp:           now.Unix() + lifetime,
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", "", reasonSignerUnavailable
	}
	compact, _, err := h.Signer.Sign(ctx, contracts.WorkloadTokenTyp, payload)
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
	verifier, err := authverify.NewWorkloadVerifier(authverify.WorkloadVerifierConfig{Issuer: h.IssuerURL, Audience: audience, Keys: keys})
	if err != nil {
		return "", "", selfCheckPrefix + string(authverify.ReasonContract)
	}
	verified, err := verifier.Verify(compact, now)
	if err != nil {
		return "", "", selfCheckReason(err)
	}
	if verified.Subject != claims.Sub || verified.TokenID != claims.Jti || verified.ExpiresAt != claims.Exp {
		return "", "", selfCheckPrefix + string(authverify.ReasonContract)
	}
	return compact, claims.Jti, ""
}
