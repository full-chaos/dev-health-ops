package acrmint

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// TestTheSharedVerifierAcceptsAMintedTokenForItsAudienceOnly: a verifier for
// acr-api accepts the minted acr-api token and refuses the acr-mcp one, and the
// reverse, and the dev-health-api and auth-service verifiers refuse both with an
// audience refusal (dev-health-go v0.36.0, the release of CHAOS-12249).
func TestTheSharedVerifierAcceptsAMintedTokenForItsAudienceOnly(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, nil)
	set, err := f.signer.KeySet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	keys := authverify.StaticKeys{set.Active().KeyID: set.Active().PublicKey}
	minted := map[string]string{}
	for _, audience := range []string{contracts.ACRAPIAudience, contracts.ACRMCPAudience} {
		compact, _, err := f.minter.Mint(ctx, request(audience))
		if err != nil {
			t.Fatal(err)
		}
		minted[audience] = compact
	}
	verifier := func(audience string) (*authverify.Verifier, error) {
		return authverify.NewVerifier(authverify.VerifierConfig{Issuer: testIssuer, Audience: audience, Keys: keys})
	}
	for verifierAudience := range minted {
		v, err := verifier(verifierAudience)
		if err != nil {
			t.Fatal(err)
		}
		for tokenAudience, compact := range minted {
			claims, err := v.Verify(compact, testNow)
			if tokenAudience == verifierAudience {
				if err != nil || claims == nil || claims.Audience != tokenAudience {
					t.Errorf("verifier %s refused its own minted token: %v", verifierAudience, authverify.ReasonOf(err))
				}
			} else if claims != nil || authverify.ReasonOf(err) != authverify.ReasonAudience {
				t.Errorf("verifier %s on the minted %s token: %v; want an audience refusal", verifierAudience, tokenAudience, authverify.ReasonOf(err))
			}
		}
	}
	for _, platform := range []string{contracts.PlatformAPIAudience, contracts.SessionRouteAudience} {
		v, err := verifier(platform)
		if err != nil {
			t.Fatal(err)
		}
		for audience, compact := range minted {
			claims, err := v.Verify(compact, testNow)
			if claims != nil || authverify.ReasonOf(err) != authverify.ReasonAudience {
				t.Errorf("verifier %s on the minted %s token: %v; want an audience refusal", platform, audience, authverify.ReasonOf(err))
			}
		}
	}
}
