package acrmint

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// TestTheSharedVerifierAcceptsAMintedTokenForItsAudienceOnly: once the pinned
// authverify knows the ACR audiences (the release of CHAOS-12249), a verifier
// for acr-api accepts the minted acr-api token and refuses the acr-mcp one,
// and the reverse, and the dev-health-api and auth-service verifiers refuse
// both with an audience refusal. While it knows them not (v0.33.0), a verifier
// for an ACR audience is not built and the platform verifiers refuse both at
// the claims contract. Either branch asserts; neither passes on nothing.
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
	_, acrErr := verifier(contracts.ACRAPIAudience)
	known := acrErr == nil
	platformReason := authverify.ReasonContract
	if known {
		platformReason = authverify.ReasonAudience
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
	} else if _, err := verifier(contracts.ACRMCPAudience); err == nil {
		t.Fatal("authverify builds an acr-mcp verifier but not an acr-api one")
	}
	for _, platform := range []string{contracts.PlatformAPIAudience, contracts.SessionRouteAudience} {
		v, err := verifier(platform)
		if err != nil {
			t.Fatal(err)
		}
		for audience, compact := range minted {
			claims, err := v.Verify(compact, testNow)
			if claims != nil || authverify.ReasonOf(err) != platformReason {
				t.Errorf("verifier %s on the minted %s token: %v; want %s", platform, audience, authverify.ReasonOf(err), platformReason)
			}
		}
	}
}
