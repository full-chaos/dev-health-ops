//go:build integration

package identity

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

// Under users-owned, a federated address that holds U+FFFD reaches no write:
// no principal, auth user, identity, public user, audit row or outbox event.
func TestOwnedProvisionFederatedRefusesTheReplacementCharacterWithoutWrites(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for _, email := range replacementEmails {
		key := ExternalKey{Issuer: "https://idp.example.com/" + uuid.NewString()[:8], Provider: e.seedSSOProvider(t, ctx, e.seedOrg(t, ctx), "oidc"), Subject: "fffd-" + uuid.NewString()}
		before := e.writeCounts(t, ctx)
		id, err := e.owned.ProvisionFederated(ctx, ProvisionRequest{Key: key, Email: email, FullName: "Lookalike", Actor: SelfSignUp(nil), LegacyProvider: "oidc", LegacySubject: key.Subject})
		if !errors.Is(err, ErrInvalidEmail) {
			t.Errorf("ProvisionFederated(%q) = %v, %v; want ErrInvalidEmail", email, id, err)
		}
		if after := e.writeCounts(t, ctx); after != before {
			t.Errorf("%q: a refused provisioning wrote rows: before %v, after %v", email, before, after)
		}
	}
}
