package identity

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
)

// TestIdentityEventNamesItsPrincipal: an identity event names the principal
// the identity belongs to, for a social and an SSO provider, and passes the
// contract check.
func TestIdentityEventNamesItsPrincipal(t *testing.T) {
	target, subject := uuid.NewString(), uuid.NewString()
	for _, key := range []ExternalKey{
		{Issuer: "https://accounts.google.com", Provider: ProviderGoogle, Subject: "s"},
		{Issuer: "https://idp.example.com", Provider: Provider(uuid.NewString()), Subject: "s"},
	} {
		for _, verb := range []string{"external_identity.linked", "external_identity.unlinked"} {
			e := identityEvent(verb, key, target, subject)
			if got := e.Change["principal_id"]; got != "prn_"+subject {
				t.Errorf("%s %s: principal_id = %v, want prn_%s", key.Provider, verb, got, subject)
			}
			if err := provisioning.CheckEvent(e.AggregateType, e.AggregateID, e.EventType, e.Change); err != nil {
				t.Errorf("%s %s: %v", key.Provider, verb, err)
			}
		}
	}
}
