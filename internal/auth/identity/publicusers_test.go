package identity

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestFederatedPublicUserMatchesWhatGoAPIWrites(t *testing.T) {
	id := uuid.New()
	social := ProvisionRequest{
		Key:      ExternalKey{Issuer: "https://github.com", Provider: ProviderGitHub, Subject: "12345"},
		Username: "someuser", LegacyFullName: "Some Name",
	}
	got, err := federatedPublicUser(id, "a@example.test", social)
	if err != nil {
		t.Fatal(err)
	}
	if got.AuthProvider != "github" || got.AuthProviderID == nil || *got.AuthProviderID != "12345" ||
		got.Username != "someuser" || got.FullName != "Some Name" || !got.Verified || got.PasswordHash != nil || got.Email != "a@example.test" {
		t.Fatalf("social row: %+v", got)
	}

	sso := ProvisionRequest{
		Key:            ExternalKey{Issuer: "https://idp.example.test", Provider: "6f0c2d58-6b43-4b8e-9a8e-3a6f7a1c9d11", Subject: "sub-1"},
		LegacyProvider: "oidc", LegacySubject: "sub-1",
	}
	got, err = federatedPublicUser(id, "b@example.test", sso)
	if err != nil || got.AuthProvider != "oidc" || got.AuthProviderID == nil || *got.AuthProviderID != "sub-1" {
		t.Fatalf("sso row: %+v %v", got, err)
	}

	for name, req := range map[string]ProvisionRequest{
		"sso without a provider": {Key: sso.Key, LegacySubject: "sub-1"},
		"sso without a subject":  {Key: sso.Key, LegacyProvider: "oidc"},
		"sso without either":     {Key: sso.Key},
	} {
		if _, err := federatedPublicUser(id, "c@example.test", req); !errors.Is(err, ErrLegacyProjection) {
			t.Errorf("%s: err = %v, want ErrLegacyProjection", name, err)
		}
	}
}
