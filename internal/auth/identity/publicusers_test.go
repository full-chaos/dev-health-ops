package identity

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestFederatedPublicUserMatchesWhatGoAPIWrites(t *testing.T) {
	id := uuid.New()
	social := ProvisionRequest{
		Key:      ExternalKey{Issuer: "https://github.com", Provider: ProviderGitHub, Subject: "12345"},
		Username: "someuser", LegacyFullName: strPtr("Some Name"),
	}
	got, err := federatedPublicUser(id, "a@example.test", social)
	if err != nil {
		t.Fatal(err)
	}
	if got.AuthProvider != "github" || got.AuthProviderID == nil || *got.AuthProviderID != "12345" ||
		got.Username != "someuser" || got.FullName == nil || *got.FullName != "Some Name" || !got.Verified || got.PasswordHash != nil || got.Email != "a@example.test" {
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

func strPtr(s string) *string { return &s }

// CHAOS-11619: the public.users row of a federated provisioning never carries
// a provider id ValidSubject refuses; a valid one is written as before.
func TestFederatedPublicUserRefusesAnInvalidSubject(t *testing.T) {
	id := uuid.New()
	key := ExternalKey{Issuer: "https://idp.example.test", Provider: "6f0c2d58-6b43-4b8e-9a8e-3a6f7a1c9d11", Subject: "sub-1"}
	for name, subject := range map[string]string{"edge whitespace": " sub-1", "control": "sub\x01", "too long": strings.Repeat("a", 1025)} {
		req := ProvisionRequest{Key: key, LegacyProvider: "oidc", LegacySubject: subject}
		if _, err := federatedPublicUser(id, "d@example.test", req); !errors.Is(err, ErrLegacyProjection) {
			t.Errorf("%s: err = %v, want ErrLegacyProjection", name, err)
		}
	}
	social := ProvisionRequest{Key: ExternalKey{Issuer: "https://github.com", Provider: ProviderGitHub, Subject: " 12345"}}
	if _, err := federatedPublicUser(id, "e@example.test", social); !errors.Is(err, ErrLegacyProjection) {
		t.Errorf("social edge whitespace: err = %v, want ErrLegacyProjection", err)
	}
}
