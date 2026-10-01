package providerfoundation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// CHAOS-7132: CredentialResolver.Resolve answered every failure with the same bare
// ErrCredentialInvalid, so a missing encryption key, a wrong key, an unreadable payload and a missing
// ciphertext read the same in a log, a CLI refusal and a run's error. Each now names its reason
// (a fixed word, never a value) and still wraps ErrCredentialInvalid.
type reasonRepository struct{ record EncryptedCredential }

func (r reasonRepository) ResolveEncrypted(context.Context, TenantScope) (EncryptedCredential, error) {
	return r.record, nil
}

func TestCredentialResolverNamesWhyACredentialIsInvalid(t *testing.T) {
	t.Parallel()
	key := secrets.NewValue("test-master-key")
	decryptor, _ := NewFernetDecryptor(key, "salt")
	wrongKey, _ := NewFernetDecryptor(secrets.NewValue("another-master-key"), "salt")
	good := "v1:" + encryptForTest(t, []byte(`{"token":"secret-value-123"}`), key.Reveal(), "salt")
	notObject := "v1:" + encryptForTest(t, []byte(`["secret-value-123"]`), key.Reveal(), "salt")
	record := func(provider, cipher string) EncryptedCredential {
		return EncryptedCredential{ID: "id", Provider: provider, Name: "default", Active: true, Ciphertext: secrets.NewValue(cipher)}
	}
	tests := []struct {
		name      string
		record    EncryptedCredential
		decryptor CredentialDecryptor
		reason    string
	}{
		{"no encryption key in this process", record("gitlab", good), FernetDecryptor{}, "encryption_key_not_configured"},
		{"a key that does not open the ciphertext", record("gitlab", good), wrongKey, "decrypt_failed"},
		{"a payload that is not a JSON object", record("gitlab", notObject), decryptor, "payload_not_a_json_object"},
		{"no ciphertext stored", record("gitlab", ""), decryptor, "ciphertext_missing"},
		{"a row of another provider", record("github", good), decryptor, "provider_mismatch"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			resolver := CredentialResolver{Repository: reasonRepository{record: test.record}, Decryptor: test.decryptor}
			_, err := resolver.Resolve(context.Background(), LeaseGuardFunc(func(context.Context) error { return nil }),
				TenantScope{OrgID: "org", Provider: "gitlab", IntegrationID: "integration"})
			if !errors.Is(err, ErrCredentialInvalid) {
				t.Fatalf("err = %v, want it to wrap ErrCredentialInvalid", err)
			}
			if !strings.Contains(err.Error(), test.reason) {
				t.Errorf("refusal %q does not name the reason %q", err.Error(), test.reason)
			}
			if strings.Contains(err.Error(), "secret-value-123") {
				t.Fatalf("refusal %q leaks a payload value", err.Error())
			}
		})
	}
}

func TestFailureReasonIsFixedVocabularyAndValueFree(t *testing.T) {
	t.Parallel()
	shape := ValidateCredentialShape(testCredential("jira", map[string]string{"email": "someone-SECRET@example.test"}))
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"a credential reason", credentialInvalid("decrypt_failed"), "decrypt_failed"},
		{"a refused jira shape", shape, "missing_fields:api_token,base_url"},
		{"a provider authentication failure", &ProviderError{Class: ErrorAuthentication, StatusCode: 401}, "authentication:401"},
		{"a wrapped credential reason", errors.Join(errors.New("context"), credentialInvalid("provider_mismatch")), "provider_mismatch"},
		{"a plain error carries no reason", errors.New("something with a secret-value-123"), ""},
	}
	for _, test := range tests {
		if got := FailureReason(test.err); got != test.want {
			t.Errorf("%s: FailureReason = %q, want %q", test.name, got, test.want)
		}
	}
	if _, err := NewFernetDecryptor(secrets.Value{}, ""); FailureReason(err) != "encryption_key_not_configured" {
		t.Errorf("NewFernetDecryptor without a key: reason %q, want encryption_key_not_configured", FailureReason(err))
	}
}

// r2 (CHAOS-7132): no ErrCredentialInvalid-derived refusal may end without a reason: the ambiguity error,
// a missing token of every provider's shape check, an unknown provider and a bare sentinel all carry one.
func TestEveryCredentialInvalidRefusalHasAReason(t *testing.T) {
	t.Parallel()
	for _, provider := range []string{"github", "gitlab", "jira", "linear", "launchdarkly", "pagerduty", "bitbucket", ""} {
		err := ValidateCredentialShape(testCredential(provider, map[string]string{}))
		if err == nil {
			t.Errorf("%q: an empty credential was accepted", provider)
			continue
		}
		if !errors.Is(err, ErrCredentialInvalid) || FailureReason(err) == "" {
			t.Errorf("%q: refusal %v has no reason (%q)", provider, err, FailureReason(err))
		}
	}
	if got := FailureReason(ValidateCredentialShape(testCredential("gitlab", map[string]string{}))); got != "missing_fields:token" {
		t.Errorf("gitlab without a token: reason %q, want missing_fields:token", got)
	}
	ambiguous := &CredentialAmbiguousError{Provider: "jira", Names: []string{"alpha", "bravo"}}
	if got := FailureReason(ambiguous); got != "credential_ambiguous" {
		t.Errorf("ambiguity: reason %q", got)
	}
	if got := FailureReason(fmt.Errorf("wrapped: %w", ErrCredentialInvalid)); got != "credential_invalid" {
		t.Errorf("a bare wrapped sentinel: reason %q, want credential_invalid", got)
	}
	if FailureReason(errors.New("unrelated")) != "" {
		t.Error("an unrelated error must carry no reason")
	}
}
