package providerfoundation

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// A credential the resolver decrypts is registered with the process logger's
// redaction registry, so a gateway that echoes the token into a log line
// (CHAOS-7409, the class of the Jira-token warning) prints a marker, for every
// provider, with no per-provider wrap. Not parallel: it installs the process
// logger.
func TestResolvedCredentialTokenNeverReachesTheProcessLogger(t *testing.T) {
	const token = "jira-token-7409-planted"
	key := secrets.NewValue("test-master-key")
	decryptor, _ := NewFernetDecryptor(key, "salt")
	resolver := CredentialResolver{
		Repository: testRepository{cipherText: "v1:" + encryptForTest(t, []byte(`{"token":"`+token+`","email":"sync7409@example.test"}`), key.Reveal(), "salt")},
		Decryptor:  decryptor,
	}
	if _, err := resolver.Resolve(context.Background(), LeaseGuardFunc(func(context.Context) error { return nil }),
		TenantScope{OrgID: "org", Provider: "gitlab", IntegrationID: "integration"}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	defer logging.InstallDefault(logging.NewJSON(&out, slog.LevelInfo))()
	slog.Warn("gateway rejected the request", "error", "401 for "+token+" sync7409@example.test")
	if !strings.Contains(out.String(), "gateway rejected") || strings.Contains(out.String(), token) || strings.Contains(out.String(), "sync7409@example.test") {
		t.Fatalf("unexpected log: %s", out.String())
	}
}

// r1: every caller of the decryptor is covered, not only the credential resolver:
// the shared API credential store, the PagerDuty OAuth hydrator and the SSO client
// secret all decrypt through FernetDecryptor.Decrypt.
func TestEveryDecryptedValueIsRegisteredAtTheDecryptor(t *testing.T) {
	key := secrets.NewValue("test-master-key")
	decryptor, _ := NewFernetDecryptor(key, "salt")
	for name, plain := range map[string]string{
		"json credential, camelCase": `{"privateKey":"decrypt-private-key-7409","email":"d7409@example.test"}`,
		"oauth token json":           `{"access_token":"decrypt-oauth-token-7409"}`,
		"bare secret (sso client)":   "decrypt-client-secret-7409",
	} {
		ciphertext := secrets.NewValue("v1:" + encryptForTest(t, []byte(plain), key.Reveal(), "salt"))
		if _, err := decryptor.Decrypt(ciphertext); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	for _, secret := range []string{"decrypt-private-key-7409", "decrypt-oauth-token-7409", "decrypt-client-secret-7409", "d7409@example.test"} {
		if got := secrets.RedactRegistered("refused: " + secret); strings.Contains(got, secret) {
			t.Errorf("%q was not registered by Decrypt", secret)
		}
	}
}

// r1 repros: the shared decryptor, the PagerDuty OAuth hydrator and the SSO client
// secret all decrypt through FernetDecryptor.Decrypt; a value it returned is
// redacted when a provider echoes it into a log line. Red on the first tip.
func TestR1DecryptedValuesNeverReachTheProcessLogger(t *testing.T) {
	key := secrets.NewValue("test-master-key")
	decryptor, _ := NewFernetDecryptor(key, "salt")
	for name, plain := range map[string]string{
		"shared credential (camelCase)": `{"privateKey":"review-shared-credential-7409"}`,
		"pagerduty oauth token":         `{"access_token":"review-pagerduty-token-7409"}`,
		"sso client secret":             "review-oidc-client-secret-7409",
	} {
		var out bytes.Buffer
		restore := logging.InstallDefault(logging.NewJSON(&out, slog.LevelInfo))
		secrets.ResetRegistered()
		ciphertext := secrets.NewValue("v1:" + encryptForTest(t, []byte(plain), key.Reveal(), "salt"))
		decrypted, err := decryptor.Decrypt(ciphertext)
		if err != nil {
			restore()
			t.Fatalf("%s: %v", name, err)
		}
		slog.Error("unexpected response body: " + string(decrypted))
		restore()
		if strings.Contains(out.String(), "review-") {
			t.Errorf("%s leaked: %s", name, out.String())
		}
	}
	secrets.ResetRegistered()
}
