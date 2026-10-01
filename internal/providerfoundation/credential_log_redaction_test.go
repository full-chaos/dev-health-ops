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
