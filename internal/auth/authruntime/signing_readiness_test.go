package authruntime

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

func writeSigningKey(t *testing.T, dir, name string) {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestReadinessProvesTheKeySetThroughTheSigner runs the service on a key-set
// manifest with two active keys: the signing_key check fails closed with the
// key-set reason, which only the token signer's check produces.
func TestReadinessProvesTheKeySetThroughTheSigner(t *testing.T) {
	useKeyStates(t, staticKeyStates(nil))
	dir := t.TempDir()
	writeSigningKey(t, dir, "a.pem")
	writeSigningKey(t, dir, "b.pem")
	manifest := filepath.Join(dir, "keyset.json")
	body := `{"keys":[{"kid":"a","state":"active","file":"a.pem"},{"kid":"b","state":"active","file":"b.pem"}]}`
	if err := os.WriteFile(manifest, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	ports := reservePorts(t, 2)
	env := brokenEnvironment(t, ports[0], ports[1])
	delete(env, authconfig.EnvSigningKeyFile)
	delete(env, authconfig.EnvSigningKeyID)
	env[authconfig.EnvSigningKeySetFile] = manifest

	logs, stop := runService(t, env)
	client := &http.Client{Timeout: 10 * time.Second}
	defer client.CloseIdleConnections()
	response := awaitSettledReadiness(t, client, "http://"+ports[1]+"/readyz")
	raw, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	var readiness struct {
		FailedChecks []string `json:"failed_checks"`
	}
	if err := json.Unmarshal(raw, &readiness); err != nil {
		t.Fatalf("/readyz body %q: %v", raw, err)
	}
	if response.StatusCode != http.StatusServiceUnavailable || !slices.Contains(readiness.FailedChecks, CheckSigningKey) {
		t.Fatalf("/readyz = %d %s, want 503 naming %s", response.StatusCode, raw, CheckSigningKey)
	}
	if strings.Contains(string(raw), dir) {
		t.Fatalf("/readyz leaked the key directory: %s", raw)
	}
	if code := stop(); code != 0 {
		t.Fatalf("Execute exited %d", code)
	}
	if transcript := logs.String(); !strings.Contains(transcript, "keyset_many_active_keys") {
		t.Fatalf("the log does not carry the key-set reason:\n%s", transcript)
	}
}
