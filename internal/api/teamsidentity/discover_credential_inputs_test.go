package teamsidentity

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func TestGitHubDiscoveryRefusesPrivateKeyPath(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	readable := filepath.Join(dir, "app.pem")
	if err := os.WriteFile(readable, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600); err != nil {
		t.Fatal(err)
	}
	unreadable := filepath.Join(dir, "a-directory")
	if err := os.Mkdir(unreadable, 0o700); err != nil {
		t.Fatal(err)
	}
	doer := &recordingDoer{}
	withDiscoveryClient(t, fakehttp.Client(doer))
	withHostLookup(t, func(context.Context, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
	})
	for state, path := range map[string]string{"readable": readable, "unreadable": unreadable, "absent": filepath.Join(dir, "absent.pem")} {
		t.Run(state, func(t *testing.T) {
			credential := providerfoundation.NewCredential("github", "c", nil, map[string]secrets.Value{
				"app_id": secrets.NewValue("1"), "installation_id": secrets.NewValue("2"),
				"private_key_path": secrets.NewValue(path), "base_url": secrets.NewValue("https://stub.example.test"),
			})
			_, err := prepareDiscoveryCredential(context.Background(), "github", credential)
			assertStatus(t, err, http.StatusBadRequest, "private_key_path is not supported: send the key as private_key")
		})
	}
	if len(doer.hosts) != 0 {
		t.Errorf("%d request(s) reached the provider (%v); want 0", len(doer.hosts), doer.hosts)
	}
}
