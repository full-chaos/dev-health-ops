//go:build unix

package teamsidentity

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// legacyRow is a stored row written before the key-path refusal: its secret holds both spellings, and its config names
// another base.
type legacyRow struct{ plain string }

func (s legacyRow) ResolveEncrypted(context.Context, providerfoundation.TenantScope) (providerfoundation.EncryptedCredential, error) {
	return providerfoundation.EncryptedCredential{ID: "c", Provider: "github", Name: "app", Active: true, Ciphertext: secrets.NewValue("sealed"),
		Config: map[string]string{"base_url": "https://config-base.example.test"}}, nil
}

func (s legacyRow) Decrypt(secrets.Value) ([]byte, error) { return []byte(s.plain), nil }

// authRecorder answers every request with an empty page and records where it went and what it carried.
type authRecorder struct{ requests []string }

func (d *authRecorder) Do(request *http.Request) (*http.Response, error) {
	d.requests = append(d.requests, request.URL.Host+" "+request.Header.Get("Authorization"))
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(`[]`)), Request: request}, nil
}

// A stored row written before the refusal, holding a valid token and BOTH key-path spellings (a path first, a falsy
// value second), through the stored-credential decode, GitHub discovery and its provider calls: no key file is opened,
// and every provider request goes to the default GitHub host with the row's own token, never to the config base.
func TestGitHubDiscoveryLegacyBothSpellingsRow(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	counter := newDiscoveryOpenCounter(t, string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})))
	withHostLookup(t, func(context.Context, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
	})
	path := strconv.Quote(counter.path)
	for name, fields := range map[string]string{
		"private_key_path=path,privateKeyPath=false": `"private_key_path":` + path + `,"privateKeyPath":false`,
		"private_key_path=path,privateKeyPath=blank": `"private_key_path":` + path + `,"privateKeyPath":""`,
		"privateKeyPath=path,private_key_path=false": `"privateKeyPath":` + path + `,"private_key_path":false`,
		"privateKeyPath=path,private_key_path=zero":  `"privateKeyPath":` + path + `,"private_key_path":0`,
	} {
		t.Run(name, func(t *testing.T) {
			doer := &authRecorder{}
			withDiscoveryClient(t, fakehttp.Client(doer))
			plain := `{"token":"ghp_synthetic_row_token",` + fields + `}`
			resolver := providerfoundation.CredentialResolver{Repository: legacyRow{plain}, Decryptor: legacyRow{plain}}
			opensBefore := counter.opens.Load()
			credential, err := resolver.Resolve(context.Background(), alwaysValidLease{},
				providerfoundation.TenantScope{OrgID: "org-1", Provider: "github", IntegrationID: "admin-discover"})
			if err != nil {
				t.Fatalf("resolve the stored row: %v", err)
			}
			prepared, err := prepareDiscoveryCredential(context.Background(), "github", credential)
			if err == nil {
				_, _ = discoverGitHub(context.Background(), prepared, "acme")
			}
			if opened := counter.opens.Load() - opensBefore; opened != 0 {
				t.Errorf("%d key-file open(s); want 0", opened)
			}
			for _, request := range doer.requests {
				if request != "api.github.com token ghp_synthetic_row_token" {
					t.Errorf("a provider request went to %q; want the default host with the row's own token", request)
				}
			}
			t.Logf("prepare err=%v, %d provider request(s)", err, len(doer.requests))
		})
	}
}
