//go:build unix

package teamsidentity

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// discoveryOpenCounter is a named pipe that serves a key to every reader and counts each open.
type discoveryOpenCounter struct {
	path  string
	opens atomic.Int32
	done  chan struct{}
}

func newDiscoveryOpenCounter(t *testing.T, content string) *discoveryOpenCounter {
	t.Helper()
	c := &discoveryOpenCounter{path: filepath.Join(t.TempDir(), "key.pem"), done: make(chan struct{})}
	if err := syscall.Mkfifo(c.path, 0o600); err != nil {
		t.Fatal(err)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		for {
			f, err := os.OpenFile(c.path, os.O_WRONLY, 0)
			if err != nil {
				return
			}
			select {
			case <-c.done:
				f.Close()
				return
			default:
			}
			c.opens.Add(1)
			_, _ = f.WriteString(content)
			f.Close()
		}
	}()
	t.Cleanup(func() {
		close(c.done)
		if f, err := os.OpenFile(c.path, os.O_RDONLY|syscall.O_NONBLOCK, 0); err == nil {
			f.Close()
		}
		<-served
	})
	return c
}

// storedRow serves one stored GitHub row as the repository and the decryptor would: the plaintext is the row's secret.
type storedRow struct{ plain string }

func (s storedRow) ResolveEncrypted(context.Context, providerfoundation.TenantScope) (providerfoundation.EncryptedCredential, error) {
	return providerfoundation.EncryptedCredential{ID: "c", Provider: "github", Name: "app", Active: true, Ciphertext: secrets.NewValue("sealed")}, nil
}

func (s storedRow) Decrypt(secrets.Value) ([]byte, error) { return []byte(s.plain), nil }

// A stored row holding both key-path spellings, in both orders, over truthy / false / blank / non-string values, through
// the stored-credential decode and GitHub discovery: it answers the fixed refusal, or goes on with no file opened and no
// provider request.
func TestGitHubDiscoveryStoredKeySpellingTable(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	counter := newDiscoveryOpenCounter(t, string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})))
	doer := &recordingDoer{}
	withDiscoveryClient(t, fakehttp.Client(doer))
	withHostLookup(t, func(context.Context, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
	})
	values := map[string]string{"truthy": strconv.Quote(counter.path), "falsy": "false", "blank": `""`, "wrongtype": "7"}
	refused, accepted := 0, 0
	for _, order := range [][2]string{{"private_key_path", "privateKeyPath"}, {"privateKeyPath", "private_key_path"}} {
		for _, first := range []string{"truthy", "falsy", "blank", "wrongtype"} {
			for _, second := range []string{"truthy", "falsy", "blank", "wrongtype"} {
				name := order[0] + "=" + first + "," + order[1] + "=" + second
				t.Run(name, func(t *testing.T) {
					plain := `{"app_id":"1","installation_id":"2",` + strconv.Quote(order[0]) + ":" + values[first] + "," +
						strconv.Quote(order[1]) + ":" + values[second] + `,"base_url":"https://stub.example.test"}`
					resolver := providerfoundation.CredentialResolver{Repository: storedRow{plain}, Decryptor: storedRow{plain}}
					opensBefore, sentBefore := counter.opens.Load(), len(doer.hosts)
					credential, err := resolver.Resolve(context.Background(), alwaysValidLease{},
						providerfoundation.TenantScope{OrgID: "org-1", Provider: "github", IntegrationID: "admin-discover"})
					if err != nil {
						t.Fatalf("resolve the stored row: %v", err)
					}
					_, err = prepareDiscoveryCredential(context.Background(), "github", credential)
					var statusErr *discoveryStatusError
					if errors.As(err, &statusErr) && statusErr.Status == http.StatusBadRequest && statusErr.Detail == keyPathRefusal {
						refused++
					} else {
						accepted++
					}
					if opened := counter.opens.Load() - opensBefore; opened != 0 {
						t.Errorf("%d file open(s); want 0", opened)
					}
					if sent := len(doer.hosts) - sentBefore; sent != 0 {
						t.Errorf("%d provider request(s) (%v); want 0", sent, doer.hosts[sentBefore:])
					}
				})
			}
		}
	}
	t.Logf("discovery: %d refused with the fixed text, %d went on", refused, accepted)
}
