package providerfoundation

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// An App credential (the secret holds app_id, private_key and installation_id, no token) talks to the secret's base
// or the default GitHub API, never a base the row's config names; the row name and config auth_mode change nothing.
func TestGitHubAppClientTakesNoBaseFromConfig(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	retry := RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond}
	lease := LeaseGuardFunc(func(context.Context) error { return nil })
	const configHost = "config-base.example.test"
	for name, config := range map[string]map[string]string{
		"config base_url":          {"base_url": "https://" + configHost},
		"config url":               {"url": "https://" + configHost},
		"config base and new mode": {"base_url": "https://" + configHost, "auth_mode": "token"},
	} {
		t.Run(name, func(t *testing.T) {
			var hosts []string
			doer := HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
				hosts = append(hosts, request.URL.Host)
				if strings.HasSuffix(request.URL.Path, "/access_tokens") {
					return testHTTPResponse(request, http.StatusCreated, nil, `{"token":"inst_ok","expires_at":"2099-01-01T00:00:00Z"}`), nil
				}
				return testHTTPResponse(request, http.StatusOK, nil, `{}`), nil
			})
			credential := NewCredential("github", "id", config, map[string]secrets.Value{
				"app_id": secrets.NewValue("1"), "private_key": secrets.NewValue(pemKey), "installation_id": secrets.NewValue("2"),
			})
			credential.Name = "renamed"
			client, err := NewGitHubClient(credential, fakehttp.Client(doer), retry, lease)
			if err != nil {
				t.Fatal(err)
			}
			response, err := client.Do(context.Background(), http.MethodGet, "/installation/repositories", nil)
			if err == nil {
				response.Body.Close()
			}
			if len(hosts) == 0 {
				t.Fatalf("no request was sent (err %v): the cell measures nothing", err)
			}
			for _, host := range hosts {
				if host == configHost {
					t.Errorf("a request went to the config base %s; want none (hosts %v)", host, hosts)
					break
				}
			}
		})
	}
	// Control: a token credential keeps its config base.
	var hosts []string
	doer := HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		hosts = append(hosts, request.URL.Host)
		return testHTTPResponse(request, http.StatusOK, nil, `{}`), nil
	})
	client, err := NewGitHubClient(NewCredential("github", "id", map[string]string{"base_url": "https://" + configHost},
		map[string]secrets.Value{"token": secrets.NewValue("tok")}), fakehttp.Client(doer), retry, lease)
	if err != nil {
		t.Fatal(err)
	}
	if response, err := client.Do(context.Background(), http.MethodGet, "/user", nil); err == nil {
		response.Body.Close()
	}
	if len(hosts) == 0 || hosts[0] != configHost {
		t.Errorf("control: a token credential sent its requests to %v; want the config base", hosts)
	}
}
