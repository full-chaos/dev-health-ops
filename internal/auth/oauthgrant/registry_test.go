package oauthgrant

import (
	"encoding/json"
	"path/filepath"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// TestTheEmbeddedRegistryIsTheContractAndNamesTheBrowserClient: the bytes the binary
// embeds pass oauth-clients.v1, and the first-party browser row is a path on acr-api.
func TestTheEmbeddedRegistryIsTheContractAndNamesTheBrowserClient(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	var doc any
	if err := json.Unmarshal(authv1.OAuthClients, &doc); err != nil {
		t.Fatal(err)
	}
	if err := contracts.Validate(filepath.Clean(root), "oauth-clients.v1", doc); err != nil {
		t.Fatalf("the embedded registry fails its schema: %v", err)
	}
	registry, err := LoadRegistry()
	if err != nil {
		t.Fatal(err)
	}
	web, ok := registry.Client("dev-health-web")
	if !ok || web.Class != ClassBrowser || !web.FirstParty || web.RedirectPath != "/ask-dev/oauth/callback" ||
		len(web.Audiences) != 1 || web.Audiences[0] != contracts.ACRAPIAudience {
		t.Fatalf("browser row = %+v, %v", web, ok)
	}
	if _, ok := registry.Client("unknown"); ok {
		t.Fatal("an unknown client resolved")
	}
}

func TestTheRegistryParseRefusesWhatASchemaCannot(t *testing.T) {
	row := `{"client_id":"web","client_name":"W","client_class":"browser","first_party":true,"audiences":["acr-api"],"redirect_path":"/x"}`
	for name, raw := range map[string]string{
		"a repeated client": `{"schema_version":"oauth-clients.v1","clients":[` + row + `,` + row + `]}`,
		"another version":   `{"schema_version":"oauth-clients.v2","clients":[` + row + `]}`,
		"an unknown member": `{"schema_version":"oauth-clients.v1","extra":1,"clients":[` + row + `]}`,
		"trailing data":     `{"schema_version":"oauth-clients.v1","clients":[` + row + `]}{}`,
	} {
		if _, err := parseRegistry([]byte(raw)); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	if _, err := parseRegistry([]byte(`{"schema_version":"oauth-clients.v1","clients":[` + row + `]}`)); err != nil {
		t.Fatalf("positive control: %v", err)
	}
}
