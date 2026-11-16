package authapi

import (
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

const ssoExtraSecret = "s3ntinel-extra-member-VALUE"

// The legacy SAMLConfigInput / OIDCConfigInput (pydantic, extra="ignore") dropped a member they do not declare; the stored
// config is the model dump. Every kind of unknown member is admitted and none is stored.
var ssoUnknownMembers = map[string]string{
	"plain":          `"x":"y"`,
	"sp members":     `"sp_acs_url":"https://sp.example/acs","sp_entity_id":"https://sp.example/metadata"`,
	"object":         `"extra":{"a":1}`,
	"array":          `"extra":[1,"two"]`,
	"null":           `"extra":null`,
	"number":         `"extra":12.5`,
	"other protocol": `"client_id":"cid","issuer":"https://i.example"`,
}

const ssoSAMLModel = `"entity_id":"e","sso_url":"https://s.example","certificate":"CERT"`

const ssoSAMLStored = `{"entity_id": "e", "sso_url": "https://s.example", "certificate": "CERT", "slo_url": null, ` +
	`"name_id_format": "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress", "attribute_mapping": {}}`

const ssoOIDCStored = `{"client_id": "cid", "issuer": "https://i.example", "authorization_endpoint": null, "token_endpoint": null, ` +
	`"userinfo_endpoint": null, "jwks_uri": null, "scopes": ["openid", "profile", "email"], "claim_mapping": {}}`

func ssoWriteBody(f *provFixture, path, members string) string {
	head := `{"organization_id":"` + f.org.String() + `","actor_principal_id":"` + f.owner.String() + `",`
	if path == PatternSSOProviderUpdate {
		return head + `"provider_id":"` + uuid.NewString() + `",` + members + `}`
	}
	return head + `"name":"Acme",` + members + `}`
}

func TestSSOProviderConfigUnknownMembersAreDroppedAsTheLegacyModelDid(t *testing.T) {
	t.Parallel()
	for _, path := range []string{PatternSSOProviderCreate, PatternSSOProviderUpdate} {
		want := map[string]int{PatternSSOProviderCreate: http.StatusCreated, PatternSSOProviderUpdate: http.StatusOK}[path]
		for kind, extra := range ssoUnknownMembers {
			f := newProvFixtureFor(t, nil, nil)
			body := ssoWriteBody(f, path, `"protocol":"saml","saml_config":{`+ssoSAMLModel+`,`+extra+`}`)
			if response := f.post(path, body, nil); response.Code != want {
				t.Errorf("%s saml %s: %d %s, want %d", path, kind, response.Code, response.Body, want)
				continue
			}
			config := f.sso.calls[0].create.Config.JSON
			if path == PatternSSOProviderUpdate {
				config = f.sso.calls[0].update.Config.JSON
			}
			if stored, err := pyjson.Dumps(config); err != nil || stored != ssoSAMLStored {
				t.Errorf("%s saml %s: stored config = %s (%v), want %s", path, kind, stored, err, ssoSAMLStored)
			}
		}
	}
}

func TestSSOProviderConfigCaseVariantMemberIsUnknownAndTheExactMemberIsStored(t *testing.T) {
	t.Parallel()
	f := newProvFixtureFor(t, nil, nil)
	body := ssoWriteBody(f, PatternSSOProviderCreate, `"protocol":"saml","saml_config":{"Entity_ID":"variant",`+ssoSAMLModel+`,"ENTITY_ID":"variant2"}`)
	if response := f.post(PatternSSOProviderCreate, body, nil); response.Code != http.StatusCreated {
		t.Fatalf("create: %d %s, want 201", response.Code, response.Body)
	}
	if stored, err := pyjson.Dumps(f.sso.calls[0].create.Config.JSON); err != nil || stored != ssoSAMLStored {
		t.Fatalf("stored config = %s (%v), want %s", stored, err, ssoSAMLStored)
	}
}

func TestSSOProviderOIDCUnknownMembersReachNeitherTheConfigNorTheSecrets(t *testing.T) {
	t.Parallel()
	for _, path := range []string{PatternSSOProviderCreate, PatternSSOProviderUpdate} {
		f := newProvFixtureFor(t, nil, nil)
		body := ssoWriteBody(f, path, `"protocol":"oidc","oidc_config":{"client_id":"cid","client_secret":"`+ssoSecretSentinel+
			`","issuer":"https://i.example","x":"y","client_secret_2":"`+ssoExtraSecret+`","entity_id":"e"}`)
		response := f.post(path, body, nil)
		if response.Code/100 != 2 {
			t.Errorf("%s: %d %s, want 2xx", path, response.Code, response.Body)
			continue
		}
		config := f.sso.calls[0].create.Config
		if path == PatternSSOProviderUpdate {
			config = *f.sso.calls[0].update.Config
		}
		if stored, err := pyjson.Dumps(config.JSON); err != nil || stored != ssoOIDCStored {
			t.Errorf("%s: stored config = %s (%v), want %s", path, stored, err, ssoOIDCStored)
		}
		if config.Secrets == nil || *config.Secrets != `{"client_secret":"sealed(`+strings.ToUpper(ssoSecretSentinel)+`)"}` {
			t.Errorf("%s: secrets = %v, want only the sealed client_secret", path, config.Secrets)
		}
		if strings.Contains(response.Body.String(), ssoExtraSecret) || strings.Contains(f.logs.String(), ssoExtraSecret) {
			t.Errorf("%s: an unknown member's value is in an answer or a log", path)
		}
	}
}

// The legacy name bound is max_length=255, counted in code points (go-api's own 422 check counts the same way).
func TestSSOProviderNameIsBoundedInCodePoints(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name   string
		points int
		create int
		update int
	}{
		{"255 two-byte code points", 255, http.StatusCreated, http.StatusOK},
		{"255 four-byte code points", 255, http.StatusCreated, http.StatusOK},
		{"256 code points", 256, http.StatusBadRequest, http.StatusBadRequest},
	} {
		char := "é"
		if strings.Contains(c.name, "four") {
			char = "\U0001F600"
		}
		name := strings.Repeat(char, c.points)
		for path, want := range map[string]int{PatternSSOProviderCreate: c.create, PatternSSOProviderUpdate: c.update} {
			f := newProvFixtureFor(t, nil, nil)
			body := map[string]any{"organization_id": f.org.String(), "actor_principal_id": f.owner.String(), "name": name}
			if path == PatternSSOProviderCreate {
				body["protocol"], body["oidc_config"] = "oidc", ssoOIDCConfig()
			} else {
				body["provider_id"] = uuid.NewString()
			}
			response := f.post(path, encode(body), nil)
			if response.Code != want {
				t.Errorf("%s %s: %d %s, want %d", path, c.name, response.Code, response.Body, want)
			}
			if want == http.StatusBadRequest {
				expectRefusal(t, response, http.StatusBadRequest, reasonMalformed)
			}
		}
	}
}

// The resource guards legacy did not have stay, and answer a client 400 (go-api passes it on as 400, never its 503).
func TestSSOProviderResourceGuardsAnswer400Malformed(t *testing.T) {
	t.Parallel()
	domains := make([]string, 257)
	for i := range domains {
		domains[i] = "d.example"
	}
	for name, extra := range map[string]map[string]any{
		"default_role over 320 bytes": {"default_role": strings.Repeat("r", 321)},
		"257 allowed domains":         {"allowed_domains": domains},
		"config over 64 KiB":          {"oidc_config": map[string]any{"client_id": "cid", "client_secret": "s", "issuer": strings.Repeat("i", 64<<10)}},
	} {
		f := newProvFixtureFor(t, nil, nil)
		body := map[string]any{"organization_id": f.org.String(), "actor_principal_id": f.owner.String(), "name": "Acme", "protocol": "oidc", "oidc_config": ssoOIDCConfig()}
		for k, v := range extra {
			body[k] = v
		}
		expectRefusal(t, f.post(PatternSSOProviderCreate, encode(body), nil), http.StatusBadRequest, reasonMalformed)
		if len(f.sso.calls) != 0 {
			t.Errorf("%s reached the store", name)
		}
	}
}
