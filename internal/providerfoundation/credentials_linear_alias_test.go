package providerfoundation

import "testing"

// TestLinearReadsTheApiKeyAliasLikePython (CHAOS-6782): linear_credentials_from_mapping
// reads `str(api_key or apiKey or "")`: the canonical spelling wins when it is
// truthy (not by document order), otherwise the camelCase spelling, and the
// shape check and the client read the same api_key. The live differential
// holds the same cases against the real builder.
func TestLinearReadsTheApiKeyAliasLikePython(t *testing.T) {
	cases := []struct {
		name, body string
		want       string
		built      bool
	}{
		{"canonical", `{"api_key": "k"}`, "k", true},
		{"alias only", `{"apiKey": "k"}`, "k", true},
		{"numeric alias", `{"apiKey": 12.5}`, "12.5", true},
		{"canonical first beats a later alias", `{"api_key": "c", "apiKey": "a"}`, "c", true},
		{"canonical last beats an earlier alias", `{"apiKey": "a", "api_key": "c"}`, "c", true},
		{"empty canonical falls to the alias", `{"api_key": "", "apiKey": "a"}`, "a", true},
		{"zero canonical falls to the alias", `{"api_key": 0, "apiKey": "a"}`, "a", true},
		{"null canonical falls to the alias", `{"api_key": null, "apiKey": "a"}`, "a", true},
		{"falsy alias with a canonical", `{"api_key": "c", "apiKey": 0}`, "c", true},
		{"empty alias only", `{"apiKey": ""}`, "", false},
		{"neither", `{"workspace_id": "w"}`, "", false},
	}
	for _, tc := range cases {
		credential, err := decodeCredential(EncryptedCredential{Provider: "linear"}, []byte(tc.body))
		if err != nil {
			t.Errorf("%s: decode: %v", tc.name, err)
			continue
		}
		if built := ValidateCredentialShape(credential) == nil; built != tc.built {
			t.Errorf("%s: built = %v, want %v", tc.name, built, tc.built)
		}
		if got, _ := credential.Secret("api_key"); got.Reveal() != tc.want {
			t.Errorf("%s: api_key = %q, want %q", tc.name, got.Reveal(), tc.want)
		}
	}
	// The alias is Linear's: another provider's apiKey is not renamed.
	other, err := decodeCredential(EncryptedCredential{Provider: "launchdarkly"}, []byte(`{"apiKey": "k"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := other.Secret("api_key"); ok || got.Configured() {
		t.Errorf("a launchdarkly apiKey was read as api_key: %q, %v", got.Reveal(), ok)
	}
}
