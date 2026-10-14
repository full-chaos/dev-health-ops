package providerfoundation

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-9678: the decode the operator verb uses is the worker's decode. Every cell is by value.
func TestDecodeStoredGitHubCredentialIsTheWorkersDecode(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "app-key.pem")
	if err := os.WriteFile(keyFile, []byte("-----BEGIN KEY-----\nfrom-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		payload string
		token   string
		appID   string
		key     string
		install string
		baseURL string
		ignored string
	}{
		{"pat", `{"token":"t"}`, "t", "", "", "", "", ""},
		{"a web-saved pat with an org key", `{"token":"t","org":"acme"}`, "t", "", "", "", "", "org"},
		{"unused keys of any type, sorted", `{"zeta":1,"token":"t","alpha":{"a":1},"":"blank"}`, "t", "", "", "", "", "alpha,zeta"},
		{"camelCase app keys are aliased", `{"appId":"1","privateKey":"k","installationId":"2"}`, "", "1", "k", "2", "", ""},
		{"base url, aliased", `{"token":"t","baseUrl":"https://ghe.example.com/api/v3"}`, "t", "", "", "", "https://ghe.example.com/api/v3", ""},
		{"the later key of the document wins over its alias", `{"app_id":"1","appId":"9","private_key":"k","installation_id":"2"}`, "", "9", "k", "2", "", ""},
		{"the later canonical key wins over an alias", `{"appId":"9","app_id":"1","private_key":"k","installation_id":"2"}`, "", "1", "k", "2", "", ""},
		{"a null never replaces a value", `{"app_id":"1","appId":null,"private_key":"k","installation_id":"2"}`, "", "1", "k", "2", "", ""},
		{"the private key from a path", `{"app_id":"1","private_key_path":"` + keyFile + `","installation_id":"2"}`, "", "1", "-----BEGIN KEY-----\nfrom-file\n", "2", "", ""},
		{"the private key wins over a path", `{"app_id":"1","private_key":"inline","private_key_path":"` + keyFile + `","installation_id":"2"}`, "", "1", "inline", "2", "", ""},
		{"a null private key and a path", `{"app_id":"1","private_key":null,"private_key_path":"` + keyFile + `","installation_id":"2"}`, "", "1", "-----BEGIN KEY-----\nfrom-file\n", "2", "", ""},
		{"a token that is not a string is read as Python str() reads it", `{"token":5}`, "5", "", "", "", "", ""},
		{"the dataclass keys are ignored by the worker, and named", `{"token":"t","provider":"github","credential_name":"x","extra":{"a":1}}`, "t", "", "", "", "", "credential_name,extra,provider"},
		{"an empty token and a full app", `{"token":"","app_id":"1","private_key":"k","installation_id":"2"}`, "", "1", "k", "2", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeStoredGitHubCredential([]byte(tc.payload))
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if got.Token.Reveal() != tc.token || got.AppID.Reveal() != tc.appID || got.PrivateKey.Reveal() != tc.key || got.InstallationID.Reveal() != tc.install || got.BaseURL.Reveal() != tc.baseURL || strings.Join(got.Ignored, ",") != tc.ignored {
				t.Fatalf("got token %q app %q key %q installation %q base %q ignored %v, want %q %q %q %q %q %q",
					got.Token.Reveal(), got.AppID.Reveal(), got.PrivateKey.Reveal(), got.InstallationID.Reveal(), got.BaseURL.Reveal(), got.Ignored,
					tc.token, tc.appID, tc.key, tc.install, tc.baseURL, tc.ignored)
			}
		})
	}
}

func TestDecodeStoredGitHubCredentialRefusalsHaveAReasonAndNoValue(t *testing.T) {
	cases := []struct {
		name     string
		payload  string
		reason   string
		hasToken bool
		hasApp   bool
	}{
		{"not json", `not json`, StoredCredentialPayloadNotJSON, false, false},
		{"a list", `["a"]`, StoredCredentialPayloadNotObject, false, false},
		{"null", `null`, StoredCredentialPayloadNotObject, false, false},
		{"a string", `"t"`, StoredCredentialPayloadNotObject, false, false},
		{"empty object", `{}`, StoredCredentialShapeInvalid, false, false},
		{"empty token", `{"token":""}`, StoredCredentialShapeInvalid, false, false},
		{"partial app", `{"app_id":"1","private_key":"k"}`, StoredCredentialShapeInvalid, false, true},
		{"token and a complete app", `{"token":"t","app_id":"1","private_key":"k","installation_id":"2"}`, StoredCredentialShapeInvalid, true, true},
		{"token and an installation id", `{"token":"t","installation_id":"2"}`, StoredCredentialShapeInvalid, true, true},
		{"token and a private key", `{"token":"t","private_key":"k"}`, StoredCredentialShapeInvalid, true, true},
		{"app without an app id", `{"private_key":"k","installation_id":"2"}`, StoredCredentialShapeInvalid, false, true},
		{"an installation id that is not text beside a token", `{"token":"t","installation_id":5}`, StoredCredentialShapeInvalid, true, true},
		{"an unreadable private key path", `{"app_id":"1","private_key_path":"/nonexistent/key.pem","installation_id":"2"}`, StoredCredentialPrivateKeyPathUnread, false, false},
		{"an unreadable private key path beside a token", `{"token":"t","private_key_path":"/nonexistent/key.pem"}`, StoredCredentialPrivateKeyPathUnread, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeStoredGitHubCredential([]byte(tc.payload))
			var refused *StoredCredentialFailure
			if !errors.As(err, &refused) || !errors.Is(err, ErrCredentialInvalid) {
				t.Fatalf("err = %v, want a *StoredCredentialFailure wrapping ErrCredentialInvalid", err)
			}
			if refused.Reason != tc.reason || refused.HasToken != tc.hasToken || refused.HasApp != tc.hasApp {
				t.Fatalf("refusal = %+v, want reason %q has_token %v has_app %v", refused, tc.reason, tc.hasToken, tc.hasApp)
			}
			if err.Error() != "stored credential refused: "+tc.reason {
				t.Fatalf("the error text is %q: it must be the fixed reason only, never a value", err.Error())
			}
		})
	}
}
