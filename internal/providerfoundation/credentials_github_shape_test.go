package providerfoundation

import (
	"errors"
	"testing"
)

// TestGitHubShapeRefusesATokenBesideAnyAppField (CHAOS-6781): Python's
// GitHubCredentials raises when a token comes with ANY App field, not only a
// complete triple, and the builder returns None. The live differential holds
// the same cases against the real builder; this runs in every unit pass.
func TestGitHubShapeRefusesATokenBesideAnyAppField(t *testing.T) {
	cases := []struct {
		name, body string
		refused    bool
	}{
		{"a token alone", `{"token": "t"}`, false},
		{"a token with app_id", `{"token": "t", "app_id": "12"}`, true},
		{"a token with a numeric app_id", `{"token": "t", "app_id": 12}`, true},
		{"a token with installation_id", `{"token": "t", "installation_id": "1"}`, true},
		{"a token with a private key", `{"token": "t", "private_key": "k"}`, true},
		{"a token with a camelCase private key", `{"token": "t", "privateKey": "k"}`, true},
		{"a token with a private key path", `{"token": "t", "private_key_path": "/k.pem"}`, true},
		{"a token with an empty private key and a path", `{"token": "t", "private_key": "", "private_key_path": "/k.pem"}`, false},
		{"a token with an empty private key", `{"token": "t", "private_key": ""}`, false},
		{"a token with falsy App fields", `{"token": "t", "app_id": 0, "installation_id": false}`, false},
		{"a token with a null private key beside a path", `{"token": "t", "private_key": null, "private_key_path": "/k.pem"}`, true},
		{"the full App triple", `{"app_id": "1", "installation_id": "2", "private_key": "k"}`, false},
		{"the triple with a token", `{"token": "t", "app_id": "1", "installation_id": "2", "private_key": "k"}`, true},
		{"a partial App credential", `{"app_id": "1", "installation_id": "2"}`, true},
		{"nothing", `{}`, true},
	}
	for _, tc := range cases {
		credential, err := decodeGitHub(t, tc.body)
		if err != nil {
			t.Errorf("%s: decode: %v", tc.name, err)
			continue
		}
		err = ValidateCredentialShape(credential)
		if tc.refused != errors.Is(err, ErrCredentialInvalid) {
			t.Errorf("%s: ValidateCredentialShape = %v, want refused=%v", tc.name, err, tc.refused)
		}
	}
}
