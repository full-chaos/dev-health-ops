package providerfoundation

import (
	"bytes"
	"crypto/sha256"
	"time"

	"golang.org/x/crypto/pbkdf2"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// DecodeCredential is decodeCredential for the external test package of the
// frozen credential oracles, which cannot import the program helper from
// inside package providerfoundation.
var DecodeCredential = decodeCredential

// GoFieldRead is the text Go's consumers get for a field: Jira's token and URL
// take the first configured spelling in Python's precedence order
// (`a or b or c`), every other field is read by its own name.
func GoFieldRead(credential Credential, provider, field string) (secrets.Value, bool) {
	names := []string{field}
	if provider == "jira" {
		switch field {
		case "api_token":
			names = jiraAPITokenAliases
		case "base_url":
			names = jiraBaseURLAliases
		}
	}
	for _, name := range names {
		if value, ok := credential.Secret(name); ok && value.Configured() {
			return value, true
		}
	}
	return secrets.Value{}, false
}

// GridGoField is what Go's consumers read for a Python dataclass field, or nil
// when no reader exists (the test then fails: a new Python field needs a
// decision). Empty means not configured.
func GridGoField(credential Credential, provider, field string) *string {
	text := func(names ...string) *string {
		out := ""
		for _, name := range names {
			if value, ok := credential.Secret(name); ok && value.Configured() {
				out = value.Reveal()
				break
			}
		}
		return &out
	}
	switch provider + "." + field {
	case "github.token", "github.app_id", "github.installation_id", "github.base_url":
		return text(field)
	case "github.private_key":
		if _, present := credential.Secret("private_key"); present {
			return text("private_key")
		}
		out := ""
		if path, ok := credential.Secret("private_key_path"); ok && path.Configured() {
			if content, err := readGitHubAppPrivateKeyFile(path.Reveal()); err == nil {
				out = content.Reveal()
			}
		}
		return &out
	case "gitlab.token":
		return text("token")
	case "gitlab.base_url":
		out := gitLabCredentialBaseURL(credential)
		return &out
	case "jira.api_token":
		return text(jiraAPITokenAliases...)
	case "jira.email":
		return text("email")
	case "jira.base_url":
		out := jiraCredentialBaseURL(credential)
		return &out
	case "linear.api_key":
		return text("api_key")
	}
	return nil
}

// SealFernetAsPythonDoes is what the Go cipher's Encrypt emits for the key, salt and plaintext when the clock is
// 1700000000 and the IV is bytes 0..15: the two inputs the frozen Python exchange pins (fernetExchangeProgram), so the
// result is byte-comparable with the recorded Python ciphertext.
func SealFernetAsPythonDoes(key, salt string, plaintext []byte) (string, error) {
	decryptor, err := NewFernetDecryptor(secrets.NewValue(key), salt)
	if err != nil {
		return "", err
	}
	derived := pbkdf2.Key([]byte(decryptor.key.Reveal()), []byte(decryptor.salt), 600000, 32, sha256.New)
	iv := make([]byte, 16)
	for index := range iv {
		iv[index] = byte(index)
	}
	token, err := encryptFernet(plaintext, derived, time.Unix(1700000000, 0), bytes.NewReader(iv))
	if err != nil {
		return "", err
	}
	return credentialCiphertextV1 + token, nil
}
