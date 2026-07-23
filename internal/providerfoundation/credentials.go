package providerfoundation

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"golang.org/x/crypto/pbkdf2"
)

const (
	credentialCiphertextV1 = "v1:"
	defaultEncryptionSalt  = "dev-health-ops-settings-encryption-v1"
	fernetVersion          = byte(0x80)
	fernetBlockSize        = aes.BlockSize
	fernetSignatureSize    = sha256.Size
	fernetHeaderSize       = 1 + 8 + fernetBlockSize
)

// FernetDecryptor is wire-compatible with core/encryption.py. It supports
// v1 PBKDF2 ciphertext and the legacy SHA-256 derived ciphertext so migration
// workers never need to mutate process-global credential environment values.
type FernetDecryptor struct {
	key  secrets.Value
	salt string
}

func NewFernetDecryptor(key secrets.Value, salt string) (FernetDecryptor, error) {
	if !key.Configured() {
		return FernetDecryptor{}, ErrCredentialInvalid
	}
	if salt == "" {
		salt = defaultEncryptionSalt
	}
	return FernetDecryptor{key: key, salt: salt}, nil
}

func (d FernetDecryptor) Decrypt(ciphertext secrets.Value) ([]byte, error) {
	if !d.key.Configured() || !ciphertext.Configured() {
		return nil, ErrCredentialInvalid
	}
	raw := ciphertext.Reveal()
	key := []byte(d.key.Reveal())
	if strings.HasPrefix(raw, credentialCiphertextV1) {
		return decryptFernet(strings.TrimPrefix(raw, credentialCiphertextV1), pbkdf2.Key(key, []byte(d.salt), 600000, 32, sha256.New))
	}
	if strings.HasPrefix(raw, "v") && strings.Contains(raw, ":") {
		return nil, ErrCredentialInvalid
	}
	legacy := sha256.Sum256(key)
	return decryptFernet(raw, legacy[:])
}

func decryptFernet(token string, key []byte) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		decoded, err = base64.URLEncoding.DecodeString(token)
	}
	if err != nil || len(decoded) < fernetHeaderSize+fernetSignatureSize || decoded[0] != fernetVersion || len(key) != 32 {
		return nil, ErrCredentialInvalid
	}
	signed := decoded[:len(decoded)-fernetSignatureSize]
	mac := hmac.New(sha256.New, key[:16])
	_, _ = mac.Write(signed)
	if !hmac.Equal(mac.Sum(nil), decoded[len(decoded)-fernetSignatureSize:]) {
		return nil, ErrCredentialInvalid
	}
	block, err := aes.NewCipher(key[16:])
	if err != nil {
		return nil, ErrCredentialInvalid
	}
	if len(signed[fernetHeaderSize:]) == 0 || len(signed[fernetHeaderSize:])%fernetBlockSize != 0 {
		return nil, ErrCredentialInvalid
	}
	plain := make([]byte, len(signed[fernetHeaderSize:]))
	cipher.NewCBCDecrypter(block, signed[9:fernetHeaderSize]).CryptBlocks(plain, signed[fernetHeaderSize:])
	padding := int(plain[len(plain)-1])
	if padding == 0 || padding > fernetBlockSize || padding > len(plain) || !bytes.Equal(plain[len(plain)-padding:], bytes.Repeat([]byte{byte(padding)}, padding)) {
		return nil, ErrCredentialInvalid
	}
	return plain[:len(plain)-padding], nil
}

func decodeCredential(record EncryptedCredential, plaintext []byte) (Credential, error) {
	var values map[string]any
	if err := json.Unmarshal(plaintext, &values); err != nil {
		return Credential{}, ErrCredentialInvalid
	}
	fields := make(map[string]secrets.Value, len(values))
	for key, value := range values {
		text, ok := value.(string)
		if !ok || strings.TrimSpace(key) == "" {
			return Credential{}, ErrCredentialInvalid
		}
		fields[key] = secrets.NewValue(text)
	}
	config := make(map[string]string, len(record.Config))
	for key, value := range record.Config {
		config[key] = value
	}
	return Credential{Provider: record.Provider, ID: record.ID, Name: record.Name, Config: config, fields: fields}, nil
}

// ValidateCredentialShape keeps auth construction explicit. It accepts only
// the auth fields that the current Python resolver accepts for this provider.
func ValidateCredentialShape(credential Credential) error {
	has := func(name string) bool { value, ok := credential.Secret(name); return ok && value.Configured() }
	switch credential.Provider {
	case "github":
		token := has("token")
		app := has("app_id") && has("private_key") && has("installation_id")
		if token == app {
			return ErrCredentialInvalid
		}
	case "gitlab":
		if !has("token") {
			return ErrCredentialInvalid
		}
	case "jira":
		if !has("api_token") || !has("email") {
			return ErrCredentialInvalid
		}
	case "linear":
		if !has("api_key") {
			return ErrCredentialInvalid
		}
	default:
		return fmt.Errorf("%w: unsupported provider", ErrCredentialInvalid)
	}
	return nil
}
