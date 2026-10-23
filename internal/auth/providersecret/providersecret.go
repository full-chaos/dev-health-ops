// Package providersecret decrypts sso_providers.encrypted_secrets values, the
// Fernet format of core/encryption.py. It is a leaf package: the auth binaries
// may not link the analytics stores that providerfoundation does.
package providersecret

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"

	"golang.org/x/crypto/pbkdf2"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const (
	defaultSalt   = "dev-health-ops-settings-encryption-v1"
	versionPrefix = "v1:"
	pbkdf2Rounds  = 600000

	fernetVersion       = 0x80
	fernetHeaderSize    = 25
	fernetSignatureSize = 32
	fernetBlockSize     = 16
)

// ErrInvalid is every refusal: a missing key, a malformed or forged value.
var ErrInvalid = errors.New("providersecret: the stored value cannot be decrypted")

// Decrypter decrypts with the settings encryption key and salt.
type Decrypter struct {
	key  secrets.Value
	salt string
}

// New binds the key; an empty salt is the default salt.
func New(key secrets.Value, salt string) (Decrypter, error) {
	if !key.Configured() {
		return Decrypter{}, ErrInvalid
	}
	if salt == "" {
		salt = defaultSalt
	}
	return Decrypter{key: key, salt: salt}, nil
}

// Decrypt returns the plaintext and registers it with the log redactor.
func (d Decrypter) Decrypt(stored string) (string, error) {
	plain, err := d.decrypt(stored)
	if err != nil {
		return "", err
	}
	secrets.RegisterDecrypted(plain)
	return string(plain), nil
}

func (d Decrypter) decrypt(stored string) ([]byte, error) {
	if !d.key.Configured() || stored == "" {
		return nil, ErrInvalid
	}
	key := []byte(d.key.Reveal())
	if strings.HasPrefix(stored, versionPrefix) {
		return decryptFernet(strings.TrimPrefix(stored, versionPrefix), pbkdf2.Key(key, []byte(d.salt), pbkdf2Rounds, 32, sha256.New))
	}
	if strings.HasPrefix(stored, "v") && strings.Contains(stored, ":") {
		return nil, ErrInvalid
	}
	legacy := sha256.Sum256(key)
	return decryptFernet(stored, legacy[:])
}

func decryptFernet(token string, key []byte) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		decoded, err = base64.URLEncoding.DecodeString(token)
	}
	if err != nil || len(decoded) < fernetHeaderSize+fernetSignatureSize || decoded[0] != fernetVersion || len(key) != 32 {
		return nil, ErrInvalid
	}
	signed := decoded[:len(decoded)-fernetSignatureSize]
	mac := hmac.New(sha256.New, key[:16])
	_, _ = mac.Write(signed)
	if !hmac.Equal(mac.Sum(nil), decoded[len(decoded)-fernetSignatureSize:]) {
		return nil, ErrInvalid
	}
	block, err := aes.NewCipher(key[16:])
	if err != nil {
		return nil, ErrInvalid
	}
	body := signed[fernetHeaderSize:]
	if len(body) == 0 || len(body)%fernetBlockSize != 0 {
		return nil, ErrInvalid
	}
	plain := make([]byte, len(body))
	cipher.NewCBCDecrypter(block, signed[9:fernetHeaderSize]).CryptBlocks(plain, body)
	padding := int(plain[len(plain)-1])
	if padding == 0 || padding > fernetBlockSize || padding > len(plain) || !bytes.Equal(plain[len(plain)-padding:], bytes.Repeat([]byte{byte(padding)}, padding)) {
		return nil, ErrInvalid
	}
	return plain[:len(plain)-padding], nil
}
