// Package fernet is the one Fernet implementation of the module, wire
// compatible with core/encryption.py: v1 PBKDF2 ciphertext and the legacy
// SHA-256 derived ciphertext. It imports only the standard library and
// x/crypto, so any binary may link it. A census test keeps a second
// implementation from appearing.
package fernet

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"time"
)

const (
	// V1Prefix marks a ciphertext whose key is derived with PBKDF2.
	V1Prefix = "v1:"
	// DefaultSalt is the PBKDF2 salt when none is configured.
	DefaultSalt = "dev-health-ops-settings-encryption-v1"
	// Version is the Fernet version byte.
	Version       = byte(0x80)
	BlockSize     = aes.BlockSize
	SignatureSize = sha256.Size
	HeaderSize    = 1 + 8 + BlockSize

	pbkdf2Rounds = 600000

	// maxPlaintext bounds what EncryptToken seals: a stored secret is a few
	// hundred bytes, and the bound keeps the buffer sizes below from overflowing.
	maxPlaintext = 1 << 24
)

// ErrInvalid is every refusal: a missing key, a malformed, forged or
// wrong-key value. It never says which.
var ErrInvalid = errors.New("fernet: the value cannot be processed")

// Cipher is a settings key with its salt.
type Cipher struct {
	key  []byte
	salt []byte
}

// New binds the key; an empty salt is DefaultSalt. An empty key is ErrInvalid.
func New(key, salt string) (Cipher, error) {
	if key == "" {
		return Cipher{}, ErrInvalid
	}
	if salt == "" {
		salt = DefaultSalt
	}
	return Cipher{key: []byte(key), salt: []byte(salt)}, nil
}

// Open decrypts a stored value: "v1:" + token (PBKDF2 key) or a bare token
// (legacy SHA-256 key). Any other "vN:" prefix is refused.
func (c Cipher) Open(stored string) ([]byte, error) {
	if len(c.key) == 0 || stored == "" {
		return nil, ErrInvalid
	}
	if strings.HasPrefix(stored, V1Prefix) {
		derived, err := derivedKey(c.key, c.salt)
		if err != nil {
			return nil, ErrInvalid
		}
		return DecryptToken(strings.TrimPrefix(stored, V1Prefix), derived)
	}
	if strings.HasPrefix(stored, "v") && strings.Contains(stored, ":") {
		return nil, ErrInvalid
	}
	legacy := sha256.Sum256(c.key)
	return DecryptToken(stored, legacy[:])
}

// Seal encrypts plaintext into the v1 format.
func (c Cipher) Seal(plaintext []byte, now time.Time, entropy io.Reader) (string, error) {
	if len(c.key) == 0 {
		return "", ErrInvalid
	}
	derived, err := derivedKey(c.key, c.salt)
	if err != nil {
		return "", ErrInvalid
	}
	token, err := EncryptToken(plaintext, derived, now, entropy)
	if err != nil {
		return "", err
	}
	return V1Prefix + token, nil
}

// EncryptToken seals plaintext under a 32-byte key (signing half first).
func EncryptToken(plaintext, key []byte, now time.Time, entropy io.Reader) (string, error) {
	if len(key) != 32 || entropy == nil || len(plaintext) > maxPlaintext {
		return "", ErrInvalid
	}
	padding := BlockSize - len(plaintext)%BlockSize
	padded := make([]byte, len(plaintext)+padding)
	copy(padded, plaintext)
	copy(padded[len(plaintext):], bytes.Repeat([]byte{byte(padding)}, padding))
	block, err := aes.NewCipher(key[16:])
	if err != nil {
		return "", ErrInvalid
	}
	iv := make([]byte, BlockSize)
	if _, err := io.ReadFull(entropy, iv); err != nil {
		return "", ErrInvalid
	}
	payload := make([]byte, HeaderSize, HeaderSize+len(padded)+SignatureSize)
	payload[0] = Version
	binary.BigEndian.PutUint64(payload[1:9], uint64(now.Unix()))
	copy(payload[9:HeaderSize], iv)
	encrypted := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(encrypted, padded)
	payload = append(payload, encrypted...)
	mac := hmac.New(sha256.New, key[:16])
	_, _ = mac.Write(payload)
	payload = append(payload, mac.Sum(nil)...)
	return base64.URLEncoding.EncodeToString(payload), nil
}

// DecryptToken opens a token under a 32-byte key. The signature is checked
// before anything is decrypted.
func DecryptToken(token string, key []byte) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		decoded, err = base64.URLEncoding.DecodeString(token)
	}
	if err != nil || len(decoded) < HeaderSize+SignatureSize || decoded[0] != Version || len(key) != 32 {
		return nil, ErrInvalid
	}
	signed := decoded[:len(decoded)-SignatureSize]
	mac := hmac.New(sha256.New, key[:16])
	_, _ = mac.Write(signed)
	if !hmac.Equal(mac.Sum(nil), decoded[len(decoded)-SignatureSize:]) {
		return nil, ErrInvalid
	}
	block, err := aes.NewCipher(key[16:])
	if err != nil {
		return nil, ErrInvalid
	}
	body := signed[HeaderSize:]
	if len(body) == 0 || len(body)%BlockSize != 0 {
		return nil, ErrInvalid
	}
	plain := make([]byte, len(body))
	cipher.NewCBCDecrypter(block, signed[9:HeaderSize]).CryptBlocks(plain, body)
	padding := int(plain[len(plain)-1])
	if padding == 0 || padding > BlockSize || padding > len(plain) || !bytes.Equal(plain[len(plain)-padding:], bytes.Repeat([]byte{byte(padding)}, padding)) {
		return nil, ErrInvalid
	}
	return plain[:len(plain)-padding], nil
}
