package providerfoundation

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// These tests open tokens that were changed after they were sealed. decryptFernet is the one place that decides whether a
// stored credential ciphertext is genuine, and its guards were pinned only by tokens that are valid. Every token here is made
// in this file from a throw-away key (a digest of a label, not a real key); none of the values is a real key or token.
//
// A case is built so that ONE guard of decryptFernet is the only thing that can refuse it: the case is validly signed when
// the guard under test is not the signature, and it changes a byte the other guards do not look at when it is. Turning that
// guard off therefore turns the case from a refusal into a success (or a panic), and the test goes red.

// sealSpec describes a token field by field, so a case can change exactly one of them.
type sealSpec struct {
	version   byte
	timestamp uint64
	iv        []byte
	body      []byte // the CBC ciphertext, already encrypted
	hmacKey   []byte
}

func testIV() []byte { return bytes.Repeat([]byte{0x11}, aes.BlockSize) }

func testKey32(label string) []byte {
	sum := sha256.Sum256([]byte("fernet-tamper-test/" + label))
	return sum[:]
}

// pkcs7 pads plaintext to a block multiple the way the writer does.
func pkcs7(plain []byte) []byte {
	pad := aes.BlockSize - len(plain)%aes.BlockSize
	return append(append([]byte{}, plain...), bytes.Repeat([]byte{byte(pad)}, pad)...)
}

// cbc encrypts blocks (already padded) with the AES half of key and the IV.
func cbc(t *testing.T, aesKey, iv, padded []byte) []byte {
	t.Helper()
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(out, padded)
	return out
}

// sealRaw builds version|timestamp|iv|body and signs it with hmacKey, standard base64 with padding.
func sealRaw(spec sealSpec) []byte {
	payload := make([]byte, 0, 1+8+len(spec.iv)+len(spec.body)+sha256.Size)
	payload = append(payload, spec.version)
	payload = binary.BigEndian.AppendUint64(payload, spec.timestamp)
	payload = append(payload, spec.iv...)
	payload = append(payload, spec.body...)
	mac := hmac.New(sha256.New, spec.hmacKey)
	_, _ = mac.Write(payload)
	return mac.Sum(payload)
}

func encode(raw []byte) string { return base64.URLEncoding.EncodeToString(raw) }

// validToken seals plaintext under key (32 bytes: HMAC half, then AES half) as the writer does.
func validToken(t *testing.T, key, plain []byte) []byte {
	t.Helper()
	iv := testIV()
	return sealRaw(sealSpec{version: fernetVersion, timestamp: 1700000000, iv: iv, body: cbc(t, key[16:], iv, pkcs7(plain)), hmacKey: key[:16]})
}

func mustRefuse(t *testing.T, token string, key []byte) {
	t.Helper()
	plain, err := decryptFernet(token, key)
	if !errors.Is(err, ErrCredentialInvalid) {
		t.Fatalf("a token that must be refused was opened or failed another way: err=%v plain length=%d", err, len(plain))
	}
	if plain != nil {
		t.Fatalf("a refused token returned %d plaintext bytes", len(plain))
	}
}

func TestDecryptFernetOpensAValidTokenInEveryEncoding(t *testing.T) {
	key := testKey32("a")
	for _, plain := range [][]byte{[]byte("x"), []byte("fifteen bytes!!"), bytes.Repeat([]byte("z"), 16), bytes.Repeat([]byte("y"), 33)} {
		raw := validToken(t, key, plain)
		for name, token := range map[string]string{
			"padded url-safe base64":   base64.URLEncoding.EncodeToString(raw),
			"unpadded url-safe base64": base64.RawURLEncoding.EncodeToString(raw),
		} {
			got, err := decryptFernet(token, key)
			if err != nil || !bytes.Equal(got, plain) {
				t.Fatalf("%s, %d-byte plaintext: err=%v, got %d bytes", name, len(plain), err, len(got))
			}
		}
	}
}

func TestDecryptFernetRefusesAChangedToken(t *testing.T) {
	key := testKey32("a")
	plain := []byte("a short credential") // two blocks: 18 bytes padded to 32
	one := []byte("abc")                  // one block: the first three bytes are free, the padding is the last 13
	raw := validToken(t, key, plain)
	rawOne := validToken(t, key, one)
	flip := func(src []byte, at int) string {
		out := append([]byte{}, src...)
		out[at] ^= 0x01
		return encode(out)
	}
	last := len(raw) - 1
	headerEnd := fernetHeaderSize

	cases := []struct {
		name  string
		token string
		key   []byte
	}{
		// The ciphertext, the signature and the header are each changed by one bit. Only the signature check stands between
		// each of these and a successful open: the timestamp and the first IV byte of a one-block token change nothing else.
		{"one byte of the ciphertext", flip(raw, headerEnd), key},
		{"one byte of the last ciphertext block", flip(raw, last-fernetSignatureSize), key},
		{"first byte of the signature", flip(raw, len(raw)-fernetSignatureSize), key},
		{"last byte of the signature", flip(raw, last), key},
		{"a timestamp byte", flip(raw, 4), key},
		{"first byte of the IV in a one-block token", flip(rawOne, 9), key},
		// A key that did not seal the token.
		{"a wrong key", encode(raw), testKey32("b")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { mustRefuse(t, c.token, c.key) })
	}
}

func TestDecryptFernetRefusesATruncatedToken(t *testing.T) {
	key := testKey32("a")
	raw := validToken(t, key, []byte("a short credential"))
	cases := []struct {
		name string
		raw  []byte
	}{
		{"empty", nil},
		{"only the version byte", raw[:1]},
		{"a header without a body or signature", raw[:fernetHeaderSize]},
		{"one byte short of header and signature", raw[:fernetHeaderSize+fernetSignatureSize-1]},
		{"the signature cut off", raw[:len(raw)-fernetSignatureSize]},
		{"the last signature byte cut off", raw[:len(raw)-1]},
		{"the last ciphertext block cut off with the signature kept", append(append([]byte{}, raw[:len(raw)-fernetSignatureSize-aes.BlockSize]...), raw[len(raw)-fernetSignatureSize:]...)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mustRefuse(t, encode(c.raw), key)
			mustRefuse(t, base64.RawURLEncoding.EncodeToString(c.raw), key)
		})
	}
	t.Run("a valid token followed by a character outside the alphabet", func(t *testing.T) {
		// base64 reports the error but still returns the bytes it decoded before it, here the whole token.
		mustRefuse(t, encode(raw)+"*", key)
		mustRefuse(t, base64.RawURLEncoding.EncodeToString(raw)+"*", key)
	})
	t.Run("not base64 at all", func(t *testing.T) { mustRefuse(t, "***not*base64***", key) })
	t.Run("an empty string", func(t *testing.T) { mustRefuse(t, "", key) })
}

// TestDecryptFernetEachGuardRefusesWhatOnlyItCatches builds tokens that carry a VALID signature, so the signature check
// passes and a different guard has to refuse.
func TestDecryptFernetEachGuardRefusesWhatOnlyItCatches(t *testing.T) {
	key := testKey32("a")
	hmacKey, aesKey := key[:16], key[16:]
	iv := testIV()
	signed := func(version byte, body []byte) string {
		return encode(sealRaw(sealSpec{version: version, timestamp: 1700000000, iv: iv, body: body, hmacKey: hmacKey}))
	}
	// Plaintext blocks that are encrypted as they are, with no padding added, so the padding bytes can be wrong on purpose.
	blockOf := func(last ...byte) []byte {
		block := bytes.Repeat([]byte{'p'}, aes.BlockSize)
		copy(block[aes.BlockSize-len(last):], last)
		return block
	}

	t.Run("a version byte that is not 0x80, correctly signed", func(t *testing.T) {
		mustRefuse(t, signed(0x81, cbc(t, aesKey, iv, pkcs7([]byte("abc")))), key)
	})
	t.Run("no ciphertext after the header, correctly signed", func(t *testing.T) {
		mustRefuse(t, signed(fernetVersion, nil), key)
	})
	t.Run("a ciphertext that is not a block multiple, correctly signed", func(t *testing.T) {
		mustRefuse(t, signed(fernetVersion, append(cbc(t, aesKey, iv, pkcs7([]byte("abc"))), 0x00)), key)
	})
	t.Run("padding byte 0, correctly signed", func(t *testing.T) {
		mustRefuse(t, signed(fernetVersion, cbc(t, aesKey, iv, blockOf(0x00))), key)
	})
	t.Run("padding byte above the block size, correctly signed", func(t *testing.T) {
		mustRefuse(t, signed(fernetVersion, cbc(t, aesKey, iv, blockOf(0x11))), key)
	})
	t.Run("padding bytes that disagree with the padding length, correctly signed", func(t *testing.T) {
		mustRefuse(t, signed(fernetVersion, cbc(t, aesKey, iv, blockOf(0x07, 0x03, 0x03))), key)
	})
	t.Run("padding byte 17 over two blocks that all carry it, correctly signed", func(t *testing.T) {
		// 17 is longer than a block but not longer than the 32 bytes of data, and the last 17 bytes all equal 17, so
		// only the `padding > block size` clause refuses it.
		first := blockOf(0x11)
		second := bytes.Repeat([]byte{0x11}, aes.BlockSize)
		mustRefuse(t, signed(fernetVersion, cbc(t, aesKey, iv, append(first, second...))), key)
	})
	t.Run("a key that is not 32 bytes, signed and sealed to open under it", func(t *testing.T) {
		// A 48-byte key: HMAC over its first 16 bytes and AES-256 over its last 32 would open this token, so the length
		// check is the only thing that refuses it.
		long := append(append([]byte{}, key...), testKey32("c")[:16]...)
		body := cbc(t, long[16:], iv, pkcs7([]byte("abc")))
		token := encode(sealRaw(sealSpec{version: fernetVersion, timestamp: 1700000000, iv: iv, body: body, hmacKey: long[:16]}))
		if plain, err := decryptFernet(token, long[:32]); err == nil {
			t.Fatalf("control: the same token opened under a 32-byte key (%d bytes)", len(plain))
		}
		mustRefuse(t, token, long)
	})
	t.Run("a 31-byte key", func(t *testing.T) { mustRefuse(t, encode(validToken(t, key, []byte("abc"))), key[:31]) })
	t.Run("no key", func(t *testing.T) { mustRefuse(t, encode(validToken(t, key, []byte("abc"))), nil) })
}

// TestFernetDecryptorRefusesAChangedStoredCiphertext goes through the decryptor a caller uses: the v1 (PBKDF2) and the
// legacy (SHA-256 of the key) formats, each with a valid token, a changed signature byte and a wrong key.
func TestFernetDecryptorRefusesAChangedStoredCiphertext(t *testing.T) {
	keyText := "fernet-tamper-test-passphrase"
	decryptor, err := NewFernetDecryptor(secrets.NewValue(keyText), "tamper-test-salt")
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewFernetDecryptor(secrets.NewValue(keyText+"-other"), "tamper-test-salt")
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := decryptor.Encrypt([]byte("stored credential"))
	if err != nil {
		t.Fatal(err)
	}
	v1 := sealed.Reveal()
	changed := func(token string) string {
		raw, err := base64.URLEncoding.DecodeString(token)
		if err != nil {
			t.Fatal(err)
		}
		raw[len(raw)-1] ^= 0x01
		return base64.URLEncoding.EncodeToString(raw)
	}
	legacyKey := sha256.Sum256([]byte(keyText))
	legacy := encode(validToken(t, legacyKey[:], []byte("stored credential")))

	for name, c := range map[string]struct {
		token string
		d     FernetDecryptor
		ok    bool
	}{
		"v1 token, right key (control)":      {v1, decryptor, true},
		"legacy token, right key (control)":  {legacy, decryptor, true},
		"v1 token, changed signature":        {credentialCiphertextV1 + changed(v1[len(credentialCiphertextV1):]), decryptor, false},
		"legacy token, changed signature":    {changed(legacy), decryptor, false},
		"v1 token, wrong key":                {v1, other, false},
		"legacy token, wrong key":            {legacy, other, false},
		"a version prefix this reader lacks": {"v2:" + v1[len(credentialCiphertextV1):], decryptor, false},
	} {
		t.Run(name, func(t *testing.T) {
			plain, err := c.d.Decrypt(secrets.NewValue(c.token))
			if c.ok {
				if err != nil || string(plain) != "stored credential" {
					t.Fatalf("control did not open: err=%v", err)
				}
				return
			}
			if !errors.Is(err, ErrCredentialInvalid) || plain != nil {
				t.Fatalf("a changed token was not refused: err=%v plain length=%d", err, len(plain))
			}
		})
	}
}
