package providersecret

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The decrypter reads what providerfoundation's Fernet encryption writes.
func TestDecryptsWhatTheSettingsCipherEncrypts(t *testing.T) {
	key := secrets.NewValue(strings.Repeat("provider-secret-test-key-", 2))
	for _, salt := range []string{"", "another-salt"} {
		writer, err := providerfoundation.NewFernetDecryptor(key, salt)
		if err != nil {
			t.Fatal(err)
		}
		stored, err := writer.Encrypt([]byte("client-secret-value"))
		if err != nil {
			t.Fatal(err)
		}
		reader, err := New(key, salt)
		if err != nil {
			t.Fatal(err)
		}
		plain, err := reader.Decrypt(stored.Reveal())
		if err != nil || plain != "client-secret-value" {
			t.Fatalf("salt %q: %q, %v", salt, plain, err)
		}
	}
}

func TestRefusesAWrongKeyAForgedValueAndAMissingKey(t *testing.T) {
	key := secrets.NewValue(strings.Repeat("provider-secret-test-key-", 2))
	writer, _ := providerfoundation.NewFernetDecryptor(key, "")
	stored, _ := writer.Encrypt([]byte("x"))
	other, _ := New(secrets.NewValue(strings.Repeat("a-different-test-key-", 2)), "")
	if _, err := other.Decrypt(stored.Reveal()); err == nil {
		t.Fatal("a wrong key decrypted")
	}
	same, _ := New(key, "")
	for _, bad := range []string{"", "v2:abc", "not-a-token", stored.Reveal() + "x"} {
		if _, err := same.Decrypt(bad); err == nil {
			t.Fatalf("%q decrypted", bad)
		}
	}
	if _, err := New(secrets.Value{}, ""); err == nil {
		t.Fatal("a decrypter was built without a key")
	}
}
