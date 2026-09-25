package credentials

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TestDecryptStoredValue pins decrypt_value + json.loads as every reader of a
// stored payload sees it: a readable JSON value of any type comes back (nil
// for JSON null), a payload that cannot decrypt or parse is "not readable"
// (Python's caught ValueError), and a process without a key is an error
// (Python's uncaught RuntimeError).
func TestDecryptStoredValue(t *testing.T) {
	cipher, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("unit-key"), "")
	if err != nil {
		t.Fatal(err)
	}
	encrypt := func(text string) string {
		value, err := cipher.Encrypt([]byte(text))
		if err != nil {
			t.Fatal(err)
		}
		return value.Reveal()
	}
	for _, c := range []struct {
		name     string
		cipher   Cipher
		text     string
		readable bool
		fails    bool
		check    func(pyjson.Value) bool
	}{
		{"object", cipher, encrypt(`{"url":"https://x.test"}`), true, false, func(v pyjson.Value) bool { _, ok := v.(*pyjson.Object); return ok }},
		{"list is readable", cipher, encrypt(`["x"]`), true, false, func(v pyjson.Value) bool { _, ok := v.([]pyjson.Value); return ok }},
		{"json null is readable and nil", cipher, encrypt(`null`), true, false, func(v pyjson.Value) bool { return v == nil }},
		{"not json", cipher, encrypt(`not json {`), false, false, nil},
		{"not a fernet token", cipher, "gAAAAABnot-a-fernet-token", false, false, nil},
		{"no cipher", nil, encrypt(`{}`), false, true, nil},
		{"unconfigured cipher", providerfoundation.FernetDecryptor{}, encrypt(`{}`), false, true, nil},
	} {
		value, readable, err := DecryptStoredValue(c.cipher, c.text)
		if (err != nil) != c.fails || readable != c.readable {
			t.Errorf("%s: readable=%v err=%v, want readable=%v fails=%v", c.name, readable, err, c.readable, c.fails)
			continue
		}
		if c.check != nil && !c.check(value) {
			t.Errorf("%s: value %#v", c.name, value)
		}
	}
}
