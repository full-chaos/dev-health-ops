package providerfoundation_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// fernetExchangeProgram is one cross-runtime exchange, run by the real Python
// encryption module with the key (and salt, when given) in its environment:
// it opens the ciphertext the Go cipher made (stdin "go") and seals the
// plaintext (stdin "plaintext"). Fernet tokens carry a random IV, so the
// Python ciphertext is the frozen answer the Go cipher then opens; the Go
// ciphertext is a constant of this file (made once by the Go cipher), so the
// request does not change from run to run.
const fernetExchangeProgram = `
import json, os, sys
from cryptography.fernet import Fernet
# Fernet seals with a random IV and the clock; both are pinned so the recording
# is the same on every run (the token is still the production format, made by
# the real encrypt_value).
Fernet.encrypt = lambda self, data: self._encrypt_from_parts(data, 1700000000, bytes(range(16)))
from dev_health_ops.core.encryption import decrypt_value, encrypt_value
request = json.load(sys.stdin)
print(json.dumps({
    "salt_in_environment": "SETTINGS_ENCRYPTION_SALT" in os.environ,
    "python_opens_go": decrypt_value(request["go"]),
    "python_ciphertext": encrypt_value(request["plaintext"]),
}))
`

type fernetExchange struct {
	SaltInEnvironment bool   `json:"salt_in_environment"`
	PythonOpensGo     string `json:"python_opens_go"`
	PythonCiphertext  string `json:"python_ciphertext"`
}

func fernetExchangeOf(t *testing.T, golden, key, salt, goCiphertext, plaintext string) fernetExchange {
	t.Helper()
	env := map[string]string{"SETTINGS_ENCRYPTION_KEY": key}
	if salt != "" {
		env["SETTINGS_ENCRYPTION_SALT"] = salt
	}
	stdin, err := json.Marshal(map[string]string{"go": goCiphertext, "plaintext": plaintext})
	if err != nil {
		t.Fatal(err)
	}
	out := frozenPython(t, golden, programoracle.Program{Name: "fernet exchange", Text: fernetExchangeProgram, Stdin: stdin, Env: env})[0]
	var got fernetExchange
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &got); err != nil {
		t.Fatalf("decode the frozen exchange: %v\n%s", err, out)
	}
	return got
}

// goCustomSaltCiphertext and goDefaultSaltCiphertext are tokens the Go cipher
// sealed once, for the keys, salts and plaintexts of the two tests below.
const (
	goCustomSaltCiphertext  = "v1:gAAAAABqvpUZqRqUGH9ddaiHoyrS1MP1YVp9A3wncgaVRXnpUFPydGJfECzXRD7RBsJBoJOsG2tkfIh8s-aMNOTCr13Q-3JxwB-v8Bs5fiVSReB2J9y_DOw="
	goDefaultSaltCiphertext = "v1:gAAAAABqvpUZ7y6FJ7PjJce6C2N6p7lBPwce01lGTBrnfv6y6mWAYvki79cNht5haIOr0FV-I2uDgYaI1P0pYGuFcwFV-NzoBcM5JQvuBZ5dz_l4gBk0nXQ="
)

func TestFernetCipherMatchesFrozenPythonCustomSalt(t *testing.T) {
	const (
		key       = "pagerduty-cross-runtime-test-key"
		salt      = "deployment-specific-salt"
		plaintext = "pagerduty-oauth-token-payload"
	)
	cipher, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(key), salt)
	if err != nil {
		t.Fatal(err)
	}
	// The Go cipher still seals and opens its own token, and opens the constant.
	own, err := cipher.Encrypt([]byte(plaintext))
	if err != nil {
		t.Fatal(err)
	}
	if opened, err := cipher.Decrypt(own); err != nil || string(opened) != plaintext {
		t.Fatalf("Go round trip: plaintext=%q err=%v", opened, err)
	}
	if opened, err := cipher.Decrypt(secrets.NewValue(goCustomSaltCiphertext)); err != nil || string(opened) != plaintext {
		t.Fatalf("Go opens its recorded ciphertext: plaintext=%q err=%v", opened, err)
	}
	exchange := fernetExchangeOf(t, "fernet-custom-salt.golden.json", key, salt, goCustomSaltCiphertext, plaintext)
	if exchange.PythonOpensGo != plaintext {
		t.Fatalf("Python decrypted Go ciphertext as %q", exchange.PythonOpensGo)
	}
	goPlaintext, err := cipher.Decrypt(secrets.NewValue(exchange.PythonCiphertext))
	if err != nil || string(goPlaintext) != plaintext {
		t.Fatalf("Go decrypt of Python ciphertext: plaintext=%q err=%v", goPlaintext, err)
	}
}

// TestFernetCipherMatchesFrozenPythonDefaultSalt is the same exchange with no
// salt configured on either side: Python derives its key with
// DEFAULT_SETTINGS_ENCRYPTION_SALT when SETTINGS_ENCRYPTION_SALT is absent
// from the environment, and NewFernetDecryptor takes an empty salt as that
// same default -- so a deployment that sets only the key (the shape the
// shared Secret has) opens the other runtime's ciphertext.
func TestFernetCipherMatchesFrozenPythonDefaultSalt(t *testing.T) {
	const (
		key       = "default-salt-cross-runtime-test-key"
		plaintext = "integration-credential-payload"
	)
	cipher, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(key), "")
	if err != nil {
		t.Fatal(err)
	}
	own, err := cipher.Encrypt([]byte(plaintext))
	if err != nil {
		t.Fatal(err)
	}
	if opened, err := cipher.Decrypt(own); err != nil || string(opened) != plaintext {
		t.Fatalf("Go round trip: plaintext=%q err=%v", opened, err)
	}
	if opened, err := cipher.Decrypt(secrets.NewValue(goDefaultSaltCiphertext)); err != nil || string(opened) != plaintext {
		t.Fatalf("Go opens its recorded ciphertext: plaintext=%q err=%v", opened, err)
	}
	exchange := fernetExchangeOf(t, "fernet-default-salt.golden.json", key, "", goDefaultSaltCiphertext, plaintext)
	if exchange.SaltInEnvironment {
		t.Fatal("the default-salt exchange ran with a salt in the environment")
	}
	if exchange.PythonOpensGo != plaintext {
		t.Fatalf("Python decrypted Go ciphertext as %q", exchange.PythonOpensGo)
	}
	goPlaintext, err := cipher.Decrypt(secrets.NewValue(exchange.PythonCiphertext))
	if err != nil || string(goPlaintext) != plaintext {
		t.Fatalf("Go decrypt of Python ciphertext: plaintext=%q err=%v", goPlaintext, err)
	}
	// A custom salt must NOT open it: the default is the salt, not "no salt".
	other, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(key), "some-other-salt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Decrypt(secrets.NewValue(exchange.PythonCiphertext)); err == nil {
		t.Fatal("a different salt decrypted the default-salt ciphertext")
	}
}

// TestFernetRefusesWithoutKeyLikePython pins the behaviour the credential
// routes inherit when a deployment has no SETTINGS_ENCRYPTION_KEY: Python's
// encrypt_value raises (the route answers 500) and the Go encryptor refuses
// too (its route answers 500), so a missing key is a refused write in both
// planes, never a plaintext or default-key write.
func TestFernetRefusesWithoutKeyLikePython(t *testing.T) {
	out := frozenPython(t, "fernet-no-key.golden.json", programoracle.Program{Name: "fernet without a key", Text: "from dev_health_ops.core.encryption import encrypt_value\n" +
		"try:\n    encrypt_value('x')\nexcept Exception as error:\n    print(type(error).__name__ + ': ' + str(error))\n"})[0]
	if !strings.Contains(out, "SETTINGS_ENCRYPTION_KEY environment variable is required") {
		t.Fatalf("Python encrypt_value without a key: output=%s", out)
	}
	if _, err := (providerfoundation.FernetDecryptor{}).Encrypt([]byte("x")); err == nil {
		t.Fatal("the Go encryptor sealed a payload without a key")
	}
}
