package providerfoundation

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

func TestFernetCipherMatchesLivePythonCustomSalt(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve providerfoundation package path")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	python := pyoracle.Resolve(t, repositoryRoot)

	const (
		key       = "pagerduty-cross-runtime-test-key"
		salt      = "deployment-specific-salt"
		plaintext = "pagerduty-oauth-token-payload"
	)
	cipher, err := NewFernetDecryptor(secrets.NewValue(key), salt)
	if err != nil {
		t.Fatal(err)
	}
	goCiphertext, err := cipher.Encrypt([]byte(plaintext))
	if err != nil {
		t.Fatal(err)
	}
	pythonPlaintext := runPythonEncryptionOracle(t, python, repositoryRoot, map[string]string{
		"SETTINGS_ENCRYPTION_KEY":  key,
		"SETTINGS_ENCRYPTION_SALT": salt,
		"CIPHERTEXT":               goCiphertext.Reveal(),
	}, "from dev_health_ops.core.encryption import decrypt_value; import os; print(decrypt_value(os.environ['CIPHERTEXT']))")
	if pythonPlaintext != plaintext {
		t.Fatalf("Python decrypted Go ciphertext as %q", pythonPlaintext)
	}

	pythonCiphertext := runPythonEncryptionOracle(t, python, repositoryRoot, map[string]string{
		"SETTINGS_ENCRYPTION_KEY":  key,
		"SETTINGS_ENCRYPTION_SALT": salt,
		"PLAINTEXT":                plaintext,
	}, "from dev_health_ops.core.encryption import encrypt_value; import os; print(encrypt_value(os.environ['PLAINTEXT']))")
	goPlaintext, err := cipher.Decrypt(secrets.NewValue(pythonCiphertext))
	if err != nil || string(goPlaintext) != plaintext {
		t.Fatalf("Go decrypt of Python ciphertext: plaintext=%q err=%v", goPlaintext, err)
	}

	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(
		filepath.Join(proofDir, "providerfoundation-credentials"),
		[]byte("executed"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
}

// TestFernetCipherMatchesLivePythonDefaultSalt is the same exchange with no
// salt configured on either side: Python derives its key with
// DEFAULT_SETTINGS_ENCRYPTION_SALT when SETTINGS_ENCRYPTION_SALT is absent
// from the environment, and NewFernetDecryptor takes an empty salt as that
// same default -- so a deployment that sets only the key (the shape the
// shared Secret has) opens the other runtime's ciphertext.
func TestFernetCipherMatchesLivePythonDefaultSalt(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve providerfoundation package path")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	python := pyoracle.Resolve(t, repositoryRoot)

	const (
		key       = "default-salt-cross-runtime-test-key"
		plaintext = "integration-credential-payload"
	)
	cipher, err := NewFernetDecryptor(secrets.NewValue(key), "")
	if err != nil {
		t.Fatal(err)
	}
	goCiphertext, err := cipher.Encrypt([]byte(plaintext))
	if err != nil {
		t.Fatal(err)
	}
	pythonPlaintext := runPythonEncryptionOracle(t, python, repositoryRoot, map[string]string{
		"SETTINGS_ENCRYPTION_KEY": key,
		"CIPHERTEXT":              goCiphertext.Reveal(),
	}, "from dev_health_ops.core.encryption import decrypt_value; import os; assert 'SETTINGS_ENCRYPTION_SALT' not in os.environ; print(decrypt_value(os.environ['CIPHERTEXT']))")
	if pythonPlaintext != plaintext {
		t.Fatalf("Python decrypted Go ciphertext as %q", pythonPlaintext)
	}
	pythonCiphertext := runPythonEncryptionOracle(t, python, repositoryRoot, map[string]string{
		"SETTINGS_ENCRYPTION_KEY": key,
		"PLAINTEXT":               plaintext,
	}, "from dev_health_ops.core.encryption import encrypt_value; import os; assert 'SETTINGS_ENCRYPTION_SALT' not in os.environ; print(encrypt_value(os.environ['PLAINTEXT']))")
	goPlaintext, err := cipher.Decrypt(secrets.NewValue(pythonCiphertext))
	if err != nil || string(goPlaintext) != plaintext {
		t.Fatalf("Go decrypt of Python ciphertext: plaintext=%q err=%v", goPlaintext, err)
	}
	// A custom salt must NOT open it: the default is the salt, not "no salt".
	other, err := NewFernetDecryptor(secrets.NewValue(key), "some-other-salt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Decrypt(secrets.NewValue(pythonCiphertext)); err == nil {
		t.Fatal("a different salt decrypted the default-salt ciphertext")
	}
	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proofDir, "providerfoundation-credentials-default-salt"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func runPythonEncryptionOracle(
	t *testing.T,
	python string,
	repositoryRoot string,
	values map[string]string,
	program string,
) string {
	t.Helper()
	command := exec.Command(python, "-c", program)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repositoryRoot, "src"))
	if _, salted := values["SETTINGS_ENCRYPTION_SALT"]; !salted {
		// An ambient salt would silently turn a default-salt exchange into a
		// custom-salt one.
		kept := command.Env[:0]
		for _, entry := range command.Env {
			if !strings.HasPrefix(entry, "SETTINGS_ENCRYPTION_SALT=") {
				kept = append(kept, entry)
			}
		}
		command.Env = kept
	}
	for key, value := range values {
		command.Env = append(command.Env, key+"="+value)
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live Python encryption oracle: %v", pyoracle.RunError(python, err, output))
	}
	return strings.TrimSpace(string(output))
}
