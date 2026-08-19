package providerfoundation

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
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
	python := filepath.Join(repositoryRoot, ".venv", "bin", "python")
	if _, err := os.Stat(python); err != nil {
		python, err = exec.LookPath("python3")
		if err != nil {
			t.Fatalf("live Python oracle interpreter: %v", err)
		}
	}

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
	for key, value := range values {
		command.Env = append(command.Env, key+"="+value)
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live Python encryption oracle: %v: %s", err, output)
	}
	return strings.TrimSpace(string(output))
}
