package providerfoundation

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// TestKeyMaterialFromEnvironBytes is a diagnostic, not an assertion: it logs whether
// the real file read + PEM parse accept an environ-shaped blob, then fails on purpose
// so CI prints the lines.
func TestKeyMaterialFromEnvironBytes(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	pemText := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	escaped := strings.ReplaceAll(strings.TrimRight(pemText, "\n"), "\n", `\n`)
	environ := func(value string) []byte { return []byte("A=1\x00KEY=" + value + "\x00B=2\x00") }

	forms := []struct {
		name    string
		content []byte
	}{
		{"real-newlines", environ(pemText)},
		{"escaped", environ(escaped)},
		{"real-newlines-leading-lf", environ("\n" + pemText)},
		{"control-pem-file", []byte(pemText)},
	}
	var lines []string
	for _, form := range forms {
		path := filepath.Join(t.TempDir(), "environ")
		if err := os.WriteFile(path, form.content, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		read := "ok"
		parse := "refused"
		value, readErr := readGitHubAppPrivateKeyFile(path)
		if readErr != nil {
			read = "refused"
		} else if _, jwtErr := githubAppJWT("1", value, time.Now()); jwtErr == nil {
			parse = "ok"
		}
		lines = append(lines, "FORM "+form.name+" parse="+parse)
		t.Logf("READ %s utf8=%t read=%s", form.name, utf8.Valid(form.content), read)
	}
	for _, line := range lines {
		t.Log(line)
	}
	t.Errorf("TestKeyMaterialFromEnvironBytes diagnostic output (fails on purpose to print):\n%s", strings.Join(lines, "\n"))
}
