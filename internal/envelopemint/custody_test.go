package envelopemint

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testKeyPEM(t *testing.T) string {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &buf
}

func TestLoadPrivateKeyFromFileRefusals(t *testing.T) {
	good := testKeyPEM(t)
	dir := t.TempDir()
	mk := func(name, body string, mode os.FileMode) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
		_ = os.Chmod(p, mode)
		return p
	}
	okPath := mk("ok.pem", good, 0o600)
	link := filepath.Join(dir, "link.pem")
	if err := os.Symlink(okPath, link); err != nil {
		t.Fatal(err)
	}
	rsa := "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VuBCIEIA==\n-----END PRIVATE KEY-----\n"
	cases := map[string]string{
		"symlink":    link,
		"world":      mk("w.pem", good, 0o644),
		"group":      mk("g.pem", good, 0o640),
		"oversize":   mk("big.pem", good+strings.Repeat("#", 17<<10), 0o600),
		"empty":      mk("e.pem", "", 0o600),
		"wrong-type": mk("x.pem", rsa, 0o600),
		"dir":        dir,
		"missing":    filepath.Join(dir, "nope.pem"),
		"empty-path": "",
	}
	for name, path := range cases {
		t.Run(name, func(t *testing.T) {
			logs := captureLogs(t)
			_, err := LoadPrivateKeyFromFile(path)
			if err == nil {
				t.Fatal("expected refusal")
			}
			all := err.Error() + logs.String()
			if path != "" && strings.Contains(all, path) {
				t.Fatalf("path leaked: %s", all)
			}
			if strings.Contains(all, "BEGIN PRIVATE KEY") || strings.Contains(all, "MC4CAQ") {
				t.Fatalf("key text leaked: %s", all)
			}
		})
	}
	for _, mode := range []os.FileMode{0o600, 0o400} {
		p := mk("m.pem", good, mode)
		if _, err := LoadPrivateKeyFromFile(p); err != nil {
			t.Fatalf("mode %o refused: %v", mode, err)
		}
	}
}

func TestLoadPrivateKeyFromEnvPrefersFileAndWarnsOnRaw(t *testing.T) {
	filePEM, rawPEM := testKeyPEM(t), testKeyPEM(t)
	p := filepath.Join(t.TempDir(), "k.pem")
	if err := os.WriteFile(p, []byte(filePEM), 0o600); err != nil {
		t.Fatal(err)
	}
	want, _ := LoadPrivateKey([]byte(filePEM))

	t.Setenv(PrivateKeyFileEnvVar, p)
	t.Setenv(PrivateKeyEnvVar, rawPEM)
	logs := captureLogs(t)
	got, err := LoadPrivateKeyFromEnv()
	if err != nil || !got.Equal(want) {
		t.Fatalf("file not preferred: %v", err)
	}
	if strings.Contains(logs.String(), "MC4CAQ") || strings.Contains(logs.String(), p) {
		t.Fatalf("log leaks: %s", logs)
	}

	t.Setenv(PrivateKeyFileEnvVar, "")
	logs = captureLogs(t)
	rawKey, _ := LoadPrivateKey([]byte(rawPEM))
	got, err = LoadPrivateKeyFromEnv()
	if err != nil || !got.Equal(rawKey) {
		t.Fatalf("raw fallback failed: %v", err)
	}
	if !strings.Contains(logs.String(), "deprecated") || strings.Contains(logs.String(), "MC4CAQ") {
		t.Fatalf("want deprecation warning without key: %s", logs)
	}

	t.Setenv(PrivateKeyEnvVar, "")
	if _, err := LoadPrivateKeyFromEnv(); err == nil {
		t.Fatal("expected error when neither set")
	}
	t.Setenv(PrivateKeyFileEnvVar, filepath.Join(filepath.Dir(p), "nope"))
	if _, err := LoadPrivateKeyFromEnv(); err == nil {
		t.Fatal("expected error for bad file")
	}
}
