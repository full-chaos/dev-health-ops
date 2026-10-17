package envelopemint

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
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

func rsaKeyPEM(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
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
	rsa := rsaKeyPEM(t)
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

// The compose go-api key file is written by GenerateKeyFiles (mode 0400); the
// loader must accept exactly that shape.
func TestLoadPrivateKeyFromFileAcceptsGeneratedKeyFile(t *testing.T) {
	paths, err := GenerateKeyFiles(t.TempDir(), "test-kid")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPrivateKeyFromFile(paths.Private); err != nil {
		t.Fatalf("generated key file refused: %v", err)
	}
}

// secretShape is key-looking material placed where the loader would echo it if
// it ever formatted input into an error: the PEM label, the PEM body, the DER
// body, and a prefix of a real key.
func TestNoLoadPathEchoesKeyInput(t *testing.T) {
	const secret = "ZmFrZS1zZWNyZXQtbWF0ZXJpYWwtMDEyMzQ1Njc4OWFiY2RlZg"
	good := testKeyPEM(t)
	realBody := strings.Split(good, "\n")[1]
	cases := map[string]string{
		"label":        "-----BEGIN " + secret + "-----\nAAAA\n-----END " + secret + "-----\n",
		"bad-pem":      secret + "\n-----BEGIN PRIVATE KEY-----\n",
		"bad-der":      string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte(secret)})),
		"two-blocks":   good + good,
		"rsa":          rsaKeyPEM(t),
		"real-prefix":  "-----BEGIN " + realBody + "-----\nAAAA\n-----END " + realBody + "-----\n",
		"not-pem-real": realBody,
	}
	dir := t.TempDir()
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			needles := []string{secret, realBody}
			if name == "rsa" {
				needles = []string{strings.Split(input, "\n")[1]}
			}
			check := func(path string, err error, logs *bytes.Buffer) {
				t.Helper()
				if err == nil {
					t.Fatalf("%s: expected an error", path)
				}
				all := err.Error() + logs.String()
				for _, n := range needles {
					if strings.Contains(all, n) {
						t.Fatalf("%s: key input echoed: %s", path, all)
					}
				}
			}
			logs := captureLogs(t)
			_, err := LoadPrivateKey([]byte(input))
			check("LoadPrivateKey", err, logs)

			logs = captureLogs(t)
			p := filepath.Join(dir, name+".pem")
			if werr := os.WriteFile(p, []byte(input), 0o600); werr != nil {
				t.Fatal(werr)
			}
			_, err = LoadPrivateKeyFromFile(p)
			check("LoadPrivateKeyFromFile", err, logs)

			logs = captureLogs(t)
			t.Setenv(PrivateKeyFileEnvVar, "")
			t.Setenv(PrivateKeyEnvVar, input)
			_, err = LoadPrivateKeyFromEnv()
			check("LoadPrivateKeyFromEnv raw", err, logs)

			logs = captureLogs(t)
			t.Setenv(PrivateKeyFileEnvVar, p)
			t.Setenv(PrivateKeyEnvVar, input)
			_, err = LoadPrivateKeyFromEnv()
			check("LoadPrivateKeyFromEnv file", err, logs)
		})
	}
}
