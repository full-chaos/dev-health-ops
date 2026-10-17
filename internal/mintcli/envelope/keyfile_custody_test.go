package envelope

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeKey(t *testing.T, dir, name, content string, mode os.FileMode) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, mode); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadKeyFileCustody(t *testing.T) {
	pemText, _ := generateKeyPEM(t)
	dir := t.TempDir()
	good := writeKey(t, dir, "good.pem", pemText, 0o600)
	if _, err := loadKey(good); err != nil {
		t.Fatalf("0600 key refused: %v", err)
	}
	ro := writeKey(t, dir, "ro.pem", pemText, 0o400)
	if _, err := loadKey(ro); err != nil {
		t.Fatalf("0400 key refused: %v", err)
	}

	link := filepath.Join(dir, "link.pem")
	if err := os.Symlink(good, link); err != nil {
		t.Fatal(err)
	}
	wide := writeKey(t, dir, "wide.pem", pemText, 0o644)
	group := writeKey(t, dir, "group.pem", pemText, 0o640)
	big := writeKey(t, dir, "big.pem", pemText+strings.Repeat("#", 20<<10), 0o600)
	dirPath := filepath.Join(dir, "adir")
	if err := os.Mkdir(dirPath, 0o700); err != nil {
		t.Fatal(err)
	}
	other := writeKey(t, dir, "other.pem", "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n", 0o600)

	cases := map[string]string{
		"symlink": link, "world-readable": wide, "group-readable": group,
		"oversize": big, "directory": dirPath, "garbage": other,
	}
	for name, path := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := loadKey(path)
			if err == nil {
				t.Fatal("expected refusal")
			}
			if strings.Contains(err.Error(), path) || strings.Contains(err.Error(), "AAAA") {
				t.Fatalf("error leaks path or key text: %v", err)
			}
		})
	}
}
