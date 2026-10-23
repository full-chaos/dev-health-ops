package fernet_test

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/fernet"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const testKey = "fernet-test-key-fernet-test-key-"

func TestOpenReadsWhatSealWrites(t *testing.T) {
	for _, salt := range []string{"", "another-salt"} {
		c, err := fernet.New(testKey, salt)
		if err != nil {
			t.Fatal(err)
		}
		sealed, err := c.Seal([]byte("client-secret-value"), time.Unix(1700000000, 0), bytes.NewReader(make([]byte, 16)))
		if err != nil {
			t.Fatal(err)
		}
		plain, err := c.Open(sealed)
		if err != nil || string(plain) != "client-secret-value" {
			t.Fatalf("salt %q: %q, %v", salt, plain, err)
		}
	}
}

func TestOpenRefusesAWrongKeyAForgedValueAndAMissingKey(t *testing.T) {
	c, _ := fernet.New(testKey, "")
	sealed, err := c.Seal([]byte("x"), time.Now(), bytes.NewReader(make([]byte, 16)))
	if err != nil {
		t.Fatal(err)
	}
	other, _ := fernet.New("a-different-key-a-different-key-", "")
	if _, err := other.Open(sealed); err == nil {
		t.Fatal("a wrong key opened the value")
	}
	for _, bad := range []string{"", "v2:abc", "not-a-token", sealed + "x"} {
		if _, err := c.Open(bad); err == nil {
			t.Fatalf("%q opened", bad)
		}
	}
	if _, err := fernet.New("", ""); err == nil {
		t.Fatal("a cipher was built without a key")
	}
}

func TestSealRefusesAnOversizedPlaintext(t *testing.T) {
	c, _ := fernet.New(testKey, "")
	if _, err := c.Seal(make([]byte, 1<<24+1), time.Now(), bytes.NewReader(make([]byte, 16))); err == nil {
		t.Fatal("an oversized plaintext was sealed")
	}
}

// The provider credential decryptor is this implementation: what it seals
// opens here and the reverse.
func TestProviderCredentialDecryptorSharesTheWireFormat(t *testing.T) {
	d, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(testKey), "")
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := d.Encrypt([]byte("shared"))
	if err != nil {
		t.Fatal(err)
	}
	c, _ := fernet.New(testKey, "")
	plain, err := c.Open(sealed.Reveal())
	if err != nil || string(plain) != "shared" {
		t.Fatalf("%q, %v", plain, err)
	}
	again, err := c.Seal([]byte("back"), time.Now(), bytes.NewReader(make([]byte, 16)))
	if err != nil {
		t.Fatal(err)
	}
	back, err := d.Decrypt(secrets.NewValue(again))
	if err != nil || string(back) != "back" {
		t.Fatalf("%q, %v", back, err)
	}
}

// TestNoSecondFernetImplementation: the CBC primitives that open or seal a
// token appear in this package only.
func TestNoSecondFernetImplementation(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	own := filepath.Join(root, "internal", "platform", "fernet")
	var scanned int
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "vendor":
				return filepath.SkipDir
			}
			if path == own {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		scanned++
		text := string(raw)
		if strings.Contains(text, "NewCBCDecrypter(") || strings.Contains(text, "NewCBCEncrypter(") {
			rel, _ := filepath.Rel(root, path)
			t.Errorf("%s runs CBC itself: use internal/platform/fernet", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 100 {
		t.Fatalf("only %d Go files scanned: the census measured nothing", scanned)
	}
}
