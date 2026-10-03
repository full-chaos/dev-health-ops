package envelopemint

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func readAll(t *testing.T, paths KeyPaths) (priv, jwks []byte) {
	t.Helper()
	priv, err := os.ReadFile(paths.Private)
	if err != nil {
		t.Fatalf("read private: %v", err)
	}
	jwks, err = os.ReadFile(paths.JWKS)
	if err != nil {
		t.Fatalf("read jwks: %v", err)
	}
	return priv, jwks
}

func TestGenerateKeyFilesWritesBothFilesWithTheRightModes(t *testing.T) {
	dir := t.TempDir()
	paths, err := GenerateKeyFiles(dir, "kid-a")
	if err != nil {
		t.Fatalf("GenerateKeyFiles: %v", err)
	}
	for path, want := range map[string]os.FileMode{
		paths.Private:               0o400,
		paths.JWKS:                  0o444,
		filepath.Dir(paths.Private): 0o700,
		filepath.Dir(paths.JWKS):    0o755,
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Errorf("%s mode = %o, want %o", path, got, want)
		}
	}
	priv, _ := readAll(t, paths)
	if _, err := LoadPrivateKey(priv); err != nil {
		t.Errorf("written private key does not load: %v", err)
	}
}

func TestGenerateKeyFilesRefusesToOverwriteAndChangesNothing(t *testing.T) {
	dir := t.TempDir()
	paths, err := GenerateKeyFiles(dir, "kid-a")
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	priv1, jwks1 := readAll(t, paths)

	_, err = GenerateKeyFiles(dir, "kid-b")
	if !errors.Is(err, ErrKeyFilesExist) {
		t.Fatalf("second run error = %v, want ErrKeyFilesExist", err)
	}
	priv2, jwks2 := readAll(t, paths)
	if string(priv1) != string(priv2) || string(jwks1) != string(jwks2) {
		t.Fatal("second run changed an existing file")
	}
}

func TestGenerateKeyFilesRefusesWhenOnlyOneFileExists(t *testing.T) {
	dir := t.TempDir()
	paths := PathsIn(dir)
	if err := os.MkdirAll(filepath.Dir(paths.JWKS), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.JWKS, []byte("planted"), 0o444); err != nil {
		t.Fatal(err)
	}
	if _, err := GenerateKeyFiles(dir, "kid-a"); !errors.Is(err, ErrKeyFilesExist) {
		t.Fatalf("error = %v, want ErrKeyFilesExist", err)
	}
	if _, err := os.Stat(paths.Private); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a private key was written beside a planted JWKS: %v", err)
	}
	if got, _ := os.ReadFile(paths.JWKS); string(got) != "planted" {
		t.Fatal("planted JWKS was changed")
	}
}

func TestEnsureKeyFilesIsIdempotentAndNeverReplaces(t *testing.T) {
	dir := t.TempDir()
	created, err := EnsureKeyFiles(dir, "kid-a")
	if err != nil || !created {
		t.Fatalf("first Ensure = %v, %v; want created", created, err)
	}
	priv1, jwks1 := readAll(t, PathsIn(dir))
	created, err = EnsureKeyFiles(dir, "kid-a")
	if err != nil || created {
		t.Fatalf("second Ensure = %v, %v; want no-op", created, err)
	}
	priv2, jwks2 := readAll(t, PathsIn(dir))
	if string(priv1) != string(priv2) || string(jwks1) != string(jwks2) {
		t.Fatal("second Ensure changed a file")
	}
	// Same files under another key id: an error, files unchanged.
	if _, err := EnsureKeyFiles(dir, "kid-other"); err == nil {
		t.Fatal("Ensure under a different key id succeeded; want an error")
	}
	priv3, jwks3 := readAll(t, PathsIn(dir))
	if string(priv1) != string(priv3) || string(jwks1) != string(jwks3) {
		t.Fatal("mismatching Ensure changed a file")
	}
}
