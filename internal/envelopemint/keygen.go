package envelopemint

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// File layout GenerateKeyFiles writes under its target directory. The private
// key and the JWKS sit in separate subdirectories so a container can mount the
// public half alone (a volume subpath) and never see the private half.
const (
	PrivateSubdir  = "private"
	PublicSubdir   = "public"
	PrivateKeyFile = "envelope-private-key.pem"
	JWKSFile       = "envelope-jwks.json"

	privateKeyMode fs.FileMode = 0o400
	jwksMode       fs.FileMode = 0o444
	privateDirMode fs.FileMode = 0o700
	publicDirMode  fs.FileMode = 0o755
)

// ErrKeyFilesExist is returned when a key or JWKS file is already present.
// Nothing is written or changed in that case: replacing a key would make
// every envelope signed with the old one fail verification.
var ErrKeyFilesExist = errors.New("envelopemint: envelope key files already exist")

// KeyPaths names the two files of a key set.
type KeyPaths struct {
	Private string
	JWKS    string
}

// PathsIn returns the file paths of a key set under dir.
func PathsIn(dir string) KeyPaths {
	return KeyPaths{
		Private: filepath.Join(dir, PrivateSubdir, PrivateKeyFile),
		JWKS:    filepath.Join(dir, PublicSubdir, JWKSFile),
	}
}

// ErrKeyFilesInconsistent is returned when the files on disk are not a state
// this program can produce or safely accept (a public key without its private
// key, a pair that does not match, a symlink, or permissions wider than
// allowed). Nothing is changed, repaired or loosened in that case.
var ErrKeyFilesInconsistent = errors.New("envelopemint: envelope key files are in a state that is not accepted")

// Crash safety. Every file is written to a temp file in its own directory,
// fsynced, and then linked to its final name (link fails if the name exists, so
// a final file is never overwritten) and the directory is fsynced. The private
// key is committed first and the JWKS second. A kill therefore leaves one of:
// nothing (plus a temp file, removed on the next run), the private key alone
// (the JWKS is derived again from it by EnsureKeyFiles), or the complete pair.
// The JWKS is never present without the private key.
const tempMarker = ".tmp-"

// crashHook is a test seam: it is called at each write point, and an error from
// it aborts the write WITHOUT cleanup, which is what a SIGKILL there leaves.
var crashHook = func(point string) error { return nil }

// GenerateKeyFiles writes a new Ed25519 private key (PKCS#8 "PRIVATE KEY" PEM,
// the shape LoadPrivateKey reads, mode 0400) and the matching JWKS (the shape
// the query-api verifier reads: OKP/Ed25519/EdDSA, use=sig, mode 0444) under
// dir. If either file already exists it returns ErrKeyFilesExist and changes
// nothing. It never prints or returns key material.
func GenerateKeyFiles(dir, keyID string) (KeyPaths, error) {
	paths := PathsIn(dir)
	if keyID == "" {
		return paths, errors.New("envelopemint: key id must not be empty")
	}
	for _, p := range []string{paths.Private, paths.JWKS} {
		if _, err := os.Lstat(p); err == nil {
			return paths, fmt.Errorf("%w: %s", ErrKeyFilesExist, p)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return paths, fmt.Errorf("envelopemint: check %s: %w", p, err)
		}
	}

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return paths, fmt.Errorf("envelopemint: generate key: %w", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return paths, fmt.Errorf("envelopemint: marshal private key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	jwks, err := jwksDocument(pub, keyID)
	if err != nil {
		return paths, err
	}

	if err := mkdirMode(filepath.Dir(paths.Private), privateDirMode); err != nil {
		return paths, err
	}
	if err := mkdirMode(filepath.Dir(paths.JWKS), publicDirMode); err != nil {
		return paths, err
	}
	if err := writeNew(paths.Private, keyPEM, privateKeyMode, "private"); err != nil {
		return paths, err
	}
	if err := writeNew(paths.JWKS, jwks, jwksMode, "jwks"); err != nil {
		return paths, err
	}
	return paths, nil
}

// EnsureKeyFiles is the idempotent form for a start-up step that runs on every
// `up`. It returns changed=true when it wrote anything.
//   - Nothing on disk: generate a pair.
//   - Private key alone (the only half state a crash of this program leaves):
//     derive the JWKS again from it, after the private key passes the same
//     checks as below.
//   - A complete pair: accept it, changing nothing, ONLY IF every clause of
//     checkKeySet holds. Otherwise return ErrKeyFilesInconsistent (or
//     ErrKeyFilesExist for the generate-refusal cases) naming file and reason.
//
// It never loosens or repairs permissions and never replaces a key.
func EnsureKeyFiles(dir, keyID string) (changed bool, err error) {
	paths := PathsIn(dir)
	removeStaleTemps(filepath.Dir(paths.Private))
	removeStaleTemps(filepath.Dir(paths.JWKS))
	privMissing, err := missing(paths.Private)
	if err != nil {
		return false, err
	}
	jwksMissing, err := missing(paths.JWKS)
	if err != nil {
		return false, err
	}
	switch {
	case privMissing && jwksMissing:
		_, err := GenerateKeyFiles(dir, keyID)
		return err == nil, err
	case privMissing:
		return false, fmt.Errorf("%w: %s exists but %s does not; a crash of this program cannot leave that, so it is not repaired", ErrKeyFilesInconsistent, paths.JWKS, paths.Private)
	case jwksMissing:
		priv, err := checkPrivate(paths)
		if err != nil {
			return false, err
		}
		jwks, err := jwksDocument(priv.Public().(ed25519.PublicKey), keyID)
		if err != nil {
			return false, err
		}
		if err := mkdirMode(filepath.Dir(paths.JWKS), publicDirMode); err != nil {
			return false, err
		}
		if err := writeNew(paths.JWKS, jwks, jwksMode, "jwks"); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, checkKeySet(paths, keyID)
}

func missing(path string) (bool, error) {
	_, err := os.Lstat(path)
	switch {
	case err == nil:
		return false, nil
	case errors.Is(err, fs.ErrNotExist):
		return true, nil
	}
	return false, fmt.Errorf("envelopemint: check %s: %w", path, err)
}

func inconsistent(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrKeyFilesInconsistent, fmt.Sprintf(format, args...))
}

// checkLeaf requires path to be a regular file (Lstat: a symlink is refused)
// and its directory to be a real directory (not a symlink).
func checkLeaf(path, what string) (fs.FileInfo, error) {
	dirInfo, err := os.Lstat(filepath.Dir(path))
	if err != nil {
		return nil, inconsistent("%s directory %s: %v", what, filepath.Dir(path), err)
	}
	// Lstat does not follow, so a symlinked directory is not IsDir.
	if !dirInfo.IsDir() {
		return nil, inconsistent("%s directory %s is a symlink or not a directory", what, filepath.Dir(path))
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, inconsistent("%s %s: %v", what, path, err)
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		return nil, inconsistent("%s %s is a symlink", what, path)
	}
	if !info.Mode().IsRegular() {
		return nil, inconsistent("%s %s is not a regular file", what, path)
	}
	return info, nil
}

// checkPrivate applies the private-side clauses: regular file in a real
// directory, no group or other bits on the file or its directory, parses as the
// key LoadPrivateKey reads.
func checkPrivate(paths KeyPaths) (ed25519.PrivateKey, error) {
	info, err := checkLeaf(paths.Private, "private key")
	if err != nil {
		return nil, err
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, inconsistent("private key %s has mode %o; group and other bits must be clear (it is not changed)", paths.Private, info.Mode().Perm())
	}
	dirInfo, err := os.Lstat(filepath.Dir(paths.Private))
	if err != nil {
		return nil, inconsistent("private directory %s: %v", filepath.Dir(paths.Private), err)
	}
	if dirInfo.Mode().Perm()&0o077 != 0 {
		return nil, inconsistent("private directory %s has mode %o; group and other bits must be clear (it is not changed)", filepath.Dir(paths.Private), dirInfo.Mode().Perm())
	}
	pemBytes, err := os.ReadFile(paths.Private)
	if err != nil {
		return nil, fmt.Errorf("envelopemint: read existing private key: %w", err)
	}
	priv, err := LoadPrivateKey(pemBytes)
	if err != nil {
		return nil, inconsistent("private key %s: %v", paths.Private, err)
	}
	return priv, nil
}

// checkKeySet is the accept predicate for an existing pair. Clauses: both are
// regular files in real directories (no symlink); the private key and its
// directory have no group or other bits; the JWKS file is world-readable
// (0004) and its directory world-searchable (0005), the mode the reader
// service needs when it runs under another uid; and the JWKS holds exactly the
// public key of the private key under keyID.
func checkKeySet(paths KeyPaths, keyID string) error {
	priv, err := checkPrivate(paths)
	if err != nil {
		return err
	}
	info, err := checkLeaf(paths.JWKS, "JWKS")
	if err != nil {
		return err
	}
	if info.Mode().Perm()&0o004 == 0 {
		return inconsistent("JWKS %s has mode %o; it must be world-readable for the reader (it is not changed)", paths.JWKS, info.Mode().Perm())
	}
	pubDir, err := os.Lstat(filepath.Dir(paths.JWKS))
	if err != nil {
		return inconsistent("public directory %s: %v", filepath.Dir(paths.JWKS), err)
	}
	if pubDir.Mode().Perm()&0o005 != 0o005 {
		return inconsistent("public directory %s has mode %o; it must be world-readable and searchable (it is not changed)", filepath.Dir(paths.JWKS), pubDir.Mode().Perm())
	}
	raw, err := os.ReadFile(paths.JWKS)
	if err != nil {
		return fmt.Errorf("envelopemint: read existing JWKS: %w", err)
	}
	want, err := jwksDocument(priv.Public().(ed25519.PublicKey), keyID)
	if err != nil {
		return err
	}
	if !bytes.Equal(bytes.TrimSpace(raw), bytes.TrimSpace(want)) {
		return inconsistent("JWKS %s does not match the private key %s under key id %q (both are left unchanged)", paths.JWKS, paths.Private, keyID)
	}
	return nil
}

func removeStaleTemps(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), tempMarker) {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

func jwksDocument(pub ed25519.PublicKey, keyID string) ([]byte, error) {
	doc := map[string]any{
		"keys": []map[string]any{{
			"kty": "OKP",
			"crv": "Ed25519",
			"x":   base64.RawURLEncoding.EncodeToString(pub),
			"kid": keyID,
			"use": "sig",
			"alg": Algorithm,
		}},
	}
	out, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("envelopemint: marshal JWKS: %w", err)
	}
	return out, nil
}

func mkdirMode(path string, mode fs.FileMode) error {
	if err := os.MkdirAll(path, mode); err != nil {
		return fmt.Errorf("envelopemint: create %s: %w", path, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		return fmt.Errorf("envelopemint: chmod %s: %w", path, err)
	}
	return nil
}

// writeNew writes data to path atomically and never over an existing file:
// temp file in the same directory, fsync, link to the final name, remove the
// temp name, fsync the directory. point names the file for crashHook.
func writeNew(path string, data []byte, mode fs.FileMode, point string) (err error) {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+tempMarker+"*")
	if err != nil {
		return fmt.Errorf("envelopemint: create temp file in %s: %w", dir, err)
	}
	tmp := f.Name()
	crashed := false
	defer func() {
		if err != nil && !crashed {
			_ = os.Remove(tmp)
		}
	}()
	step := func(name string) error {
		if herr := crashHook(point + ":" + name); herr != nil {
			crashed = true
			return herr
		}
		return nil
	}
	if _, werr := f.Write(data); werr != nil {
		_ = f.Close()
		return fmt.Errorf("envelopemint: write %s: %w", tmp, werr)
	}
	if cerr := f.Chmod(mode); cerr != nil {
		_ = f.Close()
		return fmt.Errorf("envelopemint: chmod %s: %w", tmp, cerr)
	}
	if serr := f.Sync(); serr != nil {
		_ = f.Close()
		return fmt.Errorf("envelopemint: fsync %s: %w", tmp, serr)
	}
	if cerr := f.Close(); cerr != nil {
		return fmt.Errorf("envelopemint: close %s: %w", tmp, cerr)
	}
	if err := step("temp-written"); err != nil {
		return err
	}
	if lerr := os.Link(tmp, path); lerr != nil {
		if errors.Is(lerr, fs.ErrExist) {
			return fmt.Errorf("%w: %s", ErrKeyFilesExist, path)
		}
		return fmt.Errorf("envelopemint: link %s: %w", path, lerr)
	}
	if err := step("committed"); err != nil {
		return err
	}
	_ = os.Remove(tmp)
	if d, derr := os.Open(dir); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
