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

	if err := mkdirMode(filepath.Join(dir, PrivateSubdir), privateDirMode); err != nil {
		return paths, err
	}
	if err := mkdirMode(filepath.Join(dir, PublicSubdir), publicDirMode); err != nil {
		return paths, err
	}
	if err := createExclusive(paths.Private, keyPEM, privateKeyMode); err != nil {
		return paths, err
	}
	if err := createExclusive(paths.JWKS, jwks, jwksMode); err != nil {
		_ = os.Remove(paths.Private)
		return paths, err
	}
	return paths, nil
}

// EnsureKeyFiles is the idempotent form for a start-up step that runs on every
// `up`: it generates a key set when none exists (created=true), and does
// nothing when a complete, consistent set exists (the private key parses, and
// the JWKS holds its public key under keyID). A partial set, an unreadable
// set, or a set under another key id is an error: it is never replaced.
func EnsureKeyFiles(dir, keyID string) (created bool, err error) {
	paths := PathsIn(dir)
	_, privErr := os.Lstat(paths.Private)
	_, jwksErr := os.Lstat(paths.JWKS)
	privMissing := errors.Is(privErr, fs.ErrNotExist)
	jwksMissing := errors.Is(jwksErr, fs.ErrNotExist)
	switch {
	case privMissing && jwksMissing:
		_, err := GenerateKeyFiles(dir, keyID)
		return err == nil, err
	case privMissing != jwksMissing:
		return false, fmt.Errorf("%w: only one of %s and %s exists; remove both or restore the missing one", ErrKeyFilesExist, paths.Private, paths.JWKS)
	}
	if err := checkKeySet(paths, keyID); err != nil {
		return false, err
	}
	return false, nil
}

func checkKeySet(paths KeyPaths, keyID string) error {
	pemBytes, err := os.ReadFile(paths.Private)
	if err != nil {
		return fmt.Errorf("envelopemint: read existing private key: %w", err)
	}
	priv, err := LoadPrivateKey(pemBytes)
	if err != nil {
		return err
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
		return fmt.Errorf("envelopemint: existing JWKS %s does not match the existing private key under key id %q; it is left unchanged", paths.JWKS, keyID)
	}
	return nil
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

// createExclusive creates path (failing if it exists), writes data and sets
// mode explicitly so the process umask cannot widen or narrow it.
func createExclusive(path string, data []byte, mode fs.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("%w: %s", ErrKeyFilesExist, path)
		}
		return fmt.Errorf("envelopemint: create %s: %w", path, err)
	}
	_, werr := f.Write(data)
	cerr := f.Close()
	if werr != nil || cerr != nil {
		_ = os.Remove(path)
		return fmt.Errorf("envelopemint: write %s: %w", path, errors.Join(werr, cerr))
	}
	if err := os.Chmod(path, mode); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("envelopemint: chmod %s: %w", path, err)
	}
	return nil
}
