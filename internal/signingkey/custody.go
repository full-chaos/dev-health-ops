package signingkey

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// PrivateKeyFileEnvVar names the variable that holds the path of the private key
// PEM file. No tool here reads the raw PrivateKeyEnvVar value.
const PrivateKeyFileEnvVar = PrivateKeyEnvVar + "_FILE"

// MaxPrivateKeyFileBytes bounds a key file; a PKCS#8 Ed25519 PEM is ~240 bytes.
const MaxPrivateKeyFileBytes = int64(16 << 10)

// ErrKeyFileCustody is returned for every key file that fails the custody
// contract. Its text carries a fixed reason only: never the path (an
// os.PathError embeds it) and never key bytes.
var ErrKeyFileCustody = errors.New("signingkey: private key file refused")

func custodyFailure(reason string) error {
	slog.Info("envelope private key file refused", "reason", reason)
	return fmt.Errorf("%w: %s", ErrKeyFileCustody, reason)
}

// LoadPrivateKeyFromFile reads the key under the same contract as web's
// readPrivateKey: opened O_NOFOLLOW, a regular file, 1 byte to
// MaxPrivateKeyFileBytes, no group or other permission bits, and it must
// parse as an Ed25519 PKCS#8 key.
func LoadPrivateKeyFromFile(path string) (ed25519.PrivateKey, error) {
	if strings.TrimSpace(path) == "" {
		return nil, custodyFailure("path is empty")
	}
	file, err := openNoFollow(path)
	if err != nil {
		slog.Debug("envelope private key file open failed", "error_kind", fmt.Sprintf("%T", err))
		return nil, custodyFailure("cannot open (missing, unreadable, or a symlink)")
	}
	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	if err != nil {
		return nil, custodyFailure("cannot stat")
	}
	if !info.Mode().IsRegular() {
		return nil, custodyFailure("not a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		slog.Debug("envelope private key file mode", "mode", fmt.Sprintf("%04o", info.Mode().Perm()))
		return nil, custodyFailure("group or other permission bits are set (need 0600 or stricter)")
	}
	if info.Size() < 1 || info.Size() > MaxPrivateKeyFileBytes {
		return nil, custodyFailure("size outside 1 byte to 16 KiB")
	}
	contents, err := io.ReadAll(io.LimitReader(file, MaxPrivateKeyFileBytes+1))
	if err != nil {
		return nil, custodyFailure("cannot read")
	}
	if int64(len(contents)) > MaxPrivateKeyFileBytes {
		return nil, custodyFailure("size outside 1 byte to 16 KiB")
	}
	key, err := LoadPrivateKey(contents)
	if err != nil {
		return nil, custodyFailure("not a PKCS#8 Ed25519 private key PEM")
	}
	return key, nil
}
