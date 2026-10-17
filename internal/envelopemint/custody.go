package envelopemint

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// PrivateKeyFileEnvVar names the file that holds the private key PEM. It is
// preferred over PrivateKeyEnvVar, whose raw value is deprecated.
const PrivateKeyFileEnvVar = PrivateKeyEnvVar + "_FILE"

// MaxPrivateKeyFileBytes bounds a key file; a PKCS#8 Ed25519 PEM is ~240 bytes.
const MaxPrivateKeyFileBytes = int64(16 << 10)

// ErrKeyFileCustody is returned for every key file that fails the custody
// contract. Its text carries a fixed reason only: never the path (an
// os.PathError embeds it) and never key bytes.
var ErrKeyFileCustody = errors.New("envelopemint: private key file refused")

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

// loadPrivateKeyFromLookup prefers the _FILE source; the raw value is
// deprecated and logged by name only.
func loadPrivateKeyFromLookup(fileValue string, hasFile bool, raw string) (ed25519.PrivateKey, error) {
	if hasFile {
		if strings.TrimSpace(raw) != "" {
			slog.Warn("both envelope key sources set; using the file", "file_var", PrivateKeyFileEnvVar, "ignored_var", PrivateKeyEnvVar)
		}
		return LoadPrivateKeyFromFile(fileValue)
	}
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("%s is not set", PrivateKeyFileEnvVar)
	}
	slog.Warn("envelope private key read from a raw environment variable; this form is deprecated, use the _FILE variable",
		"deprecated_var", PrivateKeyEnvVar, "use_var", PrivateKeyFileEnvVar)
	return LoadPrivateKey([]byte(raw))
}
