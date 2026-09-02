// Package keystore implements signing-key custody for the Auth Control Plane.
//
// ACP-ADR-02 (Accepted 2026-09-02) fixes three things this package implements:
//
//   - §3 the file-based custody contract, promoted from web/acr to a platform
//     requirement: O_NOFOLLOW, a regular file, a bounded size, mode with no
//     group or world bits, and a check that the parsed key really is the
//     expected algorithm.
//   - §4 direct-value secret environment variables are prohibited for signing
//     material. That prohibition is enforced one layer up, in authconfig,
//     which rejects AUTH_SIGNING_KEY outright.
//   - §5 every key carries a `kid` and a JWKS entry from day one, so that
//     rotation overlap is representable before it is needed.
//
// # What this package deliberately does NOT do in Wave 1
//
// It does not sign. CHAOS-4881 builds the service DORMANT and must not issue
// production tokens, so Describe parses the private key only to PROVE the
// configured custody is sound, derives the public half, and then drops the
// private material rather than retaining it in process memory. A process that
// never signs has no reason to hold a minting key for the lifetime of the
// process, and the readiness check this serves needs only the proof.
//
// internal/token adds a Signer to this package when there is something to
// sign (ACP-ADR-02 §1: no caller outside internal/token may reach keystore).
// Adding it is an additive change to this file; nothing here has to be
// reshaped for it.
package keystore

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
)

// MaxKeyFileBytes bounds a signing-key file. A PKCS#8 Ed25519 private key is
// ~119 bytes raw and ~240 bytes PEM-armoured; 8 KiB leaves room for comments
// and a trailing certificate block while still refusing to read an arbitrarily
// large file into memory because a path was misconfigured.
const MaxKeyFileBytes = int64(8 << 10)

// Algorithm is the only asymmetric algorithm the platform accepts for
// signing. ACP-ADR-01 §4: Ed25519/EdDSA for every asymmetric platform token,
// not RS256, and dev-health-go's JWKS verifier is Ed25519-only by design.
const Algorithm = "EdDSA"

// Reason is a bounded, path-free classification of a custody failure. It is
// safe to log and safe to expose on an operator surface; the underlying
// filesystem path and the key material never are.
type Reason string

const (
	ReasonUnreadable        Reason = "key_file_unreadable"
	ReasonNotRegularFile    Reason = "key_file_not_regular"
	ReasonPermissiveMode    Reason = "key_file_mode_too_permissive"
	ReasonTooLarge          Reason = "key_file_too_large"
	ReasonNotPEM            Reason = "key_file_not_pem"
	ReasonUnparseable       Reason = "key_file_unparseable"
	ReasonWrongAlgorithm    Reason = "key_file_wrong_algorithm"
	ReasonKeyIDUnconfigured Reason = "key_id_unconfigured"
	ReasonPathUnconfigured  Reason = "key_path_unconfigured"
)

// Error is a custody failure carrying only its Reason outwards.
//
// Error() renders the reason alone. The cause is retained for errors.Is/As by
// a caller that genuinely needs it, but it is never formatted into the
// message, because an *fs.PathError embeds the path it failed on and this
// error is logged. That is the same leak class CHAOS-4724 closed in
// cmd/query-api's /readyz body.
type Error struct {
	Reason Reason
	cause  error
}

func (e *Error) Error() string { return string(e.Reason) }

func (e *Error) Unwrap() error { return e.cause }

// DependencyReason lets internal/platform/shell-style callers log a bounded
// reason code without redaction.
func (e *Error) DependencyReason() string { return string(e.Reason) }

func failure(reason Reason, cause error) error {
	return &Error{Reason: reason, cause: cause}
}

// ReasonOf extracts the bounded reason from an error produced by this package,
// falling back to a generic label. It never falls back to err.Error(): a
// future call path that forgets to wrap fails to a LESS specific label, not to
// a raw error string.
func ReasonOf(err error) string {
	var custody *Error
	if errors.As(err, &custody) {
		return string(custody.Reason)
	}
	return "key_custody_failed"
}

// Metadata is the public, non-sensitive description of a configured signing
// key: exactly what a JWKS document and an operator diagnostic need.
type Metadata struct {
	KeyID     string
	Algorithm string
	PublicKey ed25519.PublicKey
}

// Source resolves the process's signing key custody.
//
// It is an interface so that the KMS adapter ACP-ADR-02 §2 requires for
// production can be added behind it without any caller changing: handlers and
// readiness checks depend on this type, never on a file path.
type Source interface {
	// Describe verifies custody and returns the key's public metadata. It
	// performs I/O and must be called with a bounded context.
	Describe(ctx context.Context) (Metadata, error)
}

// FileSource is the file-based custody adapter of ACP-ADR-02 §3. It is a
// first-class supported deployment (Compose and self-hosted), not a
// development shortcut.
type FileSource struct {
	// Path names the PKCS#8 PEM file holding the Ed25519 private key.
	Path string
	// KeyID is the JWKS `kid` this key is published under.
	KeyID string
}

var _ Source = FileSource{}

// Describe re-reads and re-validates the key file on every call.
//
// It is deliberately not cached. A readiness check that answers from a value
// captured at startup cannot notice a key file that was replaced, deleted, or
// had its mode widened while the process ran -- the same staleness defect
// CHAOS-4512 fixed in cmd/query-api, where a start-time-only dependency ping
// left /readyz answering 200 for a dependency that had since gone away. The
// file is small and bounded, so re-reading it per readiness request is cheap.
func (s FileSource) Describe(ctx context.Context) (Metadata, error) {
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	if s.Path == "" {
		return Metadata{}, failure(ReasonPathUnconfigured, nil)
	}
	if s.KeyID == "" {
		return Metadata{}, failure(ReasonKeyIDUnconfigured, nil)
	}

	pemBytes, err := readCustodiedFile(s.Path)
	if err != nil {
		return Metadata{}, err
	}
	public, err := parseEd25519Public(pemBytes)
	if err != nil {
		return Metadata{}, err
	}
	return Metadata{KeyID: s.KeyID, Algorithm: Algorithm, PublicKey: public}, nil
}

// readCustodiedFile applies the whole ACP-ADR-02 §3 file contract and returns
// the file's bytes.
//
// Order matters: the descriptor is obtained first (with O_NOFOLLOW), and every
// check afterwards runs against that descriptor via fstat(2), so a path
// swapped after the open cannot change what is inspected or read. The size
// bound is enforced twice -- against the fstat size, and against the bytes
// actually read through an io.LimitReader -- so a file grown between the two
// still cannot exceed the bound.
func readCustodiedFile(path string) ([]byte, error) {
	file, err := openNoFollow(path)
	if err != nil {
		return nil, failure(ReasonUnreadable, err)
	}
	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	if err != nil {
		return nil, failure(ReasonUnreadable, err)
	}
	if !info.Mode().IsRegular() {
		return nil, failure(ReasonNotRegularFile, nil)
	}
	// No group and no world bits. A signing key readable by another account on
	// the host is not in custody, however correct everything else is.
	if info.Mode().Perm()&os.FileMode(0o077) != 0 {
		return nil, failure(ReasonPermissiveMode, nil)
	}
	if info.Size() > MaxKeyFileBytes {
		return nil, failure(ReasonTooLarge, nil)
	}

	contents, err := io.ReadAll(io.LimitReader(file, MaxKeyFileBytes+1))
	if err != nil {
		return nil, failure(ReasonUnreadable, err)
	}
	if int64(len(contents)) > MaxKeyFileBytes {
		return nil, failure(ReasonTooLarge, nil)
	}
	return contents, nil
}

// parseEd25519Public decodes the PEM, parses the PKCS#8 key, asserts it really
// is Ed25519, and returns only the public half.
//
// The private key is intentionally not returned and not stored anywhere. See
// the package doc: Wave 1 is dormant and nothing signs, so nothing keeps
// minting material resident.
func parseEd25519Public(pemBytes []byte) (ed25519.PublicKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, failure(ReasonNotPEM, nil)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		// The x509 error text describes ASN.1 structure, never key bytes, but
		// it is still not surfaced: callers get the reason code only.
		return nil, failure(ReasonUnparseable, err)
	}
	private, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		return nil, failure(ReasonWrongAlgorithm, fmt.Errorf("parsed key is %T", parsed))
	}
	if len(private) != ed25519.PrivateKeySize {
		return nil, failure(ReasonWrongAlgorithm, nil)
	}
	public, ok := private.Public().(ed25519.PublicKey)
	if !ok {
		return nil, failure(ReasonWrongAlgorithm, nil)
	}
	// Copy the public half out before the private slice goes out of scope; the
	// two share no backing array after this, so nothing downstream can reach
	// the seed through the public key's capacity.
	out := make(ed25519.PublicKey, len(public))
	copy(out, public)
	return out, nil
}
