package keystore

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeKeyFile writes a PKCS#8 PEM Ed25519 private key at mode 0600 and
// returns the path and the public half the caller should expect back.
func writeKeyFile(t *testing.T, dir, name string) (string, ed25519.PublicKey) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	path := filepath.Join(dir, name)
	writePKCS8(t, path, private, 0o600)
	return path, public
}

func writePKCS8(t *testing.T, path string, key any, mode os.FileMode) {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	armoured := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	if err := os.WriteFile(path, armoured, mode); err != nil {
		t.Fatalf("write key file: %v", err)
	}
	// os.WriteFile applies the process umask, so the mode is set explicitly:
	// a test asserting a permission rule must not depend on the umask of
	// whoever runs it.
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod key file: %v", err)
	}
}

func TestDescribeAcceptsACustodiedEd25519Key(t *testing.T) {
	dir := t.TempDir()
	path, public := writeKeyFile(t, dir, "signing.pem")

	metadata, err := FileSource{Path: path, KeyID: "auth-2026-09"}.Describe(context.Background())
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if metadata.KeyID != "auth-2026-09" {
		t.Errorf("KeyID = %q", metadata.KeyID)
	}
	if metadata.Algorithm != Algorithm {
		t.Errorf("Algorithm = %q, want %q", metadata.Algorithm, Algorithm)
	}
	if !bytes.Equal(metadata.PublicKey, public) {
		t.Error("PublicKey does not match the generated key's public half")
	}
}

// TestDescribeRejects is the custody contract of ACP-ADR-02 §3, one case per
// clause. Each asserts the specific bounded Reason, not merely "an error":
// a rejection for the wrong reason is a check that happens to pass today and
// stops covering its clause tomorrow.
func TestDescribeRejects(t *testing.T) {
	cases := []struct {
		name       string
		build      func(t *testing.T, dir string) FileSource
		wantReason Reason
	}{
		{
			name: "the file does not exist",
			build: func(t *testing.T, dir string) FileSource {
				return FileSource{Path: filepath.Join(dir, "absent.pem"), KeyID: "k"}
			},
			wantReason: ReasonUnreadable,
		},
		{
			name: "the path is a symlink to a valid key",
			build: func(t *testing.T, dir string) FileSource {
				real, _ := writeKeyFile(t, dir, "real.pem")
				link := filepath.Join(dir, "link.pem")
				if err := os.Symlink(real, link); err != nil {
					t.Skipf("symlinks unavailable: %v", err)
				}
				return FileSource{Path: link, KeyID: "k"}
			},
			wantReason: ReasonUnreadable,
		},
		{
			name: "the path is a directory",
			build: func(t *testing.T, dir string) FileSource {
				nested := filepath.Join(dir, "keydir")
				if err := os.Mkdir(nested, 0o700); err != nil {
					t.Fatalf("mkdir: %v", err)
				}
				return FileSource{Path: nested, KeyID: "k"}
			},
			wantReason: ReasonNotRegularFile,
		},
		{
			name: "the file is group readable",
			build: func(t *testing.T, dir string) FileSource {
				path, _ := writeKeyFile(t, dir, "group.pem")
				if err := os.Chmod(path, 0o640); err != nil {
					t.Fatalf("chmod: %v", err)
				}
				return FileSource{Path: path, KeyID: "k"}
			},
			wantReason: ReasonPermissiveMode,
		},
		{
			name: "the file is world readable",
			build: func(t *testing.T, dir string) FileSource {
				path, _ := writeKeyFile(t, dir, "world.pem")
				if err := os.Chmod(path, 0o604); err != nil {
					t.Fatalf("chmod: %v", err)
				}
				return FileSource{Path: path, KeyID: "k"}
			},
			wantReason: ReasonPermissiveMode,
		},
		{
			name: "the file exceeds the size bound",
			build: func(t *testing.T, dir string) FileSource {
				path := filepath.Join(dir, "large.pem")
				if err := os.WriteFile(path, bytes.Repeat([]byte("x"), int(MaxKeyFileBytes)+1), 0o600); err != nil {
					t.Fatalf("write: %v", err)
				}
				return FileSource{Path: path, KeyID: "k"}
			},
			wantReason: ReasonTooLarge,
		},
		{
			name: "the file is not PEM",
			build: func(t *testing.T, dir string) FileSource {
				path := filepath.Join(dir, "plain.txt")
				if err := os.WriteFile(path, []byte("not a key"), 0o600); err != nil {
					t.Fatalf("write: %v", err)
				}
				return FileSource{Path: path, KeyID: "k"}
			},
			wantReason: ReasonNotPEM,
		},
		{
			name: "the PEM body is not a PKCS#8 key",
			build: func(t *testing.T, dir string) FileSource {
				path := filepath.Join(dir, "garbage.pem")
				armoured := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("junk")})
				if err := os.WriteFile(path, armoured, 0o600); err != nil {
					t.Fatalf("write: %v", err)
				}
				return FileSource{Path: path, KeyID: "k"}
			},
			wantReason: ReasonUnparseable,
		},
		{
			name: "the key is a valid PKCS#8 key of the wrong algorithm",
			build: func(t *testing.T, dir string) FileSource {
				key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
				if err != nil {
					t.Fatalf("generate ecdsa key: %v", err)
				}
				path := filepath.Join(dir, "ecdsa.pem")
				writePKCS8(t, path, key, 0o600)
				return FileSource{Path: path, KeyID: "k"}
			},
			wantReason: ReasonWrongAlgorithm,
		},
		{
			name: "no path is configured",
			build: func(t *testing.T, dir string) FileSource {
				return FileSource{KeyID: "k"}
			},
			wantReason: ReasonPathUnconfigured,
		},
		{
			name: "no key id is configured",
			build: func(t *testing.T, dir string) FileSource {
				path, _ := writeKeyFile(t, dir, "nokid.pem")
				return FileSource{Path: path}
			},
			wantReason: ReasonKeyIDUnconfigured,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			source := testCase.build(t, t.TempDir())
			_, err := source.Describe(context.Background())
			if err == nil {
				t.Fatalf("Describe accepted %s", testCase.name)
			}
			var custody *Error
			if !errors.As(err, &custody) {
				t.Fatalf("error %v is not a *keystore.Error", err)
			}
			if custody.Reason != testCase.wantReason {
				t.Fatalf("Reason = %q, want %q", custody.Reason, testCase.wantReason)
			}
		})
	}
}

// TestErrorNeverCarriesThePath is the leak control for this package. An
// *fs.PathError embeds the path it failed on, and these errors are logged, so
// Error() must render the bounded reason and nothing else. ReasonOf is the
// accessor every caller uses, and it must not fall back to err.Error() either.
func TestErrorNeverCarriesThePath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "very-distinctive-name.pem")

	_, err := FileSource{Path: path, KeyID: "k"}.Describe(context.Background())
	if err == nil {
		t.Fatal("Describe accepted an absent file")
	}
	if strings.Contains(err.Error(), "very-distinctive-name") || strings.Contains(err.Error(), dir) {
		t.Fatalf("error text leaked the path: %q", err)
	}
	if got := ReasonOf(err); got != string(ReasonUnreadable) {
		t.Fatalf("ReasonOf = %q, want %q", got, ReasonUnreadable)
	}
	// The cause is still reachable for a caller that genuinely needs it,
	// which is what makes withholding it from Error() a choice rather than a
	// loss of information.
	if unwrapped := errors.Unwrap(err); unwrapped == nil {
		t.Fatal("the underlying cause was discarded, not merely withheld")
	}
	// An unwrapped error from elsewhere must fall to a LESS specific label,
	// never open to its own text.
	if got := ReasonOf(errors.New("dial tcp 10.0.0.5:5432: connect: refused")); got != "key_custody_failed" {
		t.Fatalf("ReasonOf on a foreign error = %q, want the generic label", got)
	}
}

// TestDescribeIsNotCached is the CHAOS-4512 lesson applied here: a readiness
// check answering from a value captured once cannot notice a key file whose
// mode was widened, or which was deleted, while the process ran.
func TestDescribeIsNotCached(t *testing.T) {
	dir := t.TempDir()
	path, _ := writeKeyFile(t, dir, "rotating.pem")
	source := FileSource{Path: path, KeyID: "k"}

	if _, err := source.Describe(context.Background()); err != nil {
		t.Fatalf("first Describe: %v", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	_, err := source.Describe(context.Background())
	var custody *Error
	if !errors.As(err, &custody) || custody.Reason != ReasonPermissiveMode {
		t.Fatalf("second Describe = %v, want a %q rejection", err, ReasonPermissiveMode)
	}
}

// TestDescribeHonoursAnAlreadyCancelledContext keeps the check bounded: the
// health registry calls it under a deadline, and a check that ignores an
// expired context spends the caller's budget before failing.
func TestDescribeHonoursAnAlreadyCancelledContext(t *testing.T) {
	dir := t.TempDir()
	path, _ := writeKeyFile(t, dir, "ctx.pem")

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()

	if _, err := (FileSource{Path: path, KeyID: "k"}).Describe(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Describe = %v, want context.DeadlineExceeded", err)
	}
}
