package workload

import (
	"context"
	"crypto/sha256"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// The Compose bootstrap credential: a named secret, delivered to one workload
// in a restricted file and accepted only at the workload-token exchange. The
// registry keeps its SHA-256 digest; the value itself is never stored.
const (
	BootstrapClass         = "workload_bootstrap_credential"
	BootstrapPrefix        = "fcwlb_"
	BootstrapHashAlgorithm = "sha256"
)

var _ = regexp.MustCompile

// ValidBootstrapShape reports whether value has the bootstrap credential's
// shape.
func ValidBootstrapShape(value string) bool { return false }

// BootstrapDigest is the digest the registry stores for value.
func BootstrapDigest(value string) [sha256.Size]byte { return [sha256.Size]byte{} }

// BootstrapRegistration is what ResolveBootstrap returns.
type BootstrapRegistration struct {
	Registration
	CredentialID uuid.UUID
	ExpiresAt    time.Time
}

const (
	reasonWrongClass  reason = "wrong_class"
	reasonNotPlatform reason = "not_platform"
	reasonRevoked     reason = "revoked"
	reasonExpired     reason = "expired"
)

// ResolveBootstrap is not implemented yet: every call is refused.
func (s *Store) ResolveBootstrap(ctx context.Context, digest [sha256.Size]byte, now time.Time) (BootstrapRegistration, error) {
	return BootstrapRegistration{}, &RefusalError{Reason: ReasonStoreError}
}

func intersect(a, b []string) []string { return nil }
