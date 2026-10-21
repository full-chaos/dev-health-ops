package token

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/auth/keystore"
)

// Signer signs compact JWS tokens; not implemented yet.
type Signer struct {
	custody keystore.Custody
}

// FileConfig selects file custody.
type FileConfig struct {
	KeySetPath string
	KeyPath    string
	KeyID      string
}

// NewFileSigner is not implemented yet.
func NewFileSigner(config FileConfig) *Signer { return &Signer{} }

// KeySet is not implemented yet.
func (s *Signer) KeySet(ctx context.Context) (KeySet, error) { return KeySet{}, nil }

// Sign is not implemented yet.
func (s *Signer) Sign(ctx context.Context, typ string, payload []byte) (string, string, error) {
	return "", "", nil
}

// Check is not implemented yet.
func (s *Signer) Check(ctx context.Context) error { return nil }
