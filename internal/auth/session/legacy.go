package session

import (
	"crypto/sha256"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// LegacyRefresh is a go-api refresh token whose signature, type and expiry the
// caller verified: its subject, its org_id claim and its jti.
type LegacyRefresh struct {
	PrincipalID    uuid.UUID
	OrganizationID *uuid.UUID
	JTI            contracts.SecretString
}

// legacyJTIDigest is go-api's token_hash of a jti (the SHA-256 of its text),
// as refresh_credentials.legacy_jti_hash stores it.
func legacyJTIDigest(jti contracts.SecretString) []byte {
	digest := sha256.Sum256([]byte(jti.Reveal()))
	return digest[:]
}

// firstCredential is the first refresh credential of a session, with its row id
// and, for a legacy exchange, the digest of the jti. A legacy credential is
// derived from the jti, as a rotated one is from its parent, so a second
// presentation of the jti inside the grace window recomputes the same value.
func (s *Store) firstCredential(legacyJTI contracts.SecretString) (credential contracts.SecretString, id uuid.UUID, legacyDigest []byte, err error) {
	if id, err = uuid.NewRandom(); err != nil {
		return contracts.SecretString{}, uuid.UUID{}, nil, err
	}
	if legacyJTI.Reveal() == "" {
		credential, err = newCredential()
		return credential, id, nil, err
	}
	return deriveSuccessor(s.key, id, legacyJTI), id, legacyJTIDigest(legacyJTI), nil
}
