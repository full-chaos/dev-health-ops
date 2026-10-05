package goapiproof

// The review_evidence prefix `enable` writes for a root admitted from the go-served ledger's written limit instead of a
// store proof run: the one place a reader six weeks later still looks.

import (
	"crypto/sha256"
	"encoding/hex"
)

// NamedLimitEvidencePrefix starts review_evidence on a row enabled from the
// go-served ledger's written limit instead of a store proof run. The sha256
// of that written reason follows, then the operator's evidence.
const NamedLimitEvidencePrefix = "NAMED-LIMIT:"

// NamedLimitDigest is the sha256 of a ledger reason, as hex.
func NamedLimitDigest(reason string) string {
	sum := sha256.Sum256([]byte(reason))
	return hex.EncodeToString(sum[:])
}

// NamedLimitEvidence is the ONE writer of the review_evidence prefix for a
// row enabled from the ledger's written limit.
func NamedLimitEvidence(reason, operatorEvidence string) string {
	return NamedLimitEvidencePrefix + NamedLimitDigest(reason) + " " + operatorEvidence
}
