package goapiproof

// The review_evidence prefixes `enable` and `status` agree on.
//
// A row enabled without a store proof run says so on the row itself, in the
// one place a reader six weeks later still looks. `enable` writes exactly one
// such prefix (NamedLimitEvidencePrefix); the two older prefixes below are
// only READ, so a row written before the change keeps its own word in
// `status` until the next `enable` rewrites it.

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

// NamedLimitEvidencePrefix starts review_evidence on a row enabled from the
// go-served ledger's written limit instead of a store proof run. The sha256
// of that written reason follows, then the operator's evidence.
const NamedLimitEvidencePrefix = "NAMED-LIMIT:"

// legacyVenueEvidencePrefix and legacyNoProdDataEvidencePrefix start
// review_evidence on rows written by the venue-receipt path this package no
// longer has. They are read, never written.
const (
	legacyVenueEvidencePrefix      = "VENUE-PROOF:"
	legacyNoProdDataEvidencePrefix = "NO-PROD-DATA:"
)

// Legacy venue classes as `status` reports them.
const (
	VenueClassAdmin  = "admin_only"
	VenueClassNoData = "no_production_data"
)

var (
	namedLimitEvidenceRE = regexp.MustCompile(`^NAMED-LIMIT:[0-9a-f]{64}( |$)`)
	venueEvidenceRE      = regexp.MustCompile(`^(NO-PROD-DATA:[0-9a-f]{64} )?VENUE-PROOF:[0-9a-f]{64}( |$)`)
)

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

// HasNamedLimitEvidence is the ONE reader: whether a row's own review_evidence
// claims an enablement from the ledger's written limit. It reads the row's
// text; it does not prove the ledger still holds the reason.
func HasNamedLimitEvidence(evidence string) bool {
	return namedLimitEvidenceRE.MatchString(evidence)
}

// LegacyVenueEvidenceClass is the class a row written by the retired venue
// path claims, or "" for anything else.
func LegacyVenueEvidenceClass(evidence string) string {
	if !venueEvidenceRE.MatchString(evidence) {
		return ""
	}
	if strings.HasPrefix(evidence, legacyNoProdDataEvidencePrefix) {
		return VenueClassNoData
	}
	return VenueClassAdmin
}
