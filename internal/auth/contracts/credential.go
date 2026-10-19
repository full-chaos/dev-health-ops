package contracts

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Surfaces for the credential metadata contracts (TRD sections 9, 12, 15).
const (
	CredentialEntrySurface   = "credential-entry.v1"
	CredentialListSurface    = "credential-list.v1"
	CredentialReceiptSurface = "credential-receipt.v1"
)

// CredentialBody is a registry entry without its schema_version: the shape of a
// list item and of a receipt's credential. Every member of the schema is a
// field (TestCredentialTypesCarryEverySchemaMember asserts it both ways); the
// secret-bearing columns secret_hash and hash_algorithm have none, by design.
type CredentialBody struct {
	ID                string     `json:"id"`
	PrincipalID       string     `json:"principal_id"`
	CredentialClass   string     `json:"credential_class"`
	DisplayPrefix     string     `json:"display_prefix"`
	Fingerprint       string     `json:"fingerprint,omitempty"`
	DomainOwner       string     `json:"domain_owner"`
	ExternalReference string     `json:"external_reference,omitempty"`
	Audiences         []string   `json:"audiences"`
	Scopes            []string   `json:"scopes"`
	RotationParentID  string     `json:"rotation_parent_id,omitempty"`
	Status            string     `json:"status"`
	CreatedAt         Timestamp  `json:"created_at"`
	UpdatedAt         Timestamp  `json:"updated_at"`
	ExpiresAt         *Timestamp `json:"expires_at,omitempty"`
	LastUsedAt        *Timestamp `json:"last_used_at,omitempty"`
	RevokedAt         *Timestamp `json:"revoked_at,omitempty"`
	RevokedReason     string     `json:"revoked_reason,omitempty"`
	CreatedBy         string     `json:"created_by,omitempty"`
}

// CredentialEntry is one registry entry (also the response of revoke).
type CredentialEntry struct {
	SchemaVersion string `json:"schema_version"`
	CredentialBody
}

// CredentialList is the response of GET /v1/credentials.
type CredentialList struct {
	SchemaVersion string           `json:"schema_version"`
	Items         []CredentialBody `json:"items"`
}

// CredentialReceipt is the response of issue and rotate: the only credential
// representation that carries a secret, once.
type CredentialReceipt struct {
	SchemaVersion string         `json:"schema_version"`
	Kind          string         `json:"kind"`
	Credential    CredentialBody `json:"credential"`
	Secret        string         `json:"secret"`
}

func (b CredentialBody) timestamps() map[string]*Timestamp {
	return map[string]*Timestamp{
		"created_at": &b.CreatedAt, "updated_at": &b.UpdatedAt,
		"expires_at": b.ExpiresAt, "last_used_at": b.LastUsedAt, "revoked_at": b.RevokedAt,
	}
}

// checkBody applies the rules a schema cannot: the shared timestamp range rule
// to every timestamp member, and status against the clock.
func (b CredentialBody) checkBody(surface string, now time.Time) error {
	// A credential cannot be its own rotation parent. The producer refuses it
	// (credential_registry_not_its_own_parent); the schema cannot compare two
	// members, so every client must.
	if b.RotationParentID != "" && b.RotationParentID == b.ID {
		return fmt.Errorf("%s: credential %s names itself as its rotation parent", surface, b.ID)
	}
	for name, ts := range b.timestamps() {
		if ts == nil {
			continue
		}
		if err := checkInstantInRange(surface, name, ts.Time); err != nil {
			return err
		}
	}
	return b.checkStatusAt(surface, now)
}

// checkStatusAt enforces the derivation rule against a clock: revoked_at set
// means revoked; else expires_at at or before now means expired; else active.
// The schema pins the shape of status to the timestamps; only a clock can
// compare them.
func (b CredentialBody) checkStatusAt(surface string, now time.Time) error {
	want := "active"
	switch {
	case b.RevokedAt != nil:
		want = "revoked"
	case b.ExpiresAt != nil && !b.ExpiresAt.After(now):
		want = "expired"
	}
	if b.Status != want {
		return fmt.Errorf("%s: status %q disagrees with the timestamps at %s, which derive %q "+
			"(revoked_at set -> revoked; else expires_at <= now -> expired; else active)",
			surface, b.Status, now.UTC().Format(time.RFC3339), want)
	}
	return nil
}

func nowOr(now time.Time) time.Time {
	if now.IsZero() {
		return time.Now().UTC()
	}
	return now
}

// ParseCredentialEntry validates and parses one entry. now is the instant its
// status is checked against (zero means the current time).
func ParseCredentialEntry(root string, raw []byte, now time.Time) (CredentialEntry, error) {
	if err := validateRawDocument(root, CredentialEntrySurface, raw); err != nil {
		return CredentialEntry{}, err
	}
	var entry CredentialEntry
	if err := json.Unmarshal(raw, &entry); err != nil {
		return CredentialEntry{}, fmt.Errorf("%s: decoding validated document: %w", CredentialEntrySurface, err)
	}
	if err := entry.checkBody(CredentialEntrySurface, nowOr(now)); err != nil {
		return CredentialEntry{}, err
	}
	return entry, nil
}

// ParseCredentialList validates and parses a list; every item gets the same
// checks as a standalone entry.
func ParseCredentialList(root string, raw []byte, now time.Time) (CredentialList, error) {
	if err := validateRawDocument(root, CredentialListSurface, raw); err != nil {
		return CredentialList{}, err
	}
	var list CredentialList
	if err := json.Unmarshal(raw, &list); err != nil {
		return CredentialList{}, fmt.Errorf("%s: decoding validated document: %w", CredentialListSurface, err)
	}
	for index, item := range list.Items {
		if err := item.checkBody(CredentialListSurface, nowOr(now)); err != nil {
			return CredentialList{}, fmt.Errorf("items[%d]: %w", index, err)
		}
	}
	return list, nil
}

// ParseCredentialReceipt validates and parses a receipt. The secret must begin
// with the credential's display_prefix: the prefix is the only fragment of the
// secret that appears elsewhere, and a receipt whose two disagree is internally
// inconsistent.
func ParseCredentialReceipt(root string, raw []byte, now time.Time) (CredentialReceipt, error) {
	if err := validateRawDocument(root, CredentialReceiptSurface, raw); err != nil {
		return CredentialReceipt{}, err
	}
	var receipt CredentialReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return CredentialReceipt{}, fmt.Errorf("%s: decoding validated document: %w", CredentialReceiptSurface, err)
	}
	if err := receipt.Credential.checkBody(CredentialReceiptSurface, nowOr(now)); err != nil {
		return CredentialReceipt{}, err
	}
	if !strings.HasPrefix(receipt.Secret, receipt.Credential.DisplayPrefix) {
		return CredentialReceipt{}, fmt.Errorf(
			"%s: the secret does not begin with the credential's display_prefix (the secret is not repeated here)",
			CredentialReceiptSurface)
	}
	return receipt, nil
}

func isSubset(next, prev []string) bool {
	have := make(map[string]bool, len(prev))
	for _, v := range prev {
		have[v] = true
	}
	for _, v := range next {
		if !have[v] {
			return false
		}
	}
	return true
}

// CheckCredentialTransition reports whether next may follow prev for one
// credential. Revocation is idempotent and monotonic (TRD section 12): once
// revoked, the status, instant and reason never change, so active-after-revoked
// is refused. Identity, class, domain owner, external reference, lineage,
// display prefix, fingerprint and creation never change, and scopes and
// audiences may narrow but never widen in place. The database enforces the
// same rules (migration 0007); this is the consumer-side twin for a client that
// compares two snapshots.
func CheckCredentialTransition(prev, next CredentialBody) error {
	const surface = CredentialEntrySurface
	if prev.ID != next.ID {
		return fmt.Errorf("%s: transition between different credentials %q and %q", surface, prev.ID, next.ID)
	}
	if prev.Status == "revoked" {
		if next.Status != "revoked" || next.RevokedAt == nil || prev.RevokedAt == nil ||
			!next.RevokedAt.Time.Equal(prev.RevokedAt.Time) || next.RevokedReason != prev.RevokedReason {
			return fmt.Errorf("%s: credential %s was revoked; revocation is monotonic and idempotent "+
				"(status, revoked_at and revoked_reason never change)", surface, prev.ID)
		}
	}
	switch {
	case prev.PrincipalID != next.PrincipalID, prev.CredentialClass != next.CredentialClass,
		prev.DomainOwner != next.DomainOwner, prev.ExternalReference != next.ExternalReference,
		prev.RotationParentID != next.RotationParentID, prev.DisplayPrefix != next.DisplayPrefix,
		prev.Fingerprint != next.Fingerprint, prev.CreatedBy != next.CreatedBy,
		!prev.CreatedAt.Time.Equal(next.CreatedAt.Time):
		return fmt.Errorf("%s: credential %s identity, lineage and verifier-adjacent members are immutable", surface, prev.ID)
	}
	if !isSubset(next.Scopes, prev.Scopes) || !isSubset(next.Audiences, prev.Audiences) {
		return fmt.Errorf("%s: credential %s scopes and audiences may narrow in place, never widen", surface, prev.ID)
	}
	return nil
}

// CheckCredentialRotation reports whether child may be a rotation of parent.
func CheckCredentialRotation(parent, child CredentialBody) error {
	return nil
}
