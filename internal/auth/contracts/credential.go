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

// The page-size bounds of GET /v1/credentials: a request's limit is 1 to
// CredentialListMaxPageSize and a larger one is refused, never clamped.
const (
	CredentialListDefaultPageSize = 50
	CredentialListMaxPageSize     = 100
)

// CheckCredentialListLimit refuses a requested page size outside 1..100.
func CheckCredentialListLimit(limit int) error {
	if limit < 1 || limit > CredentialListMaxPageSize {
		return fmt.Errorf("%s: limit %d is outside 1..%d", CredentialListSurface, limit, CredentialListMaxPageSize)
	}
	return nil
}

// CredentialList is one page of the response of GET /v1/credentials. Items are
// in created_at, id ascending order; NextCursor is opaque and absent on the
// last page.
type CredentialList struct {
	SchemaVersion string           `json:"schema_version"`
	Items         []CredentialBody `json:"items"`
	NextCursor    *string          `json:"next_cursor,omitempty"`
}

// listOrderBefore reports whether a sorts strictly before b: created_at
// ascending, then id ascending.
func listOrderBefore(a, b CredentialBody) bool {
	if !a.CreatedAt.Time.Equal(b.CreatedAt.Time) {
		return a.CreatedAt.Time.Before(b.CreatedAt.Time)
	}
	return a.ID < b.ID
}

// checkPageOrder refuses a page whose items are not strictly increasing in
// list order and no id appears twice (a repeat with a later created_at is still
// in order, so the ids are checked on their own).
func (l CredentialList) checkPageOrder() error {
	seen := map[string]int{}
	for index, item := range l.Items {
		if first, repeated := seen[item.ID]; repeated {
			return fmt.Errorf("%s: items[%d] repeats the id %s of items[%d]", CredentialListSurface, index, item.ID, first)
		}
		seen[item.ID] = index
	}
	for index := 1; index < len(l.Items); index++ {
		if !listOrderBefore(l.Items[index-1], l.Items[index]) {
			return fmt.Errorf("%s: items[%d] (%s) does not follow items[%d] (%s) in created_at, id order",
				CredentialListSurface, index, l.Items[index].ID, index-1, l.Items[index-1].ID)
		}
	}
	return nil
}

// CheckCredentialPage refuses a page that cannot follow prev: prev carried no
// next_cursor, or next starts at or before prev's last item in list order
// and a next page that repeats an id of prev. Each page is first checked on
// its own by ParseCredentialList.
func CheckCredentialPage(prev, next CredentialList) error {
	if prev.NextCursor == nil {
		return fmt.Errorf("%s: no page follows a page without next_cursor", CredentialListSurface)
	}
	if len(prev.Items) == 0 || len(next.Items) == 0 {
		return nil
	}
	onPrev := map[string]bool{}
	for _, item := range prev.Items {
		onPrev[item.ID] = true
	}
	for index, item := range next.Items {
		if onPrev[item.ID] {
			return fmt.Errorf("%s: the next page's items[%d] repeats the id %s of the previous page", CredentialListSurface, index, item.ID)
		}
	}
	last := prev.Items[len(prev.Items)-1]
	if first := next.Items[0]; !listOrderBefore(last, first) {
		return fmt.Errorf("%s: the next page starts at %s, which does not follow the previous page's last item %s",
			CredentialListSurface, first.ID, last.ID)
	}
	return nil
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
	if list.NextCursor != nil && len(list.Items) == 0 {
		return CredentialList{}, fmt.Errorf("%s: next_cursor on a page with no items", CredentialListSurface)
	}
	if err := list.checkPageOrder(); err != nil {
		return CredentialList{}, err
	}
	at := nowOr(now)
	for index, item := range list.Items {
		if err := item.checkBody(CredentialListSurface, at); err != nil {
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
// A receipt names only its parent's id, so the receipt parser cannot see the
// parent; a client that holds the parent entry calls this with it. The rules are
// the database's (migration 0007): the child names this parent, keeps its
// principal, class and domain owner, the parent is not revoked, and the child's
// scopes and audiences are subsets of the parent's. Widening is a new credential
// issued under its own authorization, not a rotation.
func CheckCredentialRotation(parent, child CredentialBody) error {
	const surface = CredentialReceiptSurface
	if child.RotationParentID == "" || child.RotationParentID != parent.ID {
		return fmt.Errorf("%s: credential %s does not name %s as its rotation parent", surface, child.ID, parent.ID)
	}
	if parent.PrincipalID != child.PrincipalID || parent.CredentialClass != child.CredentialClass ||
		parent.DomainOwner != child.DomainOwner {
		return fmt.Errorf("%s: a rotated credential keeps its parent's principal, class and domain owner", surface)
	}
	if parent.RevokedAt != nil || parent.Status == "revoked" {
		return fmt.Errorf("%s: rotation parent %s is revoked", surface, parent.ID)
	}
	if !isSubset(child.Scopes, parent.Scopes) || !isSubset(child.Audiences, parent.Audiences) {
		return fmt.Errorf("%s: rotation may preserve or narrow scopes and audiences, never widen them", surface)
	}
	return nil
}
