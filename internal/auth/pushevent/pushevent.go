// Package pushevent holds the pieces of an External Push credential lifecycle
// event that every writer of the stream shares (the token routes, the backfill
// and the expiry sweeper), so that all of them build the same snapshot, the same
// fingerprint and the same wire ids.
package pushevent

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
)

// Fingerprint is the public fingerprint of a token: the first 16 hex
// characters of the SHA-256 of its id. It identifies the credential, is
// derived from neither the token nor its hash, and cannot authenticate.
func Fingerprint(id uuid.UUID) string {
	sum := sha256.Sum256([]byte(externalpush.Aggregate + ":" + id.String()))
	return hex.EncodeToString(sum[:8])
}

// Scopes is the contract's scope set: a sorted set, never nil.
func Scopes(scopes []string) []string {
	out := slices.Clone(scopes)
	slices.Sort(out)
	return append([]string{}, slices.Compact(out)...)
}

// Expiry is the UTC form of an optional expiry.
func Expiry(at *time.Time) *time.Time {
	if at == nil {
		return nil
	}
	utc := at.UTC()
	return &utc
}

// CreatedChange is the credential's state in the created form.
func CreatedChange(id uuid.UUID, scopes []string, expiresAt *time.Time) externalpush.Change {
	return externalpush.Change{Status: "active", Fingerprint: Fingerprint(id),
		Scopes: Scopes(scopes), ExpiresAt: Expiry(expiresAt)}
}

// WireIDs are the organization wire id, the binding and the source id of a
// token. A source-less token is organization-wide: no source id.
func WireIDs(orgID string, sourceID *uuid.UUID) (organization, binding, source string, err error) {
	organization = provisioning.WireID("organization", orgID)
	if organization == "" {
		return "", "", "", errors.New("pushevent: the organization id cannot be carried on the lifecycle wire")
	}
	if sourceID == nil {
		return organization, externalpush.BindingOrganization, "", nil
	}
	return organization, externalpush.BindingSource, sourceID.String(), nil
}

// ScopesValue is list(token.scopes or []) from the json column's text.
func ScopesValue(raw []byte) (pyjson.Value, error) {
	if len(raw) == 0 {
		return []pyjson.Value{}, nil
	}
	decoded, err := pyjson.Decode(raw)
	if err != nil {
		return nil, err
	}
	if decoded == nil {
		return []pyjson.Value{}, nil
	}
	list, ok := decoded.([]pyjson.Value)
	if !ok {
		return nil, errors.New("scopes column is not a JSON list")
	}
	for _, item := range list {
		if _, ok := item.(string); !ok {
			return nil, errors.New("scopes column holds a non-string")
		}
	}
	return list, nil
}

// ScopeNames is the scope names of the json column's text.
func ScopeNames(raw []byte) ([]string, error) {
	value, err := ScopesValue(raw)
	if err != nil {
		return nil, err
	}
	list := value.([]pyjson.Value)
	names := make([]string, len(list))
	for index, scope := range list {
		names[index] = scope.(string)
	}
	return names, nil
}
