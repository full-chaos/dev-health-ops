package llmorgsettings

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The one accessor of stored api_key rows (CHAOS-13096). A key is bound to the
// provider and base_url it was written with. No row says which those were, but
// a provider or base_url row moves only when its value changes, and the writers
// write the key after both in one transaction: a key row older than its
// scope's provider or base_url row was written for another target and is never
// served. ReadScopeKey is the row path, the loader's binding below the bulk
// path; keyreaders_census_test.go fails on a key read anywhere else.

// KeyBound is whether a key row written at keyAt may be served with the rows of
// its scope written at scopeAt (zero times are absent rows).
func KeyBound(keyAt time.Time, scopeAt ...time.Time) bool {
	for _, at := range scopeAt {
		if keyAt.Before(at) {
			return false
		}
	}
	return true
}

// KeyScope is the rows of one stored config the binding reads.
type KeyScope struct{ ProviderKey, BaseURLKey, APIKeyKey string }

// ScopeOf is the scope of one purpose override; "" is the default row.
func ScopeOf(purpose Purpose) KeyScope {
	if purpose == "" {
		return KeyScope{keyProvider, keyBaseURL, keyAPIKey}
	}
	return KeyScope{PurposeKey(purpose, keyProvider), PurposeKey(purpose, keyBaseURL), PurposeKey(purpose, keyAPIKey)}
}

// KeyScopes are the default row and every purpose override, categorize included.
func KeyScopes() []KeyScope {
	scopes := []KeyScope{ScopeOf("")}
	for _, purpose := range append(append([]Purpose{}, Purposes...), PurposeCategorize) {
		scopes = append(scopes, ScopeOf(purpose))
	}
	return scopes
}

// unboundKeys are the api_key rows older than a provider or base_url row of
// their scope, among the loaded rows' updated_at (empty rows included).
func unboundKeys(updatedAt map[string]time.Time) []string {
	var out []string
	for _, scope := range KeyScopes() {
		keyAt, hasKey := updatedAt[scope.APIKeyKey]
		if hasKey && !KeyBound(keyAt, updatedAt[scope.ProviderKey], updatedAt[scope.BaseURLKey]) {
			out = append(out, scope.APIKeyKey)
		}
	}
	return out
}

// unboundMarkPrefix starts the reserved entry that marks an unbound key in the
// loaded settings; no stored key starts with a NUL byte. The key itself stays
// loaded for the base_url leak checks (keysForLeakChecks).
const unboundMarkPrefix = "\x00unbound:"

func markUnboundKeys(settings orgSettings, updatedAt map[string]time.Time) {
	for _, key := range unboundKeys(updatedAt) {
		settings[unboundMarkPrefix+key] = "1"
	}
}

// servedKey is the loaded api_key under key, "" when it is not bound.
func servedKey(settings orgSettings, key string) string {
	if settings[unboundMarkPrefix+key] != "" {
		return ""
	}
	return settings[key]
}

// withServedDefaultKey is settings without the default row's key when that key
// is not bound; settings itself otherwise.
func withServedDefaultKey(settings orgSettings) orgSettings {
	if servedKey(settings, keyAPIKey) == settings[keyAPIKey] {
		return settings
	}
	out := make(orgSettings, len(settings))
	for key, v := range settings {
		out[key] = v
	}
	delete(out, keyAPIKey)
	return out
}

// keysForLeakChecks are the loaded keys a base_url must not carry, bound or
// not: the default row's and the investigate and explain overrides'.
func keysForLeakChecks(raw orgSettings) []string {
	keys := []string{raw[keyAPIKey]}
	for _, key := range otherPurposeAPIKeys() {
		keys = append(keys, raw[key])
	}
	return keys
}

// KeyQuerier is what ReadScopeKey reads (and locks) through: a pool or a transaction.
type KeyQuerier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// KeyOpener decrypts the stored value of the api_key row named key, with the
// caller's decryptor and its own error for an unreadable row.
type KeyOpener func(key string, value *string, encrypted bool) (*string, error)

// ScopeKey is one scope's stored rows as the accessor serves them.
type ScopeKey struct {
	// Provider and BaseURL are the stored values ("" when absent).
	Provider, BaseURL string
	stored, bound     bool
	value             string
}

// Stored is whether a non-empty api_key row exists.
func (k ScopeKey) Stored() bool { return k.stored }

// Bound is whether the stored key was written for the stored provider and base_url.
func (k ScopeKey) Bound() bool { return k.stored && k.bound }

// Served is the key a caller may send: "" when none is stored or it is not bound.
func (k ScopeKey) Served() string {
	if !k.Bound() {
		return ""
	}
	return k.value
}

// ForChecks is the stored key whatever its binding (opened only): for base_url
// leak checks and the masked display, never for a request.
func (k ScopeKey) ForChecks() string { return k.value }

// ReadScopeKey reads the rows of one scope of the llm settings table. lock takes
// their row locks first (inside a write transaction); open nil reads presence
// and binding only, without the key value.
func ReadScopeKey(ctx context.Context, q KeyQuerier, orgID string, purpose Purpose, lock bool, open KeyOpener) (ScopeKey, error) {
	scope := ScopeOf(purpose)
	if lock {
		if _, err := q.Exec(ctx, `
SELECT 1 FROM settings WHERE org_id = $1 AND category = $2 AND key = ANY($3) FOR UPDATE`,
			orgID, settingsCategoryLLM, []string{scope.ProviderKey, scope.BaseURLKey, scope.APIKeyKey}); err != nil {
			return ScopeKey{}, err
		}
	}
	var provider, baseURL, value *string
	var providerAt, baseURLAt, keyAt *time.Time
	var encrypted *bool
	if err := q.QueryRow(ctx, `
SELECT p.value, p.updated_at, b.value, b.updated_at, k.value, k.is_encrypted, k.updated_at
FROM (VALUES (1)) AS one (x)
LEFT JOIN settings AS p ON p.org_id = $1 AND p.category = $2 AND p.key = $3
LEFT JOIN settings AS b ON b.org_id = $1 AND b.category = $2 AND b.key = $4
LEFT JOIN settings AS k ON k.org_id = $1 AND k.category = $2 AND k.key = $5`,
		orgID, settingsCategoryLLM, scope.ProviderKey, scope.BaseURLKey, scope.APIKeyKey,
	).Scan(&provider, &providerAt, &baseURL, &baseURLAt, &value, &encrypted, &keyAt); err != nil {
		return ScopeKey{}, err
	}
	out := ScopeKey{Provider: deref(provider), BaseURL: deref(baseURL)}
	if value == nil || *value == "" || keyAt == nil {
		return out, nil
	}
	out.stored = true
	out.bound = KeyBound(*keyAt, derefTime(providerAt), derefTime(baseURLAt))
	if open != nil {
		plain, err := open(scope.APIKeyKey, value, encrypted != nil && *encrypted)
		if err != nil {
			return ScopeKey{}, err
		}
		out.value = deref(plain)
	}
	return out, nil
}

func deref(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func derefTime(v *time.Time) time.Time {
	if v == nil {
		return time.Time{}
	}
	return *v
}
