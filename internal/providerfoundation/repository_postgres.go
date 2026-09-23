package providerfoundation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CredentialAmbiguousError reports 2+ active credentials for a provider
// with no name/id given to disambiguate -- mirroring Python's
// AmbiguousCredentialError (integration_credentials.py:121-131) message
// shape exactly: "Multiple active credentials exist for provider
// '<provider>' (<name1>, <name2>, ...); specify credential_name or
// credential_id", names sorted. It wraps ErrCredentialInvalid (the
// ObjectTooLargeError pattern in types.go) so every existing errors.Is(err,
// ErrCredentialInvalid) classification keeps matching unchanged; a caller
// that needs the candidate list for a 409 body (CHAOS-6311) uses errors.As.
type CredentialAmbiguousError struct {
	Provider string
	Names    []string // sorted
}

func (e *CredentialAmbiguousError) Error() string {
	// r2 (round 1, P3): exact string match to AmbiguousCredentialError.
	// __init__ (integration_credentials.py:125-131) -- uppercase "Multiple",
	// single-quoted provider name, not Go's %q double quotes.
	return fmt.Sprintf("Multiple active credentials exist for provider '%s' (%s); specify credential_name or credential_id",
		e.Provider, strings.Join(e.Names, ", "))
}

func (e *CredentialAmbiguousError) Unwrap() error { return ErrCredentialInvalid }

// PostgresCredentialRepository reads the existing Python-owned
// integration_credentials table. It only returns ciphertext; decrypting is
// deliberately kept at the claimed worker boundary in CredentialResolver.
type PostgresCredentialRepository struct{ Pool *pgxpool.Pool }

func (r PostgresCredentialRepository) ResolveEncrypted(ctx context.Context, scope TenantScope) (EncryptedCredential, error) {
	if r.Pool == nil {
		return EncryptedCredential{}, ErrCredentialNotFound
	}
	if err := scope.Validate(); err != nil {
		return EncryptedCredential{}, err
	}
	scope = scope.normalized()
	query := `SELECT id::text, provider, name, is_active, credentials_encrypted, COALESCE(config::text, '{}')
FROM integration_credentials WHERE org_id = $1 AND provider = $2 AND is_active = TRUE`
	args := []any{scope.OrgID, scope.Provider}
	if scope.CredentialID != "" {
		query += " AND id = $3::uuid"
		args = append(args, scope.CredentialID)
	} else if scope.CredentialName != "" {
		query += " AND name = $3"
		args = append(args, scope.CredentialName)
	} else {
		// No id/name: fetch EVERY active candidate (no LIMIT) so an
		// ambiguous match can name every candidate, matching Python's
		// AmbiguousCredentialError exactly -- a LIMIT 2 here previously
		// could only ever see the first two candidates, never the true
		// set, whenever a provider had 3+ active credentials and no
		// "default" among them.
		query += " ORDER BY CASE WHEN name = 'default' THEN 0 ELSE 1 END, name"
	}
	rows, err := r.Pool.Query(ctx, query, args...)
	if err != nil {
		return EncryptedCredential{}, ErrCredentialNotFound
	}
	defer rows.Close()
	var matches []EncryptedCredential
	// configBlobs holds each candidate's RAW config JSON, index-aligned
	// with matches, deliberately left undecoded here. r3 (round 2, P1):
	// config was previously decoded eagerly for EVERY candidate inside
	// this loop, so a malformed config on any ONE active row (e.g. a
	// stored JSON array instead of object -- the column has no shape
	// constraint) aborted the WHOLE query with ErrCredentialInvalid
	// before the ambiguity check ever ran, discarding every
	// already-collected match, the same failure CLASS the ciphertext fix
	// above closes. Python's list_by_provider (integration_credentials.py:
	// 358) never touches `.config` during the ambiguity sweep at all --
	// SQLAlchemy loads it lazily and nothing in resolve_with_fallback's
	// candidate-counting path reads it. Config is decoded here ONLY for
	// the single candidate actually being returned, at each return site
	// below, matching that lazy Python behavior exactly.
	var configBlobs [][]byte
	for rows.Next() {
		var record EncryptedCredential
		// r2 (round 1, P1): credentials_encrypted scans into a NULLABLE
		// *string, not string. Python's list_by_provider (used by
		// resolve_with_fallback's ambiguity check,
		// integration_credentials.py:358,283) selects every row for
		// org+provider with no filter on credentials_encrypted at all -- a
		// row with a NULL or empty ciphertext is still a candidate for
		// ambiguity detection, only failing later when something actually
		// tries to decrypt it. Scanning NULL into a non-nullable Go string
		// previously errored the WHOLE query (ErrCredentialNotFound,
		// discarding every already-collected match too), and an
		// empty-string ciphertext was silently dropped from `matches`
		// outright -- both let an ambiguous set of active rows resolve to
		// a single winner (or "not found") where Python raises
		// AmbiguousCredentialError.
		var cipherText *string
		var configJSON []byte
		if err := rows.Scan(&record.ID, &record.Provider, &record.Name, &record.Active, &cipherText, &configJSON); err != nil {
			return EncryptedCredential{}, ErrCredentialNotFound
		}
		if cipherText != nil && *cipherText != "" {
			record.Ciphertext = secrets.NewValue(*cipherText)
		}
		matches = append(matches, record)
		configBlobs = append(configBlobs, configJSON)
	}
	if err := rows.Err(); err != nil || len(matches) == 0 {
		return EncryptedCredential{}, ErrCredentialNotFound
	}
	if scope.CredentialID == "" && scope.CredentialName == "" {
		for index, match := range matches {
			if match.Name == "default" {
				return finalizeConfig(match, configBlobs[index])
			}
		}
		if len(matches) != 1 {
			names := make([]string, len(matches))
			for index, match := range matches {
				names[index] = match.Name
			}
			sort.Strings(names)
			return EncryptedCredential{}, &CredentialAmbiguousError{Provider: scope.Provider, Names: names}
		}
	}
	return finalizeConfig(matches[0], configBlobs[0])
}

// finalizeConfig decodes the single candidate ResolveEncrypted is actually
// about to return -- see the configBlobs doc comment above for why this
// must never run during the ambiguity-detection sweep.
func finalizeConfig(record EncryptedCredential, configJSON []byte) (EncryptedCredential, error) {
	record.Config = map[string]string{}
	if err := decodeConfig(configJSON, record.Config); err != nil {
		return EncryptedCredential{}, ErrCredentialInvalid
	}
	return record, nil
}

func decodeConfig(raw []byte, target map[string]string) error {
	// Config is intentionally non-secret. Avoiding a generic map in the public
	// credential keeps accidental value logging less likely in provider code.
	var values map[string]any
	if err := json.Unmarshal(raw, &values); err != nil {
		return err
	}
	for key, value := range values {
		if text, ok := value.(string); ok {
			target[key] = text
		}
	}
	return nil
}

// PostgresPagerDutyOAuthTokenRepository reads and rotates only the encrypted
// OAuth token row referenced by a tokenless integration descriptor.
type PostgresPagerDutyOAuthTokenRepository struct{ Pool *pgxpool.Pool }

func (r PostgresPagerDutyOAuthTokenRepository) Load(
	ctx context.Context,
	orgID string,
	credentialName string,
) (PagerDutyOAuthTokenRecord, error) {
	if r.Pool == nil || strings.TrimSpace(orgID) == "" ||
		strings.TrimSpace(credentialName) == "" {
		return PagerDutyOAuthTokenRecord{}, ErrCredentialInvalid
	}
	var (
		ciphertext string
		version    int
		bindingID  *string
	)
	err := r.Pool.QueryRow(ctx, `
SELECT token_encrypted, version, binding_id
FROM provider_oauth_credentials
WHERE org_id = $1 AND provider = 'pagerduty' AND credential_name = $2`,
		orgID, credentialName,
	).Scan(&ciphertext, &version, &bindingID)
	if errors.Is(err, pgx.ErrNoRows) {
		return PagerDutyOAuthTokenRecord{}, ErrCredentialNotFound
	}
	if err != nil {
		return PagerDutyOAuthTokenRecord{}, &ProviderError{Class: ErrorTransient}
	}
	if bindingID == nil || strings.TrimSpace(*bindingID) == "" ||
		strings.TrimSpace(ciphertext) == "" || version < 1 {
		return PagerDutyOAuthTokenRecord{}, ErrCredentialInvalid
	}
	return PagerDutyOAuthTokenRecord{
		Ciphertext: secrets.NewValue(ciphertext), Version: version,
		BindingID: *bindingID,
	}, nil
}

func (r PostgresPagerDutyOAuthTokenRepository) WithRefreshLock(
	ctx context.Context,
	orgID string,
	credentialName string,
	refresh func(PagerDutyOAuthTokenRecord) (*PagerDutyOAuthTokenRotation, error),
) error {
	if r.Pool == nil || strings.TrimSpace(orgID) == "" ||
		strings.TrimSpace(credentialName) == "" ||
		refresh == nil {
		return ErrCredentialInvalid
	}
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return &ProviderError{Class: ErrorTransient}
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var (
		ciphertext string
		version    int
		bindingID  *string
	)
	err = tx.QueryRow(ctx, `
SELECT token_encrypted, version, binding_id
FROM provider_oauth_credentials
WHERE org_id = $1 AND provider = 'pagerduty' AND credential_name = $2
FOR UPDATE`, orgID, credentialName).Scan(&ciphertext, &version, &bindingID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrCredentialNotFound
	}
	if err != nil {
		return &ProviderError{Class: ErrorTransient}
	}
	if bindingID == nil || strings.TrimSpace(*bindingID) == "" ||
		strings.TrimSpace(ciphertext) == "" || version < 1 {
		return ErrCredentialInvalid
	}
	record := PagerDutyOAuthTokenRecord{
		Ciphertext: secrets.NewValue(ciphertext), Version: version, BindingID: *bindingID,
	}
	rotation, err := refresh(record)
	if err != nil {
		return err
	}
	if rotation == nil {
		if err := tx.Commit(ctx); err != nil {
			return &ProviderError{Class: ErrorTransient}
		}
		return nil
	}
	if rotation.ExpectedVersion != record.Version ||
		rotation.ExpectedBindingID != record.BindingID ||
		!rotation.Ciphertext.Configured() || rotation.ExpiresAt.IsZero() {
		return ErrCredentialInvalid
	}
	scopes, err := json.Marshal(normalizedPagerDutyScopes(rotation.GrantedScopes))
	if err != nil {
		return ErrCredentialInvalid
	}
	var nextVersion int
	err = tx.QueryRow(ctx, `
UPDATE provider_oauth_credentials
SET token_encrypted = $1,
    version = version + 1,
    expires_at = $2,
    granted_scopes = $3::json,
    has_refresh_token = $4,
    updated_at = $5
WHERE org_id = $6
  AND provider = 'pagerduty'
  AND credential_name = $7
  AND version = $8
  AND binding_id = $9
RETURNING version`,
		rotation.Ciphertext.Reveal(), rotation.ExpiresAt, string(scopes),
		rotation.HasRefreshToken, time.Now().UTC(), orgID, credentialName,
		rotation.ExpectedVersion, rotation.ExpectedBindingID,
	).Scan(&nextVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrCredentialInvalid
	}
	if err != nil {
		return &ProviderError{Class: ErrorTransient}
	}
	if nextVersion != rotation.ExpectedVersion+1 {
		return ErrCredentialInvalid
	}
	if err := tx.Commit(ctx); err != nil {
		return &ProviderError{Class: ErrorTransient}
	}
	return nil
}

var _ CredentialRepository = PostgresCredentialRepository{}
var _ PagerDutyOAuthTokenRepository = PostgresPagerDutyOAuthTokenRepository{}
